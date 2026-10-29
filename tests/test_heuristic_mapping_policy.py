"""Heuristic compatibility needs explicit consent independently of other losses."""

import json
from copy import deepcopy

import pytest

from brightpath import BackgroundContext, BiosphereProfile, FormatProfile, InventoryContext, TechnosphereProfile
from brightpath.background import PackageCatalogProvider, execute_background_migration, plan_background_migration
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.core.reports import Severity
from brightpath.migrations.resources import load_uvek_release_resources, load_uvek_technosphere_resource
from brightpath.models import InventoryDocument


@pytest.fixture(params=["technosphere", "biosphere"])
def operation(request):
    provider = PackageCatalogProvider()
    native = TechnosphereProfile("uvek", "2026", "cutoff")
    source_bio = BiosphereProfile("ecoinvent", "3.12")
    if request.param == "technosphere":
        source = BackgroundContext(TechnosphereProfile("ecoinvent", "3.12", "cutoff"), source_bio)
        target = BackgroundContext(native, source_bio)
        available = provider.load_technosphere(source.technosphere).identities
        bridge = next(
            row for row in load_uvek_release_resources("technosphere") if row["source_profiles"][0]["version"] == "2025"
        )
        reachable = {
            tuple(rule["source"][key] for key in ("name", "reference product", "location", "unit"))
            for rule in bridge["replace"]
            if rule["source"]["unit"] == rule["target"]["unit"]
        }
        rule = next(
            row
            for row in load_uvek_technosphere_resource()["replace"]
            if tuple(row["source"][key] for key in ("name", "reference product", "location", "unit")) in available
            and tuple(row["target"][key] for key in ("name", "reference product", "location", "unit")) in reachable
            and row.get("conversion_factor", 1) == 1
        )
        flow = {**rule["source"], "type": "technosphere"}
    else:
        legacy = TechnosphereProfile("uvek", "2025", "cutoff")
        source = BackgroundContext(TechnosphereProfile("ecoinvent", "3.12", "cutoff"), source_bio)
        target = BackgroundContext(legacy, BiosphereProfile("ecoinvent", "3.10"))
        available = provider.load_biosphere(source_bio).identities
        reverse = next(
            row
            for row in load_uvek_release_resources("biosphere")
            if row["source_profiles"][0]["family"] == "ecoinvent"
        )
        rule = next(
            row
            for row in reverse["replace"]
            if (row["source"]["name"], tuple(row["source"]["categories"]), row["source"]["unit"]) in available
            and row["source"]["unit"] == row["target"]["unit"]
        )
        flow = {**rule["source"], "type": "biosphere"}
    foreground = {
        "name": "Heuristic policy probe",
        "reference product": "output",
        "unit": "unit",
        "location": "CH",
        "comment": "Synthetic policy probe; no background inventory coefficients.",
    }
    original = InventoryDocument(
        data=[
            {
                **foreground,
                "exchanges": [
                    {**foreground, "type": "production", "amount": 1},
                    {**flow, "amount": 2, "formula": "dose", "custom": [1]},
                ],
                "parameters": [{"name": "dose", "amount": 2}],
            }
        ],
        context=InventoryContext(FormatProfile("brightway_excel"), source),
    )
    return original, target, provider


def test_default_strict_and_other_loss_permissions_do_not_accept_heuristics(operation):
    original, target, provider = operation
    before = deepcopy(original.data)
    for policy in (
        MigrationPolicy.strict(),
        MigrationPolicy(on_information_loss=PolicyAction.WARN, on_inferred_reverse=PolicyAction.WARN),
    ):
        plan = plan_background_migration(original.context.background, target, policy)
        assert not plan.executable
        result = execute_background_migration(original, target, provider, policy)
        assert not result.succeeded
        assert result.value.data == original.data == before
        assert result.value.context == original.context
        finding = next(issue for issue in result.report.issues if issue.code == "migration.heuristic_mapping")
        assert finding.severity is Severity.ERROR
        assert finding.details["policy_action"] == "error"


@pytest.mark.parametrize(
    "action,severity", [(PolicyAction.WARN, Severity.WARNING), (PolicyAction.ALLOW, Severity.INFO)]
)
def test_explicit_acceptance_keeps_provenance_and_quantity_metadata(operation, action, severity):
    original, target, provider = operation
    before = deepcopy(original.data)
    policy = MigrationPolicy(on_heuristic_mapping=action)
    result = execute_background_migration(original, target, provider, policy)
    assert result.succeeded, result.report.to_json()
    assert result.value.context.background == target
    flow = result.value.data[0]["exchanges"][1]
    assert (flow["amount"], flow["formula"], flow["custom"]) == (2, "dose", [1])
    findings = [issue for issue in result.report.issues if issue.code == "migration.heuristic_mapping"]
    assert findings and all(issue.severity is severity for issue in findings)
    assert all(issue.details["policy_action"] == action.value for issue in findings)
    assert any(loss.code == "migration.heuristic_mapping" for loss in result.report.losses)
    assert original.data == before
    repeated = execute_background_migration(result.value, target, provider)
    assert repeated.succeeded and repeated.value.data == result.value.data


def test_cli_requires_explicit_heuristic_acceptance_and_preserves_strict_other_checks(operation, tmp_path, capsys):
    from brightpath import BrightwayInventory
    from brightpath.cli import main

    original, target, _provider = operation
    source = tmp_path / "source.xlsx"
    destination = tmp_path / "dry-run.xlsx"
    BrightwayInventory.from_data(original.data, context=original.context).write_excel(source)
    saved = source.read_bytes()
    source_context = original.context.background
    arguments = [
        "migrate-background",
        str(source),
        str(destination),
        "--source-format",
        "brightway_excel",
        "--source-technosphere-family",
        source_context.technosphere.family,
        "--source-technosphere-version",
        source_context.technosphere.version,
        "--source-technosphere-system-model",
        source_context.technosphere.system_model,
        "--source-biosphere-family",
        source_context.biosphere.family,
        "--source-biosphere-version",
        source_context.biosphere.version,
        "--target-technosphere-family",
        target.technosphere.family,
        "--target-technosphere-version",
        target.technosphere.version,
        "--target-technosphere-system-model",
        target.technosphere.system_model,
        "--target-biosphere-family",
        target.biosphere.family,
        "--target-biosphere-version",
        target.biosphere.version,
        "--dry-run",
        "--json",
    ]
    assert main(arguments) != 0
    rejected = json.loads(capsys.readouterr().out)
    assert "migration.heuristic_mapping" in json.dumps(rejected)
    assert main([*arguments, "--allow-heuristic-mapping"]) == 0
    accepted = capsys.readouterr().out
    assert '"on_heuristic_mapping": "warn"' in accepted
    assert '"on_unresolved_link": "error"' in accepted
    assert source.read_bytes() == saved
    assert not destination.exists()
