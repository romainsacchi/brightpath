"""Publisher-linked process revisions retain explicit geography decisions."""

import json
from copy import deepcopy

import pytest

from brightpath import BackgroundContext, BiosphereProfile
from brightpath.background import PackageCatalogProvider
from brightpath.background.execution import execute_background_migration
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.migrations.resources import load_uvek_release_resources, load_uvek_technosphere_resource
from brightpath.migrations.schema import validate_migration_resource
from scripts.generate_uvek_2026_resources import identities
from scripts.review_uvek_2026_land import resource_hash
from scripts.review_uvek_2026_processes import METHOD, REPORT, apply_process_review, validate_decision
from scripts.review_uvek_2026_units import identity
from tests.test_uvek_2026 import context, document
from tests.test_uvek_2026_biosphere_review import CATALOGS
from tests.uvek_process_history import before_process_review


def test_review_reproduces_both_release_resources_and_preserves_all_previous_rules():
    report = json.loads(REPORT.read_text())
    assert report["summary"] == {"reviewed_processes": 7, "geographic_revisions": 4, "background_revisions": 1}
    prior = before_process_review()
    packaged = list(load_uvek_release_resources("technosphere"))
    assert apply_process_review(prior, REPORT, CATALOGS) == packaged
    for previous, resource in zip(prior, packaged, strict=True):
        direction = "forward" if resource["source_profiles"][0]["version"] == "2025" else "reverse"
        assert resource_hash(previous) == report["sources"][f"baseline_{direction}_sha256"]
        old = {row["id"]: row for row in previous["replace"]}
        assert {row["id"]: row for row in resource["replace"] if row["id"] in old} == old
        assert len(resource["replace"]) == 11747
        assert len(resource["unresolved"]) == (0 if direction == "forward" else 200)
        assert resource["coverage"]["unit_changes_without_factor"] == 1
        assert resource["coverage"]["information_loss_rules"] == 4
        validate_migration_resource(resource)


@pytest.mark.parametrize("version,other", [("2025", "2026"), ("2026", "2025")])
def test_both_process_directions_preserve_quantities_and_require_geography_consent(version, other):
    resource = next(
        row for row in load_uvek_release_resources("technosphere") if row["source_profiles"][0]["version"] == version
    )
    rules = [row for row in resource["replace"] if row["mapping_method"] == METHOD]
    assert len(rules) == 7
    biosphere = BiosphereProfile("uvek", "2026")
    for rule in rules:
        flow = {
            **rule["source"],
            "type": "technosphere",
            "amount": 2,
            "uncertainty type": 3,
            "loc": 2,
            "scale": 0.1,
            "formula": "demand",
            "custom": [1],
        }
        original = document(context(version, biosphere), [flow])
        data = original.data
        data[0]["parameters"] = [{"name": "demand", "amount": 2}]
        original = original.replace(data=data)
        saved = deepcopy(original.data)
        target = context(other, biosphere)
        strict = execute_background_migration(original, target, PackageCatalogProvider())
        geography = "technosphere_information_loss" in rule
        assert strict.succeeded != geography
        if geography:
            assert strict.value.data == saved and not strict.report.changes
        policy = MigrationPolicy(on_information_loss=PolicyAction.WARN)
        accepted = execute_background_migration(original, target, PackageCatalogProvider(), policy)
        assert accepted.succeeded, accepted.report.to_json()
        converted = accepted.value.data[0]["exchanges"][1]
        assert identity(converted) == identity(rule["target"])
        assert (
            converted["amount"],
            converted["loc"],
            converted["scale"],
            converted["formula"],
            converted["custom"],
        ) == (2, 2, 0.1, "demand", [1])
        assert (
            any(loss.code == "migration.technosphere_information_loss" for loss in accepted.report.losses) == geography
        )
        back = execute_background_migration(
            accepted.value, original.context.background, PackageCatalogProvider(), policy
        )
        assert back.succeeded and identity(back.value.data[0]["exchanges"][1]) == identity(flow)
        assert original.data == saved


def test_previous_incoming_wind_blockers_resolve_only_with_recorded_geography_consent():
    report = json.loads(REPORT.read_text())
    targets = {identity(row["source"]): row["target"] for row in report["decisions"] if row["geography_revised"]}
    rules = [row for row in load_uvek_technosphere_resource()["replace"] if identity(row["target"]) in targets]
    assert len(rules) == 6
    provider = PackageCatalogProvider()
    tested = 0
    for profile in provider.technosphere_profiles():
        if profile.family != "ecoinvent":
            continue
        valid = provider.load_technosphere(profile).identities
        for rule in rules:
            if identity(rule["source"]) not in valid:
                continue
            source = BackgroundContext(profile, BiosphereProfile("uvek", "2026"))
            original = document(source, [{**rule["source"], "type": "technosphere", "amount": 2}])
            target = context("2026", source.biosphere)
            policy = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN)
            result = execute_background_migration(original, target, provider, policy)
            assert result.succeeded, result.report.to_json()
            assert identity(result.value.data[0]["exchanges"][1]) == identity(targets[identity(rule["target"])])
            tested += 1
    assert tested > 6


@pytest.mark.parametrize("mutation", ["name", "quantity", "unit", "geography", "record", "coefficients"])
def test_process_review_rejects_unsubstantiated_correspondences(mutation):
    row = deepcopy(json.loads(REPORT.read_text())["decisions"][0])
    if mutation == "name":
        row["target"]["name"] = "different function"
    elif mutation == "quantity":
        row["native_reference_amount"] *= 2
    elif mutation == "unit":
        row["target"]["unit"] = "hour"
    elif mutation == "geography":
        row["target"]["location"] = "GLO"
    elif mutation == "record":
        row["checks"]["same_source_uuid"] = False
    else:
        row["checks"]["technosphere_flow_ids_and_coefficients_unchanged"] = not row["checks"][
            "technosphere_flow_ids_and_coefficients_unchanged"
        ]
    catalogs = [
        identities(json.loads((CATALOGS / filename).read_text()), "technosphere")
        for filename in ("uvek__2025__cutoff.json", "uvek__2026__cutoff.json")
    ]
    with pytest.raises(ValueError, match="Process correspondence"):
        validate_decision(row, *catalogs)
