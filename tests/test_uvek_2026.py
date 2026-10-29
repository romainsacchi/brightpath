"""Original UVEK 2026 catalogs, directional routes and strict migration limits."""

import importlib.util
import json
from copy import deepcopy
from pathlib import Path
from zipfile import ZipFile

import pytest

from brightpath import BackgroundContext, BiosphereProfile, FormatProfile, InventoryContext, TechnosphereProfile
from brightpath.background import PackageCatalogProvider
from brightpath.background.execution import execute_background_migration
from brightpath.background.migration import plan_background_migration
from brightpath.core.policies import MigrationPolicy
from brightpath.migrations.resources import load_uvek_release_resources
from brightpath.models import InventoryDocument, default_biosphere_profile


def context(version, biosphere=None):
    if biosphere is None:
        biosphere = BiosphereProfile("uvek", "2026") if version == "2026" else BiosphereProfile("ecoinvent", "3.10")
    return BackgroundContext(
        TechnosphereProfile("uvek", version, "cutoff"),
        biosphere,
    )


def document(background, exchanges):
    foreground = {"name": "Foreground", "reference product": "product", "location": "CH", "unit": "kilogram"}
    return InventoryDocument(
        data=[
            {
                **foreground,
                "comment": "Synthetic foreground for UVEK release migration tests.",
                "unknown": {"retain": [1, 2]},
                "exchanges": [
                    {**foreground, "type": "production", "amount": 1},
                    *exchanges,
                ],
            }
        ],
        context=InventoryContext(FormatProfile("brightway_excel"), background),
    )


def rules(axis, source_version):
    return next(
        resource
        for resource in load_uvek_release_resources(axis)
        if resource["source_profiles"][0]["version"] == source_version
    )


def identity(row, axis):
    if axis == "technosphere":
        return tuple(row[key] for key in ("name", "reference product", "location", "unit"))
    return row["name"], tuple(row["categories"]), row["unit"]


def test_native_catalog_and_default_keep_original_biosphere():
    provider = PackageCatalogProvider()
    background = context("2026")
    technosphere = provider.load_technosphere(background.technosphere)
    biosphere = provider.load_biosphere(background.biosphere)
    assert len(technosphere.identities) == 11947
    assert len(biosphere.identities) == 2679
    assert ("Carbon dioxide, fossil", ("emissions to air", "unspecified"), "kilogram") in biosphere.identities
    assert default_biosphere_profile(background.technosphere) == background.biosphere
    assert default_biosphere_profile(context("2025").technosphere) == BiosphereProfile("ecoinvent", "3.10")
    assert not technosphere.references and not biosphere.references


@pytest.mark.parametrize("axis", ["technosphere", "biosphere"])
def test_every_rule_and_unresolved_disposition_belongs_to_exact_catalog(axis):
    provider = PackageCatalogProvider()
    profile_type = TechnosphereProfile if axis == "technosphere" else BiosphereProfile
    load = getattr(provider, f"load_{axis}")
    for resource in load_uvek_release_resources(axis):
        source = load(profile_type(**resource["source_profiles"][0])).identities
        target = load(profile_type(**resource["target_profile"])).identities
        mapped = {identity(rule["source"], axis) for rule in resource["replace"]}
        unresolved = {identity(row, axis) for row in resource["unresolved"]}
        assert mapped.isdisjoint(unresolved)
        assert mapped | unresolved == source
        assert {identity(rule["target"], axis) for rule in resource["replace"]} <= target
        assert resource["reverse_policy"] == "explicit_only"
        assert not resource["delete"] and not resource["disaggregate"]
        # Explicit forward biosphere coarsening can have several source flows per target.
        grouped = {}
        for rule in resource["replace"]:
            grouped.setdefault(identity(rule["target"], axis), []).append(rule)
        for group in grouped.values():
            if len(group) > 1:
                assert axis == "biosphere"
                assert any(rule.get("report_correspondence") for rule in group)
                assert all(rule.get("review_ref") for rule in group)


@pytest.mark.parametrize("source_version,target_version", [("2025", "2026"), ("2026", "2025")])
def test_all_safe_process_rules_execute_with_uncertainty_metadata_and_round_trip(source_version, target_version):
    resource = rules("technosphere", source_version)
    safe = [
        rule
        for rule in resource["replace"]
        if (rule["source"]["unit"] == rule["target"]["unit"] or "conversion_factor" in rule)
        and not rule.get("technosphere_information_loss")
    ]
    exchanges = [
        {
            **rule["source"],
            "product": rule["source"]["reference product"],
            "type": "technosphere",
            "amount": 2,
            "uncertainty type": 3,
            "loc": 2,
            "scale": 0.2,
            "custom": [1],
        }
        for rule in safe
    ]
    # Preserve the biosphere independently while exercising all process edges.
    bio = BiosphereProfile("ecoinvent", "3.10")
    original = document(context(source_version, bio), exchanges)
    before = deepcopy(original.data)
    result = execute_background_migration(original, context(target_version, bio), PackageCatalogProvider())
    assert result.succeeded, result.report.to_json()
    actual = result.value.data[0]["exchanges"][1:]
    assert [identity(exchange, "technosphere") for exchange in actual] == [
        identity(rule["target"], "technosphere") for rule in safe
    ]
    assert all(
        (exchange["amount"], exchange["loc"], exchange["scale"], exchange["custom"]) == (2, 2, 0.2, [1])
        for exchange in actual
    )
    assert original.data == before
    assert result.value.data[0]["exchanges"][0] == before[0]["exchanges"][0]
    repeated = execute_background_migration(result.value, context(target_version, bio), PackageCatalogProvider())
    assert repeated.succeeded and repeated.value.data == result.value.data
    back = execute_background_migration(result.value, original.context.background, PackageCatalogProvider())
    assert back.succeeded and back.value.data == before


@pytest.mark.parametrize("source_version,target_version", [("2025", "2026"), ("2026", "2025")])
def test_both_axes_use_authored_forward_resources_and_preserve_source(source_version, target_version):
    process_rule = next(
        rule
        for rule in rules("technosphere", source_version)["replace"]
        if rule["mapping_method"] == "legacy_location_suffix"
    )
    bio_source = "3.10" if source_version == "2025" else "2026"
    # This regression exercises the original bidirectional correspondence set.
    bio_rules = [
        rule
        for rule in rules("biosphere", bio_source)["replace"]
        if rule["mapping_method"] == "native_compartment_and_bw2io_name"
    ]
    original = document(
        context(source_version),
        [
            {**process_rule["source"], "amount": 2, "type": "technosphere"},
            *[{**rule["source"], "amount": 3, "type": "biosphere", "custom": [5]} for rule in bio_rules],
        ],
    )
    before = deepcopy(original.data)
    plan = plan_background_migration(original.context.background, context(target_version))
    assert plan.executable and len(plan.steps) == 2
    assert all(step.direction == "forward" for step in plan.steps)
    assert "migration.inferred_reverse" not in {issue.code for issue in plan.report.issues}
    result = execute_background_migration(original, context(target_version), PackageCatalogProvider())
    assert result.succeeded, result.report.to_json()
    converted = result.value.data[0]["exchanges"]
    assert identity(converted[1], "technosphere") == identity(process_rule["target"], "technosphere")
    assert [identity(exchange, "biosphere") for exchange in converted[2:]] == [
        identity(rule["target"], "biosphere") for rule in bio_rules
    ]
    assert all(
        exchange["amount"] == 3 * rule.get("conversion_factor", 1) and exchange["custom"] == [5]
        for exchange, rule in zip(converted[2:], bio_rules, strict=True)
    )
    assert original.data == before
    json.loads(result.report.to_json())


@pytest.mark.parametrize("source_version,target_version", [("2025", "2026"), ("2026", "2025")])
def test_unapproved_washing_machine_unit_change_fails_transactionally(source_version, target_version):
    rule = next(
        rule
        for rule in rules("technosphere", source_version)["replace"]
        if rule["mapping_method"] == "unit_change_requires_factor"
    )
    assert rule["source"]["name"] == "Production, washing machine, V-ZUG"
    original = document(context(source_version), [{**rule["source"], "amount": 2, "type": "technosphere"}])
    before = original.data
    result = execute_background_migration(original, context(target_version), PackageCatalogProvider())
    assert not result.succeeded
    assert result.value is original and original.data == before
    assert any("unit_change_without_factor" in issue.code for issue in result.report.issues)


@pytest.mark.parametrize("source_version,target_version", [("2025", "2026"), ("2026", "2025")])
@pytest.mark.parametrize("name", ["Natural gas, at production", "Use, computer, laptop, active mode"])
def test_reviewed_label_changes_preserve_negative_lognormal_demand(source_version, target_version, name):
    import math

    rule = next(rule for rule in rules("technosphere", source_version)["replace"] if rule["source"]["name"] == name)
    assert rule["mapping_method"] == "reviewed_unit_label_correspondence"
    assert rule["conversion_factor"] == 1
    exchange = {
        **rule["source"],
        "type": "technosphere",
        "amount": -2,
        "uncertainty type": 2,
        "loc": math.log(2),
        "scale": 0.3,
        "negative": True,
        "minimum": -3,
        "maximum": -1,
        "comment": "Preserve reviewed reference demand.",
        "custom": {"retained": True},
    }
    original = document(context(source_version), [exchange])
    result = execute_background_migration(original, context(target_version), PackageCatalogProvider())
    assert result.succeeded, result.report.to_json()
    actual = result.value.data[0]["exchanges"][1]
    assert identity(actual, "technosphere") == identity(rule["target"], "technosphere")
    assert all(
        actual[key] == value
        for key, value in exchange.items()
        if key not in {"name", "reference product", "location", "unit"}
    )
    assert original.data[0]["exchanges"][1] == exchange


def test_unmatched_new_process_and_ambiguous_biosphere_do_not_get_guessed():
    provider = PackageCatalogProvider()
    process = rules("technosphere", "2026")["unresolved"][0]
    bio_resource = rules("biosphere", "3.10")
    ambiguous = bio_resource["provenance"]["biosphere_matching"]["ambiguous"][0]["ecoinvent"]
    for source, target, exchange in (
        (context("2026"), context("2025"), {**process, "type": "technosphere", "amount": 1}),
        (context("2025"), context("2026"), {**ambiguous, "type": "biosphere", "amount": 1}),
    ):
        original = document(source, [exchange])
        result = execute_background_migration(original, target, provider)
        assert not result.succeeded and result.value is original


def test_permissive_unit_change_is_skipped_and_reported_with_original_amount():
    rule = next(
        rule
        for rule in rules("technosphere", "2025")["replace"]
        if rule["mapping_method"] == "unit_change_requires_factor"
    )
    original = document(context("2025"), [{**rule["source"], "amount": 2, "type": "technosphere"}])
    result = execute_background_migration(
        original, context("2026"), PackageCatalogProvider(), MigrationPolicy.permissive()
    )
    assert result.succeeded and result.lossy
    assert result.value.data == original.data
    assert any(
        loss.code == "migration.rule_skipped_unsafe_unit_change"
        for stage in result.report.stages
        for loss in stage.losses
    )


def test_biosphere_only_migration_leaves_technosphere_context_unchanged():
    source = context("2025")
    target = context("2025", BiosphereProfile("uvek", "2026"))
    rule = rules("biosphere", "3.10")["replace"][0]
    original = document(source, [{**rule["source"], "type": "biosphere", "amount": 1}])
    result = execute_background_migration(original, target, PackageCatalogProvider())
    assert result.succeeded, result.report.to_json()
    assert result.value.context.background == target
    assert not plan_background_migration(source, target).technosphere_steps


def test_unsupported_uvek_system_models_and_versions_remain_unavailable():
    source = context("2025")
    for target in (
        context("2027"),
        BackgroundContext(TechnosphereProfile("uvek", "2026", "consequential"), source.biosphere),
    ):
        assert not plan_background_migration(source, target).executable


@pytest.mark.parametrize("direction", ["import", "export"])
def test_composed_ecoinvent_routes_use_uvek_2025_bridge(direction):
    ecoinvent = BackgroundContext(
        TechnosphereProfile("ecoinvent", "3.12", "cutoff"), BiosphereProfile("ecoinvent", "3.12")
    )
    source, target = (ecoinvent, context("2026")) if direction == "import" else (context("2026"), ecoinvent)
    plan = plan_background_migration(source, target, MigrationPolicy.permissive())
    assert plan.executable
    assert any(
        step.resource_name == rules("technosphere", "2025" if direction == "import" else "2026")["name"]
        for step in plan.technosphere_steps
    )
    result = execute_background_migration(
        document(source, []), target, PackageCatalogProvider(), MigrationPolicy.permissive()
    )
    assert result.succeeded, result.report.to_json()
    assert result.value.context.background == target


def test_cross_family_technosphere_route_preserves_an_unchanged_native_biosphere():
    source = context("2026")
    target = BackgroundContext(TechnosphereProfile("ecoinvent", "3.12", "cutoff"), source.biosphere)
    # This native flow has no ecoinvent correspondence; changing only suppliers
    # must never force it through the ecoinvent 3.10 intermediate biosphere.
    flow = rules("biosphere", "2026")["unresolved"][0]
    original = document(source, [{**flow, "type": "biosphere", "amount": 1}])
    plan = plan_background_migration(source, target)
    assert plan.executable and not plan.biosphere_steps
    result = execute_background_migration(original, target, PackageCatalogProvider())
    assert result.succeeded, result.report.to_json()
    assert result.value.data == original.data
    assert result.value.context.background == target


def test_real_cli_dry_run_migrates_native_context_without_writing(tmp_path, capsys):
    from brightpath import BrightwayInventory, cli

    process = next(
        rule for rule in rules("technosphere", "2025")["replace"] if rule["mapping_method"] == "legacy_location_suffix"
    )
    biosphere = next(
        rule for rule in rules("biosphere", "3.10")["replace"] if rule["source"]["name"] == "Carbon dioxide, fossil"
    )
    original = document(
        context("2025"),
        [
            {**process["source"], "type": "technosphere", "amount": 2},
            {**biosphere["source"], "type": "biosphere", "amount": 3},
        ],
    )
    data = original.data
    data[0].pop("unknown")
    source = tmp_path / "source.xlsx"
    destination = tmp_path / "destination.xlsx"
    BrightwayInventory.from_data(data, context=original.context).write_excel(source)
    capsys.readouterr()
    source_bytes = source.read_bytes()
    arguments = ["migrate-background", str(source), str(destination), "--dry-run", "--json"]
    for role, background in (("source", context("2025")), ("target", context("2026"))):
        for axis in ("technosphere", "biosphere"):
            profile = getattr(background, axis)
            for field in (("family", "version", "system_model") if axis == "technosphere" else ("family", "version")):
                arguments.extend([f"--{role}-{axis}-{field.replace('_', '-')}", getattr(profile, field)])
    exit_code = cli.main(arguments)
    payload = json.loads(capsys.readouterr().out)
    assert exit_code == cli.EXIT_SUCCESS, payload
    assert payload["dry_run"]
    assert payload["inventory"]["context"]["technosphere"]["version"] == "2026"
    assert not destination.exists() and source.read_bytes() == source_bytes


def test_generator_reads_native_namespace_free_archive_without_inventing_references(tmp_path):
    script = Path(__file__).resolve().parents[1] / "scripts/generate_uvek_2026_resources.py"
    spec = importlib.util.spec_from_file_location("generate_uvek_2026_resources", script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    xml = b"""<ecoSpold><dataset number="1"><metaInformation><processInformation>
    <referenceFunction name="Example" unit="kg"/><geography location="CH"/>
    </processInformation></metaInformation><flowData>
    <exchange name="Example" unit="kg"><outputGroup>0</outputGroup></exchange>
    <exchange name="Carbon dioxide, fossil" unit="kg" category="emissions to air" subCategory="unspecified"><outputGroup>4</outputGroup></exchange>
    </flowData></dataset></ecoSpold>"""
    archive = tmp_path / "original.zip"
    with ZipFile(archive, "w") as output:
        output.writestr("process.xml", xml)
    result = module.read_archive(archive)
    assert result["technosphere"] == [
        {"name": "Example", "reference_product": "Example", "location": "CH", "unit": "kilogram"}
    ]
    assert result["biosphere"] == [
        {"name": "Carbon dioxide, fossil", "categories": ["emissions to air", "unspecified"], "unit": "kilogram"}
    ]
    assert result["provenance"]["format"] == "ecospold1"
