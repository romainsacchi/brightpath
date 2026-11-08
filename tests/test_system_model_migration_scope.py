"""System models cannot change through direct or composed migration routes."""

import json
from copy import deepcopy

import pytest

from brightpath.background.execution import execute_background_migration
from brightpath.background.migration import plan_background_migration
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.capabilities import migration_capabilities
from brightpath.core import (
    BackgroundContext,
    BiosphereProfile,
    FormatProfile,
    InventoryContext,
    MigrationPolicy,
    TechnosphereProfile,
)
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_migration_resource, load_uvek_technosphere_resource
from brightpath.models import InventoryDocument
from scripts.generate_migration_manifest import build_manifest
from scripts.scope_uvek_system_models import cutoff_uvek_resource

VERSIONS = ("3.6", "3.7", "3.8", "3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12")
POLICIES = (MigrationPolicy.strict(), MigrationPolicy.permissive())


def context(family, version, model):
    return BackgroundContext(TechnosphereProfile(family, version, model), BiosphereProfile("ecoinvent", "3.10"))


PAIRS = [
    (context("ecoinvent", version, "consequential"), context("uvek", release, "cutoff"))
    for version in VERSIONS
    for release in ("2025", "2026")
] + [(context("ecoinvent", version, "cutoff"), context("ecoinvent", "3.12", "consequential")) for version in VERSIONS]
PAIRS += [(target, source) for source, target in PAIRS]


@pytest.mark.parametrize("source,target", PAIRS)
@pytest.mark.parametrize("policy", POLICIES)
def test_cross_model_routes_are_unavailable_before_any_hub_or_resource_selection(source, target, policy):
    plan = plan_background_migration(source, target, policy)
    assert not plan.executable
    assert plan.technosphere_steps == ()
    assert plan.source is source and plan.target is target
    assert {i.code for i in plan.report.issues} == {"migration.technosphere_cross_system_model_unavailable"}


@pytest.mark.parametrize("source,target", PAIRS[:4])
@pytest.mark.parametrize("validate_source", (True, False))
def test_cross_model_execution_rolls_back_without_creating_helpers(source, target, validate_source):
    from brightpath.background import InMemoryCatalogProvider

    class NoCatalogReads(InMemoryCatalogProvider):
        def load_technosphere(self, profile):
            raise AssertionError("An unsupported model route must not read catalogs.")

        def load_biosphere(self, profile):
            raise AssertionError("An unsupported model route must not read catalogs.")

    data = [
        {"name": "owned foreground", "reference product": "service", "location": "CH", "unit": "unit", "exchanges": []}
    ]
    document = InventoryDocument(data=data, context=InventoryContext(FormatProfile("brightway_excel"), source))
    before = deepcopy(document.data)
    policy = MigrationPolicy.permissive()
    # Source validation is an independent option; the planner must reject even
    # when no catalog validation can intercept this request.
    from dataclasses import replace

    result = execute_background_migration(
        document, target, NoCatalogReads(), replace(policy, validate_source=validate_source)
    )
    assert result.report.has_errors
    assert result.value is document and document.data == before
    assert result.value.context.background == source


@pytest.mark.parametrize("release", ("2025", "2026"))
@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("reverse", (False, True))
def test_compatible_cutoff_routes_remain_available(version, release, reverse):
    source, target = context("ecoinvent", version, "cutoff"), context("uvek", release, "cutoff")
    if reverse:
        source, target = target, source
    assert plan_background_migration(source, target, MigrationPolicy.permissive()).executable


def test_packaged_forward_scope_and_capabilities_contain_only_cutoff_routes():
    resource = load_uvek_technosphere_resource()
    assert {p["system_model"] for p in resource["source_profiles"]} == {"cutoff"}
    for rule in resource["replace"]:
        assert all(
            p["source_profile"]["system_model"] == "cutoff"
            for p in rule.get("reference_flow_sign_review", {}).get("profiles", [])
        )
    assert not any("industrial-gas" in key for key in resource["recipes"])
    assert all(
        c.system_model == "cutoff"
        for c in migration_capabilities()
        if c.axis == "technosphere" and c.target_family == "uvek"
    )
    with pytest.raises(ValueError, match="system models"):
        select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", "3.12", "consequential"))


def test_manifest_valid_resource_still_cannot_declare_cross_model_routes(tmp_path, migration_resource, monkeypatch):
    import brightpath.migrations.resources as resources

    resource = deepcopy(migration_resource)
    resource["source_profiles"][0]["system_model"] = "consequential"
    directory = tmp_path / "migrations"
    directory.mkdir()
    path = directory / "mixed-model.json"
    path.write_text(json.dumps(resource))
    (directory / "RESOURCE_MANIFEST.json").write_text(json.dumps(build_manifest(directory)))
    monkeypatch.setattr(resources, "DATA_DIR", tmp_path)
    resources._load_resource_manifest.cache_clear()
    try:
        with pytest.raises(MigrationError, match="cannot cross system models"):
            load_migration_resource(path)
    finally:
        resources._load_resource_manifest.cache_clear()


def test_cutoff_projection_preserves_approved_coefficients_and_prunes_only_unused_recipes(tmp_path, migration_resource):
    resource = deepcopy(migration_resource)
    resource["target_profile"] = {"family": "uvek", "version": "2025", "system_model": "cutoff"}
    cutoff = resource["source_profiles"][0]
    consequential = {**cutoff, "system_model": "consequential"}
    resource["source_profiles"].append(consequential)
    rule = resource["replace"][0]
    target = deepcopy(rule["target"])
    rule["reference_flow_sign_review"] = {
        "status": "unresolved",
        "evidence": ["synthetic"],
        "profiles": [
            {
                "source_profile": cutoff,
                "review": {"status": "verified"},
                "target": {"recipe_ref": "keep"},
                "conversion_factor": -1,
            },
            {
                "source_profile": consequential,
                "review": {"status": "verified"},
                "target": {"recipe_ref": "retire"},
                "conversion_factor": 1,
            },
        ],
    }
    recipe = {"inputs": [{"recipe": "nested", "amount": 0.123456789}]}
    resource["recipes"] = {
        "keep": recipe,
        "nested": {"inputs": [{**target, "type": "technosphere", "amount": -2.75}]},
        "retire": {"inputs": []},
    }
    before = deepcopy(resource)
    catalog = {
        "technosphere": [
            {
                "name": r["source"]["name"],
                "reference_product": r["source"]["reference product"],
                "location": r["source"]["location"],
                "unit": r["source"]["unit"],
            }
            for r in resource["replace"]
        ]
    }
    from scripts.scope_uvek_system_models import VERSIONS as SERIES

    for version in SERIES:
        (tmp_path / f"ecoinvent__{version}__cutoff.json").write_text(json.dumps(catalog))
    result = cutoff_uvek_resource(resource, tmp_path)
    assert resource == before
    assert result["source_profiles"] == [cutoff]
    assert set(result["recipes"]) == {"keep", "nested"}
    assert result["recipes"]["keep"] == recipe
    assert result["recipes"]["nested"] == resource["recipes"]["nested"]
    assert result["replace"][0]["target"] == {"recipe_ref": "keep"}
    assert result["replace"][0]["conversion_factor"] == -1
    assert cutoff_uvek_resource(result, tmp_path) == result


@pytest.mark.parametrize(
    "family,version",
    [
        (family, version)
        for family in ("industrial_gas", "feed", "recovery", "coal_tar")
        for version in ("3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12")
        if family != "coal_tar" or version in ("3.6", "3.7", "3.8", "3.9")
    ],
)
@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
@pytest.mark.parametrize("amount", (-10, 0, 10))
def test_retired_consequential_exchange_families_reject_conversion_without_omitting_or_rewriting_inputs(
    family, version, release, amount
):
    from test_uvek_coal_tar_omissions import coal_inventory
    from test_uvek_feed_substitution_omissions import feed_data
    from test_uvek_industrial_gas import batch
    from test_uvek_recovery_credits import credit_data
    from test_uvek_reference_signs import contexts
    from test_uvek_refrigerant_disposal import target_context

    from brightpath import BrightwayInventory

    data = {
        "industrial_gas": lambda: batch(amount),
        "coal_tar": lambda: coal_inventory(amount),
        "feed": lambda: feed_data(amount),
        "recovery": lambda: credit_data(amount, version),
    }[family]()
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, "consequential"))
    with pytest.raises(MigrationError, match="system models"):
        inventory.migrate_background(target_context(release), policy=MigrationPolicy.permissive())
    assert inventory.data == data == before
    assert inventory.context.background.technosphere.system_model == "consequential"


def test_all_compatible_catalog_profiles_preserve_historical_executable_rules_and_helper_coefficients():
    from uvek_review_helpers import historical_forward_resource
    from scripts.review_uvek_pilot_materials import apply_pilot_material_review

    from brightpath import DATA_DIR

    historical = historical_forward_resource()
    # The later approved material overlay is independent of the system-model
    # restriction. Compare both sides with that overlay, keeping exact equality
    # for every executable rule and every helper coefficient below.
    apply_pilot_material_review(historical)
    active = load_uvek_technosphere_resource()
    fields = ("name", "reference product", "location", "unit")
    profiles = [
        *active["source_profiles"],
        *[{"family": "ecoinvent", "version": version, "system_model": "cutoff"} for version in ("3.9.1", "3.10.1")],
    ]
    for profile in profiles:
        context = TechnosphereProfile(**profile)
        catalog = json.loads(
            (DATA_DIR / "export/reference_catalogs" / f"ecoinvent__{context.version}__cutoff.json").read_text()
        )
        available = {(r["name"], r["reference_product"], r["location"], r["unit"]) for r in catalog["technosphere"]}
        before, after = (select_profile_sign_rules(resource, context) for resource in (historical, active))
        old_rules, new_rules = (
            {r["id"]: r for r in resource["replace"] if tuple(r["source"][f] for f in fields) in available}
            for resource in (before, after)
        )
        assert old_rules.keys() == new_rules.keys()
        for key, old in old_rules.items():
            status = old.get("reference_flow_sign_review", {}).get("status")
            assert status == new_rules[key].get("reference_flow_sign_review", {}).get("status")
            if status != "unresolved":
                assert old == new_rules[key]
        assert [r for r in before["delete"] if tuple(r["source"][f] for f in fields) in available] == [
            r for r in after["delete"] if tuple(r["source"][f] for f in fields) in available
        ]
    for recipe_id, recipe in active["recipes"].items():
        assert recipe == historical["recipes"][recipe_id]
