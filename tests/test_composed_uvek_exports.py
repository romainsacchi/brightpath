from copy import deepcopy

import pytest

from brightpath import (
    BackgroundContext,
    BiosphereProfile,
    BrightwayInventory,
    FormatProfile,
    InventoryContext,
    MigrationError,
    TechnosphereProfile,
)
from brightpath.background import PackageCatalogProvider, plan_background_migration
from brightpath.background.uvek_export import load_uvek_export_resource
from brightpath.core.policies import MigrationPolicy, PolicyAction

POLICY = MigrationPolicy(on_inferred_reverse=PolicyAction.WARN, on_information_loss=PolicyAction.WARN)


def context(version, family="ecoinvent"):
    return BackgroundContext(
        TechnosphereProfile(family, version, "cutoff"),
        BiosphereProfile("ecoinvent", "3.10" if family == "uvek" else version),
    )


def inventory(background, exchanges):
    identity = {"name": "Foreground", "reference product": "output", "location": "GLO", "unit": "kilogram"}
    return BrightwayInventory.from_data(
        [{**identity, "exchanges": [{**identity, "type": "production", "amount": 1}, *exchanges]}],
        context=InventoryContext(FormatProfile("brightway_excel"), background),
    )


@pytest.mark.parametrize("version", ["3.6", "3.7", "3.8", "3.9.1", "3.10.1", "3.11", "3.12"])
def test_all_advertised_routes_execute_and_preserve_foreground(version):
    source = context("2025", "uvek")
    target = context(version)
    plan = plan_background_migration(source, target, POLICY)
    assert plan.executable
    assert plan.technosphere_steps[0].target_version == "3.12"
    initial = inventory(source, [])
    original = deepcopy(initial.data)
    result = initial.migrate_background(target, policy=POLICY)
    assert initial.data == original == result.data
    assert result.context.background == target


@pytest.mark.parametrize("source,target", [("3.10", "3.10.1"), ("3.10.1", "3.10"), ("3.9", "3.9.1")])
def test_patch_routes_preserve_amounts_and_uncertainty(source, target):
    flow = {
        "name": "Carbon dioxide, fossil",
        "categories": ("air",),
        "unit": "kilogram",
        "type": "biosphere",
        "amount": 2,
        "uncertainty type": 3,
        "loc": 2,
        "scale": 0.2,
    }
    initial = inventory(context(source), [flow])
    result = initial.migrate_background(context(target))
    assert result.data == initial.data
    assert result.context.background.biosphere.version == target
    assert any(issue.code == "migration.patch_identity_validation" for issue in result.last_migration_report.issues)


def test_removed_patch_supplier_is_rejected_even_with_permissive_policy():
    provider = PackageCatalogProvider()
    old = provider.load_technosphere(context("3.9").technosphere).identities
    new = provider.load_technosphere(context("3.9.1").technosphere).identities
    removed = next(iter(old - new))
    exchange = dict(zip(("name", "reference product", "location", "unit"), removed))
    initial = inventory(context("3.9"), [{**exchange, "type": "technosphere", "amount": 1}])
    original = deepcopy(initial.data)
    with pytest.raises(MigrationError):
        initial.migrate_background(context("3.9.1"), policy=MigrationPolicy.permissive())
    assert initial.data == original


def test_composed_recipe_migrates_helpers_and_preserves_consumers():
    rule = next(rule for rule in load_uvek_export_resource()["rules"] if "recipe" in rule)
    exchange = {**rule["source"], "type": "technosphere", "amount": 2, "uncertainty type": 3, "loc": 2, "scale": 0.1}
    initial = inventory(context("2025", "uvek"), [exchange, deepcopy(exchange)])
    original = deepcopy(initial.data)
    result = initial.migrate_background(context("3.10.1"), policy=POLICY)
    assert len(result.data) == 2
    assert initial.data == original
    for consumer in result.data[0]["exchanges"][1:]:
        assert consumer["name"] == result.data[1]["name"]
        assert (consumer["amount"], consumer["loc"], consumer["scale"]) == (2, 2, 0.1)
    assert result.migrate_background(context("3.10.1")).data == result.data


def test_organic_reverse_proxy_is_explicit_and_policy_controlled():
    exchange = {
        "name": "market for chemical, organic, unspecified",
        "reference product": "chemical, organic, unspecified",
        "location": "GLO",
        "unit": "kilogram",
        "type": "technosphere",
        "amount": 2,
    }
    initial = inventory(context("3.12"), [exchange])
    with pytest.raises(MigrationError):
        initial.migrate_background(context("3.10.1"))
    result = initial.migrate_background(context("3.10.1"), policy=POLICY)
    assert result.data[0]["exchanges"][1]["name"] == "market for chemical, organic"
    assert any("proxy" in issue.code for issue in result.last_migration_report.issues)
