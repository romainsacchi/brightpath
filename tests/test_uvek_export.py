import math
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
from brightpath.background.catalogs import TechnosphereCatalog
from brightpath.background.uvek_export import (
    apply_uvek_export,
    identity,
    load_uvek_export_resource,
    scale_exchange,
    validate_resource,
)
from brightpath.core.policies import MigrationPolicy, PolicyAction


def contexts():
    source = BackgroundContext(TechnosphereProfile("uvek", "2025", "cutoff"), BiosphereProfile("ecoinvent", "3.10"))
    target = BackgroundContext(
        TechnosphereProfile("ecoinvent", "3.12", "cutoff"), BiosphereProfile("ecoinvent", "3.12")
    )
    return InventoryContext(FormatProfile("brightway_excel"), source), target


def data_for(rule):
    dataset = {"name": "Example foreground", "reference product": "example", "unit": "unit", "location": "CH"}
    return [
        {
            **dataset,
            "comment": "Synthetic foreground for migration tests.",
            "production amount": 1,
            "exchanges": [
                {**dataset, "type": "production", "amount": 1},
                {**rule["source"], "type": "technosphere", "amount": 2, "uncertainty type": 3, "loc": 2, "scale": 0.1},
            ],
        }
    ]


def test_route_is_explicit_and_exact():
    source, target = contexts()
    plan = plan_background_migration(source.background, target)
    assert plan.executable
    assert [step.direction for step in plan.technosphere_steps] == ["forward"]
    assert len(plan.biosphere_steps) == 2
    for version, model in [("3.11", "cutoff"), ("3.12", "consequential"), ("3.12.1", "cutoff")]:
        unsupported = BackgroundContext(TechnosphereProfile("ecoinvent", version, model), target.biosphere)
        assert not plan_background_migration(source.background, unsupported).executable


def test_entire_catalog_has_a_disposition_and_approved_endpoints_exist():
    resource = load_uvek_export_resource()
    validate_resource(resource)
    source, target = contexts()
    provider = PackageCatalogProvider()
    catalog = provider.load_technosphere(source.background.technosphere).identities
    dispositions = resource["rules"] + resource["unresolved"]
    assert len(dispositions) == len(catalog) == 11747
    assert {identity(entry["source"]) for entry in dispositions} == catalog
    target_identities = provider.load_technosphere(target.technosphere).identities
    biosphere = provider.load_biosphere(target.biosphere).identities
    for rule in resource["rules"]:
        if "target" in rule:
            assert identity(rule["target"]) in target_identities
    for recipe in resource["recipes"].values():
        for exchange in recipe["inputs"]:
            if exchange["type"] == "technosphere":
                assert identity(exchange) in target_identities
            else:
                assert (exchange["name"], tuple(exchange["categories"]), exchange["unit"]) in biosphere


def test_direct_export_preserves_original_and_supports_noop():
    rule = next(
        rule
        for rule in load_uvek_export_resource()["rules"]
        if rule["classification"] == "correspondence" and rule["factor"] == 1
    )
    original = data_for(rule)
    saved = deepcopy(original)
    source, target = contexts()
    inventory = BrightwayInventory.from_data(original, context=source)
    result = inventory.migrate_background(target)
    assert original == saved == inventory.data
    assert identity(result.data[0]) == identity(original[0])
    assert result.data[0]["exchanges"][0] == original[0]["exchanges"][0]
    assert identity(result.data[0]["exchanges"][1]) == identity(rule["target"])
    assert result.migrate_background(target).data == result.data


def test_helpers_are_deduplicated_and_preserve_consumer_uncertainty():
    rule = next(rule for rule in load_uvek_export_resource()["rules"] if "recipe" in rule)
    data = data_for(rule)
    data[0]["exchanges"].append(deepcopy(data[0]["exchanges"][1]))
    source, target = contexts()
    inventory = BrightwayInventory.from_data(data, context=source)
    with pytest.raises(MigrationError, match="Background migration failed"):
        inventory.migrate_background(target)
    result = inventory.migrate_background(target, policy=MigrationPolicy(on_information_loss=PolicyAction.WARN))
    assert len(result.data) == 2
    helper = result.data[1]
    assert helper["brightpath_conversion"]["origin"] == "generated"
    for exchange in result.data[0]["exchanges"][1:]:
        assert identity(exchange) == identity(helper)
        assert (exchange["amount"], exchange["loc"], exchange["scale"]) == (2, 2, 0.1)
    assert not result.validate().has_errors
    repeated = inventory.migrate_background(target, policy=MigrationPolicy(on_information_loss=PolicyAction.WARN))
    assert repeated.data == result.data


def test_foreground_matching_background_is_protected():
    rule = load_uvek_export_resource()["rules"][0]
    data = data_for(rule)
    supplier = {**rule["source"], "exchanges": [{**rule["source"], "type": "production", "amount": 1}]}
    data.append(supplier)
    source, target = contexts()
    assert BrightwayInventory.from_data(data, context=source).migrate_background(target).data == data
    inventory = BrightwayInventory.from_data(data[:1], context=source)
    assert inventory.migrate_background(target, additional_foreground_targets=[identity(supplier)]).data == data[:1]


def test_unresolved_supplier_fails_and_rolls_back():
    resource = load_uvek_export_resource()
    data = data_for(resource["unresolved"][0])
    source, target = contexts()
    inventory = BrightwayInventory.from_data(data, context=source)
    for policy in (MigrationPolicy.strict(), MigrationPolicy.permissive()):
        with pytest.raises(MigrationError) as caught:
            inventory.migrate_background(target, policy=policy)
        assert "migration.uvek_supplier_unresolved" in {issue.code for issue in caught.value.report.issues}
        assert inventory.data == data


@pytest.mark.parametrize("factor", [0, float("nan"), float("inf"), True])
def test_invalid_factors_are_rejected(factor):
    with pytest.raises(ValueError):
        scale_exchange({"amount": 1}, factor)


def test_uncertainty_scaling_and_formula():
    normal = {
        "amount": 2,
        "uncertainty type": 3,
        "loc": 2,
        "scale": 0.5,
        "minimum": 1,
        "maximum": 3,
        "formula": "input_amount",
    }
    scale_exchange(normal, -10)
    assert (normal["amount"], normal["loc"], normal["scale"], normal["minimum"], normal["maximum"]) == (
        -20,
        -20,
        5,
        -30,
        -10,
    )
    assert normal["formula"] == "(input_amount) * (-10.0)"
    lognormal = {"amount": 2, "uncertainty type": 2, "loc": math.log(2), "scale": 0.2}
    scale_exchange(lognormal, -5)
    assert lognormal["negative"] is True
    assert lognormal["loc"] == pytest.approx(math.log(10))
    assert lognormal["scale"] == 0.2
    uniform = {"amount": 0, "uncertainty type": 4, "minimum": 0, "maximum": 4}
    scale_exchange(uniform, -2)
    assert uniform == {"amount": 0, "uncertainty type": 4, "minimum": -8, "maximum": 0}
    with pytest.raises(ValueError, match="uncertainty"):
        scale_exchange({"amount": 1, "uncertainty type": 8}, 2)


def test_resource_rejects_duplicates_and_cycles():
    resource = deepcopy(load_uvek_export_resource())
    resource["rules"].append(deepcopy(resource["rules"][0]))
    with pytest.raises(ValueError, match="duplicate"):
        validate_resource(resource)
    resource = deepcopy(load_uvek_export_resource())
    recipe_id = next(iter(resource["recipes"]))
    resource["recipes"][recipe_id]["inputs"] = [{"recipe": recipe_id, "amount": 1}]
    with pytest.raises(ValueError, match="Cyclic"):
        validate_resource(resource)


def test_helper_code_collision_is_an_error():
    resource = load_uvek_export_resource()
    rule = next(rule for rule in resource["rules"] if "recipe" in rule)
    policy = MigrationPolicy(on_information_loss=PolicyAction.WARN)
    report, helpers = apply_uvek_export(data_for(rule), policy)
    assert not report.has_errors
    data = data_for(rule)
    data[0]["code"] = helpers[0]["code"]
    report, _ = apply_uvek_export(data, policy)
    assert report.has_errors


@pytest.fixture(scope="module")
def catalogs():
    return PackageCatalogProvider()


@pytest.mark.parametrize("rule", load_uvek_export_resource()["rules"], ids=lambda rule: rule["id"])
def test_every_approved_rule_executes_with_complete_target_coverage(rule, catalogs):
    source, target = contexts()
    inventory = BrightwayInventory.from_data(data_for(rule), context=source)
    result = inventory.migrate_background(
        target, catalog_provider=catalogs, policy=MigrationPolicy(on_information_loss=PolicyAction.WARN)
    )
    assert result.context.background == target
    validation = result.last_migration_report.stages[-1]
    assert validation.metrics["technosphere"]["coverage"] == 1
    assert validation.metrics["biosphere"]["coverage"] == 1
    assert not validation.has_errors


def test_target_failure_rolls_back_directional_changes(monkeypatch):
    rule = next(rule for rule in load_uvek_export_resource()["rules"] if rule["classification"] == "correspondence")
    source, target = contexts()
    data = data_for(rule)
    provider = PackageCatalogProvider()
    original_loader = provider.load_technosphere
    monkeypatch.setattr(
        provider,
        "load_technosphere",
        lambda profile: (
            TechnosphereCatalog(profile, frozenset()) if profile == target.technosphere else original_loader(profile)
        ),
    )
    inventory = BrightwayInventory.from_data(data, context=source)
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(target, catalog_provider=provider)
    assert not caught.value.report.changed
    assert caught.value.report.metadata["committed"] is False
    assert inventory.data == data


def test_nested_recipe_dependencies_are_included():
    resource = deepcopy(load_uvek_export_resource())
    rule = next(rule for rule in resource["rules"] if "recipe" in rule)
    parent = rule["recipe"]
    child = next(
        recipe_id
        for recipe_id, recipe in resource["recipes"].items()
        if recipe_id != parent and recipe["unit"] == resource["recipes"][parent]["unit"]
    )
    resource["recipes"][parent]["inputs"] = [{"recipe": child, "amount": 0.5}]
    validate_resource(resource)
    report, helpers = apply_uvek_export(
        data_for(rule), MigrationPolicy(on_information_loss=PolicyAction.WARN), resource=resource
    )
    assert not report.has_errors
    assert len(helpers) == 2
    generated = {helper["brightpath_conversion"]["recipe_id"]: helper for helper in helpers}
    parent_input = generated[parent]["exchanges"][1]
    assert identity(parent_input) == identity(generated[child])
    assert parent_input["amount"] == 0.5
