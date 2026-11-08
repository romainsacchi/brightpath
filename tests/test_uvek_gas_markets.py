"""Gas markets preserve system-model boundaries and already approved country mixes."""

from collections import defaultdict
from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from test_uvek_refrigerant_disposal import target_context
from uvek_review_helpers import historical_forward_resource

from brightpath import BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_gas_markets import (
    APPROVAL,
    MARKETS,
    market_decision,
    market_recipe,
    validate_market_dependencies,
)

POLICY = MigrationPolicy.permissive()
LABELS = [
    label
    for label in sorted({p["label"] for r in MARKETS.values() for p in r["profiles"]})
    if label.endswith("-cutoff")
]


def batch(amount):
    data = None
    for source in MARKETS:
        for kind, uncertainty in (("technosphere", 2), ("substitution", 3)):
            row = consumer(dict(zip(FIELDS, source, strict=True)), amount, uncertainty, kind)
            row[0]["exchanges"][1]["caller_metadata"] = {"retain": [1, 2]}
            if data is None:
                data = row
            else:
                data[0]["exchanges"].append(row[0]["exchanges"][1])
    data[0]["comment"] = "Synthetic gas-market conversion fixture."
    return data


@pytest.mark.parametrize("label", LABELS)
@pytest.mark.parametrize("amount", [-10, 0, 10])
def test_all_28_gas_market_profiles_preserve_quantity_uncertainty_and_exact_mix(label, amount):
    _, version, model = label.split("-")
    data = batch(amount)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
    result = inventory.migrate_background(target_context("2025"), policy=POLICY)
    helpers = {tuple(d[f] for f in FIELDS): d for d in result.data[1:]}
    changes = {c.path: c for c in result.last_migration_report.changes if c.code == "migration.foreground_recipe"}
    assert len(changes) == 4
    for index, (old, new) in enumerate(
        zip(before[0]["exchanges"][1:], result.data[0]["exchanges"][1:], strict=True), 1
    ):
        source = tuple(old[f] for f in FIELDS)
        profile = next(p for p in MARKETS[source]["profiles"] if p["label"] == label)
        key, recipe = market_recipe(source, profile)
        helper = helpers[tuple(new[f] for f in FIELDS)]
        assert helper["brightpath_conversion"]["recipe_id"] == key
        factor = 1 if model == "cutoff" else -1
        assert new["amount"] == factor * amount and new["type"] == old["type"]
        assert new["caller_metadata"] == old["caller_metadata"]
        assert new["formula"] == ("dose" if factor == 1 else "(dose) * (-1.0)")
        assert new["scale"] == old["scale"]
        if old["uncertainty type"] == 2:
            assert new["loc"] == old["loc"]
            assert new["negative"] is (old["negative"] if factor == 1 else amount >= 0)
        else:
            assert new["loc"] == factor * amount
            assert new["minimum"] == factor * amount - 1 and new["maximum"] == factor * amount + 1
        change = changes[f"datasets[0].exchanges[{index}]"].to_dict()
        assert change["before"] == old == change["details"]["source_exchange"]
        assert helper["exchanges"][0]["amount"] == 1
        if profile["kind"] == "zero_supply":
            assert len(helper["exchanges"]) == 1 and helper["brightpath_conversion"]["zero_burden"]
        elif profile["kind"] == "production_proxy":
            exchange = helper["exchanges"][1]
            assert len(helper["exchanges"]) == 2 and exchange["amount"] == 1
            assert exchange["name"] == "Coke oven gas, at plant"
        else:
            expected, observed = defaultdict(float), defaultdict(float)
            for e in recipe["inputs"]:
                expected[e["recipe"]] += e["amount"]
            for e in helper["exchanges"][1:]:
                child = helpers[tuple(e[f] for f in FIELDS)]
                observed[child["brightpath_conversion"]["recipe_id"]] += e["amount"]
                assert e["type"] == "technosphere" and e["amount"] > 0
            assert dict(observed) == pytest.approx(dict(expected))
    assert len([i for i in result.last_migration_report.issues if i.code == "migration.zero_burden_supplier"]) == (
        2 if model == "cutoff" else 0
    )
    assert inventory.data == data == before


@pytest.mark.parametrize("target_kind", ["2026", "native2026"])
@pytest.mark.parametrize("model", ["cutoff"])
def test_gas_market_helpers_follow_release_bridge_and_independent_reverse_route(target_kind, model):
    data = batch(-1)
    for exchange in data[0]["exchanges"][1:]:
        exchange["type"] = "technosphere"
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12", model)).migrate_background(
        target_context(target_kind), policy=POLICY
    )
    back = result.migrate_background(contexts("ecoinvent", "3.12").background, policy=POLICY)
    assert len(back.data) >= len(result.data)
    assert back.data[0]["exchanges"][1:] == result.data[0]["exchanges"][1:]
    assert not back.validate().has_errors


def test_gas_market_strict_policy_foreground_protection_and_missing_profile():
    data = batch(-1)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12", "cutoff"))
    with pytest.raises(MigrationError):
        inventory.migrate_background(target_context("2025"))
    assert inventory.data == original
    for source in MARKETS:
        supplier = dict(zip(FIELDS, source, strict=True))
        data.append({**supplier, "exchanges": [{**supplier, "type": "production", "amount": -1}]})
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12", "cutoff")).migrate_background(
        target_context("2025"), policy=POLICY
    )
    assert result.data == data
    resource = load_uvek_technosphere_resource()
    selected = select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", "3.12.1", "cutoff"))
    assert all(
        r["reference_flow_sign_review"]["status"] == "unresolved"
        for r in selected["replace"]
        if tuple(r["source"][f] for f in FIELDS) in MARKETS
    )


@pytest.mark.parametrize("change", ["source_hash", "empty_supplier", "primitive", "profile", "prior_target"])
def test_gas_market_changed_allocation_or_profile_requires_new_review(change):
    source, approval = next((s, r) for s, r in MARKETS.items() if s[1] == "blast furnace gas")
    rule = {"source": approval["source"], "target": deepcopy(approval["previous_target"])}
    inventories = {source: [(p["label"], p["inventory_sha256"]) for p in approval["profiles"]]}
    observations = [(p["label"], -1 if p["kind"] == "treatment_mix" else 1) for p in approval["profiles"]]
    primitive = APPROVAL["primitive"]
    inventories[tuple(primitive["target"][f] for f in FIELDS)] = [("uvek-2025-cutoff", primitive["inventory_sha256"])]
    for p in approval["profiles"]:
        if p["kind"] == "zero_supply":
            inventories.setdefault(tuple(p["empty_supplier"][f] for f in FIELDS), []).append(
                (p["label"], p["empty_supplier_inventory_sha256"])
            )
    if change == "source_hash":
        inventories[source][0] = (inventories[source][0][0], "0" * 64)
    elif change == "empty_supplier":
        inventories.pop(next(k for k in inventories if k[0].endswith("Recycled Content cut-off")))
    elif change == "primitive":
        inventories[tuple(primitive["target"][f] for f in FIELDS)] = []
    elif change == "profile":
        observations[0] = ("ecoinvent-3.12.1-cutoff", 1)
    else:
        rule["target"]["name"] = "Unreviewed"
    with pytest.raises(ValueError, match="repeat the scientific review"):
        market_decision(rule, observations, inventories, "synthetic", None)


def test_historical_gas_market_child_treatment_changes_cannot_silently_inherit_approval():
    resource = deepcopy(historical_forward_resource())
    child = next(iter(APPROVAL["dependencies"]))
    resource["recipes"][child]["inputs"][0]["amount"] = 2
    with pytest.raises(ValueError, match="dependency changed"):
        validate_market_dependencies(resource)
