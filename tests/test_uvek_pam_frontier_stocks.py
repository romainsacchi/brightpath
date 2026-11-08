"""Exact allocated scrap stocks preserve caller state without zeroing actual scrap preparation."""

import json
from collections import defaultdict
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import (
    BackgroundContext,
    BiosphereProfile,
    BrightwayInventory,
    FormatProfile,
    InventoryContext,
    TechnosphereProfile,
)
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from scripts import review_uvek_pam_frontier_stocks as stocks
from scripts.review_uvek_cutoff_stocks import stock_recipe

POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN)


@pytest.mark.parametrize("version", sorted(stocks.VERSIONS))
@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_both_allocation_stocks_preserve_signed_mass_metadata_and_separate_emissions(version, release, amount):
    data = None
    for row in stocks.SOURCES.values():
        for kind, uncertainty in (("technosphere", 2), ("substitution", 3)):
            example = consumer(row["source"], amount, uncertainty, kind)
            example[0]["exchanges"][1]["caller_metadata"] = {"preserve": [1, 2]}
            if data is None:
                data = example
            else:
                data[0]["exchanges"].append(example[0]["exchanges"][1])
    data[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "amount": 0.5,
            "type": "biosphere",
        }
    )
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    target = target_context(release)
    result = inventory.migrate_background(target, policy=POLICY)
    assert len(result.data) == 3
    helpers = {d["code"]: d for d in result.data[1:]}
    for old, new in zip(before[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True):
        helper = helpers[new["input"][1]]
        source = stocks.identity(old)
        rid, _ = stock_recipe(source, review_path=stocks.REVIEW_PATH)
        assert helper["brightpath_conversion"]["recipe_id"] == rid
        assert helper["brightpath_conversion"]["zero_burden"] is True
        assert helper["unit"] == "kilogram"
        assert helper["exchanges"] == [{**{f: helper[f] for f in stocks.FIELDS}, "type": "production", "amount": 1}]
        assert new["amount"] == old["amount"] == amount
        assert all(new.get(k) == v for k, v in old.items() if k not in stocks.FIELDS)
    assert result.data[0]["exchanges"][-1]["amount"] == 0.5
    assert sum(i.code == "migration.zero_burden_supplier" for i in result.last_migration_report.issues) == 4
    assert inventory.data == data == before
    assert result.migrate_background(target).data == result.data


def evidence():
    inventories = defaultdict(list)
    observations = defaultdict(lambda: defaultdict(set))
    for row in stocks.APPROVAL["inventories"]:
        source = stocks.identity(row["source"])
        inventories[source].append({**row, "source": list(source)})
        observations[row["label"].split("-")[0]][source].add((row["label"], row["reference_amount"]))
    native = {
        stocks.identity(r["source"]): {k: v for k, v in r.items() if k != "source"}
        for r in stocks.APPROVAL["native_roles"]
    }
    return inventories, observations, native


@pytest.mark.parametrize(
    "mutation", ("none", "processing", "biosphere", "negative", "missing_profile", "changed_hash", "native_waste")
)
def test_stock_review_rejects_new_processing_releases_negative_empty_roles_or_missing_evidence(mutation):
    inventories, observations, native = evidence()
    source = next(iter(stocks.SOURCES))
    record = inventories[source][0]
    if mutation in {"processing", "biosphere"}:
        record["total_exchanges"] += 1
        record["only_self_production"] = False
    elif mutation == "negative":
        record["production_amount"] = record["reference_amount"] = -1
    elif mutation == "missing_profile":
        inventories[source].pop()
    elif mutation == "changed_hash":
        record["inventory_sha256"] = "0" * 64
    elif mutation == "native_waste":
        native[next(iter(native))]["native_flow_type"] = "WASTE_FLOW"
    if mutation == "none":
        stocks.validate_inventories(inventories, observations, native)
    else:
        with pytest.raises(ValueError, match="repeat the scientific review"):
            stocks.validate_inventories(inventories, observations, native)


def test_existing_preparation_helpers_keep_their_processing_burdens():
    resource = json.loads((stocks.ROOT / "brightpath/data/migrations/uvek/ecoinvent-to-uvek-2025.json").read_text())
    stocks.validate_bindings(resource)
    assert len(stocks.APPROVAL["existing_recipe_bindings"]) == stocks.APPROVAL["existing_recipe_binding_count"]
    assert stocks.APPROVAL["unrelated_existing_same_target_recipe_bodies"] == 19
    rid = next(iter(stocks.APPROVAL["existing_recipe_bindings"]))
    resource["recipes"][rid]["inputs"] = []
    with pytest.raises(ValueError, match="repeat the scientific review"):
        stocks.validate_bindings(resource)


@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
@pytest.mark.parametrize("role", stocks.APPROVAL["native_roles"])
def test_native_preparation_does_not_inherit_an_inverse_empty_stock_mapping(release, role):
    data = consumer(role["source"], -2, 2)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(
        data, context=InventoryContext(FormatProfile("brightway_excel"), target_context(release))
    )
    target = BackgroundContext(
        TechnosphereProfile("ecoinvent", "3.12", "cutoff"), BiosphereProfile("ecoinvent", "3.12")
    )
    with pytest.raises(MigrationError, match="No reviewed directional correspondence"):
        inventory.migrate_background(target, policy=POLICY)
    assert inventory.data == data == before
