"""Dry-paint basis refinement preserves caller quantities, delivery and copied-native uncertainty."""

import hashlib
import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pilot_nitrate_paint import apply_pilot_nitrate_paint_review

REVIEW = json.loads((DATA_DIR / "export/uvek_pilot_nitrate_paint_review.json").read_text())


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["source"]["location"])
@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_paint_keeps_producers_delivery_and_signed_uncertainty(row, amount, uncertainty):
    data = consumer(row["source"], amount, uncertainty)
    emission = {
        "name": "Carbon dioxide, fossil",
        "categories": ["air"],
        "unit": "kilogram",
        "type": "biosphere",
        "amount": 7,
    }
    data[0]["exchanges"].append(emission)
    before = deepcopy(data)
    source = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9"))
    result = source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert source.data == data == before
    records = result.data
    assert len(records) == row["expected_foreground_records"]
    assert records[0]["exchanges"][-1] == emission
    actual = records[0]["exchanges"][1]
    original = before[0]["exchanges"][1]
    assert actual["amount"] == amount and actual["formula"] == "dose"
    assert actual["scale"] == original["scale"]
    if uncertainty == 2:
        assert actual["loc"] == original["loc"] and actual["negative"] is original["negative"]
    else:
        assert actual["loc"] == original["loc"]
        assert actual["minimum"] == original["minimum"] and actual["maximum"] == original["maximum"]
    root = next(r for r in records if r.get("code") == actual["input"][1])
    inputs = [x for x in root["exchanges"] if x["type"] != "production"]
    assert len(inputs) == len(row["profiles"][0]["inputs"])
    assert all(
        all(x[k] == v for k, v in expected.items() if k != "recipe")
        for x, expected in zip(inputs, row["profiles"][0]["inputs"], strict=True)
    )
    helpers = {r.get("brightpath_conversion", {}).get("recipe_id"): r for r in records[1:]}
    coefficient_fields = (
        "amount",
        "uncertainty type",
        "loc",
        "scale",
        "minimum",
        "maximum",
        "negative",
        "pedigree",
        "scale without pedigree",
    )
    for recipe_id, recipe in REVIEW["auxiliary_recipes"].items():
        if recipe_id not in helpers:
            continue
        actual_inputs = [x for x in helpers[recipe_id]["exchanges"] if x["type"] != "production"]
        assert len(actual_inputs) == len(recipe["inputs"])
        for x, expected in zip(actual_inputs, recipe["inputs"], strict=True):
            assert {k: x[k] for k in coefficient_fields if k in x} == {
                k: expected[k] for k in coefficient_fields if k in expected
            }
            if "recipe" not in expected:
                assert x["name"] == expected["name"] and x["unit"] == expected["unit"]
            if expected.get("type") == "biosphere":
                assert tuple(x["categories"]) == tuple(expected["categories"])
    assert (
        sum(x["type"] == "biosphere" for r in records[1:] for x in r["exchanges"])
        == row["expected_foreground_biosphere_exchanges"]
    )
    assert not any(i.code == "migration.recipe_input_omission" for i in result.last_migration_report.issues)
    assert not result.last_migration_report.losses or all(
        loss.code != "migration.recipe_source_input_omission" for loss in result.last_migration_report.losses
    )


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["source"]["location"])
@pytest.mark.parametrize("version", ["3.11", "3.12"])
def test_paint_refuses_uninspected_source_versions(row, version):
    source = BrightwayInventory.from_data(consumer(row["source"], -1), context=contexts("ecoinvent", version))
    before = deepcopy(source.data)
    with pytest.raises(MigrationError):
        source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert source.data == before


@pytest.mark.parametrize("change", ["none", "late_parent", "new_helper", "component"])
def test_paint_authoring_replay_is_atomic_and_freezes_all_helpers(change):
    resource = deepcopy(load_uvek_technosphere_resource())
    if change == "late_parent":
        next(r for r in resource["replace"] if r["id"] == REVIEW["records"][-1]["rule_id"])["confidence"] = 0.123
    elif change == "new_helper":
        rid = next(
            p["target"]["recipe_ref"]
            for r in resource["replace"]
            if r["id"] == REVIEW["records"][-1]["rule_id"]
            for p in r["reference_flow_sign_review"]["profiles"]
            if p["source_profile"] == REVIEW["records"][-1]["source_profile"]
        )
        resource["recipes"][rid]["inputs"][0]["amount"] += 1
    elif change == "component":
        fields = ("name", "reference product", "location", "unit")
        source = REVIEW["records"][0]["profiles"][0]["bindings"][0]["source"]
        next(r for r in resource["replace"] if [r["source"][f] for f in fields] == source)["confidence"] = 0.123
    before = deepcopy(resource)
    if change == "none":
        apply_pilot_nitrate_paint_review(resource)
        assert resource == before
    else:
        with pytest.raises(ValueError):
            apply_pilot_nitrate_paint_review(resource)
        assert resource == before


def test_paint_binds_carrier_removals_nitrogen_difference_and_processing_variants():
    assert len(REVIEW["records"]) == 2 and len(REVIEW["auxiliary_recipes"]) == 3
    assert REVIEW["physical_carrier_removals_from_native_copies"] == 2
    assert REVIEW["original_market_freight_entries"] == 14
    assert sum(len(r["freight_witnesses"]) for r in REVIEW["records"]) == 14
    for row in REVIEW["records"]:
        assert row["native_paint_multiplier"] == 1 / 0.6
        assert row["native_resin_multiplier"] == 1
        assert not row["source_internal_inventory_reconstructed"]
        assert len(row["copied_native_carrier_removals"]) == 2
        assert [x["source_exchange"]["amount"] for x in row["source_production_boundary_differences"]] == [0.019, 0.019]
        assert all(not x["retained_in_native_proxy"] for x in row["source_production_boundary_differences"])
    paints = [h for h in REVIEW["auxiliary_recipes"].values() if "paint" in h["reference product"]]
    assert len(paints) == 2
    solvent = [next(x for x in h["inputs"] if x.get("name") == "White spirit, at plant") for h in paints]
    assert all(x["amount"] == 0.01356 for x in solvent)
    assert solvent[0] != solvent[1]
    for witness in REVIEW["authorization_evidence"]:
        assert hashlib.sha256((DATA_DIR / witness["path"]).read_bytes()).hexdigest() == witness["sha256"]


@pytest.mark.parametrize("row", REVIEW["records"])
def test_paint_preserves_separate_caller_owned_solvent_purchase(row):
    data = consumer(row["source"], 2, 2)
    solvent = {"name": "Caller solvent stock", "reference product": "solvent", "location": "CH", "unit": "kilogram"}
    separate = {
        **solvent,
        "type": "technosphere",
        "amount": 13.2,
        "formula": "separate_solvent",
        "uncertainty type": 3,
        "loc": 13.2,
        "scale": 0.7,
    }
    data[0]["exchanges"].append(separate)
    data.append({**solvent, "exchanges": [{**solvent, "type": "production", "amount": 1}]})
    source = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9"))
    before = deepcopy(source.data)
    result = source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    actual = result.data[0]["exchanges"][2]
    assert all(actual[k] == v for k, v in separate.items())
    assert source.data == before
    assert result.data[1] == before[1]


def test_paint_shares_only_the_resin_helper_across_both_market_variants():
    data = consumer(REVIEW["records"][0]["source"], 1)
    data[0]["exchanges"].append({**REVIEW["records"][1]["source"], "type": "technosphere", "amount": 2})
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        target_context("2026"), policy=MigrationPolicy.permissive()
    )
    assert len(result.data) == 6
    (resin_id,) = [rid for rid, h in REVIEW["auxiliary_recipes"].items() if h["reference product"] == "dry alkyd resin"]
    assert sum(h.get("brightpath_conversion", {}).get("recipe_id") == resin_id for h in result.data) == 1
