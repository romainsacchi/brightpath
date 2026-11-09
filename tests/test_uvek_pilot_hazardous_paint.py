"""Hazardous and paint routes retain caller data, treatment shares and audited omissions."""

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
from scripts.review_uvek_pilot_hazardous_paint import apply_pilot_hazardous_paint_review

REVIEW = json.loads((DATA_DIR / "export/uvek_pilot_hazardous_paint_review.json").read_text())


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["source"]["location"])
@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_hazardous_paint_keeps_route_stages_and_signed_uncertainty(row, amount, uncertainty):
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
    source = BrightwayInventory.from_data(data, context=contexts("ecoinvent", row["source_profile"]["version"]))
    result = source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert source.data == data == before
    records = result.data
    assert len(records) == row["expected_foreground_records"]
    assert records[0]["exchanges"][-1] == emission
    actual = records[0]["exchanges"][1]
    original = before[0]["exchanges"][1]
    assert actual["amount"] == -amount and actual["formula"] == "(dose) * (-1.0)"
    assert actual["scale"] == original["scale"]
    if uncertainty == 2:
        assert actual["loc"] == original["loc"] and actual["negative"] is not original["negative"]
    else:
        assert actual["loc"] == -original["loc"]
        assert actual["minimum"] == -original["maximum"] and actual["maximum"] == -original["minimum"]
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
    expected_omissions = row["profiles"][0].get("source_input_omissions", [])
    losses = [x for x in result.last_migration_report.losses if x.code == "migration.recipe_source_input_omission"]
    assert len(losses) == len(expected_omissions)
    assert [x.to_dict()["details"]["omission"] for x in losses] == expected_omissions
    assert root["brightpath_conversion"].get("source_input_omissions", []) == expected_omissions


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["source"]["location"])
@pytest.mark.parametrize("version", ["3.7", "3.12"])
def test_hazardous_paint_refuses_uninspected_source_versions(row, version):
    source = BrightwayInventory.from_data(consumer(row["source"], -1), context=contexts("ecoinvent", version))
    before = deepcopy(source.data)
    with pytest.raises(MigrationError):
        source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert source.data == before


@pytest.mark.parametrize("change", ["none", "late_parent", "new_helper", "component"])
def test_hazardous_paint_authoring_replay_is_atomic_and_freezes_all_helpers(change):
    resource = deepcopy(load_uvek_technosphere_resource())
    if change == "late_parent":
        next(r for r in resource["replace"] if r["id"] == REVIEW["records"][-1]["rule_id"])["confidence"] = 0.123
    elif change == "new_helper":
        resource["recipes"][next(iter(REVIEW["auxiliary_recipes"]))]["inputs"][0]["amount"] += 1
    elif change == "component":
        fields = ("name", "reference product", "location", "unit")
        source = REVIEW["records"][0]["profiles"][0]["bindings"][0]["source"]
        next(r for r in resource["replace"] if [r["source"][f] for f in fields] == source)["confidence"] = 0.123
    before = deepcopy(resource)
    if change == "none":
        apply_pilot_hazardous_paint_review(resource)
        assert resource == before
    else:
        with pytest.raises(ValueError):
            apply_pilot_hazardous_paint_review(resource)
        assert resource == before


def test_hazardous_paint_omissions_are_exact_and_do_not_become_zero_stock():
    resource = load_uvek_technosphere_resource()
    assert len(REVIEW["omitted_coprocessing_supplier_witnesses"]) == 5
    omissions = [o for r in REVIEW["records"] for p in r["profiles"] for o in p.get("source_input_omissions", [])]
    assert len(omissions) == 5
    assert all(
        o["source_exchange"]["name"] == "clinker production" and o["source_exchange"]["amount"] < 0 for o in omissions
    )
    assert {o["source_exchange"]["location"] for o in omissions} == {
        "CH",
        "RoW",
        "BR",
        "US",
        "Europe without Switzerland",
    }
    assert not any("clinker production" in h["name"] for h in REVIEW["auxiliary_recipes"].values())
    for rid, expected in REVIEW["recipe_dependencies"].items():
        assert hashlib.sha256(json.dumps(resource["recipes"][rid], sort_keys=True).encode()).hexdigest() == expected
        assert resource["recipes"][rid]["zero_burden"]
    assert REVIEW["allocated_stock_witnesses"][0]["handover_amount"] == -0.0047732
    for row in REVIEW["records"]:
        if row["source"]["reference product"] == "hazardous waste, for incineration":
            inputs = row["profiles"][0]["inputs"]
            assert [x["amount"] for x in inputs if "recipe" in x] == [0.5, 0.5]
    assert {w["documented_composition_coordinates"] for w in REVIEW["controlled_composition_matches"]} == {43, 44}
