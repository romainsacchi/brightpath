"""Wastewater routes retain untreated emissions, caller quantities and collection basis."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pilot_wastewater_routes import apply_pilot_wastewater_routes_review

REVIEW = json.loads((DATA_DIR / "export/uvek_pilot_wastewater_routes_review.json").read_text())


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["source"]["location"])
@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_wastewater_keeps_route_stages_and_signed_uncertainty(row, amount, uncertainty):
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
        all(x[k] == v for k, v in expected.items() if k not in {"recipe", "biosphere_targets", "categories"})
        for x, expected in zip(inputs, row["profiles"][0]["inputs"], strict=True)
    )
    for actual_input, expected in zip(inputs, row["profiles"][0]["inputs"], strict=True):
        if "categories" in expected:
            assert tuple(actual_input["categories"]) == tuple(expected["categories"])
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
    assert not any(
        i.code in {"migration.recipe_input_omission", "migration.recipe_source_input_omission"}
        for i in result.last_migration_report.issues
    )


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["source"]["location"])
@pytest.mark.parametrize("version", ["3.7", "3.12"])
def test_wastewater_refuses_uninspected_source_versions(row, version):
    source = BrightwayInventory.from_data(consumer(row["source"], -1), context=contexts("ecoinvent", version))
    before = deepcopy(source.data)
    with pytest.raises(MigrationError):
        source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert source.data == before


@pytest.mark.parametrize("change", ["none", "late_parent", "new_helper", "component"])
def test_wastewater_authoring_replay_is_atomic_and_freezes_all_helpers(change):
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
        apply_pilot_wastewater_routes_review(resource)
        assert resource == before
    else:
        with pytest.raises(ValueError):
            apply_pilot_wastewater_routes_review(resource)
        assert resource == before


def test_wastewater_keeps_untreated_fractions_and_isolates_grid_without_whole_treatment():
    assert len(REVIEW["records"]) == 4
    assert len(REVIEW["canonical_biosphere_witnesses"]) == 43
    assert len(REVIEW["source_collection_witnesses"]) == 2
    assert REVIEW["recipe_dependencies"] == {}
    assert len(REVIEW["all_generated_new_recipes"]) == 11
    for version, evidence in REVIEW["native_collection_isolation"].items():
        assert evidence["native_plant_coordinate_count"] == (133 if version == "2025" else 134)
        exchange = evidence["isolated_collection_exchange"]
        assert exchange["amount"] == 2.1782e-7 and exchange["unit"] == "kilometer"
        assert exchange["name"] == "Sewer grid, class 3"
    for row in REVIEW["records"]:
        inputs = row["profiles"][0]["inputs"]
        assert sum(x.get("type") == "biosphere" for x in inputs) == 43
        assert row["source_collection_m3_per_source_m3"] == 1
        assert 0 < row["source_treated_fraction"] < 1
        assert row["source_COD_kg_per_m3"] == 0.42534 and row["native_COD_kg_per_m3"] == 0.1556
        treatment = next(x for x in inputs if "recipe" in x and x["amount"] == row["source_treated_fraction"])
        assert treatment["amount"] != 1
        assert "unquantified" in row["limitations"]
    for rid, h in REVIEW["auxiliary_recipes"].items():
        if rid == REVIEW["native_plant_only_recipe"]:
            assert len(h["inputs"]) == 134
            assert sum(x["type"] == "biosphere" for x in h["inputs"]) == 105
            assert not any(x.get("name") in {"Sewer grid, class 3", "Residential sewer grid"} for x in h["inputs"])
        else:
            (x,) = h["inputs"]
            if "recipe" in x:
                assert x["recipe"] == REVIEW["native_plant_only_recipe"] and x["amount"] == 1
            else:
                assert x["amount"] == 2.1782e-7 and x["unit"] == "kilometer"
                assert x["name"] == "Sewer grid, class 3"
