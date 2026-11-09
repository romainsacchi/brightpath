"""Wool markets retain source dust, treatment shares, operating inputs and freight."""

from copy import deepcopy
import hashlib
import json

import pytest

from brightpath import BrightwayInventory, DATA_DIR
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pilot_nitrate_wool import apply_pilot_nitrate_wool_review
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

REVIEW = json.loads((DATA_DIR / "export/uvek_pilot_nitrate_wool_review.json").read_text())


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["source"]["location"])
@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_wool_keeps_dust_fates_operating_inputs_and_signed_uncertainty(row, amount, uncertainty):
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
    assert actual["amount"] == -amount and actual["formula"] == "(dose) * (-1.0)"
    assert actual["scale"] == original["scale"]
    if uncertainty == 2:
        assert actual["loc"] == original["loc"] and actual["negative"] is not original["negative"]
    else:
        assert actual["loc"] == -original["loc"]
        assert actual["minimum"] == -original["maximum"] and actual["maximum"] == -original["minimum"]
    root = next(r for r in records if r.get("code") == actual["input"][1])
    inputs = [x for x in root["exchanges"] if x["type"] != "production"]
    assert len(inputs) == len(row["source_fates"]) + len(row["freight_witnesses"])
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
    if row["source"]["location"] == "RoW":
        native_grids = [
            x for r in records[1:] for x in r["exchanges"] if x.get("name") == "Electricity, low voltage, at grid"
        ]
        assert {x["location"] for x in native_grids} == {"RNA", "RAF", "AU", "RLA", "RAS", "NZ"}
        assert len(native_grids) == 6
        machinery = [
            x
            for r in records[1:]
            for x in r["exchanges"]
            if x.get("name") == "Excavation, hydraulic digger (proj. 500)"
        ]
        assert sorted(x["amount"] for x in machinery) == [0.330592351639303, 0.669407648360697]
    assert not any(i.code == "migration.recipe_input_omission" for i in result.last_migration_report.issues)


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["source"]["location"])
@pytest.mark.parametrize("version", ["3.6", "3.12"])
def test_wool_refuses_uninspected_source_versions(row, version):
    source = BrightwayInventory.from_data(consumer(row["source"], -1), context=contexts("ecoinvent", version))
    before = deepcopy(source.data)
    with pytest.raises(MigrationError):
        source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert source.data == before


@pytest.mark.parametrize("change", ["none", "late_parent", "new_helper", "prior_helper"])
def test_wool_authoring_replay_is_atomic_and_freezes_all_helpers(change):
    resource = deepcopy(load_uvek_technosphere_resource())
    if change == "late_parent":
        next(r for r in resource["replace"] if r["id"] == REVIEW["records"][-1]["rule_id"])["confidence"] = 0.123
    elif change == "new_helper":
        resource["recipes"][next(iter(REVIEW["auxiliary_recipes"]))]["inputs"][0]["amount"] += 1
    elif change == "prior_helper":
        resource["recipes"][next(iter(REVIEW["recipe_dependencies"]))]["inputs"][0]["amount"] += 1
    before = deepcopy(resource)
    if change == "none":
        apply_pilot_nitrate_wool_review(resource)
        assert resource == before
    else:
        with pytest.raises(ValueError):
            apply_pilot_nitrate_wool_review(resource)
        assert resource == before


def test_wool_pins_complete_dust_definitions_and_scoped_utility_reuse():
    assert len(REVIEW["canonical_biosphere_witnesses"]) == 3
    assert all(x["flow_role"] == "emission" for x in REVIEW["canonical_biosphere_witnesses"])
    assert all(x["same_declared_territory_and_voltage"] for x in REVIEW["operating_utility_witnesses"])
    assert REVIEW["source_and_native_facility_land_quantities_match"] == 3
    assert REVIEW["source_and_native_facility_exact_land_identities_match"] == 2
    assert sum(x["same_publisher_uuid"] for x in REVIEW["source_native_facility_land_comparison"]) == 2
    assert {r["source"]["location"] for r in REVIEW["records"]} == {"CH", "RoW"}
    for witness in REVIEW["authorization_evidence"]:
        assert hashlib.sha256((DATA_DIR / witness["path"]).read_bytes()).hexdigest() == witness["sha256"]
