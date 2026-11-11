"""Wool markets retain source dust, treatment shares, operating inputs and freight."""

import hashlib
import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context
from test_uvek_wool_europe_direct_grid import historical_authoring_stage

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from scripts.review_uvek_pilot_nitrate_wool_europe import apply_pilot_nitrate_wool_europe_review

REVIEW = json.loads((DATA_DIR / "export/uvek_pilot_nitrate_wool_europe_review.json").read_text())


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["source"]["location"])
@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_european_wool_keeps_dust_fates_operating_inputs_and_signed_uncertainty(row, amount, uncertainty):
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
    assert len(records) == row["expected_foreground_records"] - 1
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
    assert REVIEW["country_grid_recipe"] not in helpers
    decision = json.loads((DATA_DIR / "export/uvek_wool_europe_direct_grid_review.json").read_text())
    treatment = helpers[decision["owner_recipe_id"]]
    electricity = [x for x in treatment["exchanges"] if x["type"] != "production"][decision["input_index"]]
    assert all(electricity[k] == value for k, value in decision["native_supplier"].items())
    assert electricity["amount"] == 0.0022
    assert not any(i.code == "migration.recipe_input_omission" for i in result.last_migration_report.issues)


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["source"]["location"])
@pytest.mark.parametrize("version", ["3.6", "3.12"])
def test_european_wool_refuses_uninspected_source_versions(row, version):
    source = BrightwayInventory.from_data(consumer(row["source"], -1), context=contexts("ecoinvent", version))
    before = deepcopy(source.data)
    with pytest.raises(MigrationError):
        source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert source.data == before


@pytest.mark.parametrize("change", ["none", "late_parent", "new_helper", "prior_helper"])
def test_european_wool_authoring_replay_is_atomic_and_freezes_all_helpers(change):
    resource = historical_authoring_stage("wool")
    if change == "late_parent":
        next(r for r in resource["replace"] if r["id"] == REVIEW["records"][-1]["rule_id"])["confidence"] = 0.123
    elif change == "new_helper":
        resource["recipes"][next(iter(REVIEW["auxiliary_recipes"]))]["inputs"][0]["amount"] += 1
    elif change == "prior_helper":
        resource["recipes"][next(iter(REVIEW["recipe_dependencies"]))]["inputs"][0]["amount"] += 1
    before = deepcopy(resource)
    if change == "none":
        apply_pilot_nitrate_wool_europe_review(resource)
        assert resource == before
    else:
        with pytest.raises(ValueError):
            apply_pilot_nitrate_wool_europe_review(resource)
        assert resource == before


def test_european_wool_pins_only_the_approved_seven_country_proxies():
    assert len(REVIEW["canonical_biosphere_witnesses"]) == 3
    assert all(x["flow_role"] == "emission" for x in REVIEW["canonical_biosphere_witnesses"])
    proxies = [x for x in REVIEW["operating_utility_witnesses"] if x["approved_country_proxy"]]
    matching = [x for x in REVIEW["operating_utility_witnesses"] if not x["approved_country_proxy"]]
    assert len(proxies) == 7 and len(matching) == 32
    assert {x["source"]["identity"]["location"] for x in proxies} == {"AL", "MD", "MT", "GI", "XK", "ME", "BY"}
    assert all(x["target"]["location"] == "RER" for x in proxies)
    assert all(x["same_declared_territory_and_voltage"] for x in matching)
    assert sum(x["same_publisher_uuid"] for x in REVIEW["source_native_facility_land_comparison"]) == 2
    assert {r["source"]["location"] for r in REVIEW["records"]} == {"Europe without Switzerland"}
    assert REVIEW["records"][0]["approved_country_proxy_entries"] == proxies
    for witness in REVIEW["authorization_evidence"]:
        assert hashlib.sha256((DATA_DIR / witness["path"]).read_bytes()).hexdigest() == witness["sha256"]
