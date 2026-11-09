"""Mixed heat preserves every market term and explicit allocated-burden bases."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pilot_mixed_heat_market import apply_pilot_mixed_heat_market_review

REVIEW = json.loads((DATA_DIR / "export/uvek_pilot_mixed_heat_market_review.json").read_text())
ROW = REVIEW["records"][0]


@pytest.mark.parametrize("profile", ROW["profiles"], ids=lambda p: p["label"])
@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_heat_preserves_signed_caller_demand_and_all_supplier_entries(profile, amount, uncertainty):
    data = consumer(ROW["source"], amount, uncertainty)
    before = deepcopy(data)
    source = BrightwayInventory.from_data(data, context=contexts("ecoinvent", profile["source_profile"]["version"]))
    result = source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert data == source.data == before
    assert len(result.data) == ROW["expected_foreground_records"]
    actual = result.data[0]["exchanges"][1]
    original = before[0]["exchanges"][1]
    for field in ("amount", "formula", "loc", "scale", "negative", "minimum", "maximum"):
        if field in original:
            assert actual[field] == original[field]
    root = next(r for r in result.data if r.get("code") == actual["input"][1])
    inputs = [x for x in root["exchanges"] if x["type"] != "production"]
    assert len(inputs) == 188
    assert all(
        all(x[k] == v for k, v in expected.items() if k != "recipe")
        for x, expected in zip(inputs, profile["inputs"], strict=True)
    )
    helpers = {r.get("brightpath_conversion", {}).get("recipe_id"): r for r in result.data[1:]}
    assert set(REVIEW["auxiliary_recipes"]) <= set(helpers)
    for rid, recipe in REVIEW["auxiliary_recipes"].items():
        actual_inputs = [x for x in helpers[rid]["exchanges"] if x["type"] != "production"]
        assert len(actual_inputs) == 1
        assert all(actual_inputs[0][k] == v for k, v in recipe["inputs"][0].items())
    assert not any(
        x.code.startswith("migration.recipe_source_input_omission") for x in result.last_migration_report.losses
    )


@pytest.mark.parametrize("version", ["3.6", "3.7", "3.8", "3.10", "3.11"])
def test_heat_does_not_admit_uninspected_source_releases(version):
    data = consumer(ROW["source"], 1)
    source = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    with pytest.raises(MigrationError):
        source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert source.data == data


@pytest.mark.parametrize("change", ["none", "parent", "helper", "component"])
def test_heat_authoring_is_idempotent_and_refuses_drift_before_mutation(change):
    resource = deepcopy(load_uvek_technosphere_resource())
    parent = next(r for r in resource["replace"] if r["id"] == ROW["rule_id"])
    if change == "parent":
        parent["confidence"] = 0.123
    elif change == "helper":
        rid = parent["reference_flow_sign_review"]["profiles"][0]["target"]["recipe_ref"]
        resource["recipes"][rid]["inputs"][0]["amount"] *= 2
    elif change == "component":
        fields = ("name", "reference product", "location", "unit")
        identity = ROW["profiles"][0]["bindings"][0]["source"]
        next(r for r in resource["replace"] if [r["source"][f] for f in fields] == identity)["confidence"] = 0.123
    before = deepcopy(resource)
    if change == "none":
        apply_pilot_mixed_heat_market_review(resource)
    else:
        with pytest.raises(ValueError):
            apply_pilot_mixed_heat_market_review(resource)
    assert resource == before


def test_heat_qualifies_fuel_and_companion_drivers_without_one_to_one_unit_fallback():
    assert len(REVIEW["correspondences"]) == 188
    assert REVIEW["market_entries_omitted"] == 0
    assert not REVIEW["district_network_added"] and not REVIEW["extra_efficiency_or_delivery_factor"]
    assert len(REVIEW["allocated_driver_proofs"]) == 75
    for proof in REVIEW["allocated_driver_proofs"]:
        assert proof["reconstructed_heat_reference_amount"] == 1
        recipe = REVIEW["auxiliary_recipes"][proof["recipe_id"]]
        assert recipe["unit"] == "megajoule" and len(recipe["inputs"]) == 1
        x = recipe["inputs"][0]
        assert x["amount"] == proof["factor"]
        if proof["kind"] == "allocated_electricity_driver":
            assert x["unit"] == "kilowatt hour" and proof["individual_source_coordinates_verified"] > 0
        elif proof["kind"] == "allocated_material_driver":
            assert x["unit"] == "kilogram" and proof["individual_source_coordinates_verified"] == 287
        else:
            assert x["unit"] == "megajoule"
            if "source_driver" in proof and "native_fuel_mass_per_reference_MJ" not in proof:
                assert x["amount"] == proof["source_driver"]["amount"]


def test_recovered_heat_keeps_source_allocated_zero_shares_without_an_empty_disposal_helper():
    zeros = REVIEW["allocated_zero_heat_witnesses"]
    assert len(zeros) == 12
    assert all(
        w["source_reference_amount"] == 1 and w["source_nonproduction_count"] == w["native_nonproduction_count"] == 0
        for w in zeros
    )
    assert all(w["disposal_allocation"] == 1 and w["energy_allocation"] == 0 for w in zeros)
    assert all(not r.get("zero_burden") and r["inputs"] for r in REVIEW["auxiliary_recipes"].values())
    assert not any("source_input_omissions" in r for r in REVIEW["auxiliary_recipes"].values())
