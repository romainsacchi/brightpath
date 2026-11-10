"""Fuel dependencies retain pressure, product and market reference boundaries."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_fuels_312_dependencies import REVIEW_PATH, apply_fuels_312_dependencies_review

REVIEW = json.loads((DATA_DIR / "export/uvek_fuels_312_dependencies_review.json").read_text())
RECORDS = {r["rule_id"]: r for r in REVIEW["records"]}


def migrate(rid, amount=1, version="3.12"):
    data = consumer(RECORDS[rid]["original_rule"]["source"], amount, 3)
    before = deepcopy(data)
    source = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert source.data == data == before
    return result, before


def helper(result):
    edge = result.data[0]["exchanges"][1]
    return next(a for a in result.data if a.get("code") == edge["input"][1])


@pytest.mark.parametrize("rid", RECORDS)
@pytest.mark.parametrize("amount", [-2, 2])
def test_reviewed_profiles_keep_caller_sign_and_normal_uncertainty(rid, amount):
    result, before = migrate(rid, amount)
    edge = result.data[0]["exchanges"][1]
    factor = RECORDS[rid]["compiled_rule"]["reference_flow_sign_review"]["profiles"][-1]["conversion_factor"]
    original = before[0]["exchanges"][1]
    assert edge["amount"] == amount * factor
    assert edge["loc"] == original["loc"] * factor
    assert edge["scale"] == original["scale"] and edge["uncertainty type"] == 3
    assert any(loss.code == "migration.technosphere_information_loss" for loss in result.last_migration_report.losses)


@pytest.mark.parametrize("rid", RECORDS)
def test_unreviewed_311_profiles_are_not_inferred(rid):
    with pytest.raises(MigrationError):
        migrate(rid, version="3.11")


@pytest.mark.parametrize("rid", ["bp-7fe8308839f735bb", "bp-76fe8c58029110f6"])
def test_delivered_gas_retains_explicit_36_mj_source_basis_and_pressure(rid):
    result, _ = migrate(rid)
    ds = helper(result)
    inputs = [e for e in ds["exchanges"] if e["type"] != "production"]
    (edge,) = inputs
    pressure = "high" if rid == "bp-7fe8308839f735bb" else "low"
    assert edge["name"] == f"Natural gas, {pressure} pressure, at consumer"
    assert edge["unit"] == "megajoule" and edge["amount"] == 36
    assert ds["unit"] == "cubic meter"
    assert "burned" not in edge["name"] and "at production" not in edge["name"]


def test_legacy_36_gas_calibration_is_not_overwritten_by_312_basis():
    result, _ = migrate("bp-7fe8308839f735bb", version="3.6")
    inputs = [e for e in helper(result)["exchanges"] if e["type"] != "production"]
    assert inputs[0]["amount"] == 38.256575270163715


def test_water_retains_actual_312_order_and_ch_non_ch_shares():
    result, _ = migrate("bp-fe711136ab4271bf")
    inputs = [e for e in helper(result)["exchanges"] if e["type"] != "production"]
    assert [(e["location"], e["amount"]) for e in inputs] == [("RER", 0.9979129796140143), ("CH", 0.002087020385985745)]
    assert all(e["name"] == "Tap water, at user" and e["unit"] == "kilogram" for e in inputs)


def test_wastewater_keeps_all_43_direct_emissions_and_canonical_strontium():
    result, _ = migrate("bp-abc1d07ff55a5bbe", amount=-1)
    ds = helper(result)
    biosphere = [e for e in ds["exchanges"] if e["type"] == "biosphere"]
    assert len(biosphere) == 43
    (strontium,) = [e for e in biosphere if e["name"] == "Strontium II"]
    assert strontium["amount"] == 3.66783459696386e-5
    assert tuple(strontium["categories"]) == ("water", "surface water")
    assert len([e for e in ds["exchanges"] if e["type"] == "technosphere"]) == 3
    assert result.data[0]["exchanges"][1]["amount"] == 1


def test_ae7_retains_all_three_feedstocks_and_three_freight_quantities():
    result, _ = migrate("bp-92c5d186d8e2543e")
    inputs = [e for e in helper(result)["exchanges"] if e["type"] != "production"]
    material = [e for e in inputs if e["unit"] == "kilogram"]
    freight = [e for e in inputs if e["unit"] == "ton kilometer"]
    assert len(material) == len(freight) == 3
    assert [e["amount"] for e in material] == [0.0275229357798165, 0.055045871559633, 0.917431192660551]
    assert [e["name"] for e in material] == [
        "Ethoxylated alcohols (AE7), coconut oil, at plant",
        "Ethoxylated alcohols (AE7), palm kernel oil, at plant",
        "Ethoxylated alcohols (AE7), petrochemical, at plant",
    ]
    assert [e["amount"] for e in freight] == [0.0219, 0.1713, 0.0438]


def test_zinc_is_complete_metal_and_hydrogen_keeps_a_market_interface():
    result, _ = migrate("bp-9ff26f9cc56aec97", -2)
    edge = result.data[0]["exchanges"][1]
    assert edge["name"] == "Zinc, primary, at regional storage" and edge["amount"] == -2
    assert "concentrate" not in edge["name"].casefold()
    result, _ = migrate("bp-cf83bb724223e5b7")
    ds = helper(result)
    assert ds["reference product"] == "hydrogen, gaseous, low pressure" and ds["unit"] == "kilogram"
    (edge,) = [e for e in ds["exchanges"] if e["type"] != "production"]
    assert edge["name"] == "Hydrogen production, steam methane reforming of natural gas, 25 bar"
    assert edge["amount"] == 1 and edge["unit"] == "kilogram"


def minimal_resource():
    full = load_uvek_technosphere_resource()
    ids = {r["rule_id"] for r in REVIEW["component_bindings"]}
    return {
        "replace": [deepcopy(r["original_rule"]) for r in REVIEW["records"]]
        + [deepcopy(r) for r in full["replace"] if r["id"] in ids],
        "delete": [],
        "recipes": {k: deepcopy(full["recipes"][k]) for k in REVIEW["recipe_dependencies"]},
        "reviews": {
            r["original_rule"]["review_ref"]: deepcopy(r["original_review"])
            for r in REVIEW["records"]
            if "review_ref" in r["original_rule"]
        },
        "coverage": {},
    }


def test_replay_is_idempotent_and_preserves_prior_selected_targets_and_signs():
    resource = minimal_resource()
    before = deepcopy(resource)
    apply_fuels_312_dependencies_review(resource)
    after = deepcopy(resource)
    apply_fuels_312_dependencies_review(resource)
    assert resource == after
    for record in REVIEW["records"]:
        for entry in record["previous_profiles_preserved"]:
            profile = TechnosphereProfile(**entry["source_profile"])
            old = next(r for r in select_profile_sign_rules(before, profile)["replace"] if r["id"] == record["rule_id"])
            new = next(
                r for r in select_profile_sign_rules(resource, profile)["replace"] if r["id"] == record["rule_id"]
            )
            assert new["target"] == old["target"]
            assert new["conversion_factor"] == old["conversion_factor"]
            assert new["reference_flow_sign_review"] == old["reference_flow_sign_review"]


@pytest.mark.parametrize("change", ["parent", "helper", "component", "shared_review", "evidence"])
def test_changed_decisions_are_refused_before_mutation(tmp_path, change):
    resource = minimal_resource()
    if change == "parent":
        resource["replace"][0]["confidence"] = -1
    elif change == "helper":
        rid, recipe = next(iter(REVIEW["recipes"].items()))
        resource["recipes"][rid] = deepcopy(recipe)
        resource["recipes"][rid]["inputs"][0]["amount"] *= 2
    elif change == "shared_review":
        next(iter(resource["reviews"].values()))["rationale"] += " changed"
    elif change == "component":
        next(iter(resource["recipes"].values()))["name"] += " changed"
    else:
        path = tmp_path / REVIEW_PATH
        path.parent.mkdir(parents=True)
        path.write_text("{}")
    before = deepcopy(resource)
    with pytest.raises(ValueError):
        apply_fuels_312_dependencies_review(resource, **({"root": tmp_path} if change == "evidence" else {}))
    assert resource == before
