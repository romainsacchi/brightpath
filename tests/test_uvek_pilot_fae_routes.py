"""Regional FAE routes markets retain emissions, treatment shares, operations and freight."""

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
from scripts.review_uvek_pilot_fae_routes import apply_pilot_fae_routes_review

REVIEW = json.loads((DATA_DIR / "export/uvek_pilot_fae_routes_review.json").read_text())


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["source"]["location"])
@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_fae_keeps_route_stages_and_signed_uncertainty(row, amount, uncertainty):
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


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["source"]["location"])
@pytest.mark.parametrize("version", ["3.7", "3.12"])
def test_fae_refuses_uninspected_source_versions(row, version):
    source = BrightwayInventory.from_data(consumer(row["source"], -1), context=contexts("ecoinvent", version))
    before = deepcopy(source.data)
    with pytest.raises(MigrationError):
        source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert source.data == before


@pytest.mark.parametrize("change", ["none", "late_parent", "new_helper", "component"])
def test_fae_authoring_replay_is_atomic_and_freezes_all_helpers(change):
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
        apply_pilot_fae_routes_review(resource)
        assert resource == before
    else:
        with pytest.raises(ValueError):
            apply_pilot_fae_routes_review(resource)
        assert resource == before


def test_fae_routes_retain_verified_stock_handover_and_bound_reference_formulas():
    assert len(REVIEW["records"]) == 5
    assert len(REVIEW["all_generated_new_recipes"]) == 17
    assert len(REVIEW["recipe_dependencies"]) == 2
    resource = load_uvek_technosphere_resource()
    for rid, expected in REVIEW["recipe_dependencies"].items():
        assert hashlib.sha256(json.dumps(resource["recipes"][rid], sort_keys=True).encode()).hexdigest() == expected
        assert resource["recipes"][rid]["zero_burden"] and resource["recipes"][rid]["inputs"] == []
    handovers = [
        x["amount"]
        for h in REVIEW["auxiliary_recipes"].values()
        for x in h["inputs"]
        if x.get("recipe") in REVIEW["recipe_dependencies"]
    ]
    assert -0.47289 in handovers and -0.31 in handovers
    for rid, helper in REVIEW["auxiliary_recipes"].items():
        for x in helper["inputs"]:
            if x.get("recipe") in REVIEW["recipe_dependencies"]:
                binding = x["zero_burden_supplier"]
                owners = {w["source_version"] for w in REVIEW["source_node_recipe_bindings"] if w["recipe_id"] == rid}
                assert owners == {binding["source_profile"]["version"]}
                rule = next(r for r in resource["replace"] if r["id"] == binding["rule_id"])
                approval = next(
                    p
                    for p in rule["reference_flow_sign_review"]["profiles"]
                    if p["source_profile"] == binding["source_profile"]
                )
                assert (
                    hashlib.sha256(json.dumps(approval, sort_keys=True).encode()).hexdigest()
                    == binding["profile_review_sha256"]
                )
    bindings = REVIEW["reserved_reference_formula_bindings"]
    assert len(bindings) == 2 and all(b["source_property_value"] == 1 for b in bindings)
    assert all("WM_RP" not in x.get("formula", "") for h in REVIEW["auxiliary_recipes"].values() for x in h["inputs"])
    assert len(REVIEW["canonical_biosphere_witnesses"]) == 34
    assert all(
        not r["fae_technology_reconstructed"] and r["source_other_route_stages_retained"] for r in REVIEW["records"]
    )


def test_fae_routes_share_the_scrap_market_between_joint_parent_demands():
    rows = [r for r in REVIEW["records"] if r["source_profile"]["version"] == "3.9" and "steel" in r["source"]["name"]]
    assert len(rows) == 2
    data = consumer(rows[0]["source"], -1)
    data[0]["exchanges"].append({**rows[1]["source"], "type": "technosphere", "amount": -2})
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        target_context("2026"), policy=MigrationPolicy.permissive()
    )
    (rid,) = REVIEW["child_root_recipe_ids"]
    assert sum(a.get("brightpath_conversion", {}).get("recipe_id") == rid for a in result.data) == 1
    assert all(x.get("formula", "").find("WM_RP") == -1 for a in result.data for x in a["exchanges"])
