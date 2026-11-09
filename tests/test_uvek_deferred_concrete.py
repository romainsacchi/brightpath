"""Waste markets retain original treatment shares and freight; empty terminals stay audited."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_deferred_concrete import apply_deferred_concrete_review

REVIEW = json.loads((DATA_DIR / "export/uvek_deferred_concrete_review.json").read_text())


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["source"]["location"])
@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_concrete_market_preserves_freight_fractions_and_signed_uncertainty(row, amount, uncertainty):
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
        assert actual["loc"] == original["loc"] and actual["negative"] is (not original["negative"])
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
    losses = [x for x in result.last_migration_report.losses if x.code == "migration.recipe_source_input_omission"]
    expected = {rid: values for rid, values in row["approved_recipe_source_omissions"].items() if rid in helpers}
    assert len(losses) == sum(len(values) for values in expected.values())
    assert all(x.to_dict()["details"]["omission"] in expected[x.to_dict()["details"]["recipe_id"]] for x in losses)
    processing = [r for r in helpers.values() if "treatment of waste concrete" in r["name"]]
    if row.get("auxiliary_component_qualification"):
        assert not processing
        return
    assert len(processing) == 3
    for activity in processing:
        (diesel,) = [x for x in activity["exchanges"] if x.get("name") == "Diesel, burned in building machine, average"]
        assert diesel["amount"] == 0.0437 and diesel["unit"] == "megajoule"
        pm = [x["amount"] for x in activity["exchanges"] if x["type"] == "biosphere"]
        assert sorted(pm) == sorted([1.66e-5, 8.35e-5, 6.34e-5])
    (recycling,) = [a for a in processing if "recycling" in a["name"]]
    assert len(recycling["exchanges"]) == 5
    fields = ("name", "reference product", "location", "unit")
    processing_keys = {tuple(a[k] for k in fields) for a in processing}
    fractions = [
        x["amount"]
        for x in root["exchanges"]
        if x["type"] == "technosphere" and tuple(x.get(k) for k in fields) in processing_keys
    ]
    expected_fractions = (
        [0.28, 0.62, 0.1]
        if row["source"]["location"] == "CH"
        else [0.291281424314205, 0.506227554061282, 0.202491021624513]
    )
    assert sorted(fractions) == sorted(expected_fractions)


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["source"]["location"])
@pytest.mark.parametrize("version", ["3.11", "3.12"])
def test_concrete_market_refuses_uninspected_source_versions(row, version):
    source = BrightwayInventory.from_data(consumer(row["source"], -1), context=contexts("ecoinvent", version))
    before = deepcopy(source.data)
    with pytest.raises(MigrationError):
        source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert source.data == before


@pytest.mark.parametrize("change", ["none", "late_parent", "new_helper", "component"])
def test_concrete_market_authoring_replay_is_atomic_and_freezes_all_helpers(change):
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
        apply_deferred_concrete_review(resource)
        assert resource == before
    else:
        with pytest.raises(ValueError):
            apply_deferred_concrete_review(resource)
        assert resource == before
