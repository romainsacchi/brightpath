"""The exact four HDPE omissions retain virgin shares, delivery and caller data."""

import hashlib
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
from scripts.review_uvek_packaging_hdpe import digest
from scripts.review_uvek_pilot_nitrate_hdpe import apply_pilot_nitrate_hdpe_review

REVIEW = json.loads((DATA_DIR / "export/uvek_pilot_nitrate_hdpe_review.json").read_text())


def hdpe_before_packaging_extension_resource():
    """Restore the older authoring stage only after proving preserved current behavior."""
    resource = deepcopy(load_uvek_technosphere_resource())
    newer = json.loads((DATA_DIR / "export/uvek_packaging_hdpe_review.json").read_text())
    rule = next(r for r in resource["replace"] if r["id"] == newer["rule_id"])
    assert rule == newer["compiled_rule"]
    for proof in newer["preserved_profiles"]:
        selected = next(
            r
            for r in select_profile_sign_rules(resource, TechnosphereProfile(**proof["source_profile"]))["replace"]
            if r["id"] == newer["rule_id"]
        )
        assert digest(selected) == proof["selected_rule_sha256"]
        assert digest(resource["reviews"][selected["review_ref"]]) == proof["shared_review_sha256"]
    assert all(digest(resource["recipes"][rid]) == expected for rid, expected in newer["recipe_dependencies"].items())
    rule.clear()
    rule.update(deepcopy(newer["original_rule"]))
    return resource


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["source"]["location"])
@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_hdpe_keeps_virgin_delivery_and_signed_uncertainty(row, amount, uncertainty):
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
    losses = [x for x in result.last_migration_report.losses if x.code == "migration.recipe_source_input_omission"]
    assert len(losses) == 4
    expected = row["profiles"][0]["source_input_omissions"]
    assert [x.to_dict()["details"]["omission"] for x in losses] == expected
    assert root["brightpath_conversion"]["source_input_omissions"] == expected


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["source"]["location"])
@pytest.mark.parametrize("version", ["3.7", "3.12"])  # 3.11 has a separately tested new exact approval.
def test_hdpe_refuses_uninspected_source_versions(row, version):
    source = BrightwayInventory.from_data(consumer(row["source"], -1), context=contexts("ecoinvent", version))
    before = deepcopy(source.data)
    with pytest.raises(MigrationError):
        source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert source.data == before


@pytest.mark.parametrize("change", ["none", "late_parent", "new_helper", "component"])
def test_hdpe_authoring_replay_is_atomic_and_freezes_all_helpers(change):
    resource = hdpe_before_packaging_extension_resource()
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
        apply_pilot_nitrate_hdpe_review(resource)
        assert resource == before
    else:
        with pytest.raises(ValueError):
            apply_pilot_nitrate_hdpe_review(resource)
        assert resource == before


def test_hdpe_has_only_the_four_approved_omissions_and_no_normalization():
    (row,) = REVIEW["records"]
    assert not row["renormalized"]
    assert len(row["profiles"][0]["inputs"]) == 5
    assert len(row["freight_witnesses"]) == 3
    entries = row["profiles"][0]["source_input_omissions"]
    assert len(entries) == 4 and all(x["source_exchange"]["amount"] > 0 for x in entries)
    assert len(row["approved_recipe_source_omissions"]) == 1
    assert next(iter(row["approved_recipe_source_omissions"].values())) == entries
    for witness in REVIEW["authorization_evidence"]:
        assert hashlib.sha256((DATA_DIR / witness["path"]).read_bytes()).hexdigest() == witness["sha256"]
