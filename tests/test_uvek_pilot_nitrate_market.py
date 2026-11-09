"""Nitrate assembly preserves source stages, allocated burdens and inherited losses."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pilot_nitrate_market import apply_pilot_nitrate_market_review

REVIEW = json.loads((DATA_DIR / "export/uvek_pilot_nitrate_market_review.json").read_text())
ROW = REVIEW["records"][0]


@pytest.mark.parametrize("profile", ROW["profiles"], ids=lambda p: p["label"])
@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_nitrate_preserves_signed_caller_demand_and_all_supplier_entries(profile, amount, uncertainty):
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
    assert len(inputs) == len(profile["inputs"])
    assert all(
        all(x[k] == v for k, v in expected.items() if k != "recipe")
        for x, expected in zip(inputs, profile["inputs"], strict=True)
    )
    helpers = {a.get("brightpath_conversion", {}).get("recipe_id"): a for a in result.data[1:]}
    assert set(REVIEW["auxiliary_recipes"]) <= set(helpers)
    for rid, recipe in REVIEW["auxiliary_recipes"].items():
        actual_inputs = [x for x in helpers[rid]["exchanges"] if x["type"] != "production"]
        assert len(actual_inputs) == len(recipe["inputs"])
        for x, expected in zip(actual_inputs, recipe["inputs"], strict=True):
            assert all(
                x[k] == expected[k]
                for k in ("amount", "uncertainty type", "loc", "scale", "negative", "minimum", "maximum")
                if k in expected
            )
            if "recipe" not in expected:
                binding = expected.get("biosphere_targets", [{}])[0].get("target", expected)
                assert x["name"] == binding["name"] and x["unit"] == binding["unit"]
    losses = [
        x.to_dict()["details"]["omission"]
        for x in result.last_migration_report.losses
        if x.code == "migration.recipe_source_input_omission"
    ]
    expected_losses = [x for xs in ROW["approved_recipe_source_omissions"].values() for x in xs]
    assert sorted(losses, key=lambda x: json.dumps(x, sort_keys=True)) == sorted(
        expected_losses, key=lambda x: json.dumps(x, sort_keys=True)
    )
    for rid, omissions in ROW["approved_recipe_source_omissions"].items():
        assert helpers[rid]["brightpath_conversion"]["source_input_omissions"] == omissions


@pytest.mark.parametrize("version", ["3.6", "3.7", "3.8", "3.10", "3.11"])
def test_nitrate_does_not_admit_uninspected_source_releases(version):
    data = consumer(ROW["source"], 1)
    source = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    with pytest.raises(MigrationError):
        source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert source.data == data


@pytest.mark.parametrize("change", ["none", "parent", "helper", "component", "leaf_review", "dependency"])
def test_nitrate_authoring_is_idempotent_and_refuses_drift_before_mutation(change):
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
    elif change == "leaf_review":
        b = REVIEW["external_scoped_bindings"][0]
        leaf = next(r for r in resource["replace"] if r["id"] == b["rule_id"])
        resource["reviews"][leaf["review_ref"]]["rationale"] += " changed"
    elif change == "dependency":
        rid = next(rid for rid in REVIEW["recipe_dependencies"] if resource["recipes"][rid]["inputs"])
        resource["recipes"][rid]["inputs"][0]["amount"] += 1
    before = deepcopy(resource)
    if change == "none":
        apply_pilot_nitrate_market_review(resource)
    else:
        with pytest.raises(ValueError):
            apply_pilot_nitrate_market_review(resource)
    assert resource == before


def test_nitrate_stage_graph_and_authorities_are_scoped_and_shared():
    assert len(REVIEW["auxiliary_recipes"]) == 14
    assert len(REVIEW["recipe_dependencies"]) == 384
    assert len(REVIEW["external_scoped_bindings"]) == 125
    assert sum(len(r["inputs"]) for r in REVIEW["auxiliary_recipes"].values()) == 280
    source_ids = set(REVIEW["auxiliary_recipes"])
    assert sum(x.get("recipe") in source_ids for r in REVIEW["auxiliary_recipes"].values() for x in r["inputs"]) == 19
    assert len(ROW["approved_recipe_source_omissions"]) == 4
    assert not REVIEW["new_source_omissions_authorized"]
    for group in REVIEW["inherited_proxy_limitations"]:
        assert group["limitations"] in ROW["limitations"] and group["source_scopes"]
    for w in REVIEW["canonical_biosphere_witnesses"]:
        assert w["source_role"] == w["target_role"]
        assert w["source_definition_sha256"] and w["target_definition_sha256"]
