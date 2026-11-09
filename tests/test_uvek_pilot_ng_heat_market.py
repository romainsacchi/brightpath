"""Gas heat keeps positive product references and every inspected supplier share."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pilot_ng_heat_market import apply_pilot_ng_heat_market_review

REVIEW = json.loads((DATA_DIR / "export/uvek_pilot_ng_heat_market_review.json").read_text())
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
    assert len(result.data) == 2
    actual = result.data[0]["exchanges"][1]
    original = before[0]["exchanges"][1]
    for field in ("amount", "formula", "loc", "scale", "negative", "minimum", "maximum"):
        if field in original:
            assert actual[field] == original[field]
    root = next(r for r in result.data if r.get("code") == actual["input"][1])
    inputs = [x for x in root["exchanges"] if x["type"] != "production"]
    assert len(inputs) == 43
    assert all(
        all(x[k] == v for k, v in expected.items()) for x, expected in zip(inputs, profile["inputs"], strict=True)
    )
    assert all(
        x["unit"] == "megajoule" and x["amount"] > 0 and x["name"].startswith(("Heat,", "xx Heat,", "xxx Heat,"))
        for x in inputs
    )
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
        apply_pilot_ng_heat_market_review(resource)
    else:
        with pytest.raises(ValueError):
            apply_pilot_ng_heat_market_review(resource)
    assert resource == before


def test_heat_uses_complete_output_services_without_duplicate_efficiency_or_network():
    assert len(REVIEW["correspondences"]) == 86
    assert not REVIEW["district_network_added"] and not REVIEW["extra_efficiency_or_delivery_factor"]
    assert REVIEW["closer_native_1MWe_engine_used"]
    for c in REVIEW["correspondences"]:
        assert c["source_reference_amount"] == c["target_reference_amount"] == 1
        assert not c["target"]["name"].startswith("District heat")
        if c["source"]["name"] == "heat and power co-generation, natural gas, 1MW electrical, lean burn":
            assert c["target"]["name"] == "Heat, at cogen 1MWe lean burn, allocation exergy"
    basis = next(
        w
        for w in REVIEW["heat_output_burner_basis"]
        if w["heat_output"]["name"] == "Heat, natural gas, at industrial furnace 1MW"
    )
    assert basis["allocated_input_MJ_per_output_MJ"] == 1.0526
    assert REVIEW["native_geographic_comment_conflicts"]
