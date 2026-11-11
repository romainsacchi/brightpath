"""Recovered compost keeps its inspected cut-off allocation and market freight."""

import json
from copy import deepcopy
from dataclasses import replace

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from brightpath.migrations.schema import validate_migration_resource
from scripts.review_uvek_fuels_38_compost import apply_fuels_38_compost_review

REVIEW = json.loads((DATA_DIR / "export/uvek_fuels_38_compost_review.json").read_text())
ROWS = {r["rule_id"]: r for r in REVIEW["records"]}
MARKET = REVIEW["records"][0]


def migrate(rid, amount=1, uncertainty=3, version="3.8", model="cutoff"):
    data = consumer(ROWS[rid]["original_rule"]["source"], amount, uncertainty)
    properties = {"synthetic moisture witness": {"amount": 0.61, "unit": "kg/kg"}}
    data[0]["exchanges"][1]["properties"] = properties
    source = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
    before = deepcopy(source.data)
    policy = replace(
        MigrationPolicy.permissive(),
        on_invalid_source=PolicyAction.ERROR,
        on_invalid_target=PolicyAction.ERROR,
        on_unresolved_link=PolicyAction.ERROR,
        minimum_coverage=1,
    )
    result = source.migrate_background(target_context("2026"), policy=policy)
    assert source.data == before
    edge = result.data[0]["exchanges"][1]
    assert edge["properties"] == properties
    helper = next(a for a in result.data if a.get("code") == edge["input"][1])
    return result, edge, helper


@pytest.mark.parametrize("rid", ROWS)
@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_positive_compost_reference_does_not_flip_signed_caller_mass(rid, amount, uncertainty):
    _, edge, _ = migrate(rid, amount, uncertainty)
    assert edge["amount"] == amount and edge["formula"] == "dose"
    assert edge["uncertainty type"] == uncertainty and edge["scale"] == 0.2
    if uncertainty == 2:
        assert edge["negative"] is (amount < 0)
    else:
        assert edge["loc"] == amount


def test_market_retains_six_separate_stock_shares_and_both_freight_rows():
    result, _, market = migrate(MARKET["rule_id"])
    rows = [e for e in market["exchanges"] if e["type"] != "production"]
    assert len(rows) == 8
    for actual, retained in zip(rows, MARKET["retained_rows"], strict=True):
        for field, value in retained["statistics"].items():
            assert actual[field] == value
    stocks = [e for e in rows if e["unit"] == "kilogram"]
    assert len(stocks) == 6
    assert sum(e["amount"] for e in stocks) == 0.9999999999999998
    assert [e["amount"] for e in rows if e["unit"] == "ton kilometer"] == [0.0112, 0.01932]
    stock = next(a for a in result.data if a.get("brightpath_conversion", {}).get("zero_burden"))
    assert len(stock["exchanges"]) == 1
    assert stock["exchanges"][0]["type"] == "production"
    assert stock["exchanges"][0]["amount"] == 1
    assert any(i.code == "migration.zero_burden_supplier" for i in result.last_migration_report.issues)
    assert not any(a["name"].startswith("xx Compost") for a in result.data)


@pytest.mark.parametrize("rid", [r["rule_id"] for r in REVIEW["records"][1:]])
def test_only_verified_positive_coproduct_stock_is_empty(rid):
    _, _, stock = migrate(rid)
    row = ROWS[rid]
    pair = row["cutoff_pair_witness"]
    assert stock["brightpath_conversion"]["zero_burden"] is True
    assert len(stock["exchanges"]) == 1
    assert pair["waste_reference_amounts"] == [-1] and pair["waste_nonzero_exchange_count"] > 0
    assert row["original_rule"]["source"]["reference product"] == "compost"


@pytest.mark.parametrize("rid", ROWS)
def test_uninspected_version_cannot_inherit_allocation(rid):
    with pytest.raises(MigrationError):
        migrate(rid, version="3.9")


def test_wrong_stock_profile_binding_is_rejected():
    resource = deepcopy(load_uvek_technosphere_resource())
    recipe = resource["recipes"][MARKET["compiled_rule"]["target"]["recipe_ref"]]
    next(e for e in recipe["inputs"] if "recipe" in e)["zero_burden_supplier"]["source_profile"]["version"] = "3.9"
    with pytest.raises(MigrationError, match="Zero-burden"):
        validate_migration_resource(resource)


def test_authoring_replay_is_idempotent_and_rejects_parent_drift():
    resource = deepcopy(load_uvek_technosphere_resource())
    apply_fuels_38_compost_review(resource)
    before = deepcopy(resource)
    apply_fuels_38_compost_review(resource)
    assert resource == before
    next(r for r in resource["replace"] if r["id"] == MARKET["rule_id"])["conversion_factor"] = -1
    changed = deepcopy(resource)
    with pytest.raises(ValueError, match="Compost parent"):
        apply_fuels_38_compost_review(resource)
    assert resource == changed
