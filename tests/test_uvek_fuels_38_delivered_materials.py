"""Market helpers retain final freight, producer entries and source statistics."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_fuels_38_delivered_materials import apply_fuels_38_delivered_materials_review

REVIEW = json.loads((DATA_DIR / "export/uvek_fuels_38_delivered_materials_review.json").read_text())
RECORDS = {r["rule_id"]: r for r in REVIEW["records"]}


@pytest.mark.parametrize("rid", RECORDS)
@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_market_keeps_caller_and_six_internal_entries(rid, amount, uncertainty):
    row = RECORDS[rid]
    source = BrightwayInventory.from_data(
        consumer(row["original_rule"]["source"], amount, uncertainty),
        context=contexts("ecoinvent", "3.8", "cutoff"),
    )
    before = deepcopy(source.data)
    result = source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    edge = result.data[0]["exchanges"][1]
    assert edge["amount"] == amount and edge["formula"] == "dose"
    assert edge["uncertainty type"] == uncertainty and edge["scale"] == 0.2
    if uncertainty == 2:
        assert edge["negative"] is (amount < 0)
    else:
        assert edge["loc"] == amount
    assert len(result.data) == 2
    helper = result.data[1]
    rows = [e for e in helper["exchanges"] if e["type"] != "production"]
    assert len(rows) == 6
    for actual, expected in zip(rows, REVIEW["recipes"][row["recipe_id"]]["inputs"], strict=True):
        assert all(actual.get(k) == v for k, v in expected.items())
    assert source.data == before


def test_previously_approved_39_zeolite_keeps_old_direct_target():
    row = RECORDS["bp-8bbb01130472bd02"]
    source = BrightwayInventory.from_data(
        consumer(row["original_rule"]["source"], -2.5, 2), context=contexts("ecoinvent", "3.9", "cutoff")
    )
    result = source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert len(result.data) == 1
    edge = result.data[0]["exchanges"][1]
    assert all(edge[k] == v for k, v in row["original_rule"]["target"].items())
    assert edge["amount"] == -2.5


@pytest.mark.parametrize("rid", RECORDS)
def test_uninspected_311_is_not_given_the_38_helper(rid):
    source = BrightwayInventory.from_data(
        consumer(RECORDS[rid]["original_rule"]["source"], 1, 2), context=contexts("ecoinvent", "3.11", "cutoff")
    )
    with pytest.raises(MigrationError):
        source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())


def test_replay_is_idempotent_and_rejects_changed_delivery():
    resource = deepcopy(load_uvek_technosphere_resource())
    before = deepcopy(resource)
    apply_fuels_38_delivered_materials_review(resource)
    assert resource == before
    recipe = resource["recipes"][REVIEW["records"][0]["recipe_id"]]
    recipe["inputs"][0]["amount"] *= 2
    with pytest.raises(ValueError, match="helper changed"):
        apply_fuels_38_delivered_materials_review(resource)
