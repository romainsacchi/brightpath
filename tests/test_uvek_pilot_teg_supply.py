"""TEG supply preserves recovered allocation, delivery modes and caller data."""

from copy import deepcopy
import json

import pytest

from brightpath import BrightwayInventory, DATA_DIR
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pilot_teg_supply import apply_pilot_teg_supply_review
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

REVIEW = json.loads((DATA_DIR / "export/uvek_pilot_teg_supply_review.json").read_text())
ROW = REVIEW["records"][0]
STOCK = REVIEW["stock"]


@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_teg_market_preserves_material_shares_delivery_and_signed_uncertainty(amount, uncertainty):
    data = consumer(ROW["source"], amount, uncertainty)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.6"))
    result = inventory.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert inventory.data == data == before
    edge = result.data[0]["exchanges"][1]
    for field in ("amount", "formula", "uncertainty type", "loc", "scale", "minimum", "maximum", "negative"):
        if field in before[0]["exchanges"][1]:
            assert edge[field] == before[0]["exchanges"][1][field]
    assert len(result.data) == 3
    stock = next(a for a in result.data if a.get("brightpath_conversion", {}).get("recipe_id") == STOCK["recipe_id"])
    assert len(stock["exchanges"]) == 1
    assert stock["exchanges"][0]["type"] == "production" and stock["exchanges"][0]["amount"] == 1
    helper = next(a for a in result.data[1:] if a["code"] == edge["input"][1])
    assert edge["input"][1] == helper["code"]
    inputs = [x for x in helper["exchanges"] if x["type"] == "technosphere"]
    assert len(inputs) == 5 and not any(x["type"] == "biosphere" for x in helper["exchanges"])
    quantities = {x["name"]: x["amount"] for x in inputs}
    assert quantities == pytest.approx(
        {
            "Triethylene glycol, at plant": 0.647131654973173,
            stock["name"]: 0.352868345026827,
            "Transport, freight, rail": 0.0438,
            "Transport, barge": 0.0219,
            "Transport, freight, lorry, fleet average": 0.1713,
        }
    )
    assert not any("aircraft" in x["name"].lower() or "recycling, at plant" in x["name"] for x in inputs)
    for actual, expected in zip(inputs, ROW["profiles"][0]["inputs"], strict=True):
        assert all(actual[k] == v for k, v in expected.items() if k not in ("recipe", "zero_burden_supplier"))
    assert any(
        "recovered-product" in issue.message or "recovered product" in issue.message
        for issue in result.last_migration_report.issues
    )


@pytest.mark.parametrize("source", [ROW["source"], STOCK["source"]])
@pytest.mark.parametrize("version", ["3.9", "3.12"])
def test_teg_correspondences_do_not_inherit_unreviewed_releases(source, version):
    inventory = BrightwayInventory.from_data(consumer(source, 1), context=contexts("ecoinvent", version))
    before = deepcopy(inventory.data)
    with pytest.raises(MigrationError):
        inventory.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert inventory.data == before


def test_teg_authoring_replay_is_idempotent():
    resource = deepcopy(load_uvek_technosphere_resource())
    expected = deepcopy(resource)
    for row in (ROW, STOCK):
        rule = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        rule.clear()
        rule.update(deepcopy(row["previous_rule"]))
    apply_pilot_teg_supply_review(resource)
    assert resource == expected
    apply_pilot_teg_supply_review(resource)
    assert resource == expected


def test_recovered_teg_preserves_the_positive_product_reference_without_recycling_burdens():
    data = consumer(STOCK["source"], 2)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.6")).migrate_background(
        target_context("2026"), policy=MigrationPolicy.permissive()
    )
    assert data == before and len(result.data) == 2
    assert result.data[0]["exchanges"][1]["amount"] == 2
    helper = result.data[1]
    assert helper["brightpath_conversion"]["recipe_id"] == STOCK["recipe_id"]
    assert len(helper["exchanges"]) == 1 and helper["exchanges"][0]["amount"] == 1


@pytest.mark.parametrize("change", ["stock", "freight", "parent", "stock_recipe", "parent_recipe"])
def test_teg_component_or_allocation_drift_refuses_before_mutating(change):
    resource = deepcopy(load_uvek_technosphere_resource())
    rules = {r["id"]: r for r in resource["replace"]}
    if change in ("stock", "parent"):
        rules[(STOCK if change == "stock" else ROW)["rule_id"]]["source"]["location"] = "unreviewed"
    elif change == "freight":
        rules["bp-8e691fdb86f7b1c6"]["confidence"] = 0.123
    elif change == "stock_recipe":
        resource["recipes"][STOCK["recipe_id"]]["inputs"] = [{"amount": 999}]
    else:
        resource["recipes"][rules[ROW["rule_id"]]["target"]["recipe_ref"]]["inputs"][0]["amount"] = 999
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="changed"):
        apply_pilot_teg_supply_review(resource)
    assert resource == before
