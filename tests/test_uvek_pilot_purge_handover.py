"""The nitrate-rich purge handover retains its inspected cut-off allocation."""

from copy import deepcopy
import json

import pytest

from brightpath import BrightwayInventory, DATA_DIR
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pilot_purge_handover import apply_pilot_purge_handover_review
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

REVIEW = json.loads((DATA_DIR / "export/uvek_pilot_purge_handover_review.json").read_text())
ROW = REVIEW["records"][0]
STOCK = REVIEW["stock"]


@pytest.mark.parametrize("source", [ROW["source"], STOCK["source"]])
@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_purge_handover_preserves_signed_kg_uncertainty_and_separate_emissions(source, amount, uncertainty):
    data = consumer(source, amount, uncertainty)
    emission = {
        "name": "Carbon dioxide, fossil",
        "categories": ["air"],
        "unit": "kilogram",
        "type": "biosphere",
        "amount": 7,
    }
    data[0]["exchanges"].append(emission)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9"))
    result = inventory.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert inventory.data == data == before
    graph = result.data
    market = source == ROW["source"]
    assert len(graph) == (3 if market else 2)
    caller = graph[0]["exchanges"][1]
    for field in ("amount", "formula", "uncertainty type", "loc", "scale", "minimum", "maximum", "negative"):
        if field in before[0]["exchanges"][1]:
            assert caller[field] == before[0]["exchanges"][1][field]
    assert graph[0]["exchanges"][-1] == emission
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in graph[1:]}
    stock = helpers[STOCK["recipe_id"]]
    assert len(stock["exchanges"]) == 1
    assert stock["exchanges"][0]["type"] == "production" and stock["exchanges"][0]["amount"] == 1
    assert stock["brightpath_conversion"]["zero_burden"] is True
    if market:
        wrapper = next(a for key, a in helpers.items() if key != STOCK["recipe_id"])
        assert caller["input"][1] == wrapper["code"]
        (edge,) = [x for x in wrapper["exchanges"] if x["type"] == "technosphere"]
        assert edge["name"] == stock["name"]
        original = ROW["profiles"][0]["inputs"][0]
        assert all(edge[k] == value for k, value in original.items() if k not in ("recipe", "zero_burden_supplier"))
    else:
        assert caller["input"][1] == stock["code"]
    assert not any("Potassium nitrate" in a["name"] or "indium" in a["name"] for a in graph)
    assert any(i.code == "migration.zero_burden_supplier" for i in result.last_migration_report.issues)
    assert not any(i.code == "migration.recipe_input_omission" for i in result.last_migration_report.issues)


@pytest.mark.parametrize("source", [ROW["source"], STOCK["source"]])
@pytest.mark.parametrize("version", ["3.6", "3.12"])
def test_purge_handover_does_not_admit_other_releases(source, version):
    inventory = BrightwayInventory.from_data(consumer(source, 1), context=contexts("ecoinvent", version))
    before = deepcopy(inventory.data)
    with pytest.raises(MigrationError):
        inventory.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert inventory.data == before


def test_purge_authoring_is_idempotent_and_preserves_previous_recipes():
    resource = deepcopy(load_uvek_technosphere_resource())
    expected = deepcopy(resource)
    for row in (ROW, STOCK):
        rule = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        rule.clear()
        rule.update(deepcopy(row["previous_rule"]))
    apply_pilot_purge_handover_review(resource)
    assert resource == expected
    apply_pilot_purge_handover_review(resource)
    assert resource == expected


@pytest.mark.parametrize("change", ["stock", "parent", "stock_profile", "stock_recipe", "parent_recipe"])
def test_purge_allocation_drift_refuses_before_mutating(change):
    resource = deepcopy(load_uvek_technosphere_resource())
    rules = {r["id"]: r for r in resource["replace"]}
    if change in ("stock", "parent"):
        rules[(STOCK if change == "stock" else ROW)["rule_id"]]["source"]["location"] = "unreviewed"
    elif change == "stock_profile":
        rules[STOCK["rule_id"]]["reference_flow_sign_review"]["profiles"][0]["conversion_factor"] = 2
    elif change == "stock_recipe":
        resource["recipes"][STOCK["recipe_id"]]["inputs"] = [{"amount": 999}]
    else:
        resource["recipes"][rules[ROW["rule_id"]]["target"]["recipe_ref"]]["inputs"][0]["amount"] = 999
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="changed"):
        apply_pilot_purge_handover_review(resource)
    assert resource == before
