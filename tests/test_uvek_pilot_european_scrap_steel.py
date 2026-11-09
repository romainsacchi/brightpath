"""Mixed steel disposal keeps both fates, allocated recovery and collection."""

from copy import deepcopy
import json
import math

import pytest

from brightpath import BrightwayInventory, DATA_DIR
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pilot_european_scrap_steel import apply_pilot_european_scrap_steel_review
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

REVIEW = json.loads((DATA_DIR / "export/uvek_pilot_european_scrap_steel_review.json").read_text())
ROW = REVIEW["records"][0]


@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_steel_market_retains_both_fates_recovery_and_original_freight(amount, uncertainty):
    data = consumer(ROW["source"], amount, uncertainty)
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
    edge = result.data[0]["exchanges"][1]
    assert edge["amount"] == -amount and edge["formula"] == "(dose) * (-1.0)"
    assert edge["scale"] == 0.2
    if uncertainty == 2:
        assert edge["loc"] == before[0]["exchanges"][1]["loc"]
        assert edge["negative"] is not before[0]["exchanges"][1]["negative"]
    else:
        assert edge["loc"] == -amount and edge["minimum"] == -amount - 1 and edge["maximum"] == -amount + 1
    assert result.data[0]["exchanges"][-1] == emission
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
    assert len(helpers) == 2
    stock = helpers[ROW["retained_recovery_stock_recipe"]]
    assert len(stock["exchanges"]) == 1
    assert stock["exchanges"][0]["type"] == "production" and stock["exchanges"][0]["amount"] == 1
    parent = next(a for key, a in helpers.items() if key != ROW["retained_recovery_stock_recipe"])
    assert edge["input"][1] == parent["code"]
    inputs = [x for x in parent["exchanges"] if x["type"] == "technosphere"]
    assert len(inputs) == 6 and not any(x["type"] == "biosphere" for x in parent["exchanges"])
    fields = ("name", "reference product", "location", "unit")
    recovery = next(x for x in inputs if all(x[f] == stock[f] for f in fields))
    assert recovery["amount"] == -0.47289 * ROW["source_incineration_share"]
    assert recovery["amount"] < 0
    assert edge["amount"] * recovery["amount"] == pytest.approx(amount * 0.47289 * ROW["source_incineration_share"])
    quantities = {x["name"]: x["amount"] for x in inputs if x is not recovery}
    assert quantities == pytest.approx(
        {
            "Process-specific burdens, inert material landfill": ROW["source_landfill_share"],
            "xx Inert material landfill facility": 1.48e-9 * ROW["source_landfill_share"],
            "Disposal, steel, 0% water, to municipal incineration": ROW["source_incineration_share"],
            "Transport, freight, rail": 0.0112,
            "Transport, freight, lorry, fleet average": 0.01932,
        }
    )
    for actual, expected in zip(inputs, ROW["profiles"][0]["inputs"], strict=True):
        assert all(actual[k] == v for k, v in expected.items() if k not in ("recipe", "zero_burden_supplier"))
    operation = next(x for x in inputs if x["name"] == "Process-specific burdens, inert material landfill")
    assert operation["loc"] == math.log(ROW["source_landfill_share"])
    assert operation["scale"] == 0.044721359549995794
    assert any("source water/oxygen" in i.message for i in result.last_migration_report.issues)


@pytest.mark.parametrize("version", ["3.6", "3.12"])
def test_steel_market_does_not_approve_other_source_profiles(version):
    inventory = BrightwayInventory.from_data(consumer(ROW["source"], -1), context=contexts("ecoinvent", version))
    before = deepcopy(inventory.data)
    with pytest.raises(MigrationError):
        inventory.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert inventory.data == before


def test_steel_market_authoring_replay_is_idempotent():
    resource = deepcopy(load_uvek_technosphere_resource())
    expected = deepcopy(resource)
    parent = next(r for r in resource["replace"] if r["id"] == ROW["rule_id"])
    parent.clear()
    parent.update(deepcopy(ROW["previous_rule"]))
    apply_pilot_european_scrap_steel_review(resource)
    assert resource == expected
    apply_pilot_european_scrap_steel_review(resource)
    assert resource == expected


@pytest.mark.parametrize("change", ["stock", "freight", "parent", "recipe"])
def test_steel_market_component_or_recovery_drift_refuses_atomically(change):
    resource = deepcopy(load_uvek_technosphere_resource())
    parent = next(r for r in resource["replace"] if r["id"] == ROW["rule_id"])
    if change == "stock":
        resource["recipes"][ROW["retained_recovery_stock_recipe"]]["inputs"] = [{"amount": 999}]
    elif change == "freight":
        next(r for r in resource["replace"] if r["id"] == "bp-d6b9d771677aa51e")["confidence"] = 0.123
    elif change == "parent":
        parent["source"]["location"] = "unreviewed"
    else:
        resource["recipes"][parent["target"]["recipe_ref"]]["inputs"][0]["amount"] = 999
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="changed"):
        apply_pilot_european_scrap_steel_review(resource)
    assert resource == before
