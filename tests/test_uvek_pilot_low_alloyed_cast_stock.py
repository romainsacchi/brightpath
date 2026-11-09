"""Keep source furnace shares and delivery at the low-alloyed casting boundary."""

from collections import defaultdict
from copy import deepcopy
import json

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BrightwayInventory, DATA_DIR
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pilot_low_alloyed_cast_stock import apply_pilot_low_alloyed_cast_stock_review

REVIEW = json.loads((DATA_DIR / "export/uvek_pilot_low_alloyed_cast_stock_review.json").read_text())
(ROW,) = REVIEW["records"]
ROUTES = {
    "3.6": (0.4328572718177839, 0.5671427281822161),
    "3.9": (0.2578923451030648, 0.7421076548969346),
    "3.12": (0.25789234510306475, 0.7421076548969346),
}


@pytest.mark.parametrize("version", ROUTES)
@pytest.mark.parametrize("release", ("2025", "2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_cast_stock_retains_furnace_shares_delivery_and_separate_caller_rolling(version, release, amount):
    data = consumer(ROW["source"], amount, 2)
    rolling = {
        "name": "caller-owned rolling",
        "reference product": "caller-owned rolling",
        "location": "CH",
        "unit": "kilogram",
    }
    data[0]["exchanges"].append({**rolling, "type": "technosphere", "amount": 7})
    data.append({**rolling, "exchanges": [{**rolling, "type": "production", "amount": 1}]})
    before = deepcopy(data)
    source = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = source.migrate_background(target_context(release), policy=MigrationPolicy.permissive())
    assert source.data == data == before
    edge = result.data[0]["exchanges"][1]
    assert edge["amount"] == amount and edge["formula"] == "dose"
    for field in ("loc", "scale", "negative"):
        assert edge[field] == before[0]["exchanges"][1][field]
    (helper,) = [d for d in result.data if d.get("code") == edge["input"][1]]
    assert helper["reference product"] == "steel, low-alloyed"
    assert [x["amount"] for x in helper["exchanges"] if x["type"] == "production"] == [1]
    quantities = defaultdict(float)
    for x in helper["exchanges"]:
        if x["type"] == "technosphere":
            quantities[x["name"]] += x["amount"]
    electric, converter = ROUTES[version]
    assert dict(quantities) == pytest.approx(
        {
            "Steel, electric, low-alloyed, at plant": electric,
            "Steel, converter, low-alloyed, at plant": converter,
            "Transport, barge": 0.0201,
            "Transport, freight, lorry, fleet average": 0.2065,
            "Transport, freight, rail": 0.1903,
            "Transport, transoceanic freight ship": 0.4409,
        }
    )
    assert not any("rolling" in x["name"].lower() or x["type"] == "biosphere" for x in helper["exchanges"])
    caller_edge = result.data[0]["exchanges"][2]
    assert caller_edge["name"] == rolling["name"] and caller_edge["amount"] == 7
    assert any(d["name"] == rolling["name"] for d in result.data)
    assert any("without extra rolling" in i.message for i in result.last_migration_report.issues)


@pytest.mark.parametrize("version", ("3.10", "3.9.1"))
def test_uninspected_profiles_cannot_inherit_cast_stock_helpers(version):
    source = BrightwayInventory.from_data(consumer(ROW["source"], 1), context=contexts("ecoinvent", version))
    before = deepcopy(source.data)
    with pytest.raises(MigrationError):
        source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert source.data == before


@pytest.mark.parametrize("change", ("component", "parent", "helper"))
def test_changed_component_parent_or_recipe_refuses_before_mutation(change):
    resource = deepcopy(load_uvek_technosphere_resource())
    (parent,) = [r for r in resource["replace"] if r["id"] == ROW["rule_id"]]
    if change == "component":
        coordinates = tuple(ROW["profiles"][0]["bindings"][0]["source"])
        (child,) = [
            r
            for r in resource["replace"]
            if tuple(r["source"][f] for f in ("name", "reference product", "location", "unit")) == coordinates
        ]
        child["confidence"] = 0.123
    elif change == "parent":
        parent["source"]["location"] = "unreviewed"
    else:
        resource["recipes"][parent["target"]["recipe_ref"]]["inputs"][0]["amount"] = 999
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="changed"):
        apply_pilot_low_alloyed_cast_stock_review(resource)
    assert resource == before


def test_complete_reviewed_parent_replays_idempotently():
    resource = deepcopy(load_uvek_technosphere_resource())
    expected = deepcopy(resource)
    (parent,) = [r for r in resource["replace"] if r["id"] == ROW["rule_id"]]
    parent.clear()
    parent.update(deepcopy(ROW["previous_rule"]))
    apply_pilot_low_alloyed_cast_stock_review(resource)
    assert resource == expected
    apply_pilot_low_alloyed_cast_stock_review(resource)
    assert resource == expected


def test_changed_evidence_requires_review_before_resource_lookup(tmp_path):
    path = tmp_path / "brightpath/data/export/uvek_pilot_low_alloyed_cast_stock_review.json"
    path.parent.mkdir(parents=True)
    path.write_text("{}")
    with pytest.raises(ValueError, match="evidence changed"):
        apply_pilot_low_alloyed_cast_stock_review({}, tmp_path)
