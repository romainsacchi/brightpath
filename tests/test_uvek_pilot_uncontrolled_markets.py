"""Mixed disposal markets preserve source fates, releases and marginal uncertainty."""

from copy import deepcopy
import json

import pytest

from brightpath import BrightwayInventory, DATA_DIR
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pilot_uncontrolled_markets import apply_pilot_uncontrolled_market_review
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

REVIEW = json.loads((DATA_DIR / "export/uvek_pilot_uncontrolled_market_review.json").read_text())


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("amount", [-2, 0, 2])
def test_mixed_market_preserves_all_fates_source_releases_and_caller(row, amount):
    data = consumer(row["source"], amount, 2)
    emission = {
        "name": "Carbon dioxide, fossil",
        "categories": ["air"],
        "unit": "kilogram",
        "type": "biosphere",
        "amount": 7,
    }
    data[0]["exchanges"].append(emission)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", row["source_profile"]["version"]))
    result = inventory.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert inventory.data == data == before
    edge = result.data[0]["exchanges"][1]
    assert edge["amount"] == -amount and edge["formula"] == "(dose) * (-1.0)"
    assert edge["loc"] == before[0]["exchanges"][1]["loc"] and edge["scale"] == 0.2
    assert edge["negative"] is not before[0]["exchanges"][1]["negative"]
    assert result.data[0]["exchanges"][-1] == emission
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
    root_id = next(k for k in helpers if k.startswith("pilot-uncontrolled-market-"))
    root = helpers[root_id]
    assert edge["input"][1] == root["code"]
    actual = [x for x in root["exchanges"] if x["type"] != "production"]
    expected = row["profiles"][0]["inputs"]
    assert len(actual) == len(expected)
    for exchange, original in zip(actual, expected, strict=True):
        if "recipe" in original:
            child = helpers[original["recipe"]]
            assert all(exchange[f] == child[f] for f in ("name", "reference product", "location", "unit"))
        assert all(
            exchange[k] == v
            for k, v in original.items()
            if k not in ("recipe", "biosphere_targets", "approved_omission_if_unavailable", "omission_rationale")
        )
    assert sum(x["amount"] for x in actual if x["type"] == "technosphere" and x["unit"] == "kilogram") == pytest.approx(
        1, abs=1e-14
    )
    if row["source"]["location"] == "DE":
        assert len(helpers) == 2
        assert sorted(x["amount"] for x in actual if x["unit"] == "kilogram") == pytest.approx(
            [6.36128446588608e-5, 0.00617249483625004, 0.993763892319091]
        )
    else:
        assert len(helpers) == 12
        assert sum("open dump" in a["name"] for a in helpers.values()) == 5
        assert sum("unsanitary landfill" in a["name"] for a in helpers.values()) == 5
    for key, helper in helpers.items():
        (production,) = [x for x in helper["exchanges"] if x["type"] == "production"]
        assert production["amount"] == 1
        if key == root_id:
            continue
        recipe = REVIEW["auxiliary_recipes"][key]
        exchanges = [x for x in helper["exchanges"] if x["type"] != "production"]
        assert len(exchanges) == len(recipe["inputs"])
        for exchange, original in zip(exchanges, recipe["inputs"], strict=True):
            assert all(exchange[k] == v for k, v in original.items() if k != "biosphere_targets")
        assert helper["brightpath_conversion"]["biosphere_binding_profile"] == {
            "family": "ecoinvent",
            "version": "3.10",
        }
        if "unsanitary landfill" in helper["name"]:
            assert {x["name"]: x["amount"] for x in exchanges if x["type"] == "technosphere"} == {
                "Diesel, burned in building machine, average": 0.046738,
                "Excavation, skid-steer loader": 0.00031708,
            }
        if "open burning" in helper["name"]:
            carbon = next(
                x
                for x in exchanges
                if x["name"]
                == (
                    "Carbon dioxide, non-fossil"
                    if row["source_profile"]["version"] == "3.12"
                    else "Carbon dioxide, fossil"
                )
            )
            assert carbon["amount"] == (1.3878 if row["source_profile"]["version"] == "3.12" else 2.2346)
    assert not any(i.code == "migration.recipe_input_omission" for i in result.last_migration_report.issues)
    assert any("CAS/label metadata changes" in i.message for i in result.last_migration_report.issues)


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
def test_mixed_market_does_not_approve_other_versions(row):
    version = "3.9" if row["source_profile"]["version"] == "3.12" else "3.12"
    inventory = BrightwayInventory.from_data(consumer(row["source"], -1), context=contexts("ecoinvent", version))
    before = deepcopy(inventory.data)
    with pytest.raises(MigrationError):
        inventory.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert inventory.data == before


def test_mixed_market_authoring_is_idempotent_and_preserves_earlier_recipes():
    resource = deepcopy(load_uvek_technosphere_resource())
    expected = deepcopy(resource)
    by_id = {r["id"]: r for r in resource["replace"]}
    for row in REVIEW["records"]:
        parent = by_id[row["rule_id"]]
        parent.clear()
        parent.update(deepcopy(row["previous_rule"]))
    apply_pilot_uncontrolled_market_review(resource)
    assert resource == expected
    apply_pilot_uncontrolled_market_review(resource)
    assert resource == expected


@pytest.mark.parametrize("change", ["source_helper", "operating_rule", "parent", "root_recipe"])
def test_mixed_market_drift_refuses_before_mutation(change):
    resource = deepcopy(load_uvek_technosphere_resource())
    parent = next(r for r in resource["replace"] if r["id"] == REVIEW["records"][-1]["rule_id"])
    if change == "source_helper":
        resource["recipes"][next(iter(REVIEW["auxiliary_recipes"]))]["inputs"][0]["amount"] = 999
    elif change == "operating_rule":
        rule = next(
            r
            for r in resource["replace"]
            if r["source"]["name"] == "market for diesel, burned in building machine"
            and r["source"]["location"] == "GLO"
        )
        rule["confidence"] = 0.123
    elif change == "parent":
        parent["source"]["location"] = "unreviewed"
    else:
        resource["recipes"][parent["target"]["recipe_ref"]]["inputs"][0]["amount"] = 999
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="changed"):
        apply_pilot_uncontrolled_market_review(resource)
    assert resource == before
