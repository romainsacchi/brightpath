"""Industrial KCl retains its source grade and signed PVF treatment demand."""

from collections import Counter
from copy import deepcopy
import json

import pytest

from brightpath import BrightwayInventory, DATA_DIR
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pilot_kcl_site import apply_pilot_kcl_site_review
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

REVIEW = json.loads((DATA_DIR / "export/uvek_pilot_kcl_site_review.json").read_text())
ROW = REVIEW["records"][0]
FIELDS = ("name", "reference product", "location", "unit")


@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_kcl_keeps_industrial_kg_signed_uncertainty_and_full_pvf_route(amount, uncertainty):
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
    graph = result.data
    assert len(graph) == 4
    caller = graph[0]["exchanges"][1]
    for field in ("amount", "formula", "uncertainty type", "loc", "scale", "minimum", "maximum", "negative"):
        if field in before[0]["exchanges"][1]:
            assert caller[field] == before[0]["exchanges"][1][field]
    assert graph[0]["exchanges"][-1] == emission
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in graph[1:]}
    root = next(a for rid, a in helpers.items() if rid.startswith("pilot-kcl-site-"))
    assert caller["input"][1] == root["code"]
    assert root["reference product"] == "potassium chloride, industrial grade" and root["unit"] == "kilogram"
    root_inputs = [x for x in root["exchanges"] if x["type"] != "production"]
    assert len(root_inputs) == 9 and sum(x["type"] == "biosphere" for x in root_inputs) == 5
    for actual, original in zip(root_inputs, ROW["profiles"][0]["inputs"], strict=True):
        assert all(actual[k] == v for k, v in original.items() if k not in ("recipe", "biosphere_targets"))
    assert next(x for x in root_inputs if x["unit"] == "megajoule")["amount"] == 0.01
    electricity = next(x for x in root_inputs if x["unit"] == "kilowatt hour")
    assert electricity["name"] == "Electricity, medium voltage, at grid" and electricity["location"] == "CL"
    assert electricity["amount"] == 0.08
    assert len([x for x in root_inputs if x["type"] == "technosphere" and x["unit"] == "kilogram"]) == 2
    assert all(x["amount"] == 0.018 for x in root_inputs if x["type"] == "technosphere" and x["unit"] == "kilogram")
    for rid, recipe in REVIEW["auxiliary_recipes"].items():
        helper = helpers[rid]
        inputs = [x for x in helper["exchanges"] if x["type"] != "production"]
        assert len(inputs) == len(recipe["inputs"])
        assert all(
            all(actual[k] == v for k, v in original.items())
            for actual, original in zip(inputs, recipe["inputs"], strict=True)
        )
        if "pvf-film" in rid:
            assert [x["amount"] for x in inputs if x["unit"] == "kilogram"] == [0.25, 0.75]
            assert sum(x["unit"] == "ton kilometer" for x in inputs) == 4
        else:
            (treatment,) = [x for x in inputs if x["unit"] == "kilogram"]
            assert treatment["name"] == "Disposal, polyvinylfluoride, 0.2% water, to municipal incineration"
            assert treatment["amount"] == 1
            assert sum(x["unit"] == "ton kilometer" for x in inputs) == 11
        assert all(x["amount"] >= 0 for x in inputs)
    freights = [
        x
        for a in helpers.values()
        for x in a["exchanges"]
        if x["type"] == "technosphere" and x["unit"] == "ton kilometer"
    ]
    names = Counter(x["name"] for x in freights)
    assert len(freights) == 15
    assert names["Transport, freight, lorry, 3.5t gross weight, fleet average"] == 1
    assert names["Transport, freight, rail"] == 7
    assert names["Transport, transoceanic container ship"] == 2
    assert not any("coal freight" in x["name"] for x in freights)
    assert not any(
        "K2O" in x["name"] or "polyvinylchloride" in x["name"].lower() for a in graph for x in a["exchanges"]
    )
    assert not any(i.code == "migration.recipe_input_omission" for i in result.last_migration_report.issues)
    assert any("minimum95%KCl" in i.message for i in result.last_migration_report.issues)


@pytest.mark.parametrize("version", ["3.6", "3.12"])
def test_kcl_does_not_admit_uninspected_releases(version):
    inventory = BrightwayInventory.from_data(consumer(ROW["source"], 1), context=contexts("ecoinvent", version))
    before = deepcopy(inventory.data)
    with pytest.raises(MigrationError):
        inventory.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert inventory.data == before


def test_kcl_authoring_is_atomic_and_idempotent():
    resource = deepcopy(load_uvek_technosphere_resource())
    expected = deepcopy(resource)
    rule = next(r for r in resource["replace"] if r["id"] == ROW["rule_id"])
    rule.clear()
    rule.update(deepcopy(ROW["previous_rule"]))
    apply_pilot_kcl_site_review(resource)
    assert resource == expected
    apply_pilot_kcl_site_review(resource)
    assert resource == expected


@pytest.mark.parametrize("change", ["film", "waste", "grid", "parent", "site"])
def test_kcl_drift_refuses_before_mutation(change):
    resource = deepcopy(load_uvek_technosphere_resource())
    parent = next(r for r in resource["replace"] if r["id"] == ROW["rule_id"])
    if change in ("film", "waste"):
        rid = next(k for k in REVIEW["auxiliary_recipes"] if "pvf-" + change in k)
        resource["recipes"][rid]["inputs"][0]["amount"] = 999
    elif change == "grid":
        rule = next(r for r in resource["replace"] if r["id"] == "bp-fc87385b1c0a40f4")
        rule["target"]["name"] = "Electricity, medium voltage, production CL, at grid"
    elif change == "parent":
        parent["source"]["unit"] = "kilogram K2O"
    else:
        resource["recipes"][parent["target"]["recipe_ref"]]["inputs"][0]["amount"] = 999
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="changed"):
        apply_pilot_kcl_site_review(resource)
    assert resource == before
