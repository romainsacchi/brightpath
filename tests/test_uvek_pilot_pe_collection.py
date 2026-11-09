"""PE/PP collection retains country fates, source releases and nested uncertainty."""

from collections import Counter
from copy import deepcopy
import json

import pytest

from brightpath import BrightwayInventory, DATA_DIR
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pilot_pe_collection import apply_pilot_pe_collection_review
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

REVIEW = json.loads((DATA_DIR / "export/uvek_pilot_pe_collection_review.json").read_text())
ROW = REVIEW["records"][0]
GROUP = ROW["source_group_recipe"]


@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_collection_retains_nested_uncertainty_all_fates_and_caller(amount, uncertainty):
    data = consumer(ROW["source"], amount, uncertainty)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9"))
    result = inventory.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert inventory.data == data == before
    graph = result.data
    assert len(graph) == 12
    caller = graph[0]["exchanges"][1]
    assert caller["amount"] == -amount and caller["formula"] == "(dose) * (-1.0)"
    assert caller["scale"] == 0.2
    if uncertainty == 2:
        assert caller["negative"] is not before[0]["exchanges"][1]["negative"]
        assert caller["loc"] == before[0]["exchanges"][1]["loc"]
    else:
        assert caller["loc"] == -amount
        assert caller["minimum"] == -before[0]["exchanges"][1]["maximum"]
        assert caller["maximum"] == -before[0]["exchanges"][1]["minimum"]
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in graph[1:]}
    root = next(a for k, a in helpers.items() if k.startswith("pilot-pe-collection-"))
    assert caller["input"][1] == root["code"]
    (edge,) = [x for x in root["exchanges"] if x["type"] != "production"]
    assert edge["amount"] == 1 and edge["uncertainty type"] == 2
    assert edge["loc"] == 0 and edge["scale"] == ROW["source_group_exchange"]["scale"]
    assert edge["pedigree"] == ROW["source_group_exchange"]["pedigree"]
    assert edge["negative"] is False
    assert edge["name"] == helpers[GROUP]["name"]
    for recipe_id, recipe in REVIEW["auxiliary_recipes"].items():
        helper = helpers[recipe_id]
        (production,) = [x for x in helper["exchanges"] if x["type"] == "production"]
        assert production["amount"] == 1
        inputs = [x for x in helper["exchanges"] if x["type"] != "production"]
        assert len(inputs) == len(recipe["inputs"])
        for actual, original in zip(inputs, recipe["inputs"], strict=True):
            assert all(actual[k] == v for k, v in original.items() if k not in ("recipe", "biosphere_targets"))
            if "recipe" in original:
                assert actual["name"] == helpers[original["recipe"]]["name"]
    group_inputs = [x for x in helpers[GROUP]["exchanges"] if x["type"] == "technosphere"]
    assert len(group_inputs) == 160
    assert sum(x["amount"] for x in group_inputs if x["unit"] == "kilogram") == pytest.approx(1, abs=1e-14)
    # Keep all 35 independently scaled collection entries; the previous building
    # disposal wrapper's extra 0.01 tkm must not become a second freight burden.
    assert Counter(x["name"] for x in group_inputs)["Transport, freight, lorry, fleet average"] == 35
    assert sum(x["amount"] for x in group_inputs if x["unit"] == "ton kilometer") == pytest.approx(0.07413398899739591)
    fate_totals = Counter()
    for exchange in group_inputs:
        if exchange["unit"] != "kilogram":
            continue
        name = exchange["name"]
        fate = next(
            label
            for label in ("unsanitary landfill", "sanitary landfill", "open dump", "open burning", "incineration")
            if label in name
        )
        fate_totals[fate] += exchange["amount"]
    assert fate_totals == pytest.approx(
        {
            "incineration": 0.5356444381313915,
            "sanitary landfill": 0.2788696758791017,
            "open dump": 0.04964346839649059,
            "unsanitary landfill": 0.12117748070319079,
            "open burning": 0.014664936889825446,
        }
    )
    assert sum(x["type"] == "biosphere" for a in helpers.values() for x in a["exchanges"]) == 943
    assert not any(i.code == "migration.recipe_input_omission" for i in result.last_migration_report.issues)
    assert any("original PE proxy" in i.message for i in result.last_migration_report.issues)


@pytest.mark.parametrize("version", ["3.6", "3.12"])
def test_collection_does_not_approve_uninspected_versions(version):
    inventory = BrightwayInventory.from_data(consumer(ROW["source"], -1), context=contexts("ecoinvent", version))
    before = deepcopy(inventory.data)
    with pytest.raises(MigrationError):
        inventory.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert inventory.data == before


def test_collection_authoring_replays_without_changing_previous_recipes():
    resource = deepcopy(load_uvek_technosphere_resource())
    expected = deepcopy(resource)
    rule = next(r for r in resource["replace"] if r["id"] == ROW["rule_id"])
    rule.clear()
    rule.update(deepcopy(ROW["previous_rule"]))
    apply_pilot_pe_collection_review(resource)
    assert resource == expected
    apply_pilot_pe_collection_review(resource)
    assert resource == expected


@pytest.mark.parametrize("change", ["country", "source_helper", "operating_rule", "parent", "root_recipe"])
def test_collection_drift_refuses_before_mutating(change):
    resource = deepcopy(load_uvek_technosphere_resource())
    parent = next(r for r in resource["replace"] if r["id"] == ROW["rule_id"])
    if change == "country":
        resource["recipes"][GROUP]["inputs"][0]["amount"] = 999
    elif change == "source_helper":
        resource["recipes"][next(k for k in REVIEW["auxiliary_recipes"] if k != GROUP)]["inputs"][0]["amount"] = 999
    elif change == "operating_rule":
        binding = ROW["profiles"][0]["bindings"][0]
        rule = next(
            r
            for r in resource["replace"]
            if [r["source"][f] for f in ("name", "reference product", "location", "unit")] == binding["source"]
        )
        rule["confidence"] = 0.123
    elif change == "parent":
        parent["source"]["location"] = "unreviewed"
    else:
        resource["recipes"][parent["target"]["recipe_ref"]]["inputs"][0]["amount"] = 999
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="changed"):
        apply_pilot_pe_collection_review(resource)
    assert resource == before
