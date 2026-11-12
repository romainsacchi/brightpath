"""Ozone, beet sugar and fatty alcohol retain their original supply and delivery."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pulp_heat_route_markets import apply_pulp_heat_route_markets_review

REVIEW = json.loads((DATA_DIR / "export/uvek_pulp_heat_route_markets_review.json").read_text())


def policy(version="3.9", rows=None):
    resource = load_uvek_technosphere_resource()
    rules = {r["id"]: r for r in resource["replace"]}
    return MigrationPolicy(
        approved_compatibility_rules=tuple(
            compatibility_rule_fingerprint(
                resource,
                "replace",
                rules[row["rule_id"]],
                TechnosphereProfile("ecoinvent", version, "cutoff"),
                TechnosphereProfile("uvek", "2025", "cutoff"),
            )
            for row in (rows or REVIEW["records"])
        ),
        on_information_loss=PolicyAction.WARN,
    )


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda row: row["rule_id"])
@pytest.mark.parametrize("amount", [-2, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_signed_callers_and_all_recipe_metadata_are_preserved(row, amount, uncertainty):
    data = consumer(row["source_witness"]["identity"], amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", "2026").background, policy=policy()
    )
    assert data == before and len(result.data) == 2
    actual = result.data[0]["exchanges"][1]
    for key in ("amount", "uncertainty type", "loc", "scale", "negative", "minimum", "maximum", "formula"):
        if key in before[0]["exchanges"][1]:
            assert actual[key] == before[0]["exchanges"][1][key]
    recipe = REVIEW["recipes"][row["recipe_id"]]
    helper = result.data[1]
    assert helper["brightpath_conversion"]["recipe_id"] == row["recipe_id"]
    inputs = [e for e in helper["exchanges"] if e["type"] != "production"]
    assert len(inputs) == len(recipe["inputs"])
    for actual, expected in zip(inputs, recipe["inputs"], strict=True):
        for key, value in expected.items():
            assert (tuple(actual[key]) if key == "categories" else actual[key]) == (
                tuple(value) if key == "categories" else value
            )


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda row: row["rule_id"])
def test_consequential_and_unapproved_contexts_are_refused(row):
    for version, model in (("3.6", "cutoff"), ("3.11", "cutoff"), ("3.9", "consequential")):
        with pytest.raises(MigrationError):
            BrightwayInventory.from_data(
                consumer(row["source_witness"]["identity"], 1), context=contexts("ecoinvent", version, model)
            ).migrate_background(contexts("uvek", "2026").background, policy=policy())


def test_distinct_feedstocks_allocation_and_delivery_are_not_collapsed():
    rows = {row["source_witness"]["identity"]["reference product"]: row for row in REVIEW["records"]}
    fatty = rows["fatty alcohol"]
    producers = [x for x in fatty["component_bindings"] if x["kind"] == "producer"]
    assert len(producers) == 8 and len({x["target"]["name"] for x in producers}) == 4
    assert {x["source"]["location"] for x in producers} == {"RER", "RoW"}
    assert all("sulfate" not in x["target"]["name"].lower() for x in producers)
    sugar = rows["sugar, from sugar beet"]
    assert [x["statistics"]["amount"] for x in sugar["component_bindings"] if x["kind"] == "producer"] == [
        0.00581395348837209,
        0.994186046511628,
    ]
    assert len(rows["ozone, liquid"]["component_bindings"]) == 9
    assert sum(len(row["component_bindings"]) for row in REVIEW["records"]) == 27
    assert sum(x["kind"] == "producer" for row in REVIEW["records"] for x in row["component_bindings"]) == 11
    assert sum(x["kind"] == "freight" for row in REVIEW["records"] for x in row["component_bindings"]) == 16
    assert len(REVIEW["native_2026_witnesses"]) == 12 and REVIEW["new_source_omissions"] == []


@pytest.mark.parametrize("drift", [None, "parent", "recipe"])
def test_replay_is_atomic(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    if drift == "parent":
        next(r for r in resource["replace"] if r["id"] == REVIEW["records"][-1]["rule_id"])["confidence"] = 0.1
    elif drift == "recipe":
        resource["recipes"][REVIEW["records"][0]["recipe_id"]]["inputs"][0]["amount"] = 9
    before = deepcopy(resource)
    if drift is None:
        apply_pulp_heat_route_markets_review(resource)
    else:
        with pytest.raises(ValueError):
            apply_pulp_heat_route_markets_review(resource)
    assert resource == before
