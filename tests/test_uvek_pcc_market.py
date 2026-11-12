"""Complete PCC market retains complete reviewed producer inventories and freight."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pcc_market import apply_pcc_market_review

REVIEW = json.loads((DATA_DIR / "export/uvek_pcc_market_review.json").read_text())


def policy():
    resource = load_uvek_technosphere_resource()
    rules = {r["id"]: r for r in resource["replace"]}
    return MigrationPolicy(
        approved_compatibility_rules=tuple(
            compatibility_rule_fingerprint(
                resource,
                "replace",
                rules[row["rule_id"]],
                TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
                TechnosphereProfile("uvek", "2025", "cutoff"),
            )
            for row in REVIEW["records"]
        ),
        on_information_loss=PolicyAction.WARN,
    )


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda row: row["rule_id"])
@pytest.mark.parametrize("amount", [-2, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_signed_callers_and_all_helper_inputs_are_preserved(row, amount, uncertainty):
    data = consumer(row["source_witness"]["identity"], amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", "2026").background, policy=policy()
    )
    assert data == before and len(result.data) == 32
    actual = result.data[0]["exchanges"][1]
    original = before[0]["exchanges"][1]
    for key in ("amount", "uncertainty type", "loc", "scale", "negative", "minimum", "maximum", "formula"):
        if key in original:
            assert actual[key] == original[key]
    helpers = {a.get("brightpath_conversion", {}).get("recipe_id"): a for a in result.data[1:]}
    for recipe_id, recipe in REVIEW["recipes"].items():
        helper = helpers[recipe_id]
        inputs = [e for e in helper["exchanges"] if e["type"] != "production"]
        assert len(inputs) == len(recipe["inputs"])
        for actual, source in zip(inputs, recipe["inputs"], strict=True):
            for key, value in source.items():
                if key == "recipe":
                    target = helpers[value]
                    assert actual["name"] == target["name"]
                    assert actual["reference product"] == target["reference product"]
                elif key == "categories":
                    assert tuple(actual[key]) == tuple(value)
                else:
                    assert actual[key] == value
        if recipe_id != "pcc-39-precipitated-carbonate-row-market":
            assert sum(e["type"] == "biosphere" for e in inputs) == 14


@pytest.mark.parametrize("version,model", [("3.6", "cutoff"), ("3.11", "cutoff"), ("3.9", "consequential")])
def test_other_profiles_are_not_authorized(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(REVIEW["records"][0]["source_witness"]["identity"], 1),
            context=contexts("ecoinvent", version, model),
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


def test_existing_helper_dependencies_and_source_reference_facts_are_retained():
    resource = load_uvek_technosphere_resource()
    assert len(REVIEW["recipes"]) == 2 and len(REVIEW["recipe_dependencies"]) == 29
    assert REVIEW["new_source_omissions"] == []
    assert REVIEW["records"][0]["source_witness"]["reference_amounts"] == [1]
    for witness in REVIEW["records"][0]["source_producer_witnesses"]:
        assert witness["reference_amounts"] == [1] and witness["direct_biosphere_count"] == 14
    for key, expected in REVIEW["recipe_dependencies"].items():
        assert resource["recipes"][key] == expected


@pytest.mark.parametrize("drift", [None, "parent", "recipe", "dependency"])
def test_replay_is_atomic(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    if drift == "parent":
        next(r for r in resource["replace"] if r["id"] == REVIEW["records"][-1]["rule_id"])["confidence"] = 0.1
    elif drift == "recipe":
        resource["recipes"][REVIEW["records"][0]["recipe_id"]]["inputs"][0]["amount"] = 9
    elif drift == "dependency":
        resource["recipes"][next(iter(REVIEW["recipe_dependencies"]))]["inputs"][0]["amount"] = 9
    before = deepcopy(resource)
    if drift is None:
        apply_pcc_market_review(resource)
    else:
        with pytest.raises(ValueError):
            apply_pcc_market_review(resource)
    assert resource == before


def test_all_three_original_producer_shares_and_delivery_are_retained():
    row = REVIEW["records"][0]
    producers = [b for b in row["market_bindings"] if b["kind"] == "producer"]
    assert [b["statistics"]["amount"] for b in producers] == [
        0.00934612515441106,
        0.990336071944508,
        0.000317802901081526,
    ]
    assert {b["target"] for b in producers} == {
        "pcc-39-allocated-cobalt",
        "pcc-39-dedicated-precipitation-row",
        "pcc-39-allocated-lanthanum-oxide",
    }
    assert sum(b["kind"] == "freight" for b in row["market_bindings"]) == 10
    assert sum(map(len, REVIEW["inherited_source_omissions"].values())) == 46
    assert len(REVIEW["recipes"]["pcc-39-dedicated-precipitation-row"]["inputs"]) == 43
