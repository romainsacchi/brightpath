"""PCC prerequisites retain water uncertainty, delivery, country and heat shares."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pcc_last_four import apply_pcc_last_four_review

REVIEW = json.loads((DATA_DIR / "export/uvek_pcc_last_four_review.json").read_text())


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


def test_market_boundaries_and_water_uncertainty():
    steam = REVIEW["recipes"]["pcc-39-chemical-heat-retained-water-uncertainty"]["inputs"]
    assert len(steam) == 10
    assert sum(e.get("name") == "Heat, unspecific, in chemical plant" for e in steam) == 1
    assert all(e["uncertainty type"] == 2 and e["pedigree"] and "scale without pedigree" in e for e in steam[1:])
    assert [e["categories"] for e in steam if e["type"] == "biosphere"] == [["air"], ["water"]]
    grid = REVIEW["recipes"]["pcc-39-rna-mv-ca-us-market"]["inputs"]
    assert [e["location"] for e in grid] == ["CA", "US"]
    assert [e["amount"] for e in grid] == [0.11521209723031647, 0.8847879027696831]
    heat = REVIEW["recipes"]["pcc-39-quebec-gas-production-heat-market"]["inputs"]
    assert len(heat) == 4 and len({e["name"] for e in heat}) == 4
    assert all("District heat" not in e["name"] for e in heat)
    limestone = REVIEW["recipes"]["pcc-39-washed-limestone-row-market"]["inputs"]
    assert len(limestone) == 4 and sum(e["unit"] == "ton kilometer" for e in limestone) == 3
    assert REVIEW["new_source_omissions"] == []


def test_other_chemical_heat_profiles_and_helpers_remain_exact():
    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == "bp-05200779ee64a17f")
    for row in REVIEW["preserved_chemical_heat_profiles"]:
        actual = next(
            e for e in rule["reference_flow_sign_review"]["profiles"] if e["source_profile"] == row["source_profile"]
        )
        assert actual == row["profile_entry"]
    for key, expected in REVIEW["recipe_dependencies"].items():
        assert resource["recipes"][key] == expected
    row = next(r for r in REVIEW["records"] if r["rule_id"] == rule["id"])
    result = BrightwayInventory.from_data(
        consumer(row["source_witness"]["identity"], -2, 3), context=contexts("ecoinvent", "3.8")
    ).migrate_background(contexts("uvek", "2026").background, policy=policy("3.8", [row]))
    selected = next(
        e for e in rule["reference_flow_sign_review"]["profiles"] if e["source_profile"]["version"] == "3.8"
    )
    assert result.data[1]["brightpath_conversion"]["recipe_id"] == selected["target"]["recipe_ref"]
    assert result.data[0]["exchanges"][1]["amount"] == -2


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
        apply_pcc_last_four_review(resource)
    else:
        with pytest.raises(ValueError):
            apply_pcc_last_four_review(resource)
    assert resource == before
