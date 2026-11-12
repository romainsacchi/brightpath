"""Allyl-chloride markets use the correct molecule and retain delivery."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pulp_heat_allyl_markets import apply_pulp_heat_allyl_markets_review

REVIEW = json.loads((DATA_DIR / "export/uvek_pulp_heat_allyl_markets_review.json").read_text())


def policy(rows=None):
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
            for row in (REVIEW["records"] if rows is None else rows)
        ),
        on_information_loss=PolicyAction.WARN,
    )


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda row: row["rule_id"])
@pytest.mark.parametrize("amount", [-2, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_signed_callers_and_original_market_rows_are_preserved(row, amount, uncertainty, release):
    data = consumer(row["source_witness"]["identity"], amount, uncertainty)
    original = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == original and len(result.data) == 2
    actual = result.data[0]["exchanges"][1]
    for key in ("amount", "uncertainty type", "loc", "scale", "negative", "minimum", "maximum", "formula"):
        if key in original[0]["exchanges"][1]:
            assert actual[key] == original[0]["exchanges"][1][key]
    helper = result.data[1]
    assert helper["brightpath_conversion"]["recipe_id"] == row["recipe_id"]
    expected = REVIEW["recipes"][row["recipe_id"]]["inputs"]
    actual = [x for x in helper["exchanges"] if x["type"] != "production"]
    assert len(actual) == len(expected)
    for x, y in zip(actual, expected, strict=True):
        assert all(x[key] == value for key, value in y.items())


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda row: row["rule_id"])
def test_other_profile_or_other_rule_approval_does_not_grant_consent(row):
    data = consumer(row["source_witness"]["identity"], 1)
    original = deepcopy(data)
    others = [r for r in REVIEW["records"] if r["rule_id"] != row["rule_id"]]
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
            contexts("uvek", "2026").background, policy=policy(others)
        )
    for version, model in (("3.6", "cutoff"), ("3.12", "cutoff"), ("3.9", "consequential")):
        with pytest.raises(MigrationError):
            BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model)).migrate_background(
                contexts("uvek", "2026").background, policy=policy()
            )
    assert data == original


def test_pure_allyl_chloride_is_not_vinyl_chloride_or_hydrochloric_acid_solution():
    assert len(REVIEW["records"]) == len(REVIEW["recipes"]) == 2
    bindings = [x for row in REVIEW["records"] for x in row["component_bindings"]]
    producers = [x for x in bindings if x["kind"] == "producer"]
    assert len(bindings) == 15 and len(producers) == 2
    assert {x["target"]["name"] for x in producers} == {
        "Allyl chloride, from reacting propylene and chlorine, at plant"
    }
    assert all(x["statistics"]["amount"] == 1 and x["target"]["unit"] == "kilogram" for x in producers)
    assert {row["source_witness"]["identity"]["location"] for row in REVIEW["records"]} == {"RER", "RoW"}
    assert len(REVIEW["native_2026_witnesses"]) == 5 and REVIEW["new_source_omissions"] == []


@pytest.mark.parametrize("drift", [None, "parent", "recipe"])
def test_replay_is_idempotent_or_refuses_atomically(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    if drift == "parent":
        next(r for r in resource["replace"] if r["id"] == REVIEW["records"][-1]["rule_id"])["confidence"] = 0.1
    elif drift == "recipe":
        resource["recipes"][REVIEW["records"][0]["recipe_id"]]["inputs"][0]["amount"] = 9
    before = deepcopy(resource)
    if drift is None:
        apply_pulp_heat_allyl_markets_review(resource)
    else:
        with pytest.raises(ValueError):
            apply_pulp_heat_allyl_markets_review(resource)
    assert resource == before
