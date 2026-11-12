"""Final delivered supplies preserve market quantities and earlier profile choices."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_ferrosilicon_last_supplies import apply_ferrosilicon_last_supplies_review

REVIEW = json.loads((DATA_DIR / "export/uvek_ferrosilicon_last_supplies_review.json").read_text())


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
    assert data == before and len(result.data) == 2
    actual = result.data[0]["exchanges"][1]
    original = before[0]["exchanges"][1]
    for key in ("amount", "uncertainty type", "loc", "scale", "negative", "minimum", "maximum", "formula"):
        if key in original:
            assert actual[key] == original[key]
    helper = next(a for a in result.data if a.get("brightpath_conversion", {}).get("recipe_id") == row["recipe_id"])
    inputs = [e for e in helper["exchanges"] if e["type"] == "technosphere"]
    expected = REVIEW["recipes"][row["recipe_id"]]["inputs"]
    assert len(inputs) == len(expected)
    for actual, source in zip(inputs, expected, strict=True):
        for key, value in source.items():
            assert actual[key] == value


@pytest.mark.parametrize("version,model", [("3.6", "cutoff"), ("3.11", "cutoff"), ("3.9", "consequential")])
def test_other_profiles_are_not_authorized(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(REVIEW["records"][0]["source_witness"]["identity"], 1),
            context=contexts("ecoinvent", version, model),
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


def test_preexisting_selected_gas_rule_and_recipe_are_unchanged():
    resource = load_uvek_technosphere_resource()
    row = next(r for r in REVIEW["records"] if r["preserved_profiles"])
    profile = TechnosphereProfile("ecoinvent", "3.12", "cutoff")
    assert select_profile_sign_rules({"replace": [row["compiled_rule"]]}, profile) == select_profile_sign_rules(
        {"replace": [row["original_rule"]]}, profile
    )
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
        apply_ferrosilicon_last_supplies_review(resource)
    else:
        with pytest.raises(ValueError):
            apply_ferrosilicon_last_supplies_review(resource)
    assert resource == before
