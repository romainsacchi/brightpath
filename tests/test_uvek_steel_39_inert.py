"""Steel prerequisites retain fuel supply and physical waste-treatment demand."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_steel_39_inert import apply_steel_39_inert_review

REVIEW = json.loads((DATA_DIR / "export/uvek_steel_39_inert_review.json").read_text())


def policy():
    resource = load_uvek_technosphere_resource()
    rules = {r["id"]: r for r in resource["replace"]}
    scopes = tuple(
        compatibility_rule_fingerprint(
            resource,
            "replace",
            rules[row["rule_id"]],
            TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
            TechnosphereProfile("uvek", "2025", "cutoff"),
        )
        for row in REVIEW["records"]
    )
    return MigrationPolicy(approved_compatibility_rules=scopes, on_information_loss=PolicyAction.WARN)


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda row: row["rule_id"])
@pytest.mark.parametrize("amount", [-2, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_signed_demand_uncertainty_and_foreground_are_preserved(row, amount, uncertainty):
    data = consumer(row["source_witness"]["identity"], amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", "2026").background, policy=policy()
    )
    assert data == before
    edge = result.data[0]["exchanges"][1]
    factor = row["conversion_factor"]
    assert edge["amount"] == amount * factor
    assert edge["uncertainty type"] == uncertainty and edge["scale"] == 0.2
    if uncertainty == 2:
        assert edge["loc"] == before[0]["exchanges"][1]["loc"]
        assert edge["negative"] is (edge["amount"] < 0)
    else:
        assert edge["loc"] == amount * factor
        assert edge["minimum"] == min((amount - 1) * factor, (amount + 1) * factor)
        assert edge["maximum"] == max((amount - 1) * factor, (amount + 1) * factor)
    assert edge["formula"] == ("dose" if factor == 1 else "(dose) * (-1.0)")
    helper = next(
        a
        for a in result.data
        if a.get("brightpath_conversion", {}).get("recipe_id") == "steel-39-swiss-inert-disposal-with-collection"
    )
    actual = [e for e in helper["exchanges"] if e["type"] == "technosphere"]
    expected = REVIEW["recipes"]["steel-39-swiss-inert-disposal-with-collection"]["inputs"]
    assert len(actual) == len(expected) == 4
    for a, b in zip(actual, expected, strict=True):
        for field in ("name", "unit", "amount", "uncertainty type", "loc", "scale"):
            if field in b:
                assert a[field] == b[field]
    assert not any(e["type"] == "biosphere" for e in helper["exchanges"])
    assert [e["amount"] for e in actual if e["unit"] == "ton kilometer"] == [0.0036, 0.0258]


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda row: row["rule_id"])
def test_exact_approval_does_not_authorize_another_source_release(row):
    data = consumer(row["source_witness"]["identity"], -2)
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.11")).migrate_background(
            contexts("uvek", "2026").background, policy=policy()
        )


@pytest.mark.parametrize("drift", [False, True])
def test_inert_review_replay_is_atomic(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    if drift:
        next(r for r in resource["replace"] if r["id"] == REVIEW["records"][0]["rule_id"])["confidence"] = 0.1
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError, match="parent changed"):
            apply_steel_39_inert_review(resource)
    else:
        apply_steel_39_inert_review(resource)
    assert resource == before


def test_historical_36_target_and_sign_selection_remain_unchanged():
    from brightpath.background.reference_signs import select_profile_sign_rules

    resource = load_uvek_technosphere_resource()
    selected = select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", "3.6", "cutoff"))
    row = REVIEW["records"][0]
    rule = next(r for r in selected["replace"] if r["id"] == row["rule_id"])
    original = row["original_rule"]
    for field in (
        "source",
        "target",
        "conversion_factor",
        "mapping_method",
        "confidence",
        "reference_flow_sign_review",
    ):
        assert rule[field] == original[field]


def test_recipe_drift_is_refused_atomically():
    resource = deepcopy(load_uvek_technosphere_resource())
    resource["recipes"]["steel-39-swiss-inert-disposal-with-collection"]["inputs"][0]["amount"] *= 2
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="recipe changed"):
        apply_steel_39_inert_review(resource)
    assert resource == before
