"""Generic lime retains inspected milling, allocated mining and delivery boundaries."""

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

REVIEW = json.loads((DATA_DIR / "export/uvek_lime_delivery_39_review.json").read_text())
ROW = REVIEW["records"][0]


def policy(approved=True):
    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == ROW["rule_id"])
    fp = compatibility_rule_fingerprint(
        resource,
        "replace",
        rule,
        TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    )
    return MigrationPolicy(
        approved_compatibility_rules=(fp,) if approved else (), on_information_loss=PolicyAction.WARN
    )


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_lime_market_preserves_signed_kg_and_separate_three_producers_and_freight(amount, uncertainty, release):
    source = consumer(ROW["original_rule"]["source"], amount, uncertainty)
    before = deepcopy(source)
    result = BrightwayInventory.from_data(source, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert source == before and len(result.data) == 3
    edge = result.data[0]["exchanges"][1]
    assert edge["amount"] == amount and edge["formula"] == "dose"
    assert edge["uncertainty type"] == uncertainty and edge["scale"] == 0.2
    if uncertainty == 2:
        assert edge["negative"] is (amount < 0)
    else:
        assert edge["loc"] == amount
    delivery = next(a for a in result.data if a.get("code") == edge["input"][1])
    rows = [e for e in delivery["exchanges"] if e["type"] != "production"]
    assert len(rows) == 6
    for actual, expected in zip(rows, REVIEW["recipes"][ROW["recipe_id"]]["inputs"], strict=True):
        for key in ("amount", "loc", "uncertainty type", "scale", "pedigree"):
            if key in expected:
                assert actual[key] == expected[key]
    assert len([e for e in rows if e["name"] == "Limestone, milled, loose, at plant"]) == 2
    assert len([e for e in rows if e["unit"] == "ton kilometer"]) == 3
    for freight in (e for e in rows if e["unit"] == "ton kilometer"):
        assert freight["scale"] == 0.4074309757492673
        assert freight["scale without pedigree"] == 0.5
    mine = next(
        a
        for a in result.data
        if a.get("brightpath_conversion", {}).get("recipe_id") == "lime-39-allocated-mining-reviewed-native"
    )
    native = [e for e in mine["exchanges"] if e["type"] != "production"]
    assert len(native) == 1 and native[0]["name"] == "Zinc, from combined metal production, at beneficiation"
    assert native[0]["amount"] == 0.59 * (0.117 / 0.38)
    assert not delivery["brightpath_conversion"].get("source_input_omissions")


@pytest.mark.parametrize(
    "version,model,approved",
    [("3.9", "cutoff", False), ("3.9.1", "cutoff", True), ("3.10", "cutoff", True), ("3.9", "consequential", True)],
)
def test_new_exact_permission_does_not_authorize_other_profiles(version, model, approved):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROW["original_rule"]["source"], 1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(approved))


def test_earlier_38_binding_and_frozen_recipe_closure_remain_exact():
    resource = load_uvek_technosphere_resource()
    old = ROW["original_rule"]
    current = next(r for r in resource["replace"] if r["id"] == ROW["rule_id"])
    assert current == ROW["compiled_rule"]
    assert current["reference_flow_sign_review"]["profiles"][:-1] == old["reference_flow_sign_review"]["profiles"]
    effective = next(
        r
        for r in select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", "3.8", "cutoff"))["replace"]
        if r["id"] == ROW["rule_id"]
    )
    assert effective["target"] == old["target"] and effective["conversion_factor"] == 1
    assert all(resource["recipes"][key] == value for key, value in REVIEW["recipe_dependencies"].items())
    assert ROW["boundary_checks"]["source_mining_coordinates_compared"] == 264
    assert ROW["boundary_checks"]["extra_calcination_added"] is False


@pytest.mark.parametrize("drift", [None, "parent", "old_recipe"])
def test_replay_is_atomic_and_rejects_changed_prior_recipe(drift):
    from scripts.review_uvek_lime_delivery_39 import apply_lime_delivery_39_review

    resource = deepcopy(load_uvek_technosphere_resource())
    if drift == "parent":
        next(r for r in resource["replace"] if r["id"] == ROW["rule_id"])["conversion_factor"] = 2
    elif drift == "old_recipe":
        resource["recipes"][next(iter(REVIEW["recipe_dependencies"]))]["inputs"][0]["amount"] *= 2
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_lime_delivery_39_review(resource)
    else:
        apply_lime_delivery_39_review(resource)
    assert resource == before
