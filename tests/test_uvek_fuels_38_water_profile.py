"""Exact-profile water extension preserves kg statistics and older decisions."""

import json
from copy import deepcopy
from dataclasses import replace

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_fuels_38_water_profile import apply_fuels_38_water_profile_review

REVIEW = json.loads((DATA_DIR / "export/uvek_fuels_38_water_profile_review.json").read_text())


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_signed_kg_and_caller_statistics(amount, uncertainty):
    source = BrightwayInventory.from_data(
        consumer(REVIEW["original_rule"]["source"], amount, uncertainty),
        context=contexts("ecoinvent", "3.8", "cutoff"),
    )
    before = deepcopy(source.data)
    policy = replace(MigrationPolicy.permissive(), on_invalid_target=PolicyAction.ERROR, minimum_coverage=1)
    result = source.migrate_background(target_context("2026"), policy=policy)
    edge = result.data[0]["exchanges"][1]
    assert all(edge[k] == v for k, v in REVIEW["compiled_rule"]["target"].items())
    assert edge["amount"] == amount and edge["formula"] == "dose"
    assert edge["uncertainty type"] == uncertainty and edge["scale"] == 0.2
    if uncertainty == 2:
        assert edge["negative"] is (amount < 0)
    else:
        assert edge["loc"] == amount
    assert source.data == before


@pytest.mark.parametrize("version", ["3.7", "3.11", "3.12"])
def test_uninspected_releases_still_refused(version):
    source = BrightwayInventory.from_data(
        consumer(REVIEW["original_rule"]["source"], 1, 2), context=contexts("ecoinvent", version, "cutoff")
    )
    with pytest.raises(MigrationError):
        source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())


def test_replay_is_idempotent_and_old_profiles_unchanged():
    resource = deepcopy(load_uvek_technosphere_resource())
    before = deepcopy(resource)
    apply_fuels_38_water_profile_review(resource)
    assert resource == before
    for proof in REVIEW["preserved_profiles"]:
        profile = TechnosphereProfile(**proof["source_profile"])
        old = select_profile_sign_rules({"replace": [REVIEW["original_rule"]]}, profile)["replace"][0]
        new = select_profile_sign_rules({"replace": [REVIEW["compiled_rule"]]}, profile)["replace"][0]
        assert old == new
    parent = next(r for r in resource["replace"] if r["id"] == REVIEW["rule_id"])
    parent["target"]["unit"] = "cubic meter"
    with pytest.raises(ValueError, match="Water parent changed"):
        apply_fuels_38_water_profile_review(resource)
