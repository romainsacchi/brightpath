"""Liquid nitrogen qualification is confined to the inspected 3.8 profile."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_mining_38_liquid_nitrogen_review.json").read_text())


def fingerprint():
    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == REVIEW["rule_id"])
    assert rule == REVIEW["rule"]
    return compatibility_rule_fingerprint(
        resource,
        "replace",
        rule,
        TechnosphereProfile(**REVIEW["source_profile"]),
        TechnosphereProfile(**REVIEW["bridge_target_profile"]),
    )


@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_review_preserves_signed_caller_and_uncertainty(amount, uncertainty):
    data = consumer(REVIEW["rule"]["source"], amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.8")).migrate_background(
        contexts("uvek", "2026").background,
        policy=MigrationPolicy(
            approved_compatibility_rules=(fingerprint(),),
            on_information_loss=PolicyAction.WARN,
        ),
    )
    edge = result.data[0]["exchanges"][1]
    original = before[0]["exchanges"][1]
    for field in ("amount", "uncertainty type", "loc", "scale", "minimum", "maximum", "negative", "formula"):
        if field in original:
            assert edge[field] == original[field]
    assert all(edge[k] == value for k, value in REVIEW["rule"]["target"].items())
    assert data == before


@pytest.mark.parametrize("version,approved", [("3.8", False), ("3.10", True)])
def test_new_review_does_not_authorize_unreviewed_profiles(version, approved):
    data = consumer(REVIEW["rule"]["source"], 1, 3)
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", version)).migrate_background(
            contexts("uvek", "2026").background,
            policy=MigrationPolicy(
                approved_compatibility_rules=(fingerprint(),) if approved else (),
                on_information_loss=PolicyAction.WARN,
            ),
        )
