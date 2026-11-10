"""Exact 3.8 profile reviews preserve reference coordinates and previous decisions."""

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
from scripts.review_uvek_heat_38_profiles import apply_heat_38_profiles_review, qualified_extended_profile_rule

REVIEW = json.loads((DATA_DIR / "export/uvek_heat_38_profiles_review.json").read_text())


@pytest.fixture(scope="module")
def policy():
    resource = load_uvek_technosphere_resource()
    indexed = {r["id"]: r for r in resource["replace"]}
    scopes = tuple(
        compatibility_rule_fingerprint(
            resource,
            "replace",
            indexed[row["rule_id"]],
            TechnosphereProfile("ecoinvent", "3.8", "cutoff"),
            TechnosphereProfile("uvek", "2025", "cutoff"),
        )
        for row in REVIEW["records"]
    )
    return MigrationPolicy(approved_compatibility_rules=scopes, on_information_loss=PolicyAction.WARN)


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("amount,uncertainty", [(-2, 2), (2, 2), (-2, 3), (2, 3)])
def test_same_unit_correspondence_keeps_all_caller_quantity_fields(row, amount, uncertainty, policy):
    data = consumer(row["source"], amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.8")).migrate_background(
        contexts("uvek", "2026").background,
        policy=policy,
    )
    assert data == before
    assert len(result.data) == 1
    edge = result.data[0]["exchanges"][1]
    assert all(edge[k] == v for k, v in row["target"].items())
    old = before[0]["exchanges"][1]
    factor = row["conversion_factor"]
    assert edge["amount"] == amount * factor
    assert edge["amount"] / row["target_reference_amount"] == amount / row["source_reference_amount"]
    assert edge["scale"] == old["scale"]
    if factor == 1:
        assert all(edge[k] == v for k, v in old.items() if k not in row["source"])
    else:
        assert row["previous_rule"]["conversion_factor"] == -1
        assert edge["formula"] == "(dose) * (-1.0)"
        if uncertainty == 2:
            assert edge["loc"] == old["loc"] and edge["negative"] is not old["negative"]
        else:
            assert edge["loc"] == -old["loc"]
            assert edge["minimum"] == -old["maximum"] and edge["maximum"] == -old["minimum"]


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
def test_older_effective_profiles_and_methods_are_unchanged(row):
    resource = load_uvek_technosphere_resource()
    current = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
    for scope in row["previous_scopes"]:
        source = TechnosphereProfile(**scope["source"])
        old = select_profile_sign_rules({"replace": [row["previous_rule"]]}, source)["replace"][0]
        new = select_profile_sign_rules({"replace": [current]}, source)["replace"][0]
        assert new == old
    if row["resource_action"] == "unchanged_rule":
        assert current == row["previous_rule"]


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
def test_new_permission_is_not_a_version_wide_approval(row, policy):
    data = consumer(row["source"], 1)
    before = deepcopy(data)
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.11")).migrate_background(
            contexts("uvek", "2026").background,
            policy=policy,
        )
    assert data == before


@pytest.mark.parametrize("drift", [False, True])
def test_replay_idempotence_and_atomic_late_failure(drift):
    active = load_uvek_technosphere_resource()
    resource = {**active, "replace": [deepcopy(r["previous_rule"]) for r in REVIEW["records"]]}
    apply_heat_38_profiles_review(resource)
    if drift:
        resource["replace"][-1]["target"]["location"] = "unreviewed"
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError, match="earlier rule changed"):
            apply_heat_38_profiles_review(resource)
    else:
        apply_heat_38_profiles_review(resource)
    assert resource == before


@pytest.mark.parametrize("change", ["other_profile", "old_target", "old_review"])
def test_earlier_authoring_stage_rejects_arbitrary_profile_or_boundary_changes(change):
    resource = load_uvek_technosphere_resource()
    row = next(r for r in REVIEW["records"] if r["resource_action"] == "append_exact_profile")
    actual = deepcopy(next(r for r in resource["replace"] if r["id"] == row["rule_id"]))
    if change == "other_profile":
        actual["reference_flow_sign_review"]["profiles"][-1]["source_profile"]["version"] = "3.11"
    elif change == "old_target":
        actual["target"]["location"] = "unreviewed"
    else:
        actual["reference_flow_sign_review"]["profiles"][0]["review"]["reason"] = "unreviewed"
    with pytest.raises(ValueError, match="Unreviewed concrete"):
        qualified_extended_profile_rule(actual, row["previous_rule"])
