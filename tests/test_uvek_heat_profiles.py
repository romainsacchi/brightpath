"""New exact-profile qualifications cannot change earlier effective mappings."""

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
from scripts.review_uvek_heat_profiles import apply_heat_profile_review, compiled_profile_rule

REVIEW = json.loads((DATA_DIR / "export/uvek_heat_profile_extension_review.json").read_text())


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("amount,uncertainty", [(-2, 2), (2, 2), (-2, 3), (2, 3), (0, 3)])
def test_exact_profile_preserves_signed_caller_fields(row, amount, uncertainty):
    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
    fingerprint = compatibility_rule_fingerprint(
        resource,
        "replace",
        rule,
        TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    )
    data = consumer(row["source"], amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", "2026").background,
        policy=MigrationPolicy(approved_compatibility_rules=(fingerprint,), on_information_loss=PolicyAction.WARN),
    )
    edge = result.data[0]["exchanges"][1]
    old = before[0]["exchanges"][1]
    for field in ("amount", "uncertainty type", "loc", "scale", "minimum", "maximum", "negative", "formula"):
        if field in old:
            assert edge[field] == old[field]
    if "recipe_ref" not in row["target"]:
        assert all(edge[k] == v for k, v in row["target"].items())
    else:
        assert len(result.data) == 2
        helper = result.data[1]
        assert helper["unit"] == "megajoule"
        assert [e["amount"] for e in helper["exchanges"] if e["type"] == "production"] == [1]
        assert sorted(e["amount"] for e in helper["exchanges"] if e["type"] != "production") == sorted(
            x["amount"] for x in resource["recipes"][row["target"]["recipe_ref"]]["inputs"]
        )
    assert data == before


def test_old_effective_profiles_and_shared_recipe_closure_are_unchanged():
    resource = load_uvek_technosphere_resource()
    indexed = {r["id"]: r for r in resource["replace"]}
    for row in REVIEW["records"]:
        old = row["previous_rule"]
        new = indexed[row["rule_id"]]
        for scope in row["previous_scopes"]:
            profile = TechnosphereProfile(**scope["source"])
            projected_old = select_profile_sign_rules({"replace": [old]}, profile)["replace"][0]
            projected_new = select_profile_sign_rules({"replace": [new]}, profile)["replace"][0]
            assert projected_new == projected_old
        if row["resource_action"] == "qualify_existing_rule_without_change":
            assert new == old
        if old.get("review_ref"):
            assert old["review_ref"] == new["review_ref"]


def test_replay_and_atomic_refusal_of_drift():
    active = load_uvek_technosphere_resource()
    resource = {
        **active,
        "replace": [deepcopy(r["previous_rule"]) for r in REVIEW["records"]],
        "reviews": deepcopy(active["reviews"]),
        "coverage": {},
    }
    apply_heat_profile_review(resource)
    before = deepcopy(resource)
    apply_heat_profile_review(resource)
    assert resource == before
    resource["replace"][-1]["conversion_factor"] = 42
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="existing rule changed"):
        apply_heat_profile_review(resource)
    assert resource == before


def test_added_epoxy_scope_does_not_drop_cas_warning_or_broaden_other_releases():
    row = next(r for r in REVIEW["records"] if r["rule_id"] == "bp-08f2432b62895931")
    compiled, _ = compiled_profile_rule(row, REVIEW["authorization"])
    selected = select_profile_sign_rules({"replace": [compiled]}, TechnosphereProfile("ecoinvent", "3.9", "cutoff"))
    review = selected["replace"][0]["reference_flow_sign_review"]
    assert review["status"] == "verified"
    assert "CAS" in review["reason"] and "conflict" in review["reason"]
    for version in ("3.8", "3.10"):
        other = select_profile_sign_rules({"replace": [compiled]}, TechnosphereProfile("ecoinvent", version, "cutoff"))
        assert other["replace"][0]["reference_flow_sign_review"]["status"] == "unresolved"


def test_new_profile_fingerprint_does_not_authorize_another_release():
    row = REVIEW["records"][0]
    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
    fingerprint = compatibility_rule_fingerprint(
        resource,
        "replace",
        rule,
        TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    )
    data = consumer(row["source"], 2)
    before = deepcopy(data)
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.10")).migrate_background(
            contexts("uvek", "2026").background,
            policy=MigrationPolicy(approved_compatibility_rules=(fingerprint,), on_information_loss=PolicyAction.WARN),
        )
    assert data == before
