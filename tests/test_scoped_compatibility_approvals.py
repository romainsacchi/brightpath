"""An approval covers its exact rule/profile edge, never a whole resource."""

from copy import deepcopy
from dataclasses import replace
import json

import pytest

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.execution import _audit_compatibility_rule_scope
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from test_uvek_reference_signs import consumer, contexts

REVIEW = json.loads((DATA_DIR / "export/uvek_pilot_material_review.json").read_text())
SOURCE = TechnosphereProfile("ecoinvent", "3.9", "cutoff")
BRIDGE = TechnosphereProfile("uvek", "2025", "cutoff")


def scope(decision):
    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == decision["rule_id"])
    fingerprint = compatibility_rule_fingerprint(resource, "replace", rule, SOURCE, BRIDGE)
    return (
        resource,
        rule,
        MigrationPolicy(approved_compatibility_rules=(fingerprint,), on_information_loss=PolicyAction.WARN),
    )


@pytest.mark.parametrize("decision", REVIEW["rules"], ids=lambda d: d["label"])
@pytest.mark.parametrize("target_version", ["2025", "2026"])
def test_approved_material_can_migrate_without_global_heuristic_consent(decision, target_version):
    _, _, policy = scope(decision)
    assert policy.on_heuristic_mapping is PolicyAction.ERROR
    data = consumer(decision["source"], -2, uncertainty=2)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9"))
    result = inventory.migrate_background(contexts("uvek", target_version).background, policy=policy)
    edge = result.data[0]["exchanges"][1]
    assert edge["amount"] == pytest.approx(-2 * decision["conversion_factor"])
    assert all(edge[key] == value for key, value in decision["target"].items())
    assert inventory.data == data == before
    assert any(i.code == "migration.approved_correspondence" for i in result.last_migration_report.issues)
    assert not any(loss.code == "migration.heuristic_mapping" for loss in result.last_migration_report.losses)


def test_mixed_inventory_cannot_borrow_approval_from_one_reviewed_material():
    decision = next(r for r in REVIEW["rules"] if r["label"] == "fan")
    resource, _, policy = scope(decision)
    bad = next(r for r in resource["replace"] if r["id"] == "bp-49e229b952f6983c")
    data = consumer(decision["source"], -2)
    data[0]["exchanges"].append({**bad["source"], "type": "technosphere", "amount": 0.1})
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9"))
    before = inventory.data
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(contexts("uvek", "2026").background, policy=policy)
    findings = [i for i in caught.value.report.issues if i.code == "migration.unapproved_correspondence"]
    assert [i.details["rule_id"] for i in findings] == [bad["id"]]
    assert inventory.data == before


@pytest.mark.parametrize("change", ["target", "factor", "review", "source version", "target version", "operation"])
def test_scope_changes_invalidate_a_rule_even_with_unchanged_id(change):
    resource, original, _ = scope(REVIEW["rules"][0])
    rule, source, target, operation = deepcopy(original), SOURCE, BRIDGE, "replace"
    before = compatibility_rule_fingerprint(resource, operation, rule, source, target)
    if change == "target":
        rule["target"]["name"] += " changed"
    elif change == "factor":
        rule["conversion_factor"] = 3
    elif change == "review":
        resource = {**resource, "reviews": deepcopy(resource["reviews"])}
        resource["reviews"][rule["review_ref"]]["rationale"] += " changed"
    elif change == "source version":
        source = TechnosphereProfile("ecoinvent", "3.9.1", "cutoff")
    elif change == "target version":
        target = TechnosphereProfile("uvek", "2026", "cutoff")
    else:
        operation = "delete"
    assert compatibility_rule_fingerprint(resource, operation, rule, source, target) != before


@pytest.mark.parametrize("operation", ["replace", "delete", "disaggregate"])
def test_every_matching_operation_needs_approval_and_foreground_is_protected(operation):
    base, original, _ = scope(REVIEW["rules"][0])
    rule = deepcopy(original)
    if operation == "disaggregate":
        rule["targets"] = [rule.pop("target")]
    resource = {"name": "synthetic", "axis": "technosphere", "reviews": base["reviews"], operation: [rule]}
    data = consumer(rule["source"], -2)
    policy = MigrationPolicy(approved_compatibility_rules=("0" * 64,))
    report = _audit_compatibility_rule_scope(data, resource, SOURCE, BRIDGE, (), policy)
    assert report.has_errors
    fingerprint = compatibility_rule_fingerprint(resource, operation, rule, SOURCE, BRIDGE)
    allowed = replace(policy, approved_compatibility_rules=(fingerprint,))
    assert not _audit_compatibility_rule_scope(data, resource, SOURCE, BRIDGE, (), allowed).has_errors
    identity = tuple(rule["source"][key] for key in ("name", "reference product", "location", "unit"))
    assert not _audit_compatibility_rule_scope(data, resource, SOURCE, BRIDGE, (identity,), policy).issues


@pytest.mark.parametrize("value", [True, "0" * 64, ["invalid"], [None], ["A" * 64]])
def test_invalid_policy_scopes_fail(value):
    with pytest.raises(ValueError, match="SHA256"):
        MigrationPolicy(approved_compatibility_rules=value)


def test_policy_round_trip_preserves_owned_deduplicated_scopes():
    values = ["0" * 64, "1" * 64, "0" * 64]
    policy = MigrationPolicy(approved_compatibility_rules=values)
    values.clear()
    assert policy.to_dict()["approved_compatibility_rules"] == ["0" * 64, "1" * 64]
    assert MigrationPolicy.from_json(policy.to_json()) == policy


def test_rule_approval_does_not_override_information_loss_policy():
    decision = REVIEW["rules"][0]
    _, _, allowed = scope(decision)
    inventory = BrightwayInventory.from_data(consumer(decision["source"], 2), context=contexts("ecoinvent", "3.9"))
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(
            contexts("uvek", "2025").background, policy=replace(allowed, on_information_loss=PolicyAction.ERROR)
        )
    assert any(i.code == "migration.technosphere_information_loss" for i in caught.value.report.issues)


def test_rule_approval_does_not_override_unresolved_reference_coordinate():
    decision = next(r for r in REVIEW["rules"] if r["label"] == "fan")
    resource, rule, allowed = scope(decision)
    source = TechnosphereProfile("ecoinvent", "3.11", "cutoff")
    fingerprint = compatibility_rule_fingerprint(resource, "replace", rule, source, BRIDGE)
    inventory = BrightwayInventory.from_data(consumer(decision["source"], 2), context=contexts("ecoinvent", "3.11"))
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(
            contexts("uvek", "2025").background, policy=replace(allowed, approved_compatibility_rules=(fingerprint,))
        )
    assert any(i.code == "migration.reference_flow_sign_unresolved" for i in caught.value.report.issues)


def test_helper_approval_binds_the_recipe_and_transitive_dependencies():
    rule = {"id": "helper", "source": {"name": "source"}, "target": {"recipe_ref": "outer"}}
    resource = {
        "name": "synthetic",
        "axis": "technosphere",
        "recipes": {
            "outer": {"inputs": [{"recipe": "inner", "amount": 1}]},
            "inner": {"inputs": [{"name": "material", "amount": 2}]},
        },
    }
    before = compatibility_rule_fingerprint(resource, "replace", rule, SOURCE, BRIDGE)
    resource["recipes"]["inner"]["inputs"][0]["amount"] = 3
    assert compatibility_rule_fingerprint(resource, "replace", rule, SOURCE, BRIDGE) != before


def test_profile_recipe_override_and_nested_review_contents_are_bound():
    rule = {
        "id": "helper",
        "source": {"name": "source"},
        "target": {"recipe_ref": "base"},
        "reference_flow_sign_review": {
            "profiles": [
                {
                    "source_profile": {"version": "3.9"},
                    "target": {"recipe_ref": "override"},
                    "review_ref": "profile-review",
                }
            ]
        },
    }
    resource = {
        "name": "synthetic",
        "axis": "technosphere",
        "reviews": {"profile-review": {"status": "verified"}},
        "recipes": {name: {"inputs": [{"name": "material", "amount": 2}]} for name in ("base", "override", "unused")},
    }
    before = compatibility_rule_fingerprint(resource, "replace", rule, SOURCE, BRIDGE)
    resource["recipes"]["unused"]["inputs"][0]["amount"] = 3
    assert compatibility_rule_fingerprint(resource, "replace", rule, SOURCE, BRIDGE) == before
    resource["recipes"]["override"]["inputs"][0]["amount"] = 3
    changed = compatibility_rule_fingerprint(resource, "replace", rule, SOURCE, BRIDGE)
    assert changed != before
    resource["reviews"]["profile-review"]["status"] = "unresolved"
    assert compatibility_rule_fingerprint(resource, "replace", rule, SOURCE, BRIDGE) != changed


def test_technosphere_approval_does_not_authorize_unreviewed_biosphere_or_system_models():
    from brightpath import BackgroundContext, BiosphereProfile
    from brightpath.background import plan_background_migration
    from brightpath.background.migration import MigrationAxis, _heuristic_mapping_finding

    _, _, policy = scope(REVIEW["rules"][0])
    issues, losses = [], []
    _heuristic_mapping_finding(MigrationAxis.BIOSPHERE, {"name": "unreviewed-bio"}, policy, issues, losses)
    assert issues[0].severity.value == "error"
    assert losses[0].code == "migration.heuristic_mapping"
    plan = plan_background_migration(
        BackgroundContext(
            TechnosphereProfile("ecoinvent", "3.9", "consequential"), BiosphereProfile("ecoinvent", "3.9")
        ),
        BackgroundContext(BRIDGE, BiosphereProfile("ecoinvent", "3.10")),
        policy,
    )
    assert not plan.executable
