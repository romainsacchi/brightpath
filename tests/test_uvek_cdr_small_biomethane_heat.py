"""Small-boiler heat mapping preserves useful MJ and explicit source-profile bounds."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_cdr_small_biomethane_heat_review.json").read_text())["rules"][0]


def rule_and_scope(version):
    resource = load_uvek_technosphere_resource()
    rule = next(row for row in resource["replace"] if row["id"] == REVIEW["rule_id"])
    return rule, compatibility_rule_fingerprint(
        resource,
        "replace",
        rule,
        TechnosphereProfile("ecoinvent", version, "cutoff"),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    )


@pytest.mark.parametrize("version", [p["source_profile"]["version"] for p in REVIEW["profiles"]])
@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_complete_small_biomethane_heat_retains_signed_units_statistics_and_caller(version, amount, uncertainty):
    rule, fingerprint = rule_and_scope(version)
    data = consumer(rule["source"], amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version)).migrate_background(
        contexts("uvek", "2026").background,
        policy=MigrationPolicy(
            approved_compatibility_rules=(fingerprint,),
            on_information_loss=PolicyAction.WARN,
            on_inferred_reverse=PolicyAction.WARN,
        ),
    )
    assert len(result.data) == 1
    edge, original = result.data[0]["exchanges"][1], before[0]["exchanges"][1]
    assert all(edge[key] == value for key, value in REVIEW["target"].items())
    for field in ("amount", "formula", "uncertainty type", "loc", "scale", "minimum", "maximum", "negative"):
        if field in original:
            assert edge[field] == original[field]
    assert data == before
    assert any(issue.code == "migration.approved_correspondence" for issue in result.last_migration_report.issues)


@pytest.mark.parametrize("version,grant", [("3.12", None), ("3.12", "3.9"), ("3.11", "3.12"), ("3.12.1", "3.12")])
def test_other_or_absent_source_scope_cannot_authorize_the_small_biomethane_heat(version, grant):
    rule, _ = rule_and_scope(version)
    source = BrightwayInventory.from_data(consumer(rule["source"], 1, 3), context=contexts("ecoinvent", version))
    before = deepcopy(source.data)
    with pytest.raises(MigrationError):
        source.migrate_background(
            contexts("uvek", "2026").background,
            policy=MigrationPolicy(
                approved_compatibility_rules=(rule_and_scope(grant)[1],) if grant else (),
                on_information_loss=PolicyAction.WARN,
            ),
        )
    assert source.data == before


def test_cutoff_scope_cannot_authorize_cross_system_model_conversion():
    rule, fingerprint = rule_and_scope("3.12")
    source = BrightwayInventory.from_data(
        consumer(rule["source"], 1, 3), context=contexts("ecoinvent", "3.12", "consequential")
    )
    with pytest.raises(MigrationError):
        source.migrate_background(
            contexts("uvek", "2026").background,
            policy=MigrationPolicy(
                approved_compatibility_rules=(fingerprint,),
                on_information_loss=PolicyAction.WARN,
            ),
        )


def test_authoring_changes_only_the_reviewed_component_and_replays_idempotently():
    from scripts.review_uvek_pilot_materials import apply_cdr_small_biomethane_heat_review

    resource = deepcopy(load_uvek_technosphere_resource())
    rule = next(r for r in resource["replace"] if r["id"] == REVIEW["rule_id"])
    current = deepcopy(rule)
    rule.clear()
    rule.update(deepcopy(REVIEW["previous_rule"]))
    unrelated = [deepcopy(r) for r in resource["replace"] if r["id"] != REVIEW["rule_id"]]
    apply_cdr_small_biomethane_heat_review(resource)
    assert rule == current
    assert [r for r in resource["replace"] if r["id"] != REVIEW["rule_id"]] == unrelated
    before = deepcopy(resource)
    apply_cdr_small_biomethane_heat_review(resource)
    assert resource == before
    rule["target"]["unit"] = "kilogram"
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="source, supplier or unit changed"):
        apply_cdr_small_biomethane_heat_review(resource)
    assert resource == before
