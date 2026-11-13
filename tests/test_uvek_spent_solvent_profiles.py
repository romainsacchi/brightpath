"""Wet spent-solvent treatment preserves demand, credits and exact reviewed scopes."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_spent_solvent_profiles_review.json").read_text())


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


@pytest.mark.parametrize("version", [p["source_profile"]["version"] for p in REVIEW["source_profiles"]])
@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_complete_solvent_treatment_preserves_signed_wet_mass_statistics_and_caller(version, amount, uncertainty):
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
    assert all(edge[key] == value for key, value in REVIEW["rule"]["target"].items())
    assert edge["amount"] == -amount
    assert edge["formula"] == "(dose) * (-1.0)"
    assert edge["scale"] == original["scale"]
    assert edge["uncertainty type"] == uncertainty
    if uncertainty == 2:
        assert edge["loc"] == original["loc"]
        assert edge["negative"] is (amount > 0)
    else:
        assert edge["loc"] == -amount
        assert edge["minimum"] == -original["maximum"]
        assert edge["maximum"] == -original["minimum"]
    assert data == before
    assert any(issue.code == "migration.approved_correspondence" for issue in result.last_migration_report.issues)


@pytest.mark.parametrize(
    "version,grant", [("3.9", None), ("3.9", "3.6"), ("3.10", "3.9"), ("3.9.1", "3.9"), ("3.7.1", "3.7")]
)
def test_other_or_absent_source_scope_cannot_authorize_solvent_treatment(version, grant):
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
    rule, fingerprint = rule_and_scope("3.9")
    source = BrightwayInventory.from_data(
        consumer(rule["source"], 1, 3), context=contexts("ecoinvent", "3.9", "consequential")
    )
    with pytest.raises(MigrationError):
        source.migrate_background(
            contexts("uvek", "2026").background,
            policy=MigrationPolicy(
                approved_compatibility_rules=(fingerprint,),
                on_information_loss=PolicyAction.WARN,
            ),
        )
