"""Reviewed chiller and turbine construction retain exact source-profile scopes."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

RECORDS = json.loads((DATA_DIR / "export/uvek_cdr_equipment_profiles_review.json").read_text())["records"]
CASES = [(r, p["source_profile"]["version"]) for r in RECORDS for p in r["source_profiles"]]


def scope(record, version):
    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == record["rule_id"])
    assert rule == record["rule"]
    return compatibility_rule_fingerprint(
        resource,
        "replace",
        rule,
        TechnosphereProfile("ecoinvent", version, "cutoff"),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    )


@pytest.mark.parametrize("record,version", CASES)
@pytest.mark.parametrize("amount,uncertainty", [(-2.5, 2), (2.5, 2), (-2.5, 3), (2.5, 3)])
def test_complete_equipment_preserves_signed_units_formula_statistics_and_caller(record, version, amount, uncertainty):
    data = consumer(record["source"], amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version)).migrate_background(
        contexts("uvek", "2026").background,
        policy=MigrationPolicy(
            approved_compatibility_rules=(scope(record, version),), on_information_loss=PolicyAction.WARN
        ),
    )
    assert len(result.data) == 1
    edge = result.data[0]["exchanges"][1]
    assert all(edge[k] == v for k, v in record["target"].items())
    original = before[0]["exchanges"][1]
    assert all(edge[k] == v for k, v in original.items() if k not in record["source"])
    assert data == before
    assert any(i.code == "migration.approved_correspondence" for i in result.last_migration_report.issues)


@pytest.mark.parametrize("record", RECORDS)
@pytest.mark.parametrize(
    "version,grant,model",
    [
        ("3.9", None, "cutoff"),
        ("3.9", "3.6", "cutoff"),
        ("3.9.1", "3.9", "cutoff"),
        ("3.7", "3.6", "cutoff"),
        ("3.7.1", "3.6", "cutoff"),
        ("3.9", "3.9", "consequential"),
    ],
)
def test_unreviewed_version_patch_or_system_model_cannot_borrow_equipment_approval(record, version, grant, model):
    data = consumer(record["source"], 1, 3)
    before = deepcopy(data)
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model)).migrate_background(
            contexts("uvek", "2026").background,
            policy=MigrationPolicy(
                approved_compatibility_rules=(scope(record, grant),) if grant else (),
                on_information_loss=PolicyAction.WARN,
            ),
        )
    assert data == before
