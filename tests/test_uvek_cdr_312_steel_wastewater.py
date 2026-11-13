"""Source-version approvals preserve rolled steel and signed wastewater volume."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

RECORDS = json.loads((DATA_DIR / "export/uvek_cdr_312_steel_wastewater_review.json").read_text())["records"]


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


@pytest.mark.parametrize("record", RECORDS)
@pytest.mark.parametrize("amount,uncertainty", [(-2.5, 2), (2.5, 2), (-2.5, 3), (2.5, 3)])
def test_native_service_preserves_reviewed_reference_coordinate_and_uncertainty(record, amount, uncertainty):
    data = consumer(record["source"], amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        contexts("uvek", "2026").background,
        policy=MigrationPolicy(
            approved_compatibility_rules=(scope(record, "3.12"),), on_information_loss=PolicyAction.WARN
        ),
    )
    assert len(result.data) == 1
    edge, original = result.data[0]["exchanges"][1], before[0]["exchanges"][1]
    assert all(edge[k] == v for k, v in record["target"].items())
    factor = record["conversion_factor"]
    assert edge["amount"] == amount * factor
    assert edge["amount"] / record["target_reference_amount"] == amount / record["source_reference_amount"]
    assert edge["scale"] == original["scale"]
    if factor == 1:
        assert all(edge[k] == v for k, v in original.items() if k not in record["source"])
    else:
        assert edge["formula"] == "(dose) * (-1.0)"
        if uncertainty == 2:
            assert edge["loc"] == original["loc"] and edge["negative"] is not original["negative"]
        else:
            assert edge["loc"] == -amount
            assert edge["minimum"] == -original["maximum"] and edge["maximum"] == -original["minimum"]
    assert data == before


@pytest.mark.parametrize("record", RECORDS)
@pytest.mark.parametrize(
    "version,grant,model",
    [
        ("3.12", None, "cutoff"),
        ("3.12", "3.9", "cutoff"),
        ("3.12.1", "3.12", "cutoff"),
        ("3.12", "3.12", "consequential"),
    ],
)
def test_unreviewed_source_profile_cannot_borrow_a_different_approval(record, version, grant, model):
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
