"""The complete steel dismantling service preserves signed waste demand."""

from copy import deepcopy
import hashlib
import json

import pytest

from brightpath import BrightwayInventory, DATA_DIR, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy
from brightpath.migrations.resources import load_uvek_technosphere_resource
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

REVIEW = json.loads((DATA_DIR / "export/uvek_pilot_nitrate_steel_recycling_review.json").read_text())


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_direct_services_keep_signed_units_uncertainty_and_separate_emissions(row, amount, uncertainty):
    data = consumer(row["source"], amount, uncertainty)
    emission = {
        "name": "Carbon dioxide, fossil",
        "categories": ["air"],
        "unit": "kilogram",
        "type": "biosphere",
        "amount": 7,
    }
    data[0]["exchanges"].append(emission)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9"))
    result = inventory.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert inventory.data == data == before
    assert len(result.data) == 1
    actual = result.data[0]["exchanges"][1]
    assert all(actual[k] == v for k, v in row["target"].items())
    original = before[0]["exchanges"][1]
    assert actual["amount"] == -amount
    assert actual["formula"] == "(dose) * (-1.0)"
    assert actual["uncertainty type"] == original["uncertainty type"]
    assert actual["scale"] == original["scale"]
    if uncertainty == 2:
        assert actual["loc"] == original["loc"]
        assert actual["negative"] is not original["negative"]
    else:
        assert actual["loc"] == -original["loc"]
        assert actual["minimum"] == -original["maximum"]
        assert actual["maximum"] == -original["minimum"]
    assert result.data[0]["exchanges"][-1] == emission
    assert not any(i.code == "migration.recipe_input_omission" for i in result.last_migration_report.issues)


def test_direct_service_evidence_binds_unchanged_rules_and_exact_prior_authorities():
    resource = load_uvek_technosphere_resource()
    rules = {r["id"]: r for r in resource["replace"]}
    assert len(REVIEW["records"]) == 1
    assert len({r["fingerprint"] for r in REVIEW["records"]}) == 1
    for authority in REVIEW["authorization_evidence"]:
        assert hashlib.sha256((DATA_DIR / authority["path"]).read_bytes()).hexdigest() == authority["sha256"]
    for row in REVIEW["records"]:
        rule = rules[row["rule_id"]]
        assert hashlib.sha256(json.dumps(rule, sort_keys=True).encode()).hexdigest() == row["raw_rule_sha256"]
        assert row["source_reference_amount"] == row["conversion_factor"] == -1
        assert row["native_reference_amounts"] == {"2025": 1, "2026": 1}
        assert row["native2025_2026_direct_technosphere_and_uncertainty_match"]
        assert row["source_operating_amount_MJ_per_kg"] == row["native_operating_amount_MJ_per_kg"] == 0.626
        assert row["source_internal_uncertainty_scale"] != row["native_internal_uncertainty_scale"]
        assert rule["target"] == row["target"] and "recipe_ref" not in rule["target"]
        operator_review = json.loads((DATA_DIR / REVIEW["authorization_evidence"][0]["path"]).read_text())
        operator = next(r for r in operator_review["records"] if r["rule_id"] == row["operating_authority_rule"])
        assert row["operating_service_witnesses"] == [operator["source_witness"], *operator["native_witnesses"]]
        assert (
            compatibility_rule_fingerprint(
                resource,
                "replace",
                rule,
                TechnosphereProfile(**row["source_profile"]),
                TechnosphereProfile(**row["bridge_target_profile"]),
            )
            == row["fingerprint"]
        )
