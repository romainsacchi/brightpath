"""Matching nitrate services preserve equipment and processing reference boundaries."""

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

REVIEW = json.loads((DATA_DIR / "export/uvek_pilot_nitrate_matching_service_review.json").read_text())


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
    for field in ("amount", "formula", "uncertainty type", "loc", "scale", "negative", "minimum", "maximum"):
        if field in before[0]["exchanges"][1]:
            assert actual[field] == before[0]["exchanges"][1][field]
    assert result.data[0]["exchanges"][-1] == emission
    assert not any(i.code == "migration.recipe_input_omission" for i in result.last_migration_report.issues)


def test_direct_service_evidence_binds_unchanged_rules_and_exact_prior_authorities():
    resource = load_uvek_technosphere_resource()
    rules = {r["id"]: r for r in resource["replace"]}
    assert len(REVIEW["records"]) == 4
    assert len({r["fingerprint"] for r in REVIEW["records"]}) == 4
    for authority in REVIEW["authorization_evidence"]:
        assert hashlib.sha256((DATA_DIR / authority["path"]).read_bytes()).hexdigest() == authority["sha256"]
    for row in REVIEW["records"]:
        rule = rules[row["rule_id"]]
        assert hashlib.sha256(json.dumps(rule, sort_keys=True).encode()).hexdigest() == row["raw_rule_sha256"]
        assert row["source_reference_amount"] == row["conversion_factor"] == 1
        assert row["native_reference_amounts"] == {"2025": 1, "2026": 1}
        assert row["matching_function_evidence"]["native2025_2026_direct_technosphere_vectors_match"]
        assert rule["target"] == row["target"] and "recipe_ref" not in rule["target"]
        assert row["matching_function_evidence"]["market_only_unit_positive_supply"]
        assert row["matching_function_evidence"]["positive_source_producer_reference_count"] > 0
        assert row["source_producer_witnesses"]
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
