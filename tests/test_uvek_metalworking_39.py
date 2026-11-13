"""Metalworking is a processing service; caller-owned bulk metal remains separate."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_metalworking_39_review.json").read_text())
ROWS = REVIEW["records"]


def policy(row, approved=True):
    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
    fingerprint = compatibility_rule_fingerprint(
        resource,
        "replace",
        rule,
        TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    )
    return MigrationPolicy(
        approved_compatibility_rules=(fingerprint,) if approved else (),
        on_information_loss=PolicyAction.WARN,
    )


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_complete_grid_preserves_caller_and_uncertainty(row, amount, uncertainty, release):
    data = consumer(row["original_rule"]["source"], amount, uncertainty)
    emission = {
        "name": "Carbon dioxide, fossil",
        "categories": ["air"],
        "unit": "kilogram",
        "type": "biosphere",
        "amount": 0.5,
    }
    data[0]["exchanges"].append(emission)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy(row)
    )
    assert data == before and len(result.data) == 1
    assert len(result.data[0]["exchanges"]) == 3
    actual = result.data[0]["exchanges"][1]
    for field in ("amount", "uncertainty type", "loc", "scale", "negative", "formula", "minimum", "maximum"):
        if field in before[0]["exchanges"][1]:
            assert actual[field] == before[0]["exchanges"][1][field]
    assert all(actual[k] == v for k, v in row["compiled_rule"]["target"].items())
    assert result.data[0]["exchanges"][-1] == emission


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
def test_grid_needs_its_own_exact_approval(row):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], 1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(row, False))


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
@pytest.mark.parametrize(
    "version,model", [("3.8", "cutoff"), ("3.9.1", "cutoff"), ("3.10", "cutoff"), ("3.9", "consequential")]
)
def test_new_scope_does_not_authorize_other_profiles(row, version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], 1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(row))


def test_all_rules_and_default_targets_remain_unchanged():
    resource = load_uvek_technosphere_resource()
    for row in ROWS:
        assert row["original_rule"] == row["compiled_rule"]
        assert next(r for r in resource["replace"] if r["id"] == row["rule_id"]) == row["original_rule"]
        assert row["source_witness"]["reference_amounts"] == [1]
        assert all(w["reference_amounts"] == [1] for w in row["native_witnesses"].values())
        checks = row["boundary_checks"]
        assert checks["metal_class_quantities_match"]
        assert checks["native_loss_metal_total_kg_per_service_kg"] < 0.25
        assert checks["main_metal_and_degreasing_excluded"]
    assert REVIEW["new_helpers"] == 0 and REVIEW["new_source_omissions"] == []


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
def test_separate_bulk_metal_is_not_consumed_or_duplicated_by_service_mapping(row):
    data = consumer(row["original_rule"]["source"], 1)
    stock = {
        "name": "Caller-owned semi-finished stock",
        "reference product": "semi-finished stock",
        "location": "CH",
        "unit": "kilogram",
    }
    bulk = {
        **stock,
        "type": "technosphere",
        "amount": 0.9,
        "uncertainty type": 3,
        "loc": 0.9,
        "scale": 0.05,
        "formula": "bulk_kg",
    }
    data[0]["exchanges"].append(bulk)
    data.append({**stock, "exchanges": [{**stock, "type": "production", "amount": 1}]})
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", "2026").background, policy=policy(row)
    )
    assert data == before and len(result.data) == 2
    assert len(result.data[0]["exchanges"]) == 3
    assert result.data[0]["exchanges"][2] == bulk
    assert result.data[1] == before[1]
