"""Uncalcined limestone and hydraulic binder retain distinct complete native kg inventories."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from brightpath.migrations.scaling import scale_exchange

REVIEW = json.loads((DATA_DIR / "export/uvek_mineral_gate_stocks_39_review.json").read_text())
ROWS = REVIEW["records"]


def policy(row, approved=True):
    r = load_uvek_technosphere_resource()
    rule = next(x for x in r["replace"] if x["id"] == row["rule_id"])
    fp = compatibility_rule_fingerprint(
        r,
        "replace",
        rule,
        TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    )
    return MigrationPolicy(
        approved_compatibility_rules=(fp,) if approved else (), on_information_loss=PolicyAction.WARN
    )


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_mineral_kg_keeps_signed_demand_uncertainty_and_separate_emissions(row, amount, uncertainty, release):
    data = consumer(row["original_rule"]["source"], amount, uncertainty)
    data[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.5,
        }
    )
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy(row)
    )
    assert data == before and len(result.data) == 1
    actual = result.data[0]["exchanges"][1]
    expected = deepcopy(before[0]["exchanges"][1])
    scale_exchange(expected, 1)
    for k in ("amount", "uncertainty type", "loc", "scale", "negative", "formula", "minimum", "maximum"):
        if k in expected:
            assert actual[k] == expected[k]
    assert all(actual[k] == value for k, value in row["original_rule"]["target"].items())
    assert result.data[0]["exchanges"][-1] == before[0]["exchanges"][-1]


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
def test_exact_39_service_requires_its_own_approval(row):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], -1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(row, False))


@pytest.mark.parametrize(
    "version,model", [("3.8", "cutoff"), ("3.9.1", "cutoff"), ("3.10", "cutoff"), ("3.9", "consequential")]
)
@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
def test_39_scope_does_not_expand_to_other_profiles(row, version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], -1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(row))


def test_distinct_minerals_and_no_market_delivery_or_added_processing():
    resource = load_uvek_technosphere_resource()
    for row in ROWS:
        assert (
            next(r for r in resource["replace"] if r["id"] == row["rule_id"])
            == row["original_rule"]
            == row["compiled_rule"]
        )
        assert row["source_witness"]["reference_amounts"] == [1]
        assert all(w["reference_amounts"] == [1] for w in row["native_witnesses"].values())
        assert row["boundary_checks"]["source_market_freight_count"] == 0
        assert row["boundary_checks"]["source_market_producer_amount"] == 1
        assert row["boundary_checks"]["additional_source_processing_or_delivery"] is False
        assert row["boundary_checks"]["moisture_or_stoichiometric_multiplier"] is False
    assert len({r["compiled_rule"]["target"]["name"] for r in ROWS}) == 2
    assert REVIEW["new_helpers"] == 0 and REVIEW["new_source_omissions"] == []
