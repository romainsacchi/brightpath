"""Average metalworking markets keep processing-service and principal-metal ownership distinct."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_metalworking_markets_39_review.json").read_text())
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
def test_complete_material_or_freight_preserves_caller_and_uncertainty(row, amount, uncertainty, release):
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
    assert data == before and len(result.data) == (2 if row["recipe_id"] else 1)
    assert len(result.data[0]["exchanges"]) == 3
    actual = result.data[0]["exchanges"][1]
    for field in ("amount", "uncertainty type", "loc", "scale", "negative", "formula", "minimum", "maximum"):
        if field in before[0]["exchanges"][1]:
            assert actual[field] == before[0]["exchanges"][1][field]
    assert all(actual[k] == v for k, v in row["compiled_rule"]["target"].items())
    assert result.data[0]["exchanges"][-1] == emission


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
def test_material_or_freight_needs_its_own_exact_approval(row):
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


def test_processing_services_exclude_main_stock_and_keep_loss_allowance_once():
    assert len(ROWS) == 2 and REVIEW["new_helpers"] == 0
    assert REVIEW["new_source_omissions"] == []
    for row in ROWS:
        assert row["compiled_rule"] == row["original_rule"]
        assert row["recipe_id"] is None
        facts = row["boundary_checks"]
        assert facts["source_market_producer_count"] == 2
        assert facts["source_market_supply_total_kg"] == pytest.approx(1)
        assert not facts["principal_stock_included"] and not facts["degreasing_included"]
        assert facts["additional_processing_loss_metal_kg"] == 0.227
        assert facts["factory_units_per_service_kg"] == 4.58e-10
        assert facts["machine_kg_per_service_kg"] == 3.95e-5
        assert not facts["additional_stock_yield_or_scrap_credit_added"]
        assert facts["source_market_actual_freight_and_biosphere_count"] == 0
        assert row["source_witness"]["reference_amounts"] == [1]
        assert all(w["reference_amounts"] == [1] for w in row["native_witnesses"].values())


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
def test_caller_principal_stock_is_not_replaced_by_or_added_to_processing(row):
    stock = {
        "name": "Caller-owned principal metal stock",
        "reference product": "principal metal",
        "location": "CH",
        "unit": "kilogram",
    }
    data = consumer(row["original_rule"]["source"], 1)
    data[0]["exchanges"].append({**stock, "type": "technosphere", "amount": 1.0, "formula": "principal_mass"})
    data.append({**stock, "exchanges": [{**stock, "type": "production", "amount": 1.0}]})
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", "2026").background, policy=policy(row)
    )
    assert data == before and len(result.data) == 2
    assert result.data[0]["exchanges"][-1] == before[0]["exchanges"][-1]
    assert result.data[1] == before[1]
    assert len(result.data[0]["exchanges"]) == 3
