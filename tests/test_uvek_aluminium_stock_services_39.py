"""Primary aluminium kg, rolling-service kg and anodising m2 retain distinct interfaces."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_aluminium_stock_services_39_review.json").read_text())
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


def test_stock_mass_and_area_service_boundaries_are_explicit():
    assert REVIEW["new_helpers"] == 0 and REVIEW["recipes"] == {} and REVIEW["new_source_omissions"] == []
    assert len(ROWS) == 3
    for row in ROWS:
        assert row["original_rule"] == row["compiled_rule"]
        assert row["source_witness"]["reference_amounts"] == [1]
        assert all(w["reference_amounts"] == [1] for w in row["native_witnesses"].values())
    stock, anodising, rolling = ROWS
    facts = stock["boundary_checks"]
    assert (
        facts["source_market_producer_entries"] == 8 and facts["source_positive_deterministic_material_total_kg"] == 1
    )
    assert facts["source_separate_delivery_entries"] == facts["source_direct_biosphere_entries"] == 0
    assert facts["native_liquid_primary_input_kg"] == 1.019
    assert (
        facts["extra_casting_yield_or_loss_multiplier_added"] is False
        and facts["redundant_same_supplier_helper_added"] is False
    )
    assert len(stock["source_producer_witnesses"]) == 8
    facts = anodising["boundary_checks"]
    assert facts["reference_area_m2"] == 1 and facts["oxide_coating_micrometres"] == 20
    assert facts["source_nitric_acid_active_kg"] == facts["native_labelled_half_strength_nitric_solution_kg"] == 0.03
    assert facts["source_wastewater_m3"] == 0.459375 and facts["native_wastewater_m3"] == 0.75
    assert facts["whole_process_acid_or_sheet_mass_multiplier_added"] is False
    assert rolling["boundary_checks"]["main_metal_excluded"]
    assert rolling["boundary_checks"]["internal_aluminium_replacement_kg"] == 0.012
    assert rolling["boundary_checks"]["extra_main_stock_yield_or_heat_multiplier_added"] is False


def test_three_inputs_stay_independent_without_a_duplicate_stock_or_helper():
    quantities = [2.5, 1.0, 2.5]
    data = consumer(ROWS[0]["original_rule"]["source"], quantities[0], 3)
    for row, amount in zip(ROWS[1:], quantities[1:], strict=True):
        data[0]["exchanges"].append({**row["original_rule"]["source"], "type": "technosphere", "amount": amount})
    before = deepcopy(data)
    scopes = tuple(fp for row in ROWS for fp in policy(row).approved_compatibility_rules)
    combined_policy = MigrationPolicy(approved_compatibility_rules=scopes, on_information_loss=PolicyAction.WARN)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", "2026").background, policy=combined_policy
    )
    assert data == before and len(result.data) == 1 and len(result.data[0]["exchanges"]) == 4
    for edge, row, amount in zip(result.data[0]["exchanges"][1:], ROWS, quantities, strict=True):
        assert all(edge[k] == v for k, v in row["compiled_rule"]["target"].items())
        assert edge["amount"] == amount


def test_anodising_area_cannot_be_read_as_processed_metal_kg():
    row = ROWS[1]
    source = dict(row["original_rule"]["source"], unit="kilogram")
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(consumer(source, 1), context=contexts("ecoinvent", "3.9")).migrate_background(
            contexts("uvek", "2026").background, policy=policy(row)
        )
