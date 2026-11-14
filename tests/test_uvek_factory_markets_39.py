"""Defined construction-module units remain distinct from product mass, operation and capacity."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_factory_markets_39_review.json").read_text())
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


def test_defined_factory_module_boundaries_and_source_limits_are_explicit():
    assert (
        len(ROWS) == 7
        and REVIEW["new_helpers"] == 0
        and REVIEW["recipes"] == {}
        and REVIEW["new_source_omissions"] == []
    )
    for row in ROWS:
        assert row["original_rule"] == row["compiled_rule"] and row["source_witness"]["reference_amounts"] == [1]
        assert row["original_rule"]["source"]["unit"] == row["compiled_rule"]["target"]["unit"] == "unit"
        assert all(p["reference_amounts"] == [1] for p in row["source_producer_witnesses"])
        assert all(p["reference_amounts"] == [1] for p in row["native_witnesses"].values())
        f = row["boundary_checks"]
        assert f["source_deterministic_construction_total_units"] == 1
        assert f["source_final_delivery_or_direct_biosphere_entries"] == 0
        assert (
            f["redundant_market_helper_added"] is False and f["extra_capacity_lifetime_or_site_scaling_added"] is False
        )
        assert f["selected_geometry_checks"]
    by_purpose = {r["boundary_checks"]["purpose"]: r["boundary_checks"] for r in ROWS}
    assert (
        by_purpose["alumina_calcination_and_storage_infrastructure_only"]["whole_integrated_bauxite_refinery_included"]
        is False
    )
    assert by_purpose["hot_water_tank_factory_building_and_land_only"]["machinery_included"] is False
    assert (
        by_purpose["metalworking_factory_buildings_roads_and_land"]["machine_processing_and_operation_included"]
        is False
    )
    assert by_purpose["packaging_box_production_site_infrastructure"]["one_finished_box_included"] is False
    f = by_purpose["defined_electronic_component_production_site"]
    assert f["native_throughput_metadata_internally_inconsistent"] is True
    assert f["source_extra_unspecified_cable_manufacture_kg"] == f["native_end_of_life_cable_treatment_kg"] == 1968800


def test_a_factory_module_unit_is_not_finished_box_kg():
    row = next(r for r in ROWS if r["rule_id"] == "bp-67b96639de18a3f9")
    source = dict(row["original_rule"]["source"], unit="kilogram")
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(consumer(source, 1), context=contexts("ecoinvent", "3.9")).migrate_background(
            contexts("uvek", "2026").background, policy=policy(row)
        )
