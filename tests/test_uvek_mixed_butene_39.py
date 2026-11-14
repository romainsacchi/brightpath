"""Mixed-butene chemical kg retains complete production and source rail/road freight."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_mixed_butene_39_review.json").read_text())
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
    helper = result.data[1]
    assert helper["brightpath_conversion"]["recipe_id"] == row["recipe_id"]
    inputs = [e for e in helper["exchanges"] if e["type"] != "production"]
    expected = REVIEW["recipes"][row["recipe_id"]]["inputs"]
    assert len(inputs) == len(expected) == 3
    for edge, reference in zip(inputs, expected, strict=True):
        for key, value in reference.items():
            assert edge[key] == value
    assert not helper["brightpath_conversion"].get("source_input_omissions")
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


def test_mixed_butene_stays_a_chemical_mass_not_an_isomer_fuel_or_zero_stock():
    (row,) = ROWS
    facts = row["boundary_checks"]
    assert facts["source_and_native_declared_production_sites"] == 19
    assert facts["source_and_native_declared_naphtha_steam_cracking"]
    assert facts["source_processing_internal_waste_interfaces"] == 9
    assert facts["native_processing_internal_treatment_interfaces"] == 8
    assert not facts["exact_isomer_grade_or_phase_equivalence_established"]
    assert not facts["new_calorific_density_phase_or_purity_factor"]
    assert not facts["new_combustion_or_energy_credit"]
    assert not facts["native_hidden_upstream_energy_exposed_for_relinking"]
    assert facts["retained_product_kg"] == 1
    assert facts["retained_rail_tkm"] == 0.0044 and facts["retained_road_tkm"] == 0.0171
    assert row["source_witness"]["reference_amounts"] == [1]
    assert all(
        w["reference_amounts"] == [1] and w["nonproduction_count"] > 100 for w in row["native_witnesses"].values()
    )
    assert REVIEW["new_helpers"] == 1 and REVIEW["new_source_omissions"] == []


@pytest.mark.parametrize("drift", [None, "parent", "stock", "freight"])
def test_mixed_butene_replay_is_atomic_and_idempotent(drift):
    from scripts.review_uvek_mixed_butene_39 import apply_mixed_butene_39_review

    resource = deepcopy(load_uvek_technosphere_resource())
    (row,) = ROWS
    if drift == "parent":
        next(r for r in resource["replace"] if r["id"] == row["rule_id"])["confidence"] = 0.01
    elif drift in ("stock", "freight"):
        resource["recipes"][row["recipe_id"]]["inputs"][0 if drift == "stock" else -1]["amount"] *= 2
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_mixed_butene_39_review(resource)
    else:
        apply_mixed_butene_39_review(resource)
    assert resource == before
