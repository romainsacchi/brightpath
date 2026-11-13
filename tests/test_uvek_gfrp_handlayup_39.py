"""Finished GFRP material retains both producer shares and final light/heavy/sea delivery."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_gfrp_handlayup_39_review.json").read_text())
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
    if row["recipe_id"]:
        helper = result.data[1]
        assert helper["brightpath_conversion"]["recipe_id"] == row["recipe_id"]
        edges = [e for e in helper["exchanges"] if e["type"] != "production"]
        assert len(edges) == 5
        assert [e["amount"] for e in edges] == [0.330592351639303, 0.669407648360697, 0.0246, 0.3857, 1.1439]
        for edge, trace in zip(edges, REVIEW["retained_market_interfaces"][row["recipe_id"]], strict=True):
            assert all(edge[k] == v for k, v in trace["statistics"].items())
            assert all(edge[k] == v for k, v in trace["target"].items())
    else:
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


@pytest.mark.parametrize("drift", [None, 0, 1, 2, 3, "delivery"])
def test_frozen_replay_is_atomic_and_idempotent(drift):
    from scripts.review_uvek_gfrp_handlayup_39 import apply_gfrp_handlayup_39_review

    resource = deepcopy(load_uvek_technosphere_resource())
    if drift == "delivery":
        resource["recipes"][ROWS[0]["recipe_id"]]["inputs"][2]["amount"] *= 2
    elif drift is not None:
        next(r for r in resource["replace"] if r["id"] == ROWS[drift]["rule_id"])["confidence"] = 0.01
    before = deepcopy(resource)
    if drift is None:
        apply_gfrp_handlayup_39_review(resource)
    else:
        with pytest.raises(ValueError):
            apply_gfrp_handlayup_39_review(resource)
    assert resource == before


def test_material_contents_waste_reference_and_freight_class_are_explicit():
    assert REVIEW["new_source_omissions"] == []
    assert len(REVIEW["recipes"]) == REVIEW["new_helpers"] == 1
    assert len(ROWS) == 4
    for row in ROWS:
        assert row["source_witness"]["reference_amounts"] == [1]
        assert row["conversion_factor"] == 1
        assert row["original_rule"]["target"] == row["compiled_rule"]["target"]
    for row in ROWS[1:3]:
        assert row["original_rule"] == row["compiled_rule"]
        facts = row["boundary_checks"]
        assert facts["kind"] == "complete_manufactured_composite"
        assert facts["nominal_glass_fibre_input_kg"] == 0.632
        assert facts["nominal_polyester_resin_input_kg"] == 0.421
        assert facts["mineral_wool_waste_kg"] == 0.0526
        assert facts["aromatic_emission_kg"] == 6.25e-5
        assert facts["direct_energy_data_available"] is False
    van = ROWS[3]
    assert van["original_rule"] == van["compiled_rule"]
    assert van["boundary_checks"]["kind"] == "complete_light_commercial_freight"
    assert van["boundary_checks"]["target_gross_vehicle_tonnes"] == 3.5
    assert van["boundary_checks"]["payload_or_vehicle_km_multiplier_added"] is False
    assert all(w["reference_amounts"] == [1] for w in van["native_witnesses"].values())
    assert all(
        c["aromatic_emission_and_compartment_match_native2026"] for c in REVIEW["source_aromatic_emission_checks"]
    )
    inputs = REVIEW["recipes"][ROWS[0]["recipe_id"]]["inputs"]
    assert "3.5t" in inputs[2]["name"]
    assert inputs[3]["name"] == "Transport, freight, lorry, fleet average"
    assert inputs[2]["location"] == "CH" and inputs[3]["location"] == "RER"
    assert inputs[4]["unit"] == "ton kilometer"
