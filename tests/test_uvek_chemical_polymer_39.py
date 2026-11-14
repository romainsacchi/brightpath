"""Named phthalic anhydride, defined bulk-PVC mixture and formulated EPDM keep distinct kg bases."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_chemical_polymer_39_review.json").read_text())
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


def test_named_chemical_and_inherited_polymer_definitions_are_explicit():
    assert REVIEW["new_helpers"] == 0 and REVIEW["recipes"] == {} and REVIEW["new_source_omissions"] == []
    assert len(ROWS) == 3
    for row in ROWS:
        assert row["original_rule"] == row["compiled_rule"] and row["source_witness"]["reference_amounts"] == [1]
        assert all(w["reference_amounts"] == [1] for w in row["native_witnesses"].values())
    anhydride, pvc, rubber = ROWS
    facts = anhydride["boundary_checks"]
    assert facts["source_CAS"] == "85-44-9" and facts["source_formula"] == "C8H4O3"
    assert facts["inherited_further_inputs_and_solid_wastes_incomplete"] is True
    assert facts["stoichiometry_yield_or_purity_multiplier_added"] is False
    facts = pvc["boundary_checks"]
    assert facts["native_emulsion_fraction_kg"] == 0.12766 and facts["native_suspension_fraction_kg"] == 0.87234
    assert facts["source_native_nominal_mixture_weights_match"] is True
    assert facts["actual_bulk_route_independently_modelled"] is False
    assert facts["duplicate_mixture_or_polymerisation_helper_added"] is False
    facts = rubber["boundary_checks"]
    assert facts["pure_unvulcanised_rubber"] is False and facts["extrusion_and_vulcanisation_already_included"]
    assert facts["source_mineral_text_and_actual_inventory_conflict"]
    assert facts["source_labelled_zinc_fraction"] == 0.59 and facts["native_primary_zinc_kg"] == 0.0191
    assert facts["standalone_mineral_or_zinc_equivalence_authorized"] is False
    assert facts["extra_filler_or_curing_or_metal_content_multiplier_added"] is False


@pytest.mark.parametrize(
    "source",
    [
        {
            "name": "market for acetoacetic acid",
            "reference product": "acetoacetic acid",
            "location": "GLO",
            "unit": "kilogram",
        },
        {"name": "market for latex", "reference product": "latex", "location": "RER", "unit": "kilogram"},
        {
            "name": "market for zinc concentrate",
            "reference product": "zinc concentrate",
            "location": "GLO",
            "unit": "kilogram",
        },
    ],
)
def test_whole_product_permissions_do_not_admit_other_chemistry_or_internal_feedstock(source):
    scopes = tuple(fp for row in ROWS for fp in policy(row).approved_compatibility_rules)
    combined = MigrationPolicy(approved_compatibility_rules=scopes, on_information_loss=PolicyAction.WARN)
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(consumer(source, 1), context=contexts("ecoinvent", "3.9")).migrate_background(
            contexts("uvek", "2026").background, policy=combined
        )
