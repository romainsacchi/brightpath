"""Hydrogen portfolio and borohydride product references retain native allocation and caller mass."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_hydrogen_borohydride_39_review.json").read_text())
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
    assert len(inputs) == len(expected)
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


def test_hydrogen_portfolio_keeps_cells_minor_supply_and_light_freight():
    row = ROWS[0]
    recipe = REVIEW["recipes"][row["recipe_id"]]
    assert len(recipe["inputs"]) == 11
    stocks = [e for e in recipe["inputs"] if e["unit"] == "kilogram"]
    assert len(stocks) == 8 and sum(e["amount"] for e in stocks) == pytest.approx(1)
    cells = {
        e["name"]: e["amount"]
        for e in stocks
        if any(c in e["name"] for c in ("diaphragm cell", "membrane cell", "mercury cell"))
    }
    assert cells == {
        "Hydrogen, liquid, diaphragm cell, at plant": 0.004474019133043309,
        "Hydrogen, liquid, membrane cell, at plant": 0.02108684986736691,
        "Hydrogen, liquid, mercury cell, at plant": 0.00655495826469136,
    }
    assert next(e["amount"] for e in stocks if e["name"] == "Hydrogen, cracking, APME, at plant") == 0.9673359749371764
    minor = [e for e in stocks if e["name"] == "Hydrogen, liquid, at plant"]
    assert len(minor) == 4
    assert sum(e["amount"] for e in minor) == pytest.approx(
        row["boundary_checks"]["minor_coproduct_proxy_mass_kg"], rel=1e-14, abs=1e-18
    )
    (lcv,) = [e for e in recipe["inputs"] if "3.5t gross weight" in e["name"]]
    assert lcv["amount"] == 0.00018 and lcv["unit"] == "ton kilometer"
    assert not row["boundary_checks"]["native_full_mix_applied_to_entire_market"]
    assert not row["boundary_checks"][
        "actual_cryogenic_specification_or_liquefaction_coverage_independently_established"
    ]
    assert not row["boundary_checks"]["new_phase_or_allocation_or_LHV_factor"]


def test_borohydride_reference_wins_over_conflicting_comment_without_allocation_rescaling():
    row = ROWS[1]
    facts = row["boundary_checks"]
    assert facts["source_comment_wrongly_names_trimethyl_borate_reference"]
    assert facts["source_reference_dry_mass_kg"] == facts["source_reference_wet_mass_kg"] == 1
    assert facts["source_reference_water_content"] == 0
    assert facts["source_reference_allocation_factor"] == 0.9238425898812566
    assert facts["electricity_diagnostic_ratio"] == pytest.approx(4.862329420427667)
    assert not facts["diagnostic_ratio_used_as_product_factor"]
    assert not facts["native_precursor_solution_basis_independently_approved_here"]
    inputs = REVIEW["recipes"][row["recipe_id"]]["inputs"]
    assert len(inputs) == 3 and inputs[0]["amount"] == 1
    assert inputs[0]["name"] == "Sodium tetrahydroborate, at plant"
    assert [e["amount"] for e in inputs[1:]] == [0.09273, 0.06264]
    assert all(row["compiled_rule"]["target"] == row["original_rule"]["target"] for row in ROWS)
    assert all(row["source_witness"]["reference_amounts"] == [1] for row in ROWS)
    assert REVIEW["new_source_omissions"] == [] and REVIEW["new_helpers"] == 2


@pytest.mark.parametrize("drift", [None, "first_parent", "last_parent", "cell", "minor", "freight", "borohydride"])
def test_both_market_replays_are_atomic_and_idempotent(drift):
    from scripts.review_uvek_hydrogen_borohydride_39 import apply_hydrogen_borohydride_39_review

    resource = deepcopy(load_uvek_technosphere_resource())
    if drift in ("first_parent", "last_parent"):
        rid = ROWS[0 if drift == "first_parent" else -1]["rule_id"]
        next(r for r in resource["replace"] if r["id"] == rid)["confidence"] = 0.01
    elif drift in ("cell", "minor", "freight", "borohydride"):
        inputs = resource["recipes"][ROWS[1 if drift == "borohydride" else 0]["recipe_id"]]["inputs"]
        index = {"cell": 0, "minor": 1, "freight": -2, "borohydride": 0}[drift]
        inputs[index]["amount"] *= 2
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_hydrogen_borohydride_39_review(resource)
    else:
        apply_hydrogen_borohydride_39_review(resource)
    assert resource == before
