"""Powder CMC and synthetic SBR latex retain source market delivery and caller bases."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_cmc_latex_markets_39_review.json").read_text())
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
    actual_inputs = [e for e in helper["exchanges"] if e["type"] != "production"]
    expected = REVIEW["recipes"][row["recipe_id"]]["inputs"]
    assert len(actual_inputs) == len(expected)
    for edge, reference in zip(actual_inputs, expected, strict=True):
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


def test_product_aggregate_is_not_an_empty_stock_or_an_extra_solids_factor():
    assert REVIEW["new_helpers"] == 2 and REVIEW["new_source_omissions"] == []
    for row in ROWS:
        facts = row["boundary_checks"]
        assert facts["retained_product_mass_kg"] == pytest.approx(1)
        assert facts["native_aggregated_nonempty_biosphere_inventory"]
        assert facts["native_direct_technosphere_inputs"] == 0
        assert not facts["native_embedded_utilities_exposed_for_relinking"]
        assert not facts["new_solution_solids_or_grade_multiplier"]
        assert not facts["new_physical_omission"]
        assert all(n > 1000 for n in facts["native_nonproduction_counts"].values())
        assert row["source_witness"]["reference_amounts"] == [1]
    cmc, latex = ROWS
    assert cmc["boundary_checks"]["source_producer_count"] == 2
    assert len(cmc["boundary_checks"]["retained_freight_entries"]) == 4
    assert latex["boundary_checks"]["source_producer_count"] == 1
    assert len(latex["boundary_checks"]["retained_freight_entries"]) == 3
    checks = REVIEW["prior312_recorded_primary_hash_checks"]
    assert all(v for k, v in checks.items() if not k.startswith("native2026_"))
    assert checks["native2026_activity_comment_sha256"]
    assert not checks["native2026_inventory_sha256"]
    assert not checks["native2026_metadata_sha256"]


@pytest.mark.parametrize("drift", [None, "first_parent", "last_parent", "recipe"])
def test_delivered_market_replay_is_atomic_and_idempotent(drift):
    from scripts.review_uvek_cmc_latex_markets_39 import apply_cmc_latex_markets_39_review

    resource = deepcopy(load_uvek_technosphere_resource())
    if drift in ("first_parent", "last_parent"):
        rid = ROWS[0 if drift == "first_parent" else -1]["rule_id"]
        next(r for r in resource["replace"] if r["id"] == rid)["confidence"] = 0.01
    elif drift == "recipe":
        resource["recipes"][ROWS[-1]["recipe_id"]]["inputs"][-1]["amount"] *= 2
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_cmc_latex_markets_39_review(resource)
    else:
        apply_cmc_latex_markets_39_review(resource)
    assert resource == before
