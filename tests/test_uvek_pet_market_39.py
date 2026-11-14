"""Whole-market PET screening retains mass and freight with explicit recycled-content limits."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_pet_market_39_review.json").read_text())
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
    assert len(result.data) == 2
    helper = result.data[1]
    assert helper["brightpath_conversion"]["recipe_id"] == row["recipe_id"]
    inputs = [e for e in helper["exchanges"] if e["type"] != "production"]
    expected = REVIEW["recipes"][row["recipe_id"]]["inputs"]
    assert len(inputs) == len(expected) == 10
    for edge, original in zip(inputs, expected, strict=True):
        for key, value in original.items():
            assert edge[key] == value
    assert not any("Recycling PET" in e.get("name", "") for e in inputs)
    assert sum(e["amount"] for e in inputs if e["unit"] == "kilogram") == pytest.approx(1)
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


def test_recycled_feedstock_substitution_is_explicit_and_no_empty_handover_is_used():
    (row,) = ROWS
    facts = row["boundary_checks"]
    assert row["compiled_rule"]["target"] == row["original_rule"]["target"]
    assert (
        row["compiled_rule"]["reference_flow_sign_review"]["profiles"][:-1]
        == row["original_rule"]["reference_flow_sign_review"]["profiles"]
    )
    assert REVIEW["new_helpers"] == 1 and REVIEW["new_source_omissions"] == []
    assert facts["source_recycled_entries"] == 4
    assert facts["source_recycled_mass_kg"] == pytest.approx(0.000999000999001)
    assert facts["recycled_share_represented_by_virgin_resin"]
    assert facts["source_producer_entries_preserved_individually"]
    assert not facts["empty_recycling_handover_used"]
    assert not facts["new_mass_omission_or_renormalization"]
    assert all(n > 0 for n in facts["source_recycled_production_nonproduction_counts"].values())
    assert all(w["nonproduction_count"] == 0 for w in REVIEW["unused_empty_native_recycling_controls"].values())
    assert facts["source_freight_amounts_and_supported_uncertainty_preserved"]
    assert row["source_witness"]["reference_amounts"] == [1]
    for target in row["native_witnesses"].values():
        for witness in target["native_witnesses"].values():
            assert witness["reference_amounts"] == [1] and witness["nonproduction_count"] > 0


def test_whole_market_scope_does_not_authorize_standalone_recycled_PET():
    source = {
        "name": "polyethylene terephthalate production, granulate, amorphous, recycled",
        "reference product": "polyethylene terephthalate, granulate, amorphous, recycled",
        "location": "CH",
        "unit": "kilogram",
    }
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(consumer(source, 1), context=contexts("ecoinvent", "3.9")).migrate_background(
            contexts("uvek", "2026").background, policy=policy(ROWS[0])
        )


@pytest.mark.parametrize("drift", [None, "parent", "old_recipe", "new_recipe", "old_card"])
def test_pet_profile_replay_is_atomic_and_idempotent(drift):
    from scripts.review_uvek_pet_market_39 import apply_pet_market_39_review

    r = deepcopy(load_uvek_technosphere_resource())
    if drift == "parent":
        next(a for a in r["replace"] if a["id"] == ROWS[0]["rule_id"])["confidence"] = 0.01
    elif drift in ("old_recipe", "new_recipe"):
        key = next(iter(REVIEW["recipe_dependencies"])) if drift == "old_recipe" else ROWS[0]["recipe_id"]
        r["recipes"][key]["inputs"][0]["amount"] *= 2
    elif drift == "old_card":
        ref = next(iter(REVIEW["retired_review_cards"]))
        r["reviews"][ref] = {"rationale": "changed"}
    before = deepcopy(r)
    if drift:
        with pytest.raises(ValueError):
            apply_pet_market_39_review(r)
    else:
        apply_pet_market_39_review(r)
    assert r == before
