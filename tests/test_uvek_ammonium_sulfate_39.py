"""Compound kg becomes native kg nitrogen without changing signs or inventing cobalt."""

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

REVIEW = json.loads((DATA_DIR / "export/uvek_ammonium_sulfate_39_review.json").read_text())
ROWS = REVIEW["records"]


def policy(row, approved=True):
    r = load_uvek_technosphere_resource()
    rule = next(a for a in r["replace"] if a["id"] == row["rule_id"])
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
def test_nitrogen_basis_keeps_nominal_compound_mass_uncertainty_and_separate_emissions(
    row, amount, uncertainty, release
):
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
    edge = result.data[0]["exchanges"][1]
    assert edge["amount"] == pytest.approx(amount * 0.21)
    assert edge["amount"] / REVIEW["basis"]["native_documented_N_fraction"] == pytest.approx(amount)
    assert edge["reference product"] == "Ammonium sulphate, as N, at regional storehouse"
    assert edge["name"] != "Cobalt sulfate" and edge["location"] == "RER" and edge["unit"] == "kilogram"
    expected = deepcopy(before[0]["exchanges"][1])
    scale_exchange(expected, 0.21)
    for key in ("amount", "uncertainty type", "loc", "scale", "negative", "formula", "minimum", "maximum"):
        if key in expected:
            assert edge[key] == expected[key]
    assert result.data[0]["exchanges"][-1] == before[0]["exchanges"][-1]


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
def test_each_market_needs_its_own_exact_permission(row):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], 1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(row, False))


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
@pytest.mark.parametrize(
    "version,model", [("3.8", "cutoff"), ("3.9.1", "cutoff"), ("3.10", "cutoff"), ("3.9", "consequential")]
)
def test_corrected_candidate_does_not_authorize_unreviewed_profiles(row, version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], 1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(row))


@pytest.mark.parametrize("drift", [None, "first_parent", "second_parent", "basis"])
def test_replay_validates_both_parents_before_mutating(drift):
    from scripts.review_uvek_ammonium_sulfate_39 import apply_ammonium_sulfate_39_review

    r = deepcopy(load_uvek_technosphere_resource())
    if drift:
        row = ROWS[1] if drift == "second_parent" else ROWS[0]
        parent = next(a for a in r["replace"] if a["id"] == row["rule_id"])
        if drift == "basis":
            parent["conversion_factor"] = 1
        else:
            parent["confidence"] = 0.01
    before = deepcopy(r)
    if drift:
        with pytest.raises(ValueError):
            apply_ammonium_sulfate_39_review(r)
    else:
        apply_ammonium_sulfate_39_review(r)
    assert r == before
