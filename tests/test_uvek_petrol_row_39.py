"""Fossil petrol uses complete delivered supply with explicit model differences."""

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

REVIEW = json.loads((DATA_DIR / "export/uvek_petrol_row_39_review.json").read_text())
ROW = REVIEW["records"][0]


def policy(approved=True):
    r = load_uvek_technosphere_resource()
    rule = next(x for x in r["replace"] if x["id"] == ROW["rule_id"])
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


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_whole_service_keeps_signed_demand_uncertainty_and_separate_emissions(amount, uncertainty, release):
    data = consumer(ROW["original_rule"]["source"], amount, uncertainty)
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
        contexts("uvek", release).background, policy=policy()
    )
    assert data == before and len(result.data) == 1
    actual = result.data[0]["exchanges"][1]
    expected = deepcopy(before[0]["exchanges"][1])
    scale_exchange(expected, 1)
    for k in ("amount", "uncertainty type", "loc", "scale", "negative", "formula", "minimum", "maximum"):
        if k in expected:
            assert actual[k] == expected[k]
    assert all(actual[k] == value for k, value in ROW["compiled_rule"]["target"].items())
    assert result.data[0]["exchanges"][-1] == before[0]["exchanges"][-1]


def test_exact_39_service_requires_its_own_approval():
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROW["original_rule"]["source"], -1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(False))


@pytest.mark.parametrize(
    "version,model", [("3.8", "cutoff"), ("3.9.1", "cutoff"), ("3.10", "cutoff"), ("3.9", "consequential")]
)
def test_39_scope_does_not_expand_to_other_profiles(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROW["original_rule"]["source"], -1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


def test_complete_fossil_delivery_boundary_and_loss_model_are_explicit():
    r = load_uvek_technosphere_resource()
    assert next(x for x in r["replace"] if x["id"] == ROW["rule_id"]) == ROW["compiled_rule"]
    assert ROW["compiled_rule"]["target"]["name"] == "Petrol, fossil, at regional storage"
    assert ROW["source_witness"]["reference_amounts"] == [1]
    assert all(w["reference_amounts"] == [1] for w in ROW["native_witnesses"].values())
    assert REVIEW["source_inventory_replaced_whole"] is True
    assert (REVIEW["source_internal_nonproduction_rows"], REVIEW["native_nonproduction_rows"]) == (122, 28)
    assert REVIEW["source_declared_nonfossil_carbon_kg_per_kg"] == 0
    assert [x["amount"] for x in REVIEW["native_fossil_blend"]] == [0.25735, 0.7249, 0.018944]
    assert REVIEW["native_blend_and_loss_input_mass_kg"] == pytest.approx(1.001194)
    assert REVIEW["source_distribution_electricity_kWh_per_kg"] == 0.0067
    assert REVIEW["native_distribution_electricity_kWh_per_kg"] == 0.050762
    bio = REVIEW["direct_biosphere_comparison_2026"]
    assert (bio["source_count"], bio["native_count"], bio["exact_amount_match_count"]) == (10, 11, 0)
    assert len(bio["quantity_differences"]) == 10
    assert REVIEW["new_caller_exchange_omissions"] == []


@pytest.mark.parametrize("drift", [None, "parent", "target"])
def test_replay_is_atomic_and_does_not_inherit_changed_fuel_boundaries(drift):
    from scripts.review_uvek_petrol_row_39 import apply_petrol_row_39_review

    r = deepcopy(load_uvek_technosphere_resource())
    rule = next(a for a in r["replace"] if a["id"] == ROW["rule_id"])
    if drift == "parent":
        rule["confidence"] = 0.01
    elif drift == "target":
        rule["target"]["name"] = "Petrol, at refinery"
    before = deepcopy(r)
    if drift:
        with pytest.raises(ValueError):
            apply_petrol_row_39_review(r)
    else:
        apply_petrol_row_39_review(r)
    assert r == before
