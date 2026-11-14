"""Petroleum kerosene uses complete delivered supply with explicit model differences."""

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

REVIEW = json.loads((DATA_DIR / "export/uvek_kerosene_delivery_39_review.json").read_text())
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


def test_complete_kerosene_delivery_boundary_is_not_refinery_only():
    resource = load_uvek_technosphere_resource()
    assert next(x for x in resource["replace"] if x["id"] == ROW["rule_id"]) == ROW["compiled_rule"]
    target = ROW["compiled_rule"]["target"]
    assert (target["name"], target["location"], target["unit"]) == ("Kerosene, at regional storage", "RER", "kilogram")
    assert ROW["source_witness"]["reference_amounts"] == [1]
    assert all(w["reference_amounts"] == [1] for w in ROW["native_witnesses"].values())
    facts = ROW["boundary_checks"]
    assert facts["whole_delivery_proxy"] and facts["native_petroleum_only_supplier"]
    assert facts["native_refinery_kg_per_delivered_kg"] == 1.0001
    assert facts["source_distribution_electricity_kWh_per_kg"] == 0.0067
    assert facts["native_distribution_electricity_kWh_per_kg"] == 0.050762
    assert facts["source_NMVOC_kg_per_kg"] == 0.00001
    assert facts["native_NMVOC_kg_per_kg"] == 0.000075
    assert facts["source_internal_inventory_preserved"] is False
    assert facts["extra_source_delivery_added"] is False
    assert facts["caller_mass_or_energy_scaling"] is False
    assert REVIEW["new_helpers"] == 0 and REVIEW["new_source_omissions"] == []


@pytest.mark.parametrize("drift", [None, "parent", "target"])
def test_replay_is_atomic_and_does_not_inherit_changed_fuel_boundaries(drift):
    from scripts.review_uvek_kerosene_delivery_39 import apply_kerosene_delivery_39_review

    r = deepcopy(load_uvek_technosphere_resource())
    rule = next(a for a in r["replace"] if a["id"] == ROW["rule_id"])
    if drift == "parent":
        rule["confidence"] = 0.01
    elif drift == "target":
        rule["target"]["name"] = "Kerosene, at refinery"
    before = deepcopy(r)
    if drift:
        with pytest.raises(ValueError):
            apply_kerosene_delivery_39_review(r)
    else:
        apply_kerosene_delivery_39_review(r)
    assert r == before
