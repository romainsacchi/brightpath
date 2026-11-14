"""Complete native grids retain delivered kWh and explicit industrial profile limits."""

import hashlib
import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_remaining_electricity_grids_39_review.json").read_text())
ROWS = REVIEW["records"]
COBALT, EUROPE, GERMANY, SOUTH_AFRICA = ROWS


def policy(approved=True, version="3.9"):
    resource = load_uvek_technosphere_resource()
    ids = {row["rule_id"] for row in ROWS}
    fingerprints = tuple(
        compatibility_rule_fingerprint(
            resource,
            "replace",
            rule,
            TechnosphereProfile("ecoinvent", version, "cutoff"),
            TechnosphereProfile("uvek", "2025", "cutoff"),
        )
        for rule in resource["replace"]
        if rule["id"] in ids
    )
    return MigrationPolicy(
        approved_compatibility_rules=fingerprints if approved else (), on_information_loss=PolicyAction.WARN
    )


@pytest.mark.parametrize("row", ROWS, ids=["cobalt", "Europe", "Germany", "South-Africa"])
@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_signed_uncertain_product_demand_keeps_native_inventory_and_final_freight(row, amount, uncertainty, release):
    data = consumer(row["original_rule"]["source"], amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == before
    edge = result.data[0]["exchanges"][1]
    for key in ("amount", "uncertainty type", "loc", "scale", "negative", "formula", "minimum", "maximum"):
        if key in before[0]["exchanges"][1]:
            assert edge[key] == before[0]["exchanges"][1][key]
    if row["recipe_id"]:
        assert len(result.data) == 2
        helper = result.data[1]
        assert all(edge[k] == helper[k] for k in ["name", "reference product", "location", "unit"])
        actual = [e for e in helper["exchanges"] if e["type"] != "production"]
        expected = REVIEW["recipes"][row["recipe_id"]]["inputs"]
        assert len(actual) == len(expected) == 10
        assert all(all(e[k] == v for k, v in d.items()) for e, d in zip(actual, expected, strict=True))
        assert not helper["brightpath_conversion"].get("source_input_omissions")
    else:
        assert len(result.data) == 1
        assert all(edge[k] == v for k, v in row["original_rule"]["target"].items())


def test_native_grid_boundaries_and_major_industrial_mix_difference_are_explicit():
    comparison = REVIEW["cobalt_generation_profile_comparison"]
    assert comparison["source_nonpumped_hydro_fraction"] == pytest.approx(0.9109039635633036)
    assert comparison["native_hydro_fraction"] == 0.54
    assert comparison["source_nominal_generator_kWh_per_delivered_kWh"] == pytest.approx(1.0471344594862182)
    assert comparison["native_nominal_generator_kWh_per_delivered_kWh"] == pytest.approx(1.052792)
    assert comparison["caller_factor"] == 1 and not comparison["source_generation_profile_reconstructed"]
    assert all(row["conversion_factor"] == 1 and row["recipe_id"] is None for row in ROWS)
    assert all(row["source_witness"]["reference_amounts"] == [1] for row in ROWS)
    assert all(w["reference_amounts"] == [1] for row in ROWS for w in row["native_witnesses"].values())
    assert REVIEW["new_helpers"] == 0 and REVIEW["recipes"] == {}
    assert REVIEW["new_source_omissions"] == REVIEW["runtime_python_changes"] == []


@pytest.mark.parametrize("amount,uncertainty", [(-2.5, 2), (2.5, 2), (-2.5, 3), (2.5, 3)])
def test_earlier_European_38_grid_permission_keeps_supplier_quantity_and_authority(amount, uncertainty):
    (old,) = REVIEW["preserved_existing_scopes"]
    assert (
        hashlib.sha256((DATA_DIR / old["previous_review_path"]).read_bytes()).hexdigest()
        == old["previous_review_sha256"]
    )
    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == EUROPE["rule_id"])
    source = TechnosphereProfile(**old["source_profile"])
    target = TechnosphereProfile(**old["target_profile"])
    assert compatibility_rule_fingerprint(resource, "replace", rule, source, target) == old["rebound_fingerprint"]
    baseline = deepcopy(resource)
    parent = next(r for r in baseline["replace"] if r["id"] == EUROPE["rule_id"])
    parent.clear()
    parent.update(deepcopy(EUROPE["original_rule"]))
    assert compatibility_rule_fingerprint(baseline, "replace", parent, source, target) == old["previous_fingerprint"]
    result = BrightwayInventory.from_data(
        consumer(EUROPE["original_rule"]["source"], amount, uncertainty), context=contexts("ecoinvent", "3.8")
    ).migrate_background(contexts("uvek", "2026").background, policy=policy(version="3.8"))
    assert len(result.data) == 1 and result.data[0]["exchanges"][1]["amount"] == amount
    assert all(result.data[0]["exchanges"][1][k] == v for k, v in old["unchanged_physical_binding"]["target"].items())


@pytest.mark.parametrize("row", ROWS)
def test_new_source_profiles_need_their_own_permission(row):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], 1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(False))


@pytest.mark.parametrize(
    "version,model", [("3.8", "cutoff"), ("3.9.1", "cutoff"), ("3.11", "cutoff"), ("3.9", "consequential")]
)
def test_new_39_permission_does_not_widen_profiles(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(EUROPE["original_rule"]["source"], 1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


@pytest.mark.parametrize("drift", [None, "Europe", "other-parent"])
def test_frozen_profile_extension_refuses_drift_atomically(drift):
    from scripts.review_uvek_remaining_electricity_grids_39 import apply_remaining_electricity_grids_39_review

    resource = deepcopy(load_uvek_technosphere_resource())
    if drift:
        selected = EUROPE if drift == "Europe" else COBALT
        next(r for r in resource["replace"] if r["id"] == selected["rule_id"])["confidence"] = 0.001
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_remaining_electricity_grids_39_review(resource)
    else:
        apply_remaining_electricity_grids_39_review(resource)
    assert resource == before
