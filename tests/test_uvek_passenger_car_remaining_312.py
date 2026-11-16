"""Older models keep their identity; gaseous hydrogen retains a compression stage."""

import json
import math
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from brightpath.migrations.scaling import scale_exchange
from scripts.review_uvek_passenger_car_remaining_312 import apply_passenger_car_remaining_312_review

REVIEW = json.loads((DATA_DIR / "export/uvek_passenger_car_remaining_312_review.json").read_text())


def policy(loss=PolicyAction.WARN):
    resource = load_uvek_technosphere_resource()
    indexed = {r["id"]: r for r in resource["replace"]}
    return MigrationPolicy(
        approved_compatibility_rules=tuple(
            compatibility_rule_fingerprint(
                resource,
                "replace",
                indexed[row["rule_id"]],
                TechnosphereProfile(**REVIEW["source_profile"]),
                TechnosphereProfile(**resource["target_profile"]),
            )
            for row in REVIEW["records"]
        ),
        on_information_loss=loss,
    )


@pytest.mark.parametrize("amount,uncertainty", [(-2, 2), (2, 3)])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_current_callers_keep_signed_demand_and_older_models_keep_provenance(amount, uncertainty, release):
    rows = REVIEW["records"]
    data = consumer(rows[0]["source"], amount, uncertainty)
    data[0]["exchanges"].extend(consumer(r["source"], amount, uncertainty)[0]["exchanges"][1] for r in rows[1:])
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == before
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
    assert len(helpers) == 13
    for row, original, edge in zip(rows, before[0]["exchanges"][1:], result.data[0]["exchanges"][1:], strict=True):
        expected = deepcopy(original)
        scale_exchange(expected, row["conversion_factor"])
        root = helpers[row["recipe_id"]]
        assert edge["name"] == root["name"] and edge["unit"] == root["unit"]
        assert edge["amount"] == original["amount"] / row["source_witness"]["reference_amount"]
        for key in ("amount", "loc", "scale", "negative", "formula", "uncertainty type"):
            if key in expected:
                assert edge[key] == expected[key]
        if "inventory_model_profile" in row:
            for recipe_id in row["model_recipe_closure_ids"]:
                provenance = helpers[recipe_id]["brightpath_conversion"]
                assert provenance["inventory_source_profile"] == row["inventory_model_profile"]
                assert provenance["caller_source_profile"] == row["source_profile"]
    pressure = helpers["car-312-gaseous-hydrogen-200bar-screening-compression"]
    purchases = [e for e in pressure["exchanges"] if e["type"] == "technosphere"]
    assert [(e["unit"], e["amount"]) for e in purchases] == [
        ("kilogram", 1),
        ("kilowatt hour", pytest.approx(1.0882062648061839)),
    ]
    assert "25 bar" in purchases[0]["name"] and "liquid" not in purchases[0]["name"]
    assert not any(
        loss.code == "migration.recipe_source_input_omission" for loss in result.last_migration_report.losses
    )


def original_resource():
    resource = load_uvek_technosphere_resource()
    return {
        "replace": [deepcopy(r["original_rule"]) for r in REVIEW["records"]],
        "delete": [],
        "recipes": {key: deepcopy(resource["recipes"][key]) for key in REVIEW["recipe_dependency_sha256"]},
        "reviews": deepcopy(resource["reviews"]),
        "coverage": {"unresolved_identities": 0},
    }


def test_replay_preserves_previous_profile_rules_and_is_idempotent():
    resource = original_resource()
    before = deepcopy(resource)
    apply_passenger_car_remaining_312_review(resource)
    once = deepcopy(resource)
    apply_passenger_car_remaining_312_review(resource)
    assert resource == once
    for version in ("3.9", "3.10", "3.11"):
        for row in REVIEW["records"][:4]:
            source = TechnosphereProfile("ecoinvent", version, "cutoff")
            after = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
            old = next(r for r in before["replace"] if r["id"] == row["rule_id"])
            assert select_profile_sign_rules({"replace": [old]}, source) == select_profile_sign_rules(
                {"replace": [after]}, source
            )


@pytest.mark.parametrize("change", ["parent", "dependency", "recipe"])
def test_changed_evidence_fails_before_mutation(change):
    resource = original_resource()
    if change == "parent":
        resource["replace"][0]["conversion_factor"] = 2
    elif change == "dependency":
        key = next(iter(resource["recipes"]))
        resource["recipes"][key]["inputs"] = []
    else:
        key, recipe = next(iter(REVIEW["recipes"].items()))
        resource["recipes"][key] = {**recipe, "inputs": []}
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="changed"):
        apply_passenger_car_remaining_312_review(resource)
    assert resource == before


def test_compression_is_positive_and_bounded_by_thermodynamic_limits():
    row = REVIEW["records"][-1]
    p = row["compression_parameters"]
    r_specific = p["gas_constant_J_per_mol_K"] / p["molar_mass_kg_per_mol"]
    ratio = p["outlet_pressure_bar"] / p["inlet_pressure_bar"]
    isothermal = r_specific * p["inlet_temperature_K"] * math.log(ratio) / 3.6e6
    g = p["heat_capacity_ratio"]
    single_stage = (
        g
        / (g - 1)
        * r_specific
        * p["inlet_temperature_K"]
        * (ratio ** ((g - 1) / g) - 1)
        / p["isentropic_efficiency"]
        / p["motor_efficiency"]
        / 3.6e6
    )
    assert isothermal < row["compression_electricity_kWh_per_kg"] < single_stage
    assert p["inlet_pressure_bar"] == 25 and p["outlet_pressure_bar"] == 200


def test_explicit_proxy_permission_does_not_remove_strict_loss_refusal():
    data = consumer(REVIEW["records"][0]["source"], -1)
    before = deepcopy(data)
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
            contexts("uvek", "2026").background, policy=policy(PolicyAction.ERROR)
        )
    assert data == before
