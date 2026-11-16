"""Native Euro-VI conditioning preserves transport work; complete European rail stays direct."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from uvek_review_helpers import compiled_authoring_stage_resource

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from brightpath.migrations.scaling import scale_exchange

REVIEW = json.loads((DATA_DIR / "export/uvek_remaining_freight_39_review.json").read_text())
ROWS = REVIEW["records"]


def policy(exclude=None):
    resource = load_uvek_technosphere_resource()
    fps = tuple(
        compatibility_rule_fingerprint(
            resource,
            "replace",
            next(r for r in resource["replace"] if r["id"] == row["rule_id"]),
            TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
            TechnosphereProfile("uvek", "2025", "cutoff"),
        )
        for row in ROWS
        if row["rule_id"] != exclude
    )
    return MigrationPolicy(approved_compatibility_rules=fps, on_information_loss=PolicyAction.WARN)


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_transport_work_uncertainty_and_complete_service_quantities_survive(amount, uncertainty, release):
    data = consumer(ROWS[0]["original_rule"]["source"], amount, uncertainty)
    data[0]["exchanges"].extend(
        consumer(r["original_rule"]["source"], amount, uncertainty)[0]["exchanges"][1] for r in ROWS[1:]
    )
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
    assert data == before and len(result.data) == 3
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
    for i, row in enumerate(ROWS, 1):
        edge = result.data[0]["exchanges"][i]
        expected = deepcopy(before[0]["exchanges"][i])
        scale_exchange(expected, 1)
        for k in ("amount", "uncertainty type", "loc", "scale", "negative", "formula", "minimum", "maximum"):
            if k in expected:
                assert edge[k] == expected[k]
        target = helpers[row["recipe_id"]] if row["recipe_id"] else row["compiled_rule"]["target"]
        assert all(edge[k] == target[k] for k in ["name", "reference product", "location", "unit"])
        if row["recipe_id"]:
            entries = [e for e in target["exchanges"] if e["type"] != "production"]
            definitions = REVIEW["recipes"][row["recipe_id"]]["inputs"]
            assert len(entries) == len(definitions)
            assert all(all(e[k] == v for k, v in d.items()) for e, d in zip(entries, definitions, strict=True))
            assert sum(e["amount"] for e in entries) == pytest.approx(1)
            assert not target["brightpath_conversion"].get("source_input_omissions")
    assert result.data[0]["exchanges"][-1] == before[0]["exchanges"][-1]


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
def test_each_exact_freight_permission_is_required(row):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], -1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(row["rule_id"]))


@pytest.mark.parametrize(
    "version,model", [("3.8", "cutoff"), ("3.9.1", "cutoff"), ("3.10", "cutoff"), ("3.9", "consequential")]
)
def test_new_permissions_do_not_widen_versions_or_models(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROWS[0]["original_rule"]["source"], -1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


@pytest.mark.parametrize("row", [r for r in ROWS if r["recipe_id"]], ids=lambda r: r["rule_id"])
def test_conditioned_native_weights_preserve_one_tkm_and_correct_vehicle_class(row):
    basis = row["native_weighting_basis"]["2026"]
    inputs = REVIEW["recipes"][row["recipe_id"]]["inputs"]
    raw = basis["raw_selected_share"]
    assert 0 < raw < 1
    assert len(inputs) == row["boundary_checks"]["conditional_supplier_count"]
    for entry, evidence in zip(inputs, basis["conditional_weights"], strict=True):
        assert entry["amount"] == pytest.approx(evidence["weight"] / raw)
        assert entry["amount"] > 0 and entry["unit"] == "ton kilometer"
        assert ", diesel, " in entry["name"] and "2020, EURO-VI" in entry["name"]
        assert all(entry[k] == evidence["identity"][k] for k in ["name", "reference product", "unit", "location"])
    assert sum(e["amount"] for e in inputs) == pytest.approx(1)
    assert row["boundary_checks"]["source_transport_work_reduced_by_native_EuroVI_share"] is False
    assert row["boundary_checks"]["caller_payload_or_efficiency_factor_added"] is False
    assert row["boundary_checks"]["source_duty_and_payload_reproduced"] is False
    heavy = row["boundary_checks"]["kind"] == "heavy_EuroVI_lorry"
    assert len(inputs) == (3 if heavy else 9)
    assert (
        all("40t gross weight" in e["name"] for e in inputs)
        if heavy
        else all(any(f"{t}t gross weight" in e["name"] for t in [18, 26, 32]) for e in inputs)
    )


def test_rail_keeps_complete_european_service_with_explicit_traction_difference():
    row = next(r for r in ROWS if not r["recipe_id"])
    actual = next(r for r in load_uvek_technosphere_resource()["replace"] if r["id"] == row["rule_id"])
    assert actual == row["original_rule"] == row["compiled_rule"]
    assert row["boundary_checks"]["source_electric_share"] == 0.6
    assert row["boundary_checks"]["source_diesel_share"] == 0.4
    assert row["boundary_checks"]["source_60_40_traction_split_reproduced"] is False
    assert row["boundary_checks"]["native_complete_service_includes_shunting"] is True
    assert row["compiled_rule"]["target"]["name"] == "Transport, freight, rail"
    assert row["compiled_rule"]["target"]["location"] == "RER"
    assert REVIEW["new_helpers"] == 2 and REVIEW["new_source_omissions"] == []


@pytest.mark.parametrize("drift", [None, "parent", "direct_rail", "weight", "supplier"])
def test_replay_refuses_unreviewed_weight_or_supplier_changes_atomically(drift):
    from scripts.review_uvek_remaining_freight_39 import apply_remaining_freight_39_review

    resource = compiled_authoring_stage_resource(
        "review_uvek_remaining_freight_39", "apply_remaining_freight_39_review"
    )
    row = next(r for r in ROWS if r["recipe_id"])
    if drift in ("parent", "direct_rail"):
        changed = row if drift == "parent" else next(r for r in ROWS if not r["recipe_id"])
        next(r for r in resource["replace"] if r["id"] == changed["rule_id"])["confidence"] = 0.001
    elif drift == "weight":
        resource["recipes"][row["recipe_id"]]["inputs"][0]["amount"] *= 2
    elif drift == "supplier":
        resource["recipes"][row["recipe_id"]]["inputs"][0]["location"] = "unreviewed"
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_remaining_freight_39_review(resource)
    else:
        apply_remaining_freight_39_review(resource)
    assert resource == before
