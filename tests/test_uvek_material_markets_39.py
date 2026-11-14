"""Four delivered material markets preserve product units, producer entries and freight."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_material_markets_39_review.json").read_text())
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


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
def test_original_supplier_entries_and_delivery_are_retained_individually(row):
    inputs = REVIEW["recipes"][row["recipe_id"]]["inputs"]
    bindings = row["retained_source_market_bindings"]
    assert len(inputs) == len(bindings)
    assert [e["amount"] for e in inputs] == [b["source_amount"] for b in bindings]
    assert all(all(e[k] == v for k, v in b["target"].items()) for e, b in zip(inputs, bindings, strict=True))
    stocks = [e for e in inputs if e["unit"] == row["original_rule"]["source"]["unit"]]
    assert sum(e["amount"] for e in stocks) == pytest.approx(1)
    expected_freight = {
        "bp-f1abd225900bb9b6": [0.3091, 0.0246, 0.2088, 0.599],
        "bp-93ee78d8de9d24cd": [10.8102, 80.245998],
        "bp-6405b8335e873e4f": [0.312, 0.3436, 0.5892],
        "bp-a565c3865fb33f66": [0.553099869120841, 4.10560832],
    }
    assert [e["amount"] for e in inputs if e["unit"] == "ton kilometer"] == expected_freight[row["rule_id"]]
    assert row["compiled_rule"]["target"] == row["original_rule"]["target"]
    assert row["source_witness"]["reference_amounts"] == [1]
    assert all(w["reference_amounts"] == [1] for w in row["native_witnesses"].values())
    assert REVIEW["new_helpers"] == 4 and REVIEW["new_source_omissions"] == []


def test_distinct_material_bases_are_not_erased():
    facts = {r["rule_id"]: r["boundary_checks"] for r in ROWS}
    graphite = facts["bp-f1abd225900bb9b6"]
    assert graphite["named_battery_grade_not_generic_graphite"]
    assert graphite["native_mining_model_uses_limestone_approximation"]
    assert graphite["native_documentation_reports_large_process_uncertainty"]
    assert graphite["native_water_emissions_reported"] is False
    assert graphite["new_graphitization_or_coke_added"] is False
    timber = facts["bp-93ee78d8de9d24cd"]
    assert timber["native_signed_allocation_corrections_preserved"]
    assert timber["source_wood_input_moisture_label"] == "u=20%"
    assert timber["native_wood_input_moisture_label"] == "u=10%"
    assert timber["density_or_moisture_volume_multiplier_added"] is False
    board = facts["bp-6405b8335e873e4f"]
    assert board["same_recycled_core_board_role"] and board["original_producer_entries_retained"] == 3
    assert board["new_recycling_credit_or_yield_factor"] is False
    pallet = facts["bp-a565c3865fb33f66"]
    assert pallet["reference_unit_is_pallet_not_kg"]
    assert pallet["construction_and_disposal_inside_native_boundary"] is False
    assert pallet["mass_or_reuse_multiplier_added"] is False


@pytest.mark.parametrize("drift", [None, "first_parent", "last_parent", "stock", "freight", "last_stock"])
def test_four_market_replays_are_atomic_and_idempotent(drift):
    from scripts.review_uvek_material_markets_39 import apply_material_markets_39_review

    resource = deepcopy(load_uvek_technosphere_resource())
    if drift in ("first_parent", "last_parent"):
        rid = ROWS[0 if drift == "first_parent" else -1]["rule_id"]
        next(r for r in resource["replace"] if r["id"] == rid)["confidence"] = 0.01
    elif drift:
        row = ROWS[-1 if drift == "last_stock" else 0]
        resource["recipes"][row["recipe_id"]]["inputs"][-1 if drift == "freight" else 0]["amount"] *= 2
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_material_markets_39_review(resource)
    else:
        apply_material_markets_39_review(resource)
    assert resource == before
