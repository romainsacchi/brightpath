"""LMO cells cannot become NMC cells, packs or cathodes; retain final delivery."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_lmo_cell_39_review.json").read_text())
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
    if row["recipe_id"]:
        helper = result.data[1]
        assert helper["brightpath_conversion"]["recipe_id"] == row["recipe_id"]
        edges = [e for e in helper["exchanges"] if e["type"] != "production"]
        assert len(edges) == 4
        assert [e["amount"] for e in edges] == [0.950627953362496, 0.0493720466375044, 0.02, 0.02]
        for edge, trace in zip(edges, REVIEW["retained_market_interfaces"][row["recipe_id"]], strict=True):
            assert all(edge[k] == v for k, v in trace["statistics"].items())
            assert all(edge[k] == v for k, v in trace["target"].items())
    else:
        assert all(actual[k] == v for k, v in row["compiled_rule"]["target"].items())
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


@pytest.mark.parametrize("drift", [None, 0, 1, 2, "delivery"])
def test_frozen_replay_is_atomic_and_idempotent(drift):
    from scripts.review_uvek_lmo_cell_39 import apply_lmo_cell_39_review

    resource = deepcopy(load_uvek_technosphere_resource())
    if drift == "delivery":
        resource["recipes"][ROWS[0]["recipe_id"]]["inputs"][2]["amount"] *= 2
    elif drift is not None:
        next(r for r in resource["replace"] if r["id"] == ROWS[drift]["rule_id"])["confidence"] = 0.01
    before = deepcopy(resource)
    if drift is None:
        apply_lmo_cell_39_review(resource)
    else:
        with pytest.raises(ValueError):
            apply_lmo_cell_39_review(resource)
    assert resource == before


def test_lmo_is_a_complete_cell_with_matching_reference_basis():
    assert REVIEW["new_source_omissions"] == []
    assert len(REVIEW["recipes"]) == REVIEW["new_helpers"] == 1
    assert len(ROWS) == 3
    for row in ROWS:
        assert row["source_witness"]["reference_amounts"] == [1]
        assert row["conversion_factor"] == 1
        assert row["original_rule"]["target"]["name"] == "Battery cell, Li-ion, NMC111"
        target = row["compiled_rule"]["target"]
        assert target["name"] == "Single cell, lithium-ion battery, lithium manganese oxide/graphite, at plant"
        assert target["location"] == "CN" and target["unit"] == "kilogram"
        assert row["compiled_rule"]["reference_flow_sign_review"]["status"] == "unresolved"
        assert row["boundary_checks"]["pack_or_capacity_multiplier_added"] is False
    for row in ROWS[1:]:
        facts = row["boundary_checks"]
        assert facts["kind"] == "complete_LMO_graphite_cell"
        assert facts["anode_kg"] == 0.4011 and facts["cathode_kg"] == 0.32686
        assert facts["assembly_electricity_kWh"] == 0.1056
        assert facts["assembly_purchased_heat_MJ"] == 0.065234
        assert facts["native_waste_heat_MJ"] == 0.38016
        assert all(w["reference_amounts"] == [1] for w in row["native_witnesses"].values())
    assert all(
        c["matching_nominal_cell_components_and_assembly_energy"] for c in REVIEW["nominal_cell_boundary_checks"]
    )
    inputs = REVIEW["recipes"][ROWS[0]["recipe_id"]]["inputs"]
    assert inputs[0]["name"] == inputs[1]["name"] == ROWS[1]["compiled_rule"]["target"]["name"]
    assert inputs[2]["name"] == "Transport, freight, rail"
    assert inputs[3]["name"] == "Transport, freight, lorry, fleet average"
    assert inputs[2]["location"] == inputs[3]["location"] == "RER"
