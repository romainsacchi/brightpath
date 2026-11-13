"""Selected landfill profile and cut-off handoff keep waste-service direction."""

import json
from copy import deepcopy

import numpy as np
import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from brightpath.migrations.scaling import scale_exchange

REVIEW = json.loads((DATA_DIR / "export/uvek_secondary_metallurgy_slag_39_review.json").read_text())
ROWS = REVIEW["records"]
RECIPES = {**REVIEW["recipe_dependencies"], **REVIEW["recipes"]}
STATS = (
    "amount",
    "uncertainty type",
    "loc",
    "scale",
    "shape",
    "minimum",
    "maximum",
    "negative",
    "pedigree",
    "scale without pedigree",
    "formula",
)


def policy(row, approved=True):
    resource = load_uvek_technosphere_resource()
    rule = next(a for a in resource["replace"] if a["id"] == row["rule_id"])
    fp = compatibility_rule_fingerprint(
        resource,
        "replace",
        rule,
        TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    )
    return MigrationPolicy(
        approved_compatibility_rules=(fp,) if approved else (), on_information_loss=PolicyAction.WARN
    )


def closure(key):
    result = {key}
    for e in RECIPES[key]["inputs"]:
        if "recipe" in e:
            result.update(closure(e["recipe"]))
    return result


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_exact_profile_keeps_waste_sign_uncertainty_and_retained_collection(row, amount, uncertainty, release):
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
    assert data == before
    expected = deepcopy(before[0]["exchanges"][1])
    scale_exchange(expected, -1)
    actual = result.data[0]["exchanges"][1]
    for key in STATS:
        if key in expected:
            assert actual[key] == expected[key]
    assert actual["amount"] == -amount and actual["unit"] == "kilogram"
    assert result.data[0]["exchanges"][-1] == before[0]["exchanges"][-1]
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
    assert set(helpers) == closure(row["recipe_id"])
    assert not any(a["brightpath_conversion"].get("zero_burden") for a in helpers.values())
    for key, helper in helpers.items():
        edges = [e for e in helper["exchanges"] if e["type"] != "production"]
        assert len(edges) == len(RECIPES[key]["inputs"])
        for edge, definition in zip(edges, RECIPES[key]["inputs"], strict=True):
            assert all(edge[k] == definition[k] for k in STATS if k in definition)
    losses = [a for a in result.last_migration_report.losses if a.code == "migration.recipe_source_input_omission"]
    omission = REVIEW["approved_recipe_source_omissions"].get(row["recipe_id"], [])
    assert len(losses) == len(omission)
    if omission:
        assert helpers[row["recipe_id"]]["brightpath_conversion"]["source_input_omissions"] == omission
        assert RECIPES[row["recipe_id"]]["inputs"][0]["amount"] == 0.761954261954262


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
def test_each_interface_needs_its_own_exact_permission(row):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], -1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(row, False))


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
@pytest.mark.parametrize(
    "version,model", [("3.8", "cutoff"), ("3.9.1", "cutoff"), ("3.10", "cutoff"), ("3.9", "consequential")]
)
def test_permissions_do_not_extend_to_other_profiles(row, version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], -1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(row))


def test_source_matrix_preserves_disposal_and_collection_without_a_stock_credit():
    trace = REVIEW["retained_market_interfaces"]
    (land,) = [a for a in trace if a["kind"] == "landfill"]
    freight = [a for a in trace if a["kind"] == "freight"]
    (omitted,) = REVIEW["approved_recipe_source_omissions"][ROWS[0]["recipe_id"]]
    matrix = np.diag([-1.0] * 3 + [1.0] * 11)
    matrix[1:13, 0] = -np.array(
        [land["source_amount"], omitted["source_exchange"]["amount"], *[a["source_amount"] for a in freight]]
    )
    matrix[13, 2] = 1
    demand = np.zeros(14)
    demand[0] = -2.5
    supply = np.linalg.solve(matrix, demand)
    assert supply[:3] == pytest.approx([2.5, 2.5 * land["target_amount"], -2.5 * omitted["source_exchange"]["amount"]])
    assert supply[3:13] == pytest.approx([2.5 * a["source_amount"] for a in freight])
    assert supply[13] == pytest.approx(-supply[2])
    control = REVIEW["recovery_handoff_control"]
    assert control["handoff_reference_amount"] == -1 and control["handoff_nonproduction_count"] == 1
    assert (
        control["only_input_amount"] == -1
        and control["stock_reference_amount"] == 1
        and control["stock_nonproduction_count"] == 0
    )
    assert land["target_amount"] - omitted["source_exchange"]["amount"] == pytest.approx(1)


def test_existing_landfill_rule_recipe_and_effective_source_profile_are_frozen():
    resource = load_uvek_technosphere_resource()
    child = ROWS[1]
    assert (
        child["original_rule"]
        == child["compiled_rule"]
        == next(a for a in resource["replace"] if a["id"] == child["rule_id"])
    )
    assert resource["recipes"][child["recipe_id"]] == REVIEW["recipe_dependencies"][child["recipe_id"]]
    assert len(resource["recipes"][child["recipe_id"]]["inputs"]) == 58
    assert REVIEW["landfill_effective_profile"]["full_existing_recipe_matches_frozen_profile_inputs"]
    assert (
        REVIEW["landfill_effective_profile"]["source_inventory_sha256"]
        == REVIEW["landfill_effective_profile"]["frozen_source_inventory_sha256"]
    )


@pytest.mark.parametrize("drift", [None, "parent", "landfill_rule", "landfill_recipe", "market", "omission"])
def test_replay_rejects_landfill_or_literal_market_audit_drift_atomically(drift):
    from scripts.review_uvek_secondary_metallurgy_slag_39 import apply_secondary_metallurgy_slag_39_review

    resource = deepcopy(load_uvek_technosphere_resource())
    if drift in ("parent", "landfill_rule"):
        row = ROWS[0] if drift == "parent" else ROWS[1]
        next(a for a in resource["replace"] if a["id"] == row["rule_id"])["confidence"] = 0.01
    elif drift == "landfill_recipe":
        resource["recipes"][ROWS[1]["recipe_id"]]["inputs"][0]["amount"] *= 2
    elif drift == "market":
        resource["recipes"][ROWS[0]["recipe_id"]]["inputs"][1]["amount"] *= 2
    elif drift == "omission":
        resource["recipes"][ROWS[0]["recipe_id"]]["source_input_omissions"][0]["source_exchange"]["amount"] *= -1
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_secondary_metallurgy_slag_39_review(resource)
    else:
        apply_secondary_metallurgy_slag_39_review(resource)
    assert resource == before
