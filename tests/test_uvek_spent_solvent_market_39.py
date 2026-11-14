"""Negative waste references retain incineration shares, collection and literal empty handoffs."""

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

REVIEW = json.loads((DATA_DIR / "export/uvek_spent_solvent_market_39_review.json").read_text())
ROWS = REVIEW["records"]
MARKET, DIRECT = ROWS
RECIPE = REVIEW["recipes"][MARKET["recipe_id"]]


def policy(row, approved=True):
    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
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


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_waste_sign_and_caller_fields_keep_collection_and_empty_kiln_audit(row, amount, uncertainty, release):
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
    edge = result.data[0]["exchanges"][1]
    expected = deepcopy(before[0]["exchanges"][1])
    scale_exchange(expected, -1)
    for key in ("amount", "uncertainty type", "loc", "scale", "negative", "formula", "minimum", "maximum"):
        if key in expected:
            assert edge[key] == expected[key]
    assert edge["amount"] == -amount
    assert result.data[0]["exchanges"][-1] == before[0]["exchanges"][-1]
    losses = [x for x in result.last_migration_report.losses if x.code == "migration.recipe_source_input_omission"]
    if row["recipe_id"]:
        assert len(result.data) == 2 and len(losses) == 2
        helper = result.data[1]
        entries = [e for e in helper["exchanges"] if e["type"] != "production"]
        assert len(entries) == 10 and all(e["amount"] > 0 for e in entries)
        assert all(
            all(e[k] == v for k, v in definition.items())
            for e, definition in zip(entries, RECIPE["inputs"], strict=True)
        )
        assert helper["brightpath_conversion"]["source_input_omissions"] == RECIPE["source_input_omissions"]
        assert not helper["brightpath_conversion"].get("zero_burden")
    else:
        assert len(result.data) == 1 and not losses
        assert all(edge[k] == v for k, v in row["compiled_rule"]["target"].items())


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
def test_each_exact_source_profile_requires_independent_permission(row):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], -1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(row, False))


@pytest.mark.parametrize(
    "version,model", [("3.8", "cutoff"), ("3.9.1", "cutoff"), ("3.10", "cutoff"), ("3.9", "consequential")]
)
def test_market_permission_does_not_widen_source_version_or_system_model(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(MARKET["original_rule"]["source"], -1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(MARKET))


def test_source_matrix_retains_two_incinerators_eight_freights_and_empty_allocated_kilns():
    trace = REVIEW["retained_market_interfaces"]
    burns = [x for x in trace if x["kind"] == "incineration"]
    freight = [x for x in trace if x["kind"] != "incineration"]
    omitted = RECIPE["source_input_omissions"]
    assert len(burns) == len(omitted) == 2 and len(freight) == 8
    matrix = np.diag([-1.0] * 5 + [1.0] * 8)
    matrix[1:, 0] = -np.array(
        [
            *[x["source_amount"] for x in burns],
            *[x["source_exchange"]["amount"] for x in omitted],
            *[x["source_amount"] for x in freight],
        ]
    )
    demand = np.zeros(13)
    demand[0] = -2.5
    supply = np.linalg.solve(matrix, demand)
    assert supply[0] == 2.5
    assert supply[1:3] == pytest.approx([2.5 * x["target_amount"] for x in burns])
    assert supply[3:5] == pytest.approx([-2.5 * x["source_exchange"]["amount"] for x in omitted])
    assert supply[5:] == pytest.approx([2.5 * x["source_amount"] for x in freight])
    assert sum(x["target_amount"] for x in burns) == pytest.approx(0.608659809187402)
    assert sum(x["target_amount"] for x in burns) - sum(
        x["source_exchange"]["amount"] for x in omitted
    ) == pytest.approx(1)
    assert all(
        x["reference_amounts"] == [-1] and x["nonproduction_count"] == 0
        for x in REVIEW["empty_allocated_kiln_witnesses"]
    )
    assert REVIEW["incinerator_inventory_equality"]["full_nonproduction_records_equal"]
    assert REVIEW["treatment_shares"]["renormalized"] is False


@pytest.mark.parametrize("drift", [None, "parent", "direct", "dependency", "recipe", "omission"])
def test_frozen_replay_refuses_unreviewed_changes_atomically(drift):
    from scripts.review_uvek_spent_solvent_market_39 import apply_spent_solvent_market_39_review

    resource = deepcopy(load_uvek_technosphere_resource())
    if drift in ("parent", "direct", "dependency"):
        rid = (
            MARKET["rule_id"]
            if drift == "parent"
            else DIRECT["rule_id"] if drift == "direct" else REVIEW["source_rule_dependency_scopes"][0]["rule_id"]
        )
        next(r for r in resource["replace"] if r["id"] == rid)["confidence"] = 0.001
    elif drift == "recipe":
        resource["recipes"][MARKET["recipe_id"]]["inputs"][0]["amount"] *= 2
    elif drift == "omission":
        resource["recipes"][MARKET["recipe_id"]]["source_input_omissions"][0]["source_exchange"]["amount"] *= -1
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_spent_solvent_market_39_review(resource)
    else:
        apply_spent_solvent_market_39_review(resource)
    assert resource == before


def test_existing_direct_rule_and_old_default_target_are_unchanged():
    resource = load_uvek_technosphere_resource()
    rules = {r["id"]: r for r in resource["replace"]}
    assert rules[DIRECT["rule_id"]] == DIRECT["original_rule"] == DIRECT["compiled_rule"]
    assert rules[MARKET["rule_id"]]["target"] == MARKET["original_rule"]["target"]
    assert REVIEW["new_caller_exchange_omissions"] == []
