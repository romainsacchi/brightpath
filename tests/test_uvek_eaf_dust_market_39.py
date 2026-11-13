"""Allocated-empty clinker routes stay audited while dust treatment signs remain correct."""

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

REVIEW = json.loads((DATA_DIR / "export/uvek_eaf_dust_market_39_review.json").read_text())
ROW = REVIEW["records"][0]
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


def policy(approved=True):
    r = load_uvek_technosphere_resource()
    rule = next(a for a in r["replace"] if a["id"] == ROW["rule_id"])
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
def test_market_landfills_and_collection_keep_signs_and_uncertainty(amount, uncertainty, release):
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
    assert data == before and len(result.data) == 4
    expected = deepcopy(before[0]["exchanges"][1])
    scale_exchange(expected, -1)
    edge = result.data[0]["exchanges"][1]
    for key in STATS:
        if key in expected:
            assert edge[key] == expected[key]
    assert edge["amount"] == -amount and edge["unit"] == "kilogram"
    assert result.data[0]["exchanges"][-1] == before[0]["exchanges"][-1]
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
    assert set(helpers) == set(RECIPES)
    assert not any(h["brightpath_conversion"].get("zero_burden") for h in helpers.values())
    for key, helper in helpers.items():
        actual = [e for e in helper["exchanges"] if e["type"] != "production"]
        assert len(actual) == len(RECIPES[key]["inputs"])
        for a, b in zip(actual, RECIPES[key]["inputs"], strict=True):
            for field in STATS:
                if field in b:
                    assert a[field] == b[field]
    root = helpers[ROW["recipe_id"]]
    assert (
        root["brightpath_conversion"]["source_input_omissions"]
        == REVIEW["approved_recipe_source_omissions"][ROW["recipe_id"]]
    )
    losses = [
        loss for loss in result.last_migration_report.losses if loss.code == "migration.recipe_source_input_omission"
    ]
    assert len(losses) == 2
    assert [e["amount"] for e in RECIPES[ROW["recipe_id"]]["inputs"]][:2] == [0.932628099547659, 0.000999466371545205]


def test_source_negative_reference_matrix_keeps_treatment_and_freight_demands():
    retained = REVIEW["retained_market_interfaces"]
    land = [x for x in retained if x["kind"] == "landfill"]
    freight = [x for x in retained if x["kind"] == "freight"]
    omit = REVIEW["approved_recipe_source_omissions"][ROW["recipe_id"]]
    original = (
        [x["source_amount"] for x in land]
        + [x["source_exchange"]["amount"] for x in omit]
        + [x["amount"] for x in freight]
    )
    source = np.diag([-1.0] * 5 + [1.0] * 8)
    source[1:, 0] = -np.array(original)
    demand = np.zeros(13)
    demand[0] = -2.5
    supply = np.linalg.solve(source, demand)
    assert supply[:5] == pytest.approx(
        [
            2.5,
            2.5 * land[0]["target_amount"],
            2.5 * land[1]["target_amount"],
            -2.5 * omit[0]["source_exchange"]["amount"],
            -2.5 * omit[1]["source_exchange"]["amount"],
        ]
    )
    assert supply[5:] == pytest.approx([2.5 * x["amount"] for x in freight])
    assert len(freight) == 8 and len(land) == 2
    assert REVIEW["retained_landfill_fraction"] + REVIEW["audited_allocated_coprocessing_fraction"] == pytest.approx(1)
    for control in REVIEW["clinker_allocation_controls"]:
        assert control["same_publisher_activity"]
        assert control["waste_reference"]["reference_amounts"] == [-1]
        assert control["waste_reference"]["nonproduction_count"] == 0
        assert control["clinker_reference"]["reference_amounts"] == [1]
        assert control["clinker_reference"]["nonproduction_count"] > 0
    assert all(x["matches_prior_exact_source_witness"] for x in REVIEW["landfill_source_witness_matches"])
    assert all("coal freight" not in e.get("name", "").lower() for e in RECIPES[ROW["recipe_id"]]["inputs"])


def test_market_still_requires_its_own_exact_scope():
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROW["original_rule"]["source"], -1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(False))


@pytest.mark.parametrize(
    "version,model", [("3.8", "cutoff"), ("3.9.1", "cutoff"), ("3.10", "cutoff"), ("3.9", "consequential")]
)
def test_other_profiles_do_not_inherit_the_39_omission_scope(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROW["original_rule"]["source"], -1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


@pytest.mark.parametrize("drift", [None, "parent", "upstream", "recipe", "omission"])
def test_replay_keeps_dependency_and_literal_omission_audits_atomic(drift):
    from scripts.review_uvek_eaf_dust_market_39 import apply_eaf_dust_market_39_review

    r = deepcopy(load_uvek_technosphere_resource())
    if drift in ("parent", "upstream"):
        rid = ROW["rule_id"] if drift == "parent" else REVIEW["source_rule_dependency_scopes"][0]["rule_id"]
        next(a for a in r["replace"] if a["id"] == rid)["confidence"] = 0.01
    elif drift == "recipe":
        r["recipes"][ROW["recipe_id"]]["inputs"][0]["amount"] = 1
    elif drift == "omission":
        r["recipes"][ROW["recipe_id"]]["source_input_omissions"][0]["source_exchange"]["amount"] *= 2
    before = deepcopy(r)
    if drift:
        with pytest.raises(ValueError):
            apply_eaf_dust_market_39_review(r)
    else:
        apply_eaf_dust_market_39_review(r)
    assert r == before


def test_other_profile_entries_and_default_target_remain_unchanged():
    old, new = ROW["original_rule"], ROW["compiled_rule"]
    assert old["target"] == new["target"]
    for entry in old["reference_flow_sign_review"]["profiles"]:
        if entry["source_profile"] != REVIEW["source_profile"]:
            assert entry in new["reference_flow_sign_review"]["profiles"]
