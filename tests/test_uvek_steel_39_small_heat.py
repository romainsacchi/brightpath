"""Steel prerequisites retain fuel supply and physical waste-treatment demand."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_steel_39_small_heat import apply_steel_39_small_heat_review

REVIEW = json.loads((DATA_DIR / "export/uvek_steel_39_small_heat_review.json").read_text())


def policy():
    resource = load_uvek_technosphere_resource()
    rules = {r["id"]: r for r in resource["replace"]}
    scopes = tuple(
        compatibility_rule_fingerprint(
            resource,
            "replace",
            rules[row["rule_id"]],
            TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
            TechnosphereProfile("uvek", "2025", "cutoff"),
        )
        for row in REVIEW["records"]
    )
    return MigrationPolicy(approved_compatibility_rules=scopes, on_information_loss=PolicyAction.WARN)


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda row: row["rule_id"])
@pytest.mark.parametrize("amount", [-2, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_signed_demand_uncertainty_and_foreground_are_preserved(row, amount, uncertainty):
    data = consumer(row["source_witness"]["identity"], amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", "2026").background, policy=policy()
    )
    assert data == before
    edge = result.data[0]["exchanges"][1]
    factor = row["conversion_factor"]
    assert edge["amount"] == amount * factor
    assert edge["uncertainty type"] == uncertainty and edge["scale"] == 0.2
    if uncertainty == 2:
        assert edge["loc"] == before[0]["exchanges"][1]["loc"]
        assert edge["negative"] is (edge["amount"] < 0)
    else:
        assert edge["loc"] == amount * factor
        assert edge["minimum"] == min((amount - 1) * factor, (amount + 1) * factor)
        assert edge["maximum"] == max((amount - 1) * factor, (amount + 1) * factor)
    assert edge["formula"] == ("dose" if factor == 1 else "(dose) * (-1.0)")
    helpers = {a.get("brightpath_conversion", {}).get("recipe_id"): a for a in result.data}
    assert len(result.data) == 10
    for recipe_id, recipe in REVIEW["recipes"].items():
        actual = [e for e in helpers[recipe_id]["exchanges"] if e["type"] != "production"]
        assert len(actual) == len(recipe["inputs"])
        for a, b in zip(actual, recipe["inputs"], strict=True):
            for field in ("amount", "uncertainty type", "loc", "scale", "minimum", "maximum", "negative", "formula"):
                if field in b:
                    assert a[field] == b[field]
            if "recipe" in b:
                assert a["name"] == helpers[b["recipe"]]["name"]
            else:
                assert a["name"] == b["name"] and a["unit"] == b["unit"]
                if b.get("type") == "biosphere":
                    assert tuple(a["categories"]) == tuple(b["categories"])
    root = helpers["steel-39-small-non-gas-heat-production-mix"]
    assert sum(e["amount"] for e in root["exchanges"] if e["type"] == "technosphere") == pytest.approx(1)
    assert not any("solar+gas" in e.get("name", "").lower() for a in helpers.values() if a for e in a["exchanges"])


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda row: row["rule_id"])
def test_exact_approval_does_not_authorize_another_source_release(row):
    data = consumer(row["source_witness"]["identity"], -2)
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.11")).migrate_background(
            contexts("uvek", "2026").background, policy=policy()
        )


@pytest.mark.parametrize("drift", [False, True])
def test_inert_review_replay_is_atomic(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    if drift:
        next(r for r in resource["replace"] if r["id"] == REVIEW["records"][0]["rule_id"])["confidence"] = 0.1
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError, match="parent changed"):
            apply_steel_39_small_heat_review(resource)
    else:
        apply_steel_39_small_heat_review(resource)
    assert resource == before


def test_recipe_drift_is_refused_atomically():
    resource = deepcopy(load_uvek_technosphere_resource())
    resource["recipes"]["steel-39-small-non-gas-heat-production-mix"]["inputs"][0]["amount"] *= 2
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="recipe changed"):
        apply_steel_39_small_heat_review(resource)
    assert resource == before


def test_original_shares_solar_geometry_and_stalk_reference_are_explicit():
    assert len(REVIEW["supplier_bindings"]) == 52
    assert REVIEW["root_share_sum"] == pytest.approx(1)
    assert len(REVIEW["solar_operation_records"]) == 8
    assert REVIEW["solar_share"] == pytest.approx(1.2383390806845222e-5)
    assert REVIEW["collector_areas_m2"] == [12.3, 58.3, 4.0, 10.5]
    assert REVIEW["stalk_share"] == pytest.approx(0.0006264488297394893)
    assert REVIEW["stalk_is_avoided_treatment_credit"] is False
    assert REVIEW["stalk_supply_market_witness"]["reference_amounts"] == [1]
    assert all(w["reference_amounts"] == [1] for w in REVIEW["stalk_supply_producer_witnesses"])
