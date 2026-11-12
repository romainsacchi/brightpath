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
from scripts.review_uvek_steel_39_wastewater import apply_steel_39_wastewater_review

REVIEW = json.loads((DATA_DIR / "export/uvek_steel_39_wastewater_review.json").read_text())


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
    assert len(result.data) == 6
    for recipe_id, recipe in {**REVIEW["recipe_dependencies"], **REVIEW["recipes"]}.items():
        if recipe_id not in helpers:
            continue
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
            apply_steel_39_wastewater_review(resource)
    else:
        apply_steel_39_wastewater_review(resource)
    assert resource == before


def test_recipe_drift_is_refused_atomically():
    resource = deepcopy(load_uvek_technosphere_resource())
    resource["recipes"][next(iter(REVIEW["recipes"]))]["inputs"][0]["amount"] *= 2
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="recipe changed"):
        apply_steel_39_wastewater_review(resource)
    assert resource == before


def test_treated_fraction_untreated_discharge_and_sewer_boundary_are_preserved():
    for row in REVIEW["records"]:
        assert row["source_witness"]["reference_amounts"] == [-1]
        assert row["native_2026_witness"]["reference_amounts"] == [1]
        assert row["conversion_factor"] == -1
        assert sum(s["amount"] for s in row["sewer_services"]) == pytest.approx(1)
        assert len(row["untreated_discharges"]) == 12
        water = next(e for e in row["untreated_discharges"] if e["name"] == "Water")
        assert row["treated_fraction"] + water["amount"] == pytest.approx(1, abs=1e-8)
        assert water["unit"] == "cubic meter"
        assert all(tuple(e["categories"]) == ("water", "surface water") for e in row["untreated_discharges"])
        correction = row["embedded_native_sewer_correction"]
        assert correction["amount"] == -row["treated_fraction"] * 2.1782e-7
        assert correction["name"] == "Sewer grid, class 3" and correction["unit"] == "kilometer"


def test_sewer_dependency_drift_is_refused_atomically():
    resource = deepcopy(load_uvek_technosphere_resource())
    key = next(iter(REVIEW["recipe_dependencies"]))
    resource["recipes"][key]["inputs"][0]["amount"] *= 2
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="dependency changed"):
        apply_steel_39_wastewater_review(resource)
    assert resource == before
