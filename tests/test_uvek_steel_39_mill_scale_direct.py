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
from scripts.review_uvek_steel_39_mill_scale_direct import apply_steel_39_mill_scale_direct_review

REVIEW = json.loads((DATA_DIR / "export/uvek_steel_39_mill_scale_direct_review.json").read_text())


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
    assert len(result.data) == 2
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
@pytest.mark.parametrize(
    "version, model", [("3.6", "cutoff"), ("3.11", "cutoff"), ("3.12", "cutoff"), ("3.9", "consequential")]
)
def test_exact_approval_does_not_authorize_another_source_release(row, version, model):
    data = consumer(row["source_witness"]["identity"], -2)
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model)).migrate_background(
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
            apply_steel_39_mill_scale_direct_review(resource)
    else:
        apply_steel_39_mill_scale_direct_review(resource)
    assert resource == before


def test_reuses_the_exact_market_landfill_without_adding_collection():
    from brightpath import DATA_DIR

    prior = json.loads((DATA_DIR / "export/uvek_steel_39_first_residue_review.json").read_text())
    assert REVIEW["new_helpers"] == 0
    ((key, recipe),) = REVIEW["recipe_dependencies"].items()
    assert recipe == prior["recipes"][key]
    assert len(recipe["inputs"]) == 76
    assert sum(e["type"] == "biosphere" for e in recipe["inputs"]) == 68
    assert not any(e.get("unit") == "ton kilometer" for e in recipe["inputs"])
    (row,) = REVIEW["records"]
    assert row["source_witness"]["reference_amounts"] == [-1]
    assert row["compiled_rule"]["target"] == {"recipe_ref": key}
    assert row["compiled_rule"]["reference_flow_sign_review"]["status"] == "unresolved"
    assert len(row["compiled_rule"]["reference_flow_sign_review"]["profiles"]) == 1


def test_landfill_dependency_drift_is_refused_atomically():
    resource = deepcopy(load_uvek_technosphere_resource())
    key = next(iter(REVIEW["recipe_dependencies"]))
    resource["recipes"][key]["inputs"][0]["amount"] *= 2
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="dependency changed"):
        apply_steel_39_mill_scale_direct_review(resource)
    assert resource == before
