"""Sodium remains a metal service; inspected KCl retains its nutrient reference basis."""

import json
import math
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_heat_salts import apply_heat_salts_review

REVIEW = json.loads((DATA_DIR / "export/uvek_heat_salts_review.json").read_text())
CORRECTIONS = REVIEW["records"][:2]


@pytest.fixture(scope="module")
def policy():
    r = load_uvek_technosphere_resource()
    indexed = {x["id"]: x for x in r["replace"]}
    scopes = tuple(
        compatibility_rule_fingerprint(
            r,
            "replace",
            indexed[row["rule_id"]],
            TechnosphereProfile("ecoinvent", "3.10", "cutoff"),
            TechnosphereProfile("uvek", "2025", "cutoff"),
        )
        for row in REVIEW["records"]
    )
    return MigrationPolicy(approved_compatibility_rules=scopes, on_information_loss=PolicyAction.WARN)


@pytest.mark.parametrize("row", CORRECTIONS, ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_signed_reference_basis_and_supported_uncertainty(row, amount, uncertainty, policy):
    data = consumer(row["source"], amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.10")).migrate_background(
        contexts("uvek", "2026").background,
        policy=policy,
    )
    assert data == before
    e = result.data[0]["exchanges"][1]
    old = before[0]["exchanges"][1]
    factor = row["conversion_factor"]
    assert e["amount"] == amount * factor
    assert e.get("negative", False) == old.get("negative", False)
    if factor == 1:
        for field in ("loc", "scale", "minimum", "maximum", "formula"):
            if field in old:
                assert e[field] == old[field]
        helper = next(
            a for a in result.data if a.get("brightpath_conversion", {}).get("recipe_id") == row["recipe_ref"]
        )
        assert helper["reference product"] == "sodium"
        assert len(helper["exchanges"]) == 10
        assert len([x for x in helper["exchanges"] if x["type"] == "biosphere"]) == 5
        for original in row["recipe"]["inputs"]:
            if "recipe" in original:
                edge = next(x for x in helper["exchanges"] if x["amount"] == original["amount"])
            else:
                edge = next(
                    x
                    for x in helper["exchanges"]
                    if x["name"] == original["name"] and x["amount"] == original["amount"]
                )
            for field in ("amount", "uncertainty type", "loc", "scale", "minimum", "maximum", "negative", "formula"):
                if field in original:
                    assert edge[field] == original[field]
        assert len([a for a in result.data if a.get("brightpath_conversion", {}).get("recipe_id")]) == 2
    else:
        assert factor == 0.6
        assert all(e[k] == v for k, v in row["target"].items())
        assert "0.6" in e["formula"]
        if uncertainty == 2:
            assert e["loc"] == pytest.approx(old["loc"] + math.log(factor))
            assert e["scale"] == old["scale"]
        else:
            for field in ("loc", "scale", "minimum", "maximum"):
                assert e[field] == pytest.approx(old[field] * factor)


@pytest.mark.parametrize("row", CORRECTIONS, ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("version,model", [("3.9", "cutoff"), ("3.12", "cutoff"), ("3.10", "consequential")])
def test_other_profiles_cannot_inherit_the_corrections(row, version, model, policy):
    data = consumer(row["source"], 1)
    before = deepcopy(data)
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model)).migrate_background(
            contexts("uvek", "2026").background,
            policy=policy,
        )
    assert data == before


@pytest.mark.parametrize("drift", [None, "last_rule", "sodium_recipe", "heat_dependency"])
def test_replay_and_atomic_validation(drift):
    r = deepcopy(load_uvek_technosphere_resource())
    if drift == "last_rule":
        next(x for x in r["replace"] if x["id"] == REVIEW["records"][-1]["rule_id"])["confidence"] = 0.1
    elif drift == "sodium_recipe":
        r["recipes"][CORRECTIONS[0]["recipe_ref"]]["inputs"][0]["amount"] *= 2
    elif drift == "heat_dependency":
        row = next(x for x in REVIEW["records"] if "selected_recipe_ref" in x)
        r["recipes"][row["selected_recipe_ref"]]["inputs"][0]["amount"] *= 2
    before = deepcopy(r)
    if drift:
        with pytest.raises(ValueError):
            apply_heat_salts_review(r)
    else:
        apply_heat_salts_review(r)
    assert r == before
