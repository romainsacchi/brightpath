"""Finished oxide keeps roasting, primary/co-product shares and both freight stages."""

import json
from copy import deepcopy
from dataclasses import replace

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_fuels_39_molybdenum import REVIEW_PATH, apply_fuels_39_molybdenum_review

REVIEW = json.loads((DATA_DIR / "export/uvek_fuels_39_molybdenum_review.json").read_text())
RECORDS = {r["rule_id"]: r for r in REVIEW["records"]}
MINERAL = "bp-ae3a127d0bb53da7"
ROAST = "bp-da6fa8e08abc0952"
MARKET = "bp-54fa038eaae8ff64"


def migrate(rid, amount=1, uncertainty=3, version="3.9", model="cutoff", strict=False):
    source = BrightwayInventory.from_data(
        consumer(RECORDS[rid]["original_rule"]["source"], amount, uncertainty),
        context=contexts("ecoinvent", version, model),
    )
    before = deepcopy(source.data)
    policy = MigrationPolicy.permissive()
    if strict:
        policy = replace(
            policy,
            on_invalid_source=PolicyAction.ERROR,
            on_invalid_target=PolicyAction.ERROR,
            on_unresolved_link=PolicyAction.ERROR,
            minimum_coverage=1.0,
        )
    result = source.migrate_background(target_context("2026"), policy=policy)
    assert source.data == before
    return result


def dataset(result, product):
    return next(a for a in result.data if a.get("reference product") == product)


@pytest.mark.parametrize("rid", RECORDS)
@pytest.mark.parametrize("amount", [-2, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_caller_quantity_sign_uncertainty_and_formula_survive(rid, amount, uncertainty):
    result = migrate(rid, amount, uncertainty)
    edge = result.data[0]["exchanges"][1]
    assert edge["amount"] == amount and edge["uncertainty type"] == uncertainty
    assert edge["scale"] == 0.2 and edge["formula"] == "dose"
    if uncertainty == 2:
        assert edge["negative"] is (amount < 0)
    else:
        assert edge["loc"] == amount
    assert any(loss.code == "migration.technosphere_information_loss" for loss in result.last_migration_report.losses)


@pytest.mark.parametrize("rid", RECORDS)
@pytest.mark.parametrize("version,model", [("3.10", "cutoff"), ("3.9", "consequential")])
def test_unreviewed_profiles_are_not_inferred(rid, version, model):
    with pytest.raises(MigrationError):
        migrate(rid, version=version, model=model)


def test_complete_finished_oxide_closure_passes_strict_target_validation():
    result = migrate(MARKET, strict=True)
    mineral = dataset(result, "molybdenite")
    inputs = [e for e in mineral["exchanges"] if e["type"] == "technosphere"]
    assert len(inputs) == 11
    producers = [e for e in inputs if e["unit"] == "kilogram"]
    assert len(producers) == 7 and sum(e["amount"] for e in producers) == pytest.approx(1)
    primary = [e for e in producers if e["name"] == "Molybdenum concentrate, main product"]
    coproduct = [e for e in producers if e["name"] == "Molybdenum concentrate, couple production Cu"]
    assert len(primary) == 1 and primary[0]["amount"] == 0.178813867230228
    assert len(coproduct) == 6
    assert sum(e["amount"] for e in coproduct) == pytest.approx(0.821186132769772)
    # The market and producer share a product: distinguish the roasting inventory.
    roast = next(a for a in result.data if any(e.get("name") == "Sulfur dioxide" for e in a["exchanges"]))
    rows = [e for e in roast["exchanges"] if e["type"] != "production"]
    assert len(rows) == 4
    assert [e["amount"] for e in rows] == [1.333e-11, 1.665232, 1.112, 0.0623]
    assert rows[-1]["type"] == "biosphere" and rows[-1]["unit"] == "kilogram"
    freight = [e for a in result.data for e in a["exchanges"] if e.get("unit") == "ton kilometer"]
    assert len(freight) == 8
    assert sorted(e["amount"] for e in freight) == sorted([0.3448, 0.0363, 0.3614, 0.3633] * 2)
    for rid in RECORDS:
        recipe = REVIEW["recipes"][RECORDS[rid]["new_recipe_id"]]
        actual = next(a for a in result.data if a["name"].startswith(recipe["name"] + " [BrightPath UVEK conversion "))
        actual_inputs = [e for e in actual["exchanges"] if e["type"] != "production"]
        for expected, observed in zip(recipe["inputs"], actual_inputs, strict=True):
            for key in ("amount", "uncertainty type", "loc", "scale", "negative", "formula", "pedigree"):
                if key in expected:
                    assert observed[key] == expected[key]


def minimal_resource():
    full = load_uvek_technosphere_resource()
    originals = {row["rule_id"]: row["original_rule"] for row in REVIEW["records"]}
    ids = set(originals) | {r["rule_id"] for r in REVIEW["component_bindings"]}
    return {
        "replace": [deepcopy(originals.get(r["id"], r)) for r in full["replace"] if r["id"] in ids],
        "delete": [],
        "recipes": {},
        "reviews": {},
        "coverage": {},
    }


def test_replay_is_idempotent():
    resource = minimal_resource()
    apply_fuels_39_molybdenum_review(resource)
    before = deepcopy(resource)
    apply_fuels_39_molybdenum_review(resource)
    assert resource == before
    assert not any(r.get("zero_burden") for r in REVIEW["recipes"].values())
    assert all(w["reference_amount"] == 1 and w["nonproduction_count"] > 0 for w in REVIEW["native_2026_witnesses"])


@pytest.mark.parametrize("change", ["parent", "component", "recipe", "shared_review", "evidence"])
def test_drift_refuses_before_mutation(tmp_path, change):
    resource = minimal_resource()
    options = {}
    if change == "parent":
        next(r for r in resource["replace"] if r["id"] in RECORDS)["confidence"] = -1
    elif change == "component":
        next(r for r in resource["replace"] if r["id"] not in RECORDS)["confidence"] = -1
    elif change in ("recipe", "shared_review"):
        apply_fuels_39_molybdenum_review(resource)
        if change == "recipe":
            resource["recipes"][RECORDS[MINERAL]["new_recipe_id"]]["inputs"][0]["amount"] *= 2
        else:
            next(iter(resource["reviews"].values()))["rationale"] += " changed"
    else:
        path = tmp_path / REVIEW_PATH
        path.parent.mkdir(parents=True)
        path.write_text("{}")
        options["root"] = tmp_path
    before = deepcopy(resource)
    with pytest.raises(ValueError):
        apply_fuels_39_molybdenum_review(resource, **options)
    assert resource == before
