"""Activated silica retains chemical inputs, water flows and treatment signs."""

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
from scripts.review_uvek_fuels_39_silica import REVIEW_PATH, apply_fuels_39_silica_review, digest

REVIEW = json.loads((DATA_DIR / "export/uvek_fuels_39_silica_review.json").read_text())
RECORDS = {r["rule_id"]: r for r in REVIEW["records"]}
SILICA = "bp-c6828ba55cd05f7d"


def migrate(rid, amount=1, uncertainty=3, version="3.9", model="cutoff"):
    data = consumer(RECORDS[rid]["original_rule"]["source"], amount, uncertainty)
    source = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
    before = deepcopy(source.data)
    result = source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert source.data == before
    return result


def helper(result):
    edge = result.data[0]["exchanges"][1]
    return next(a for a in result.data if a.get("code") == edge["input"][1])


@pytest.mark.parametrize("rid", RECORDS)
@pytest.mark.parametrize("amount", [-2, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_profiles_preserve_caller_sign_uncertainty_and_formula(rid, amount, uncertainty):
    result = migrate(rid, amount, uncertainty)
    edge = result.data[0]["exchanges"][1]
    assert edge["amount"] == amount and edge["uncertainty type"] == uncertainty
    assert edge["scale"] == 0.2 and edge["formula"] == "dose"
    if uncertainty == 2:
        assert edge["negative"] is (amount < 0)
    else:
        assert edge["loc"] == amount
    if rid == SILICA:
        assert helper(result)["reference product"] == "activated silica"
    else:
        assert all(edge[k] == v for k, v in RECORDS[rid]["compiled_rule"]["target"].items())
    assert any(loss.code == "migration.technosphere_information_loss" for loss in result.last_migration_report.losses)


@pytest.mark.parametrize("rid", RECORDS)
@pytest.mark.parametrize("version,model", [("3.10", "cutoff"), ("3.9", "consequential")])
def test_unreviewed_profiles_are_not_inferred(rid, version, model):
    with pytest.raises(MigrationError):
        migrate(rid, version=version, model=model)


def test_complete_silica_inventory_keeps_all_water_parameters_and_signed_services():
    result = migrate(SILICA)
    ds = helper(result)
    inputs = [e for e in ds["exchanges"] if e["type"] != "production"]
    tech = [e for e in inputs if e["type"] == "technosphere"]
    bio = [e for e in inputs if e["type"] == "biosphere"]
    assert len(tech) == 14 and len(bio) == 5
    assert all(e["uncertainty type"] == 2 for e in inputs)
    assert len(ds["parameters"]) == 4
    assert {p["name"] for p in ds["parameters"]} == {
        "Na2SiO3_to_SiO2",
        "Na2SO4_to_SiO2",
        "H2SO4_to_SiO2",
        "H2O_to_SiO2",
    }
    assert len([e for e in tech if e["name"] == "Sodium silicate, furnace process, pieces, at plant"]) == 2
    assert not any(e["name"] == "Silica sand, at plant" for e in tech)
    assert not any("spray powder" in e["name"] for e in tech)
    wastewater = [e for e in tech if "wastewater" in e["name"].lower()]
    assert len(wastewater) == 4
    assert all(e["amount"] > 0 and e["negative"] is False for e in wastewater)
    recipe = REVIEW["recipes"][RECORDS[SILICA]["new_recipe_id"]]
    assert ds["parameters"] == recipe["parameters"]
    for actual, expected_input in zip(inputs, recipe["inputs"], strict=True):
        for key in (
            "amount",
            "uncertainty type",
            "loc",
            "scale",
            "negative",
            "minimum",
            "maximum",
            "pedigree",
            "scale without pedigree",
            "formula",
        ):
            if key in expected_input:
                assert actual[key] == expected_input[key]
    expected = [e for e in recipe["inputs"] if e.get("type") == "biosphere"]
    assert [(e["name"], tuple(e["categories"]), e["unit"], e["amount"], e["loc"], e["scale"]) for e in bio] == [
        (e["name"], tuple(e["categories"]), e["unit"], e["amount"], e["loc"], e["scale"]) for e in expected
    ]


def minimal_resource():
    full = load_uvek_technosphere_resource()
    originals = {row["rule_id"]: row["original_rule"] for row in REVIEW["records"]}
    ids = set(originals) | {r["rule_id"] for r in REVIEW["component_bindings"]}
    return {
        "replace": [deepcopy(originals.get(r["id"], r)) for r in full["replace"] if r["id"] in ids],
        "delete": [],
        "recipes": {k: deepcopy(full["recipes"][k]) for k in REVIEW["recipe_dependencies"]},
        "reviews": {},
        "coverage": {},
    }


def test_replay_is_idempotent_and_existing_service_closures_are_unchanged():
    resource = minimal_resource()
    apply_fuels_39_silica_review(resource)
    before = deepcopy(resource)
    apply_fuels_39_silica_review(resource)
    assert resource == before
    assert all(digest(resource["recipes"][rid]) == sha for rid, sha in REVIEW["recipe_dependencies"].items())
    zero = [w for w in REVIEW["native_2026_witnesses"] if w["nonproduction_count"] == 0]
    assert len(zero) == 2
    assert {w["identity"]["unit"] for w in zero} == {"kilowatt hour", "megajoule"}
    assert all(w["frozen_zero_burden_dependency"] in REVIEW["recipe_dependencies"] for w in zero)
    assert not any(r.get("zero_burden") for r in REVIEW["recipes"].values())


@pytest.mark.parametrize("change", ["parent", "component", "dependency", "recipe", "shared_review", "evidence"])
def test_changed_source_bindings_or_helpers_refuse_before_mutation(tmp_path, change):
    resource = minimal_resource()
    options = {}
    if change == "parent":
        resource["replace"][0]["confidence"] = -1
    elif change == "component":
        ingredient = next(r for r in resource["replace"] if r["id"] not in RECORDS)
        ingredient["confidence"] = -1
    elif change == "dependency":
        next(iter(resource["recipes"].values()))["inputs"][0]["amount"] *= 2
    elif change in ("recipe", "shared_review"):
        apply_fuels_39_silica_review(resource)
        if change == "recipe":
            resource["recipes"][RECORDS[SILICA]["new_recipe_id"]]["inputs"][0]["amount"] *= 2
        else:
            next(iter(resource["reviews"].values()))["rationale"] += " changed"
    else:
        path = tmp_path / REVIEW_PATH
        path.parent.mkdir(parents=True)
        path.write_text("{}")
        options["root"] = tmp_path
    before = deepcopy(resource)
    with pytest.raises(ValueError):
        apply_fuels_39_silica_review(resource, **options)
    assert resource == before


def test_silica_full_helper_closure_passes_strict_target_validation():
    source = BrightwayInventory.from_data(
        consumer(RECORDS[SILICA]["original_rule"]["source"], 1),
        context=contexts("ecoinvent", "3.9"),
    )
    policy = replace(
        MigrationPolicy.permissive(),
        on_invalid_source=PolicyAction.ERROR,
        on_invalid_target=PolicyAction.ERROR,
        on_unresolved_link=PolicyAction.ERROR,
        minimum_coverage=1.0,
    )
    result = source.migrate_background(target_context("2026"), policy=policy)
    assert helper(result)["unit"] == "kilogram"
