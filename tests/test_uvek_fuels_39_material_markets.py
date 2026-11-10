"""Material stocks retain delivery; kg equipment must not become a whole plant."""

import json
from copy import deepcopy
from dataclasses import replace

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_fuels_39_material_markets import REVIEW_PATH, apply_fuels_39_material_markets_review, digest

REVIEW = json.loads((DATA_DIR / "export/uvek_fuels_39_material_markets_review.json").read_text())
RECORDS = {r["rule_id"]: r for r in REVIEW["records"]}
FACTORY = "bp-9860f8d2b0136d04"
NICKEL = "bp-1104956dff1c701e"
FOAM = "bp-e25771265d4d36c5"


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


def helper(result):
    edge = result.data[0]["exchanges"][1]
    return next(a for a in result.data if a.get("code") == edge["input"][1])


@pytest.mark.parametrize("rid", RECORDS)
@pytest.mark.parametrize("amount", [-2, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_caller_stock_mass_uncertainty_and_formula_survive(rid, amount, uncertainty):
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


@pytest.mark.parametrize("rid", RECORDS)
def test_strict_conversion_keeps_all_producer_and_freight_rows(rid):
    result = migrate(rid, strict=True)
    ds = helper(result)
    row = RECORDS[rid]
    recipe = REVIEW["recipes"][row["new_recipe_id"]]
    inputs = [e for e in ds["exchanges"] if e["type"] != "production"]
    assert len(inputs) == row["retention"]["source_nonproduction_rows"]
    assert len([e for e in inputs if e["unit"] == "kilogram"]) == row["retention"]["source_producer_rows"]
    assert len([e for e in inputs if e["unit"] == "ton kilometer"]) == row["retention"]["source_freight_rows"]
    assert sum(e["amount"] for e in inputs if e["unit"] == "kilogram") == pytest.approx(1)
    for actual, expected in zip(inputs, recipe["inputs"], strict=True):
        for key in (
            "amount",
            "uncertainty type",
            "loc",
            "scale",
            "shape",
            "negative",
            "minimum",
            "maximum",
            "pedigree",
            "scale without pedigree",
            "formula",
        ):
            if key in expected:
                assert actual[key] == expected[key]
    assert not any(
        loss.code == "migration.recipe_source_input_omission" for loss in result.last_migration_report.losses
    )


def test_factory_uses_kg_facilities_and_not_a_multimillion_kg_plant():
    ds = helper(migrate(FACTORY, strict=True))
    inputs = [e for e in ds["exchanges"] if e["type"] == "technosphere"]
    assert len(inputs) == 2
    assert all(e["name"] == "Facilities, chemical production" and e["unit"] == "kilogram" for e in inputs)
    assert [e["amount"] for e in inputs] == [0.330592351639303, 0.669407648360697]
    basis = REVIEW["factory_basis_comparison"]
    assert basis["native_reference_unit"] == "kilogram" and len(basis["matches"]) == 9
    assert all(p["same_total_quantity_and_unit"] for p in basis["matches"])


def test_flexible_foam_preserves_three_branches_and_reports_formulation_proxy():
    ds = helper(migrate(FOAM, strict=True))
    inputs = [e for e in ds["exchanges"] if e["type"] == "technosphere"]
    assert [e["amount"] for e in inputs[:3]] == [0.2, 0.4, 0.4]
    assert len(inputs) == 12
    assert all(e["name"] == "Polyurethane, flexible foam, at plant" for e in inputs[:3])
    basis = RECORDS[FOAM]["limitations"][0]
    assert "20% MDI-based" in basis and "does not reproduce the MDI-specific chemistry" in basis


def test_prior_nickel_312_selection_and_direct_native_output_are_unchanged():
    resource = load_uvek_technosphere_resource()
    profile = TechnosphereProfile("ecoinvent", "3.12", "cutoff")
    selected = next(r for r in select_profile_sign_rules(resource, profile)["replace"] if r["id"] == NICKEL)
    (proof,) = RECORDS[NICKEL]["preserved_profiles"]
    assert digest(selected) == proof["selected_rule_sha256"]
    original = {**resource, "replace": [RECORDS[NICKEL]["original_rule"]]}
    assert selected == select_profile_sign_rules(original, profile)["replace"][0]
    result = migrate(NICKEL, version="3.12", strict=True)
    edge = result.data[0]["exchanges"][1]
    assert all(edge[k] == v for k, v in RECORDS[NICKEL]["original_rule"]["target"].items())
    assert len(result.data) == 1


def minimal_resource():
    full = load_uvek_technosphere_resource()
    originals = {row["rule_id"]: row["original_rule"] for row in REVIEW["records"]}
    ids = set(originals) | {r["rule_id"] for r in REVIEW["component_bindings"]}
    ref = RECORDS[NICKEL]["original_rule"]["review_ref"]
    return {
        "replace": [deepcopy(originals.get(r["id"], r)) for r in full["replace"] if r["id"] in ids],
        "delete": [],
        "recipes": {},
        "reviews": {ref: deepcopy(full["reviews"][ref])},
        "coverage": {},
    }


def test_replay_is_idempotent():
    resource = minimal_resource()
    apply_fuels_39_material_markets_review(resource)
    before = deepcopy(resource)
    apply_fuels_39_material_markets_review(resource)
    assert resource == before
    assert sum(r["retention"]["source_producer_rows"] for r in REVIEW["records"]) == 22
    assert sum(r["retention"]["source_freight_rows"] for r in REVIEW["records"]) == 29


@pytest.mark.parametrize("change", ["parent", "component", "recipe", "shared_review", "evidence"])
def test_drift_refuses_before_mutation(tmp_path, change):
    resource = minimal_resource()
    options = {}
    if change == "parent":
        next(r for r in resource["replace"] if r["id"] in RECORDS)["confidence"] = -1
    elif change == "component":
        next(r for r in resource["replace"] if r["id"] not in RECORDS)["confidence"] = -1
    elif change in ("recipe", "shared_review"):
        apply_fuels_39_material_markets_review(resource)
        if change == "recipe":
            resource["recipes"][RECORDS[FACTORY]["new_recipe_id"]]["inputs"][0]["amount"] *= 2
        else:
            next(iter(resource["reviews"].values()))["rationale"] += " changed"
    else:
        path = tmp_path / REVIEW_PATH
        path.parent.mkdir(parents=True)
        path.write_text("{}")
        options["root"] = tmp_path
    before = deepcopy(resource)
    with pytest.raises(ValueError):
        apply_fuels_39_material_markets_review(resource, **options)
    assert resource == before
