"""Acetoacetic chemistry must retain its reactants and releases, not use chloroacetic acid."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_acetoacetic_acid_39_review.json").read_text())
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
    rule = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
    fingerprint = compatibility_rule_fingerprint(
        resource,
        "replace",
        rule,
        TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    )
    return MigrationPolicy(
        approved_compatibility_rules=(fingerprint,) if approved else (), on_information_loss=PolicyAction.WARN
    )


def closure(key):
    result = set()

    def walk(recipe):
        if recipe in result:
            return
        result.add(recipe)
        for e in RECIPES[recipe]["inputs"]:
            if "recipe" in e:
                walk(e["recipe"])

    if key:
        walk(key)
    return result


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_chemistry_routes_water_and_caller_fields_survive(row, amount, uncertainty, release):
    data = consumer(row["original_rule"]["source"], amount, uncertainty)
    emission = {
        "name": "Carbon dioxide, fossil",
        "categories": ["air"],
        "unit": "kilogram",
        "type": "biosphere",
        "amount": 0.5,
    }
    data[0]["exchanges"].append(emission)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy(row)
    )
    assert data == before
    assert result.data[0]["exchanges"][-1] == emission
    assert len(result.data[0]["exchanges"]) == 3
    for key in STATS:
        if key in before[0]["exchanges"][1]:
            assert result.data[0]["exchanges"][1][key] == before[0]["exchanges"][1][key]
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
    assert set(helpers) == closure(row["recipe_id"])
    assert not any(a["brightpath_conversion"].get("zero_burden") for a in helpers.values())
    for key, helper in helpers.items():
        actual = [e for e in helper["exchanges"] if e["type"] != "production"]
        assert len(actual) == len(RECIPES[key]["inputs"])
        for edge, expected in zip(actual, RECIPES[key]["inputs"], strict=True):
            for field in STATS:
                if field in expected:
                    assert edge[field] == expected[field]
            if expected.get("type") == "biosphere":
                assert (edge["name"], tuple(edge["categories"]), edge["unit"]) == (
                    expected["name"],
                    tuple(expected["categories"]),
                    expected["unit"],
                )
            assert "Chloroacetic acid" not in edge.get("name", "")
        assert helper["brightpath_conversion"].get("source_input_omissions", []) == RECIPES[key].get(
            "source_input_omissions", []
        )
    if not helpers:
        assert all(result.data[0]["exchanges"][1][k] == v for k, v in row["compiled_rule"]["target"].items())


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
def test_each_new_exact_scope_is_required(row):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], 1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(row, False))


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
@pytest.mark.parametrize(
    "version,model", [("3.8", "cutoff"), ("3.9.1", "cutoff"), ("3.10", "cutoff"), ("3.9", "consequential")]
)
def test_no_new_other_release_or_system_model_permission(row, version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], 1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(row))


def test_source_acid_recipe_retains_reactant_mass_water_and_separate_releases():
    assert len(ROWS) == 7 and REVIEW["new_helpers"] == 4
    assert REVIEW["new_source_omissions"] == [] and REVIEW["inherited_source_omission_count"] == 46
    assert len(REVIEW["recipe_dependencies"]) == 12
    for proof in REVIEW["source_acid_production_reconstruction"]:
        recipe = REVIEW["recipes"][proof["recipe_id"]]
        assert len(recipe["inputs"]) == proof["retained_nonproduction_count"]
        biosphere = [e for e in recipe["inputs"] if e.get("type") == "biosphere"]
        assert len(biosphere) == proof["retained_direct_biosphere_count"] == 11
        assert next(e["amount"] for e in biosphere if e["name"] == "Carbon dioxide, fossil") == 1.8952
        assert (
            next(e["amount"] for e in biosphere if e["name"] == "Water, cooling, unspecified natural origin")
            == 0.056198
        )
        water_returns = [e for e in biosphere if e["name"] == "Water"]
        assert len(water_returns) == 2 and tuple(water_returns[0]["categories"]) != tuple(
            water_returns[1]["categories"]
        )
        (reactant,) = [e for e in recipe["inputs"] if e.get("recipe") == ROWS[1]["recipe_id"]]
        assert reactant["amount"] == 1.9119
    routes = REVIEW["ethyl_acetate_route_bindings"]
    assert len(routes) == 4 and {r["kind"] for r in routes} == {"esterification", "butane"}
    assert sum(r["amount"] for r in routes) == pytest.approx(1)
    assert all(not r["boundary_checks"]["native_ethyl_acetate_CAS_annotation_matches_source"] for r in ROWS[:2])
    assert all(
        r["boundary_checks"]["native_ethyl_acetate_name_formula_reaction_and_inputs_independently_checked"]
        for r in ROWS[:2]
    )
    for row in ROWS[2:]:
        assert row["compiled_rule"] == row["original_rule"]
        assert row["boundary_checks"]["source_wet_water_kg"] == 1 and row["boundary_checks"]["source_dry_mass_kg"] == 0


@pytest.mark.parametrize(
    "drift", [None, "parent", "last_water_parent", "dependency", "recipe", "omission", "authority"]
)
def test_replay_preserves_all_parents_dependencies_and_omissions_atomically(drift):
    from scripts.review_uvek_acetoacetic_acid_39 import apply_acetoacetic_acid_39_review

    resource = deepcopy(load_uvek_technosphere_resource())
    if drift in ("parent", "last_water_parent", "authority"):
        rid = (
            ROWS[-1 if drift == "last_water_parent" else 0]["rule_id"]
            if drift != "authority"
            else REVIEW["source_rule_dependency_scopes"][0]["rule_id"]
        )
        next(r for r in resource["replace"] if r["id"] == rid)["confidence"] = 0.01
    elif drift == "dependency":
        key = next(iter(REVIEW["recipe_dependencies"]))
        resource["recipes"][key]["inputs"][0]["amount"] *= 2
    elif drift == "recipe":
        resource["recipes"][ROWS[0]["recipe_id"]]["inputs"][0]["amount"] *= 2
    elif drift == "omission":
        key = next(k for k, recipe in REVIEW["recipe_dependencies"].items() if recipe.get("source_input_omissions"))
        resource["recipes"][key]["source_input_omissions"][0]["source_exchange"]["amount"] *= 2
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_acetoacetic_acid_39_review(resource)
    else:
        apply_acetoacetic_acid_39_review(resource)
    assert resource == before
