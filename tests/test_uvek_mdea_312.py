"""MDEA uses its own reactants and a bounded native processing proxy."""

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
from scripts.review_uvek_mdea_312 import apply_mdea_312_review

REVIEW = json.loads((DATA_DIR / "export/uvek_mdea_312_review.json").read_text())
ROW = REVIEW["records"][0]


def policy():
    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == ROW["rule_id"])
    return MigrationPolicy(
        approved_compatibility_rules=(
            compatibility_rule_fingerprint(
                resource,
                "replace",
                rule,
                TechnosphereProfile("ecoinvent", "3.12", "cutoff"),
                TechnosphereProfile("uvek", "2025", "cutoff"),
            ),
        ),
        on_information_loss=PolicyAction.WARN,
    )


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_signed_caller_and_correct_chemical_recipes(amount, uncertainty, release):
    data = consumer(ROW["original_rule"]["source"], amount, uncertainty)
    before = deepcopy(data)
    migrated = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == before
    edge = migrated.data[0]["exchanges"][1]
    for k in ("amount", "uncertainty type", "loc", "scale", "negative", "minimum", "maximum", "formula"):
        if k in before[0]["exchanges"][1]:
            assert edge[k] == before[0]["exchanges"][1][k]
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in migrated.data[1:]}
    assert set(helpers) == set(REVIEW["recipes"]) | set(REVIEW["recipe_dependencies"])
    for helper in helpers.values():
        production = [e for e in helper["exchanges"] if e["type"] == "production"]
        assert len(production) == 1 and production[0]["amount"] == 1
    reaction = helpers[ROW["production_recipe_id"]]
    assert reaction["reference product"] == "methyldiethanolamine" and reaction["unit"] == "kilogram"
    tech = {e["name"]: e for e in reaction["exchanges"] if e["type"] == "technosphere"}
    assert tech["Methylamine, at plant"]["amount"] == REVIEW["derived_checks"]["reactants_kg"]["methylamine"]
    assert tech["Ethylene oxide, at plant"]["amount"] == REVIEW["derived_checks"]["reactants_kg"]["ethylene oxide"]
    assert not any(
        "2-methyl-2-butanol" in e.get("name", "") or "Ammonia, liquid" in e.get("name", "")
        for e in reaction["exchanges"]
    )
    assert not any(e.get("name") == "Ammonia" for e in reaction["exchanges"])
    assert all(e.get("biosphere_targets") is None for e in reaction["exchanges"])
    heat = next(e for e in tech.values() if e["reference product"] == "heat, from steam, in chemical industry")
    assert heat["unit"] == "megajoule" and heat["amount"] == 3.72
    assert sum(e["amount"] for e in tech.values() if e["unit"] == "kilowatt hour") == 0.358
    assert sum(e["amount"] for e in tech.values() if e["unit"] == "unit") == 4e-10
    assert not any(
        e.get("name") == "Steam, for chemical processes, at plant" for a in migrated.data for e in a["exchanges"]
    )


@pytest.mark.parametrize("version,model", [("3.10", "cutoff"), ("3.12.1", "cutoff"), ("3.12", "consequential")])
def test_no_other_profile_inherits_screening_review(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROW["original_rule"]["source"], 1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


def test_unapproved_and_changed_helper_refused():
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROW["original_rule"]["source"], 1), context=contexts("ecoinvent", "3.12")
        ).migrate_background(contexts("uvek", "2026").background)
    resource = deepcopy(load_uvek_technosphere_resource())
    rule = next(r for r in resource["replace"] if r["id"] == ROW["rule_id"])
    old = policy().approved_compatibility_rules[0]
    resource["recipes"][ROW["production_recipe_id"]]["inputs"][0]["amount"] *= 2
    changed = compatibility_rule_fingerprint(
        resource,
        "replace",
        rule,
        TechnosphereProfile("ecoinvent", "3.12", "cutoff"),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    )
    assert changed != old


def test_derived_elemental_bounds_and_explicit_limitations():
    check = REVIEW["derived_checks"]
    assert check["feed_MA_to_EO_molar_ratio"] == pytest.approx(2, rel=3e-5)
    assert check["implied_reaction_yield_MA"] == pytest.approx(0.94056, rel=1e-5)
    for element in ("carbon", "nitrogen"):
        amounts = check["elemental_kg_per_kg_product"]
        assert 0 < amounts[f"represented_{element}_release"] < amounts[f"{element}_feed_minus_product"]
    assert check["complete_atom_balance"] is False
    assert REVIEW["new_source_omissions"] == []
    text = " ".join(ROW["limitations"])
    assert "0.00025" in text and "Methydiethanolamine" in text and "unquantified" in text
    assert "not complete atom balance" in text
    reaction = REVIEW["recipes"][ROW["production_recipe_id"]]
    air = [e for e in reaction["inputs"] if e.get("categories", [None])[0] == "air"]
    for name, reactant in [("Methylamine", "methylamine"), ("Ethylene oxide", "ethylene oxide")]:
        release = next(e for e in air if e["name"] == name)
        assert release["amount"] == pytest.approx(0.002 * check["reactants_kg"][reactant])
        assert math.exp(release["loc"]) == pytest.approx(release["amount"])
    market = REVIEW["recipes"][ROW["recipe_id"]]
    assert [e["amount"] for e in market["inputs"]] == [1, 0.0219, 0.1713, 0.0438]
    assert all(e.get("uncertainty type") == 2 for e in market["inputs"][1:])


@pytest.mark.parametrize("drift", [None, "parent", "dependency", "recipe"])
def test_atomic_and_idempotent_authoring(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    if drift == "parent":
        next(r for r in resource["replace"] if r["id"] == ROW["rule_id"])["confidence"] = 0.01
    elif drift in ("dependency", "recipe"):
        key = next(iter(REVIEW["recipe_dependencies"])) if drift == "dependency" else ROW["production_recipe_id"]
        resource["recipes"][key]["inputs"][0]["amount"] *= 2
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_mdea_312_review(resource)
    else:
        apply_mdea_312_review(resource)
    assert resource == before
