"""Production heat mappings retain source basis and exclude invented delivery losses."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_remaining_production_heat import apply_remaining_production_heat_review

REVIEW = json.loads((DATA_DIR / "export/uvek_remaining_production_heat_review.json").read_text())
CASES = [(r, p) for r in REVIEW["records"] for p in r["source_profiles"]]


def policy(row, profile):
    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
    scope = compatibility_rule_fingerprint(
        resource, "replace", rule, TechnosphereProfile(**profile), TechnosphereProfile("uvek", "2025", "cutoff")
    )
    return MigrationPolicy(approved_compatibility_rules=(scope,), on_information_loss=PolicyAction.WARN)


@pytest.mark.parametrize("row,profile", CASES, ids=[r["rule_id"] + "-" + p["version"] for r, p in CASES])
@pytest.mark.parametrize("amount,uncertainty", [(-2, 2), (2, 3)])
def test_signed_heat_and_uncertainty_survive_exact_profile(row, profile, amount, uncertainty):
    resource = load_uvek_technosphere_resource()
    selected = select_profile_sign_rules(resource, TechnosphereProfile(**profile))
    rule = next(r for r in selected["replace"] if r["id"] == row["rule_id"])
    data = consumer(rule["source"], amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", profile["version"])).migrate_background(
        contexts("uvek", "2026").background, policy=policy(row, profile)
    )
    edge = result.data[0]["exchanges"][1]
    for field in ("amount", "uncertainty type", "loc", "scale", "minimum", "maximum", "negative", "formula"):
        if field in before[0]["exchanges"][1]:
            assert edge[field] == before[0]["exchanges"][1][field]
    if "recipe_ref" in rule["target"]:
        recipe_id = rule["target"]["recipe_ref"]
        helper = next(a for a in result.data if a.get("brightpath_conversion", {}).get("recipe_id") == recipe_id)
        inputs = [e for e in helper["exchanges"] if e["type"] == "technosphere"]
        assert len(inputs) == 57
        assert sum(e["amount"] for e in inputs) == pytest.approx(1)
        assert all(e["unit"] == "megajoule" and e["amount"] > 0 and "district" not in e["name"].lower() for e in inputs)
        assert not any(e["type"] == "biosphere" for e in helper["exchanges"])
        for actual, expected in zip(inputs, REVIEW["recipes"][recipe_id]["inputs"], strict=True):
            for field in ("name", "product", "location", "unit", "amount", "uncertainty type", "loc"):
                assert actual[field] == expected.get("reference product" if field == "product" else field)
    else:
        assert all(edge[k] == v for k, v in rule["target"].items())
    assert data == before


def test_direct_1mwe_correction_selects_existing_exergy_product():
    row = next(r for r in REVIEW["records"] if r["rule_id"] == "bp-0ca8243428726044")
    assert row["compiled_rule"]["target"]["name"] == "Heat, at cogen 1MWe lean burn, allocation exergy"
    assert row["original_rule"]["target"]["name"] == "Heat, at cogen 1MWe lean burn, allocation heat"


@pytest.mark.parametrize("drift", [None, "parent", "recipe"])
def test_replay_is_idempotent_and_refuses_drift_atomically(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    if drift == "parent":
        next(r for r in resource["replace"] if r["id"] == REVIEW["records"][0]["rule_id"])["confidence"] = 0.1
    if drift == "recipe":
        resource["recipes"][next(iter(REVIEW["recipes"]))]["inputs"][0]["amount"] *= 2
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_remaining_production_heat_review(resource)
    else:
        apply_remaining_production_heat_review(resource)
    assert resource == before


def test_new_scope_does_not_authorize_another_release():
    row, profile = CASES[0]
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], -2), context=contexts("ecoinvent", "3.11")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(row, profile))


def test_previously_reviewed_global_recipe_remains_unchanged():
    old = json.loads((DATA_DIR / "export/uvek_steel_39_ng_heat_review.json").read_text())
    resource = load_uvek_technosphere_resource()
    for name, recipe in old["recipes"].items():
        assert resource["recipes"][name] == recipe
