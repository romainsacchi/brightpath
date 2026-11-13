"""Mining material qualifications preserve old profiles and source coordinates."""

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
from scripts.review_uvek_mining_38_material_suppliers import apply_mining_38_material_suppliers_review

REVIEW = json.loads((DATA_DIR / "export/uvek_mining_38_material_suppliers_review.json").read_text())


def policy(row):
    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
    fp = compatibility_rule_fingerprint(
        resource,
        "replace",
        rule,
        TechnosphereProfile("ecoinvent", "3.8", "cutoff"),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    )
    return MigrationPolicy(approved_compatibility_rules=(fp,), on_information_loss=PolicyAction.WARN)


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda row: row["rule_id"])
@pytest.mark.parametrize("amount,uncertainty", [(-2, 2), (2, 3)])
def test_signed_caller_and_recipe_quantities_are_preserved(row, amount, uncertainty):
    selected = select_profile_sign_rules(
        load_uvek_technosphere_resource(), TechnosphereProfile("ecoinvent", "3.8", "cutoff")
    )
    rule = next(r for r in selected["replace"] if r["id"] == row["rule_id"])
    data = consumer(rule["source"], amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.8")).migrate_background(
        contexts("uvek", "2026").background, policy=policy(row)
    )
    edge = result.data[0]["exchanges"][1]
    factor = row["conversion_factor"]
    assert edge["amount"] == amount * factor
    assert edge["scale"] == before[0]["exchanges"][1]["scale"]
    if uncertainty == 2:
        assert edge["loc"] == before[0]["exchanges"][1]["loc"]
        assert edge["negative"] is (amount * factor < 0)
    else:
        assert edge["loc"] == amount * factor
        assert edge["minimum"] == min((amount - 1) * factor, (amount + 1) * factor)
        assert edge["maximum"] == max((amount - 1) * factor, (amount + 1) * factor)
    assert edge["formula"] == ("dose" if factor == 1 else "(dose) * (-1.0)")
    if "recipe_ref" in rule["target"]:
        recipe_id = rule["target"]["recipe_ref"]
        helper = next(a for a in result.data if a.get("brightpath_conversion", {}).get("recipe_id") == recipe_id)
        inputs = [e for e in helper["exchanges"] if e["type"] == "technosphere"]
        expected = REVIEW["recipes"][recipe_id]["inputs"]
        assert len(inputs) == len(expected)
        for actual, reference in zip(inputs, expected, strict=True):
            for field in (
                "name",
                "location",
                "unit",
                "amount",
                "uncertainty type",
                "loc",
                "scale",
                "pedigree",
                "scale without pedigree",
            ):
                if field in reference:
                    assert actual[field] == reference[field]
        if "cement" in recipe_id:
            assert inputs[0]["name"] == "Portland cement, strength class Z 42.5, at plant"
            assert "slag" not in inputs[0]["name"].lower()
            assert any("3.5t gross weight, fleet average" in e["name"] for e in inputs)
        if "ethanol" in recipe_id:
            assert inputs[0]["name"] == "Ethanol from ethylene, at plant" and inputs[0]["amount"] == 1
    else:
        assert all(edge[k] == v for k, v in rule["target"].items())
    assert data == before


def test_existing_profile_projections_and_helper_recipes_are_identical():
    resource = load_uvek_technosphere_resource()
    for row in REVIEW["records"]:
        for profile in row["original_rule"].get("reference_flow_sign_review", {}).get("profiles", []):
            p = TechnosphereProfile(**profile["source_profile"])
            assert select_profile_sign_rules({"replace": [row["original_rule"]]}, p) == select_profile_sign_rules(
                {"replace": [row["compiled_rule"]]}, p
            )
    for name, expected in REVIEW["recipe_dependencies"].items():
        assert resource["recipes"][name] == expected


@pytest.mark.parametrize("drift", [None, "parent", "new_recipe", "old_recipe"])
def test_replay_is_idempotent_and_refuses_drift_atomically(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    if drift == "parent":
        next(r for r in resource["replace"] if r["id"] == REVIEW["records"][0]["rule_id"])["confidence"] = 0.1
    if drift in ("new_recipe", "old_recipe"):
        recipes = REVIEW["recipes"] if drift == "new_recipe" else REVIEW["recipe_dependencies"]
        resource["recipes"][next(iter(recipes))]["inputs"][0]["amount"] += 1
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_mining_38_material_suppliers_review(resource)
    else:
        apply_mining_38_material_suppliers_review(resource)
    assert resource == before


def test_scope_does_not_grant_another_profile():
    row = REVIEW["records"][0]
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], 1), context=contexts("ecoinvent", "3.11")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(row))
