"""Required material markets keep bases, complete production and delivery."""

import hashlib
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

REVIEW = json.loads((DATA_DIR / "export/uvek_recycling_materials_38_review.json").read_text())
ROWS = REVIEW["records"] + REVIEW["additional_approvals"]
PRIOR = [(op, scope) for op in REVIEW["preserved_prior_approval_operations"] for scope in op["scopes"]]
FIELDS = ("name", "reference product", "location", "unit")


def policy(approved=True):
    resource = load_uvek_technosphere_resource()
    rules = {r["id"]: r for r in resource["replace"]}
    scopes = tuple(
        compatibility_rule_fingerprint(
            resource,
            "replace",
            rules[row["rule_id"]],
            TechnosphereProfile(**REVIEW["source_profile"]),
            TechnosphereProfile(**REVIEW["target_profile"]),
        )
        for row in ROWS
    )
    return MigrationPolicy(
        approved_compatibility_rules=scopes if approved else (), on_information_loss=PolicyAction.WARN
    )


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_signed_material_demands_keep_units_statistics_and_delivery(amount, uncertainty, release):
    data = consumer(ROWS[0]["source"], amount, uncertainty)
    data[0]["exchanges"].extend(consumer(r["source"], amount, uncertainty)[0]["exchanges"][1] for r in ROWS[1:])
    original = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.8")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == original and len(result.data) == 10
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
    for edge, old, row in zip(result.data[0]["exchanges"][1:], original[0]["exchanges"][1:], ROWS, strict=True):
        for key in ("amount", "loc", "scale", "formula", "negative", "minimum", "maximum", "uncertainty type"):
            if key in old:
                assert edge[key] == old[key]
        target = row["target"]
        if "recipe_ref" in target:
            recipe = target["recipe_ref"]
            target = helpers[recipe]
            expected = REVIEW["recipes"][recipe]["inputs"]
            assert len(target["exchanges"]) == len(expected) + 1
            assert len(row["source_component_checksums"]) == len(expected)
            for actual, raw in zip(target["exchanges"][1:], expected, strict=True):
                assert all(actual[k] == value for k, value in raw.items())
            assert not target["brightpath_conversion"].get("source_input_omissions")
        assert all(edge[k] == target[k] for k in FIELDS)
        assert edge["unit"] == row["source"]["unit"]
        if "corrugated board box" in row["source"]["name"]:
            stock = [e for e in target["exchanges"][1:] if e["unit"] == "kilogram"]
            assert sum(e["amount"] for e in stock) == 1
            assert all(e["name"] == "Packaging, corrugated board, mixed fibre, single wall, at plant" for e in stock)
            assert all("base paper" not in e["name"].lower() for e in stock)
        if "tin plated" in row["source"]["name"]:
            assert edge["unit"] == "square meter" and edge["amount"] == amount
            stock = [e for e in target["exchanges"][1:] if e["unit"] == "square meter"]
            assert sum(e["amount"] for e in stock) == 1
        if "market for fleece" in row["source"]["name"]:
            assert len(target["exchanges"]) == 7
            assert sorted(e["amount"] for e in target["exchanges"][1:] if e["unit"] == "ton kilometer") == [
                0.0246,
                0.0511,
                0.3857,
                1.1439,
            ]


@pytest.mark.parametrize("operation,scope", PRIOR, ids=[f"{o['rule_id']}-{s['source']['version']}" for o, s in PRIOR])
def test_older_physical_correspondences_and_borrowed_recipes_remain_unchanged(operation, scope):
    resource = load_uvek_technosphere_resource()
    row = next((r for r in REVIEW["records"] if r["rule_id"] == operation["rule_id"]), None)
    if row:
        profile = TechnosphereProfile(**scope["source"])
        actual = next(r for r in select_profile_sign_rules(resource, profile)["replace"] if r["id"] == row["rule_id"])
        original = select_profile_sign_rules({**resource, "replace": [row["original_rule"]]}, profile)["replace"][0]
        assert all(
            actual.get(k) == original.get(k)
            for k in ("source", "target", "mapping_method", "classification", "confidence")
        )
        assert actual.get("conversion_factor", 1) == original.get("conversion_factor", 1)
    for recipe, digest in REVIEW["recipe_dependency_sha256"].items():
        value = json.dumps(resource["recipes"][recipe], sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        assert hashlib.sha256(value.encode()).hexdigest() == digest


def test_reserved_acid_market_is_closed_but_does_not_grant_a_global_conversion():
    row = REVIEW["parent_only_component_decisions"][0]
    recipe = load_uvek_technosphere_resource()["recipes"][row["target"]["recipe_ref"]]
    assert recipe["unit"] == "kilogram" and len(recipe["inputs"]) == 26
    acid = [e for e in recipe["inputs"] if e["unit"] == "kilogram"]
    assert all(e["name"] == "Sulphuric acid, liquid, at plant" for e in acid)
    assert sum(e["amount"] for e in acid) == pytest.approx(1)
    with pytest.raises(MigrationError, match=row["rule_id"]):
        BrightwayInventory.from_data(
            consumer(row["source"], 1), context=contexts("ecoinvent", "3.8")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


@pytest.mark.parametrize("version,model", [("3.8.1", "cutoff"), ("3.8", "consequential")])
def test_the_new_scope_does_not_admit_other_versions_or_system_models(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROWS[0]["source"], 1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


def test_without_the_exact_permission_the_material_conversion_still_refuses():
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROWS[0]["source"], 1), context=contexts("ecoinvent", "3.8")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(False))
