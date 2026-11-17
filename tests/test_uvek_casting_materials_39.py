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

REVIEW = json.loads((DATA_DIR / "export/uvek_casting_materials_39_review.json").read_text())
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
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == original and len(result.data) == 35
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
        if "recipe_ref" in row["target"]:
            supplies = [e for e in target["exchanges"][1:] if e["unit"] == "kilogram"]
            assert sum(e["amount"] for e in supplies) == pytest.approx(1)
            assert not target["brightpath_conversion"].get("zero_burden")
            product = row["source"]["reference product"]
            if product == "ethoxylated alcohol (AE7)":
                expected = {
                    "Ethoxylated alcohols (AE7), palm kernel oil, at plant": 0.055045871559633,
                    "Ethoxylated alcohols (AE7), petrochemical, at plant": 0.917431192660551,
                    "Ethoxylated alcohols (AE7), coconut oil, at plant": 0.0275229357798165,
                }
                assert {e["name"]: e["amount"] for e in supplies} == expected
            if product == "nitric acid, without water, in 50% solution state":
                assert len(supplies) == 1 and supplies[0]["amount"] == 1
                assert supplies[0]["name"] == "Nitric acid, 50% in H2O, at plant"
            if product == "zircon":
                assert len(supplies) == 3
                assert all(e["name"] == "Zircon, 50% zirconium, at plant" for e in supplies)
            if product == "titanium dioxide":
                assert [(e["name"], e["amount"]) for e in supplies] == [
                    ("Titanium dioxide, chloride process, at plant", 0.629470198675497),
                    ("Titanium dioxide at plant, sulphate process, at plant", 0.370529801324503),
                ]
            if product == "silicon carbide":
                recycled = [e for e in supplies if e["name"] == "Silicon carbide, recycling, at plant"]
                assert len(recycled) == 2
                assert sum(e["amount"] for e in recycled) == pytest.approx(0.1478219366786424)
            if product == "ammonia, anhydrous, liquid":
                assert all(
                    e["name"]
                    in ("Ammonia, steam reforming, liquid, at plant", "Ammonia, partial oxidation, liquid, at plant")
                    for e in supplies
                )
            if product == "steel, low-alloyed, hot rolled":
                assert all(e["name"] == "Steel, low-alloyed, at plant" for e in supplies)
                assert len(supplies) == 2 and len(target["exchanges"]) == 3


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


@pytest.mark.parametrize("version,model", [("3.10", "cutoff"), ("3.9", "consequential")])
def test_the_new_scope_does_not_admit_other_versions_or_system_models(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROWS[0]["source"], 1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


def test_without_the_exact_permission_the_material_conversion_still_refuses():
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROWS[0]["source"], 1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(False))
