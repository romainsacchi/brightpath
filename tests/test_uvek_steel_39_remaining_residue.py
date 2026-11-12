"""Residue markets retain waste chemistry, allocation interfaces and audited zero losses."""

import json
from collections import Counter
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from brightpath.migrations.schema import validate_migration_resource
from scripts.review_uvek_steel_39_remaining_residue import apply_steel_39_remaining_residue_review

REVIEW = json.loads((DATA_DIR / "export/uvek_steel_39_remaining_residue_review.json").read_text())


def policy():
    resource = load_uvek_technosphere_resource()
    rules = {r["id"]: r for r in resource["replace"]}
    scopes = tuple(
        compatibility_rule_fingerprint(
            resource,
            "replace",
            rules[row["rule_id"]],
            TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
            TechnosphereProfile("uvek", "2025", "cutoff"),
        )
        for row in REVIEW["records"]
    )
    return MigrationPolicy(approved_compatibility_rules=scopes, on_information_loss=PolicyAction.WARN)


def graph(amount, uncertainty=0):
    data = consumer(REVIEW["records"][0]["source_witness"]["identity"], amount, uncertainty)
    data[0]["exchanges"] = data[0]["exchanges"][:1] + [
        consumer(r["source_witness"]["identity"], amount, uncertainty)[0]["exchanges"][1] for r in REVIEW["records"]
    ]
    return data


@pytest.mark.parametrize("amount", [-2, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_all_exact_profiles_preserve_signs_uncertainty_helpers_and_source(amount, uncertainty):
    data = graph(amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", "2026").background, policy=policy()
    )
    assert data == before and len(result.data) == 33
    for row, edge in zip(REVIEW["records"], result.data[0]["exchanges"][1:], strict=True):
        factor = row["conversion_factor"]
        assert edge["amount"] == amount * factor and edge["scale"] == 0.2
        assert edge["formula"] == ("dose" if factor == 1 else "(dose) * (-1.0)")
        if uncertainty == 2:
            assert edge["negative"] is (edge["amount"] < 0)
        else:
            assert edge["loc"] == amount * factor
            assert edge["minimum"] == min((amount - 1) * factor, (amount + 1) * factor)
            assert edge["maximum"] == max((amount - 1) * factor, (amount + 1) * factor)
    helpers = {a.get("brightpath_conversion", {}).get("recipe_id"): a for a in result.data[1:]}
    assert set(helpers) == set(REVIEW["recipes"])
    for key, recipe in REVIEW["recipes"].items():
        actual = [e for e in helpers[key]["exchanges"] if e["type"] != "production"]
        assert len(actual) == len(recipe["inputs"])
        for a, b in zip(actual, recipe["inputs"], strict=True):
            for field in (
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
                if field in b:
                    assert a[field] == b[field]
            if b.get("type") == "biosphere":
                assert (a["name"], tuple(a["categories"]), a["unit"]) == (b["name"], tuple(b["categories"]), b["unit"])
        assert helpers[key].get("brightpath_conversion", {}).get("source_input_omissions", []) == recipe.get(
            "source_input_omissions", []
        )
    omissions = [i for i in result.last_migration_report.issues if i.code == "migration.recipe_source_input_omission"]
    assert len(omissions) == 3


@pytest.mark.parametrize("version, model", [("3.6", "cutoff"), ("3.12", "cutoff"), ("3.9", "consequential")])
def test_other_contexts_cannot_inherit_allocation_approvals(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(graph(1), context=contexts("ecoinvent", version, model)).migrate_background(
            contexts("uvek", "2026").background, policy=policy()
        )


@pytest.mark.parametrize("row", REVIEW["refused_records"], ids=lambda r: r["rule_id"])
def test_empty_negative_clinker_references_never_use_the_old_native_slag_match(row):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["source_witness"]["identity"], -1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())
    assert row["source_witness"]["reference_amounts"] == [-1]
    assert row["source_witness"]["nonproduction_count"] == 0
    assert row["main_clinker_witness"]["reference_amounts"] == [1]
    assert row["main_clinker_witness"]["nonproduction_count"] > 0


@pytest.mark.parametrize("drift", [None, "first", "last", "refused"])
def test_atomic_replay_rejects_parent_drift(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    if drift:
        row = REVIEW["refused_records"][0] if drift == "refused" else REVIEW["records"][0 if drift == "first" else -1]
        next(r for r in resource["replace"] if r["id"] == row["rule_id"])["confidence"] = 0.123
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError, match="parent changed"):
            apply_steel_39_remaining_residue_review(resource)
    else:
        apply_steel_39_remaining_residue_review(resource)
    assert resource == before


def test_source_boundary_counts_and_omission_evidence_are_literal():
    assert Counter(r["kind"] for r in REVIEW["records"]) == {
        "allocated_stock": 6,
        "landfill": 10,
        "recovery_handover": 7,
        "market": 9,
    }
    assert REVIEW["source_omissions_total"] == 3
    for row in REVIEW["records"]:
        recipe = REVIEW["recipes"][row["recipe_id"]]
        assert row["source_witness"]["reference_amounts"] == [row["conversion_factor"]]
        if row["kind"] == "allocated_stock":
            assert row["source_witness"]["nonproduction_count"] == 0
            assert recipe["zero_burden"] is True and recipe["inputs"] == []
        if row["kind"] == "recovery_handover":
            assert len(recipe["inputs"]) == 1 and recipe["inputs"][0]["amount"] == -1
            assert recipe["inputs"][0]["zero_burden_supplier"]["source_profile"] == REVIEW["source_profile"]
        for omission in row["source_omissions"]:
            assert omission["source_activity"] == row["source_witness"]["identity"]
            assert (
                omission["source_exchange"]["amount"] < 0
                and omission["source_exchange"]["name"] == "clinker production"
            )
            assert omission["source_profile"] == REVIEW["source_profile"]


def test_recipe_drift_is_refused_before_any_mutation():
    resource = deepcopy(load_uvek_technosphere_resource())
    key = next(r["recipe_id"] for r in REVIEW["records"] if r["kind"] == "market")
    resource["recipes"][key]["inputs"][0]["amount"] *= 2
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="recipe changed"):
        apply_steel_39_remaining_residue_review(resource)
    assert resource == before


def test_negative_empty_reference_cannot_bypass_positive_stock_validation():
    resource = deepcopy(load_uvek_technosphere_resource())
    row = next(r for r in REVIEW["records"] if r["kind"] == "allocated_stock")
    rule = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
    rule["reference_flow_sign_review"]["profiles"][0]["review"]["source_sign"] = -1
    with pytest.raises(MigrationError):
        validate_migration_resource(resource)


def test_replay_from_all_original_parents_recreates_the_packaged_resource():
    resource = deepcopy(load_uvek_technosphere_resource())
    expected = deepcopy(resource)
    indexed = {r["id"]: r for r in resource["replace"]}
    for row in REVIEW["records"] + REVIEW["refused_records"]:
        indexed[row["rule_id"]].clear()
        indexed[row["rule_id"]].update(deepcopy(row["original_rule"]))
    for key in REVIEW["recipes"]:
        del resource["recipes"][key]
    new_review = REVIEW["records"][0]["compiled_rule"]["review_ref"]
    assert all(r["original_rule"].get("review_ref") != new_review for r in REVIEW["records"])
    del resource["reviews"][new_review]
    apply_steel_39_remaining_residue_review(resource)
    validate_migration_resource(resource)
    assert resource == expected
