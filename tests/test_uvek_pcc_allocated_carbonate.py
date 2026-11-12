"""Allocated carbonate retains its product and applies process ratios once."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pcc_allocated_carbonate import apply_pcc_allocated_carbonate_review

REVIEW = json.loads((DATA_DIR / "export/uvek_pcc_allocated_carbonate_review.json").read_text())
FIELDS = ("name", "reference product", "location", "unit")


def policy():
    resource = load_uvek_technosphere_resource()
    rules = {r["id"]: r for r in resource["replace"]}
    return MigrationPolicy(
        approved_compatibility_rules=tuple(
            compatibility_rule_fingerprint(
                resource,
                "replace",
                rules[row["rule_id"]],
                TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
                TechnosphereProfile("uvek", "2025", "cutoff"),
            )
            for row in REVIEW["records"]
        ),
        on_information_loss=PolicyAction.WARN,
    )


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("amount", [-2, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_caller_is_not_scaled_again_and_helper_is_not_a_metal_product(row, amount, uncertainty):
    data = consumer(row["source_witness"]["identity"], amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", "2026").background, policy=policy()
    )
    assert data == before and len(result.data) == 2
    actual = result.data[0]["exchanges"][1]
    original = before[0]["exchanges"][1]
    for key in ("amount", "uncertainty type", "loc", "scale", "negative", "minimum", "maximum", "formula"):
        if key in original:
            assert actual[key] == original[key]
    helper = result.data[1]
    assert helper["reference product"] == "calcium carbonate, precipitated" and helper["unit"] == "kilogram"
    assert helper["brightpath_conversion"]["recipe_id"] == row["recipe_id"]
    production = [e for e in helper["exchanges"] if e["type"] == "production"]
    assert len(production) == 1 and production[0]["amount"] == 1
    inputs = [e for e in helper["exchanges"] if e["type"] == "technosphere"]
    assert len(inputs) == 1
    assert {k: inputs[0][k] for k in FIELDS} == row["native_2026_witness"]["identity"]
    assert inputs[0]["amount"] == row["allocation_ratio"]
    assert inputs[0]["comment"] == REVIEW["recipes"][row["recipe_id"]]["inputs"][0]["comment"]


@pytest.mark.parametrize("version,model", [("3.6", "cutoff"), ("3.12", "cutoff"), ("3.9", "consequential")])
def test_other_contexts_cannot_inherit_allocation_review(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(REVIEW["records"][0]["source_witness"]["identity"], 1),
            context=contexts("ecoinvent", version, model),
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


def test_ratios_follow_full_source_vectors_and_keep_nominal_oxide_basis():
    assert len(REVIEW["records"]) == 2 and len(REVIEW["recipes"]) == 2
    assert sum(row["source_witness"]["nonproduction_count"] for row in REVIEW["records"]) == 334
    assert REVIEW["new_source_omissions"] == []
    for row in REVIEW["records"]:
        assert row["conversion_factor"] == row["compiled_rule"]["conversion_factor"] == 1
        assert row["source_witness"]["reference_amounts"] == row["source_main_witness"]["reference_amounts"] == [1]
        assert row["native_2026_witness"]["reference_amounts"] == [1]
        assert row["native_2026_witness"]["nonproduction_count"] > 0
        assert row["allocation_ratio"] == (
            0.01704144503403511 if row["native_reference_basis"] == "kg cobalt metal" else 0.12852314449326874
        )
    assert REVIEW["source_LCI_verification"]["signed_paired_LCI_comparisons"] == 4
    for record in REVIEW["source_LCI_verification"]["records"]:
        for check in record["paired_signed_LCI_checks"]:
            assert check["relative_L2_difference"] < check["comparison_relative_precision"]


@pytest.mark.parametrize("drift", [None, "first", "last", "recipe"])
def test_atomic_replay_rejects_changed_parents_or_allocation_ratios(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    if drift in ("first", "last"):
        row = REVIEW["records"][0 if drift == "first" else -1]
        next(r for r in resource["replace"] if r["id"] == row["rule_id"])["confidence"] = 0.1
    elif drift == "recipe":
        resource["recipes"][REVIEW["records"][0]["recipe_id"]]["inputs"][0]["amount"] = 1
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_pcc_allocated_carbonate_review(resource)
    else:
        apply_pcc_allocated_carbonate_review(resource)
    assert resource == before
