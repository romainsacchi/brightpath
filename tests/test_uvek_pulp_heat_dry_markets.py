"""Chemical markets preserve original shares, delivery rows and reference quantities."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pulp_heat_dry_markets import apply_pulp_heat_dry_markets_review

REVIEW = json.loads((DATA_DIR / "export/uvek_pulp_heat_dry_markets_review.json").read_text())


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


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda row: row["rule_id"])
@pytest.mark.parametrize("amount", [-2, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_signed_callers_and_all_helper_inputs_are_preserved(row, amount, uncertainty):
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
    helper = next(a for a in result.data if a.get("brightpath_conversion", {}).get("recipe_id") == row["recipe_id"])
    inputs = [e for e in helper["exchanges"] if e["type"] == "technosphere"]
    expected = REVIEW["recipes"][row["recipe_id"]]["inputs"]
    assert len(inputs) == len(expected)
    for actual, source in zip(inputs, expected, strict=True):
        for key, value in source.items():
            assert actual[key] == value


@pytest.mark.parametrize("version,model", [("3.6", "cutoff"), ("3.11", "cutoff"), ("3.9", "consequential")])
def test_other_profiles_are_not_authorized(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(REVIEW["records"][0]["source_witness"]["identity"], 1),
            context=contexts("ecoinvent", version, model),
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


def test_reference_bases_and_all_original_market_rows_are_retained():
    assert len(REVIEW["records"]) == 9 and len(REVIEW["recipes"]) == 9
    assert len(REVIEW["native_2026_witnesses"]) == 17
    assert REVIEW["new_source_omissions"] == []
    assert sum(len(r["component_bindings"]) for r in REVIEW["records"]) == 104
    for row in REVIEW["records"]:
        assert row["source_witness"]["reference_amounts"] == [1]
        assert row["source_nominal_dry_mass_per_reference_kg"] == 1
        assert row["source_witness"]["nonproduction_count"] == len(row["component_bindings"])
        recipe = REVIEW["recipes"][row["recipe_id"]]
        for binding, edge in zip(row["component_bindings"], recipe["inputs"], strict=True):
            assert edge == {**binding["target"], **binding["statistics"], "type": "technosphere"}
            assert edge["unit"] == binding["source_unit"]
        producers = [b for b in row["component_bindings"] if b["kind"] == "producer"]
        assert sum(b["statistics"]["amount"] for b in producers) == pytest.approx(1)
        if row["compiled_rule"]["source"]["reference product"] == "sulfur":
            assert all(b["target"]["name"] == "Secondary sulphur, at refinery" for b in producers)
        if row["compiled_rule"]["source"]["reference product"] == "sodium sulfate, anhydrite":
            assert all(b["target"]["name"] == "Sodium sulphate, powder, production mix, at plant" for b in producers)


@pytest.mark.parametrize("drift", [None, "parent", "recipe"])
def test_replay_is_atomic(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    if drift == "parent":
        next(r for r in resource["replace"] if r["id"] == REVIEW["records"][-1]["rule_id"])["confidence"] = 0.1
    elif drift == "recipe":
        resource["recipes"][REVIEW["records"][0]["recipe_id"]]["inputs"][0]["amount"] = 9
    before = deepcopy(resource)
    if drift is None:
        apply_pulp_heat_dry_markets_review(resource)
    else:
        with pytest.raises(ValueError):
            apply_pulp_heat_dry_markets_review(resource)
    assert resource == before
