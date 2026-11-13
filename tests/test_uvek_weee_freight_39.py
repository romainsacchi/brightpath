"""Reviewed native whole services preserve their mass/transport basis and signs."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from brightpath.migrations.scaling import scale_exchange

REVIEW = json.loads((DATA_DIR / "export/uvek_weee_freight_39_review.json").read_text())
REFINEMENT = json.loads((DATA_DIR / "export/uvek_euro_v_freight_39_review.json").read_text())
ACTIVE_ROWS = [REVIEW["records"][0], REFINEMENT["records"][0]]


def policy(rows=None):
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
            for row in (ACTIVE_ROWS if rows is None else rows)
        ),
        on_information_loss=PolicyAction.WARN,
    )


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_native_services_keep_functional_units_and_scaled_uncertainty(amount, uncertainty, release):
    rows = ACTIVE_ROWS
    data = consumer(rows[0]["original_rule"]["source"], amount, uncertainty)
    data[0]["exchanges"].append(consumer(rows[1]["original_rule"]["source"], amount, uncertainty)[0]["exchanges"][1])
    data[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.5,
        }
    )
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == before and len(result.data) == 2
    assert result.data[0]["exchanges"][-1]["amount"] == 0.5
    for i, row in enumerate(rows, 1):
        actual = result.data[0]["exchanges"][i]
        expected = deepcopy(before[0]["exchanges"][i])
        scale_exchange(expected, row["conversion_factor"])
        for key in ("amount", "uncertainty type", "loc", "scale", "minimum", "maximum", "negative", "formula"):
            if key in expected:
                assert actual[key] == expected[key]
        if row.get("recipe_id"):
            helper = result.data[1]
            assert helper["brightpath_conversion"]["recipe_id"] == row["recipe_id"]
            assert actual["reference product"] == helper["reference product"]
            edges = [e for e in helper["exchanges"] if e["type"] != "production"]
            expected_inputs = REFINEMENT["recipes"][row["recipe_id"]]["inputs"]
            assert [e["amount"] for e in edges] == [e["amount"] for e in expected_inputs]
            assert [e["name"] for e in edges] == [e["name"] for e in expected_inputs]
            assert all(", 2010, EURO-V, " in e["name"] for e in edges)
        else:
            assert all(actual[k] == v for k, v in row["original_rule"]["target"].items())
        assert actual["unit"] == row["source_witness"]["identity"]["unit"]


@pytest.mark.parametrize("row", ACTIVE_ROWS, ids=lambda r: r["rule_id"])
def test_both_services_still_require_their_independent_scope(row):
    remaining = [r for r in ACTIVE_ROWS if r["rule_id"] != row["rule_id"]]
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], -1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(remaining))


@pytest.mark.parametrize("row", ACTIVE_ROWS, ids=lambda r: r["rule_id"])
@pytest.mark.parametrize(
    "version,model", [("3.8", "cutoff"), ("3.9.1", "cutoff"), ("3.10", "cutoff"), ("3.9", "consequential")]
)
def test_unreviewed_profiles_do_not_receive_the_39_scope(version, model, row):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], -1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


def test_frozen_native_service_boundaries_and_unchanged_rules_are_explicit():
    rules = {r["id"]: r for r in load_uvek_technosphere_resource()["replace"]}
    for row in ACTIVE_ROWS:
        assert rules[row["rule_id"]] == row.get("compiled_rule", row["original_rule"])
        assert row["source_witness"]["reference_amounts"] == [row["conversion_factor"]]
    assert [r["conversion_factor"] for r in REVIEW["records"]] == [-1, 1]
    comparison = REVIEW["weee_biosphere_comparison_2026"]
    assert comparison["source_emission_count"] == comparison["matched_emission_count"] == 15
    assert comparison["all_source_direct_emissions_match"]
    assert len(comparison["native_only_flows"]) == 1
    assert comparison["native_only_flows"][0]["name"] == "Heat, waste"
    assert comparison["native_only_flows"][0]["amount"] == 0.2376
    assert "0.066 kWh" in " ".join(REVIEW["records"][0]["limitations"])
    assert "0.02 kWh" in " ".join(REVIEW["records"][0]["limitations"])
    assert REFINEMENT["records"][0]["original_rule"] == REVIEW["records"][1]["original_rule"]
    recipe = REFINEMENT["recipes"][REFINEMENT["records"][0]["recipe_id"]]
    assert len(recipe["inputs"]) == 9
    assert all(", 2010, EURO-V, " in e["name"] for e in recipe["inputs"])
    assert sum(e["amount"] for e in recipe["inputs"]) == pytest.approx(1)
    assert REVIEW["new_source_omissions"] == []


def test_conditional_native_weights_keep_only_euro_v_and_their_size_duty_mix():
    recipe = REFINEMENT["recipes"][REFINEMENT["records"][0]["recipe_id"]]
    total = REFINEMENT["native_Euro_V_subset_total"]
    assert 0 < total < 1
    for edge, witness in zip(recipe["inputs"], REFINEMENT["native_supplier_witnesses"], strict=True):
        assert edge["amount"] == witness["original_native_fleet_weight"] / total
        assert all(a["reference_amounts"] == [1] for a in witness["native_witnesses"].values())
        assert ", 2010, EURO-V, " in edge["name"]
        assert edge["location"] == "RER" and edge["unit"] == "ton kilometer"
    assert set(REFINEMENT["conditional_size_shares"]) == {"18", "26", "32"}
    assert REFINEMENT["conditional_duty_shares"] == pytest.approx(
        {"long haul": 0.8, "regional delivery": 0.1, "urban delivery": 0.1}
    )
    assert REFINEMENT["new_source_omissions"] == []


@pytest.mark.parametrize("drift", [None, "parent", "recipe"])
def test_euro_v_replay_remains_atomic_and_idempotent(drift):
    from scripts.review_uvek_euro_v_freight_39 import apply_euro_v_freight_39_review

    resource = deepcopy(load_uvek_technosphere_resource())
    row = REFINEMENT["records"][0]
    if drift == "parent":
        next(r for r in resource["replace"] if r["id"] == row["rule_id"])["confidence"] = 0.01
    elif drift == "recipe":
        resource["recipes"][row["recipe_id"]]["inputs"][0]["amount"] *= 2
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_euro_v_freight_39_review(resource)
    else:
        apply_euro_v_freight_39_review(resource)
    assert resource == before
