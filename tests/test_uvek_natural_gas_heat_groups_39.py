"""Useful gas-heat wrappers retain regions while using reviewed native supplies."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_natural_gas_heat_groups_39_review.json").read_text())
ROWS = REVIEW["records"]
RECIPES = {**REVIEW["recipe_dependencies"], **REVIEW["recipes"]}


def policy(rows=ROWS):
    r = load_uvek_technosphere_resource()
    rules = {a["id"]: a for a in r["replace"]}
    return MigrationPolicy(
        approved_compatibility_rules=tuple(
            compatibility_rule_fingerprint(
                r,
                "replace",
                rules[a["rule_id"]],
                TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
                TechnosphereProfile("uvek", "2025", "cutoff"),
            )
            for a in rows
        ),
        on_information_loss=PolicyAction.WARN,
    )


def check_helper(actual, recipe_id, helpers):
    edges = [e for e in actual["exchanges"] if e["type"] != "production"]
    expected = RECIPES[recipe_id]["inputs"]
    assert len(edges) == len(expected)
    for edge, wanted in zip(edges, expected, strict=True):
        wanted = deepcopy(wanted)
        for binding in wanted.get("biosphere_targets", []):
            if binding["profile"] == {"family": "ecoinvent", "version": "3.10"}:
                wanted.update(binding["target"])
        for key in (
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
        ):
            if key in wanted:
                assert edge[key] == wanted[key]
        if "recipe" in wanted:
            supplier = helpers[wanted["recipe"]]
            assert all(edge[k] == supplier[k] for k in ("name", "reference product", "unit", "location"))
        else:
            for k in ("name", "reference product", "unit", "location", "categories"):
                if k in wanted:
                    assert edge[k] == wanted[k]


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_heat_interfaces_keep_regional_quantities_uncertainty_and_caller(amount, uncertainty, release):
    data = consumer(ROWS[0]["original_rule"]["source"], amount, uncertainty)
    for row in ROWS[1:]:
        data[0]["exchanges"].append(consumer(row["original_rule"]["source"], amount, uncertainty)[0]["exchanges"][1])
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == before and len(result.data) == 3
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
    assert set(helpers) == set(RECIPES)
    for i, row in enumerate(ROWS, 1):
        edge = result.data[0]["exchanges"][i]
        for k in ("amount", "uncertainty type", "loc", "scale", "negative", "formula", "minimum", "maximum"):
            if k in before[0]["exchanges"][i]:
                assert edge[k] == before[0]["exchanges"][i][k]
        supplier = helpers[row["recipe_id"]] if row.get("recipe_id") else row["compiled_rule"]["target"]
        assert all(edge[k] == supplier[k] for k in ("name", "reference product", "unit", "location"))
    for key, helper in helpers.items():
        check_helper(helper, key, helpers)
    assert all(
        e["type"] == "production" or e["type"] == "technosphere" for h in helpers.values() for e in h["exchanges"]
    )


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
def test_each_new_interface_needs_its_own_scope(row):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], 1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy([r for r in ROWS if r != row]))


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
@pytest.mark.parametrize(
    "version,model", [("3.8", "cutoff"), ("3.9.1", "cutoff"), ("3.10", "cutoff"), ("3.9", "consequential")]
)
def test_other_profiles_remain_unapproved(row, version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], 1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


@pytest.mark.parametrize("drift", [None, "parent", "upstream_rule", "new_recipe"])
def test_replay_checks_the_full_existing_dependency_basis_before_changes(drift):
    from scripts.review_uvek_natural_gas_heat_groups_39 import apply_natural_gas_heat_groups_39_review

    r = deepcopy(load_uvek_technosphere_resource())
    if drift in ("parent", "upstream_rule"):
        rid = ROWS[0]["rule_id"] if drift == "parent" else REVIEW["source_rule_dependency_scopes"][0]["rule_id"]
        next(a for a in r["replace"] if a["id"] == rid)["confidence"] = 0.01
    elif drift:
        key = next(iter(REVIEW["recipes"] if drift == "new_recipe" else REVIEW["recipe_dependencies"]))
        r["recipes"][key]["inputs"][0]["amount"] *= 2
    before = deepcopy(r)
    if drift:
        with pytest.raises(ValueError):
            apply_natural_gas_heat_groups_39_review(r)
    else:
        apply_natural_gas_heat_groups_39_review(r)
    assert r == before


def test_region_shares_and_whole_market_proxy_limits_remain_explicit():
    assert len(REVIEW["source_rule_dependency_scopes"]) == 2
    assert REVIEW["new_source_omissions"] == []
    assert len(RECIPES) == 2
    for row in ROWS:
        assert row["source_witness"]["reference_amounts"] == [1]
    assert set(e["amount"] for e in RECIPES[ROWS[1]["recipe_id"]]["inputs"]) == {
        0.0006443656888594714,
        0.9993556343111405,
    }
    assert set(e["amount"] for e in RECIPES[ROWS[2]["recipe_id"]]["inputs"]) == {
        0.9998381022867082,
        0.00016189771329179467,
    }
    assert REVIEW["source_CH_CHP_share"] == pytest.approx(0.1221907267256028)
    assert REVIEW["native_CH_CHP_share"] == 0.198966897647854
    assert REVIEW["native_CH_heat_plant_share"] == 0.801033102352146
    assert REVIEW["native_representativeness_percent"] == 90
    assert "not a 0.9 output coefficient" in " ".join(ROWS[0]["limitations"])
