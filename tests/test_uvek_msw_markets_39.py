"""Municipal waste retains country geography, fate shares and uncontrolled emissions."""

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
from brightpath.migrations.scaling import scale_exchange

REVIEW = json.loads((DATA_DIR / "export/uvek_msw_markets_39_review.json").read_text())
MODELS = json.loads((DATA_DIR / "export/uvek_msw_39_models_review.json").read_text())
ROWS = REVIEW["records"]


def policy():
    r = load_uvek_technosphere_resource()
    rules = {x["id"]: x for x in r["replace"]}
    scopes = tuple(
        compatibility_rule_fingerprint(
            r,
            "replace",
            rules[row["rule_id"]],
            TechnosphereProfile(**REVIEW["source_profile"]),
            TechnosphereProfile(**REVIEW["target_profile"]),
        )
        for row in ROWS
    )
    return MigrationPolicy(approved_compatibility_rules=scopes, on_information_loss=PolicyAction.WARN)


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_msw_signs_shares_country_locations_and_statistics_survive(amount, uncertainty, release):
    data = consumer(ROWS[0]["source"], amount, uncertainty)
    data[0]["exchanges"].append(consumer(ROWS[1]["source"], amount, uncertainty)[0]["exchanges"][1])
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == before and len(result.data) == 49
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
    for edge, old, row in zip(result.data[0]["exchanges"][1:], before[0]["exchanges"][1:], ROWS, strict=True):
        expected = deepcopy(old)
        scale_exchange(expected, -1)
        assert all(
            edge[k] == expected[k]
            for k in ("amount", "loc", "scale", "minimum", "maximum", "negative", "formula")
            if k in old
        )
        assert edge["name"] == helpers[row["target"]["recipe_ref"]]["name"]
    for recipe_id, recipe in MODELS["recipes"].items():
        actual = helpers[recipe_id]
        assert actual["location"] == recipe["location"] and actual["exchanges"][0]["amount"] == 1
        for got, raw in zip(actual["exchanges"][1:], recipe["inputs"], strict=True):
            if "recipe" in raw:
                target = helpers[raw["recipe"]]
                assert all(got[k] == target[k] for k in ("name", "reference product", "location", "unit"))
                assert all(
                    got[k] == raw[k] for k in ("amount", "loc", "scale", "negative", "uncertainty type") if k in raw
                )
            else:
                assert all(got[k] == v for k, v in raw.items())
    zeros = {
        a["brightpath_conversion"]["recipe_id"]
        for a in result.data[1:]
        if a["brightpath_conversion"].get("zero_burden")
    }
    assert zeros == {s["recipe_id"] for s in MODELS["reused_stock_authorities"]}
    assert not any(a["brightpath_conversion"].get("source_input_omissions") for a in result.data[1:])


def test_uncontrolled_models_keep_all_emission_entries_operations_and_recovered_stocks():
    recipes = MODELS["recipes"]
    bios = [e for r in recipes.values() for e in r["inputs"] if e.get("type") == "biosphere"]
    assert len(bios) == MODELS["canonical_biosphere_entries"] == 1008
    assert len(recipes) == 46
    country = [r for r in recipes.values() if r["name"].startswith(("market for ", "market group for "))]
    assert len(country) == 37
    unsanitary = [r for r in recipes.values() if "unsanitary landfill" in r["name"]]
    assert len(unsanitary) == 4
    for recipe in unsanitary:
        tech = [e for e in recipe["inputs"] if e.get("type") == "technosphere"]
        assert [(e["unit"], e["amount"]) for e in tech] == [("megajoule", 0.046738), ("cubic meter", 0.00031708)]
    burning = next(r for r in recipes.values() if "open burning" in r["name"])
    stocks = [e for e in burning["inputs"] if "zero_burden_supplier" in e]
    assert [e["amount"] for e in stocks] == [-0.00083857, -0.017098]
    assert all(e["zero_burden_supplier"]["source_profile"] == REVIEW["source_profile"] for e in stocks)
    assert MODELS["new_zero_authorities"] == 0 and not MODELS["new_source_omissions"]


def test_prior_38_root_and_all_its_recipes_stay_unchanged():
    r = load_uvek_technosphere_resource()
    old = json.loads((DATA_DIR / "export/uvek_mining_38_msw_portfolio_review.json").read_text())
    for row in old["records"]:
        selected = next(
            x
            for x in select_profile_sign_rules(r, TechnosphereProfile(**old["source_profile"]))["replace"]
            if x["id"] == row["rule_id"]
        )
        previous = select_profile_sign_rules(
            {**r, "replace": [row["compiled_rule"]]}, TechnosphereProfile(**old["source_profile"])
        )["replace"][0]
        assert selected == previous
    assert all(r["recipes"][k] == v for k, v in old["recipes"].items())


@pytest.mark.parametrize("version,model", [("3.10", "cutoff"), ("3.12", "cutoff"), ("3.9", "consequential")])
def test_new_39_permission_does_not_leak_to_other_profiles(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROWS[0]["source"], -1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())
