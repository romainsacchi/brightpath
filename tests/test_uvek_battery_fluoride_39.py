"""NaF keeps reaction/delivery boundaries rather than acquiring arsenide burdens."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_battery_fluoride_39 import apply_battery_fluoride_39_review

REVIEW = json.loads((DATA_DIR / "export/uvek_battery_fluoride_39_review.json").read_text())
STATS = ("amount", "uncertainty type", "loc", "scale", "shape", "minimum", "maximum", "negative", "formula")


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
            for row in (REVIEW["records"] if rows is None else rows)
        ),
        on_information_loss=PolicyAction.WARN,
    )


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_reaction_market_and_reagent_closure_preserve_signs_and_uncertainty(amount, uncertainty, release):
    rows = REVIEW["records"]
    data = consumer(rows[0]["original_rule"]["source"], amount, uncertainty)
    for row in rows[1:]:
        data[0]["exchanges"].append(consumer(row["original_rule"]["source"], amount, uncertainty)[0]["exchanges"][1])
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
    assert data == before
    assert result.data[0]["exchanges"][-1]["amount"] == 0.5
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
    expected = {**REVIEW["recipes"], **REVIEW["recipe_dependencies"]}
    assert set(helpers) == set(expected)
    for i, row in enumerate(rows, 1):
        actual = result.data[0]["exchanges"][i]
        original = before[0]["exchanges"][i]
        assert actual["unit"] == row["original_rule"]["source"]["unit"] == "kilogram"
        for key in STATS:
            if key in original:
                assert actual[key] == original[key]
    for key, helper in helpers.items():
        prod = [e for e in helper["exchanges"] if e["type"] == "production"]
        assert len(prod) == 1 and prod[0]["amount"] == 1
        edges = [e for e in helper["exchanges"] if e["type"] != "production"]
        assert len(edges) == len(expected[key]["inputs"])
        for actual, original in zip(edges, expected[key]["inputs"], strict=True):
            for field in STATS:
                if field in original:
                    assert actual[field] == original[field]
            if original.get("type") == "biosphere":
                assert actual["name"] == original["name"]
                assert tuple(actual["categories"]) == tuple(original["categories"])
                assert actual["unit"] == original["unit"]
        assert all("arsenide" not in e.get("name", "").lower() for e in edges)


def test_active_caustic_basis_cell_shares_and_retained_emissions_are_explicit():
    recipes = REVIEW["recipes"]
    reaction = recipes["battery-39-sodium-fluoride-reaction"]
    inputs = reaction["inputs"]
    assert reaction["unit"] == "kilogram"
    assert len(inputs) == 11
    hf = [e for e in inputs if "hydrogen-fluoride" in e.get("recipe", "")]
    assert sum(e["amount"] for e in hf) == 0.502449441024997
    caustic = next(e for e in inputs if "caustic-soda" in e.get("recipe", ""))
    assert caustic["amount"] == 1.00489888204999
    water = [e for e in inputs if e.get("name") == "Water"]
    assert [(e["amount"], e["categories"]) for e in water] == [(0.0093, ["air"]), (0.0147, ["water"])]
    assert len([e for e in inputs if e.get("type") == "biosphere"]) == 5
    cells = recipes["battery-39-caustic-soda-cell-mix-delivered-market"]["inputs"]
    assert len(cells) == 11
    assert [e["name"].split(", ")[2] for e in cells[:7]] == [
        "diaphragm cell",
        "mercury cell",
        "membrane cell",
        "diaphragm cell",
        "mercury cell",
        "membrane cell",
        "membrane cell",
    ]
    assert sum(e["amount"] for e in cells[:7] if "mercury cell" in e["name"]) == pytest.approx(0.04217127699763196)
    assert all("production mix" not in e["name"] for e in cells[:7])
    assert REVIEW["new_source_omissions"] == []
    assert REVIEW["primary_basis_source"]["pdf_page"] == 68
    freight = recipes["battery-39-hydrogen-fluoride-row-delivered-market"]["inputs"][5:]
    assert len(freight) == 8 and all("coal freight" not in e["name"] for e in freight)


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
def test_each_new_rule_requires_its_own_permission(row):
    remaining = [r for r in REVIEW["records"] if r["rule_id"] != row["rule_id"]]
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], 1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(remaining))


@pytest.mark.parametrize("version,model", [("3.8", "cutoff"), ("3.10", "cutoff"), ("3.9", "consequential")])
def test_unreviewed_profiles_do_not_gain_a_fluoride_route(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(REVIEW["records"][1]["original_rule"]["source"], 1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


@pytest.mark.parametrize("drift", [None, "parent", "new_recipe", "dependency"])
def test_frozen_replay_is_atomic_and_idempotent(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    if drift == "parent":
        next(r for r in resource["replace"] if r["id"] == REVIEW["records"][-1]["rule_id"])["confidence"] = 0.01
    elif drift in ("new_recipe", "dependency"):
        key = next(iter(REVIEW["recipes" if drift == "new_recipe" else "recipe_dependencies"]))
        resource["recipes"][key]["inputs"][0]["amount"] *= 2
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_battery_fluoride_39_review(resource)
    else:
        apply_battery_fluoride_39_review(resource)
    assert resource == before
