"""Six battery chemical supplies retain producer routes, freight and signed reference quantities."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from uvek_review_helpers import before_train_three_markets_312_review

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from brightpath.migrations.scaling import scale_exchange

REVIEW = json.loads((DATA_DIR / "export/uvek_battery_chemicals_39_review.json").read_text())
ROWS = REVIEW["records"]


def policy(approved=True):
    resource = load_uvek_technosphere_resource()
    fps = tuple(
        compatibility_rule_fingerprint(
            resource,
            "replace",
            next(r for r in resource["replace"] if r["id"] == row["rule_id"]),
            TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
            TechnosphereProfile("uvek", "2025", "cutoff"),
        )
        for row in ROWS
    )
    return MigrationPolicy(approved_compatibility_rules=fps if approved else (), on_information_loss=PolicyAction.WARN)


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_all_six_chemicals_keep_signed_statistics_routes_and_delivery(amount, uncertainty, release):
    source = consumer(ROWS[0]["original_rule"]["source"], amount, uncertainty)
    source[0]["exchanges"] = (
        [source[0]["exchanges"][0]]
        + [consumer(row["original_rule"]["source"], amount, uncertainty)[0]["exchanges"][1] for row in ROWS]
        + [
            {
                "name": "Carbon dioxide, fossil",
                "categories": ["air"],
                "unit": "kilogram",
                "type": "biosphere",
                "amount": 0.5,
            }
        ]
    )
    before = deepcopy(source)
    result = BrightwayInventory.from_data(source, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert source == before and len(result.data) == 6
    nodes = {a.get("brightpath_conversion", {}).get("recipe_id"): a for a in result.data[1:]}
    assert result.data[0]["exchanges"][-1] == before[0]["exchanges"][-1]
    for i, row in enumerate(ROWS, 1):
        edge = result.data[0]["exchanges"][i]
        expected = deepcopy(before[0]["exchanges"][i])
        scale_exchange(expected, 1)
        for k in ("amount", "uncertainty type", "loc", "scale", "negative", "minimum", "maximum", "formula"):
            if k in expected:
                assert edge[k] == expected[k]
        if row["recipe_id"]:
            helper = nodes[row["recipe_id"]]
            actual = [e for e in helper["exchanges"] if e["type"] != "production"]
            recipe = REVIEW["recipes"][row["recipe_id"]]
            assert len(actual) == row["boundary_checks"]["source_market_entries"]
            for got, want in zip(actual, recipe["inputs"], strict=True):
                for k in (
                    "name",
                    "reference product",
                    "location",
                    "unit",
                    "amount",
                    "uncertainty type",
                    "loc",
                    "scale",
                    "scale without pedigree",
                    "negative",
                    "pedigree",
                    "formula",
                ):
                    if k in want:
                        assert got[k] == want[k]
            assert (
                len([e for e in actual if e["unit"] == "ton kilometer"])
                == row["boundary_checks"]["final_delivery_entries"]
            )
            assert all(
                e["name"] in row["boundary_checks"]["correct_native_products"]
                for e in actual
                if e["unit"] == "kilogram"
            )
        else:
            assert edge["name"] in row["boundary_checks"]["correct_native_products"]
        assert edge["unit"] == "kilogram"


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
@pytest.mark.parametrize(
    "version,model,approved",
    [
        ("3.9", "cutoff", False),
        ("3.8", "cutoff", True),
        ("3.9.1", "cutoff", True),
        ("3.10", "cutoff", True),
        ("3.9", "consequential", True),
    ],
)
def test_exact_permissions_do_not_expand_to_unreviewed_profiles(row, version, model, approved):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], 1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(approved))


def test_titanium_routes_and_grade_limits_are_explicit_without_new_omissions():
    assert REVIEW["new_recipe_definitions"] == 5
    assert REVIEW["retained_market_producer_entries"] == 11
    assert REVIEW["retained_market_freight_entries"] == 18
    assert sum(len(r["inputs"]) for r in REVIEW["recipes"].values()) == 29
    titanium = REVIEW["recipes"][ROWS[1]["recipe_id"]]["inputs"]
    assert [e["amount"] for e in titanium[:2]] == [0.241059602649006, 0.758940397350994]
    assert "chloride process" in titanium[0]["name"]
    assert "sulphate process" in titanium[1]["name"]
    assert all("production mix" not in e["name"] for e in titanium[:2])
    assert "nanomaterial" in " ".join(ROWS[3]["limitations"])
    assert REVIEW["zirconium_identifier_screening"]["source_CAS"] == "12036-23-6"
    assert REVIEW["zirconium_identifier_screening"]["native_CAS"] == "1314-23-4"
    assert ROWS[5]["original_rule"] == ROWS[5]["compiled_rule"]
    assert ROWS[5]["recipe_id"] is None
    assert REVIEW["new_source_omissions"] == []


@pytest.mark.parametrize("drift", [None, "parent", "dependency", "new_recipe"])
def test_review_replay_is_atomic_and_checks_exact_freight_dependencies(drift):
    from scripts.review_uvek_battery_chemicals_39 import apply_battery_chemicals_39_review

    resource = before_train_three_markets_312_review(deepcopy(load_uvek_technosphere_resource()))
    if drift == "parent":
        next(r for r in resource["replace"] if r["id"] == ROWS[0]["rule_id"])["conversion_factor"] = 2
    elif drift == "dependency":
        next(r for r in resource["replace"] if r["id"] == REVIEW["source_rule_dependency_scopes"][0]["rule_id"])[
            "confidence"
        ] = 0.01
    elif drift == "new_recipe":
        recipe_id = next(row["recipe_id"] for row in ROWS if row["recipe_id"])
        resource["recipes"][recipe_id]["inputs"][0]["amount"] *= 2
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_battery_chemicals_39_review(resource)
    else:
        apply_battery_chemicals_39_review(resource)
    assert resource == before
