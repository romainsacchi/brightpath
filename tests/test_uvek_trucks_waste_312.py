"""Current waste markets retain fates, canonical emissions and disposal signs."""

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

REVIEW = json.loads((DATA_DIR / "export/uvek_trucks_waste_312_review.json").read_text())
MODELS = json.loads((DATA_DIR / REVIEW["model_review_path"]).read_text())
ROWS = REVIEW["records"]


def policy(approved=True):
    resource = load_uvek_technosphere_resource()
    rules = {r["id"]: r for r in resource["replace"]}
    return MigrationPolicy(
        on_information_loss=PolicyAction.WARN,
        approved_compatibility_rules=(
            tuple(
                compatibility_rule_fingerprint(
                    resource,
                    "replace",
                    rules[r["rule_id"]],
                    TechnosphereProfile(**REVIEW["source_profile"]),
                    TechnosphereProfile(**REVIEW["target_profile"]),
                )
                for r in ROWS
            )
            if approved
            else ()
        ),
    )


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_treatment_signs_keep_the_full_fate_portfolio_and_signed_emissions(amount, uncertainty, release):
    data = consumer(ROWS[0]["source"], amount, uncertainty)
    data[0]["exchanges"].extend(consumer(r["source"], amount, uncertainty)[0]["exchanges"][1] for r in ROWS[1:])
    original = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == original and len(result.data) == 37
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
    for edge, old, row in zip(result.data[0]["exchanges"][1:], original[0]["exchanges"][1:], ROWS, strict=True):
        assert edge["amount"] == -amount and edge["formula"] == "(dose) * (-1.0)"
        assert edge["scale"] == old["scale"] and edge["loc"] == (old["loc"] if uncertainty == 2 else -old["loc"])
        if uncertainty == 2:
            assert edge["negative"] is not old["negative"]
        else:
            assert edge["minimum"] == -old["maximum"] and edge["maximum"] == -old["minimum"]
        market = helpers[row["target"]["recipe_ref"]]
        disposal = [e for e in market["exchanges"][1:] if e["unit"] == "kilogram"]
        freight = [e for e in market["exchanges"][1:] if e["unit"] == "ton kilometer"]
        assert len(disposal) == 12 and len(freight) == 1
        assert all(e["amount"] > 0 for e in disposal + freight)
        assert sum(e["amount"] for e in disposal) == pytest.approx(1)
        assert freight[0]["amount"] == 0.077
        assert sum("open burning" in e["name"] for e in disposal) == 1
        assert sum("open dump" in e["name"] for e in disposal) == 5
        assert sum("unsanitary landfill" in e["name"] for e in disposal) == 5
        assert sum("municipal incineration" in e["name"] for e in disposal) == 1
    for rid, helper in helpers.items():
        assert not helper["brightpath_conversion"].get("zero_burden")
        assert not helper["brightpath_conversion"].get("source_input_omissions")
        expected = [e for e in MODELS["recipes"][rid]["inputs"] if e.get("type") == "biosphere"]
        actual = [e for e in helper["exchanges"] if e["type"] == "biosphere"]
        assert len(actual) == len(expected)
        for e, raw in zip(actual, expected, strict=True):
            for k, value in raw.items():
                assert (tuple(e[k]) == tuple(value)) if k == "categories" else e[k] == value
        for e in helper["exchanges"]:
            assert "Deconstruction" not in e["name"] and "construction and demolition waste to sorting" not in e["name"]
        if "unsanitary landfill" in helper["name"]:
            operations = [e for e in helper["exchanges"][1:] if e["type"] == "technosphere"]
            assert [(e["unit"], e["amount"]) for e in operations] == [
                ("megajoule", 0.046738),
                ("cubic meter", 0.00031708),
            ]


def test_older_selected_rules_and_recipe_bodies_remain_unchanged():
    resource = load_uvek_technosphere_resource()
    for op in REVIEW["preserved_prior_approval_operations"]:
        row = next((r for r in ROWS if r["rule_id"] == op["rule_id"]), None)
        for scope in op["scopes"]:
            profile = TechnosphereProfile(**scope["source"])
            actual = next(
                r for r in select_profile_sign_rules(resource, profile)["replace"] if r["id"] == op["rule_id"]
            )
            if row:
                previous = select_profile_sign_rules({**resource, "replace": [row["original_rule"]]}, profile)[
                    "replace"
                ][0]
                assert actual == previous
            else:
                assert actual["target"] == {"recipe_ref": "trucks-38-waste-wood-market"}
                assert actual["conversion_factor"] == -1
    for recipe, expected in REVIEW["recipe_dependency_sha256"].items():
        raw = json.dumps(resource["recipes"][recipe], sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        assert hashlib.sha256(raw.encode()).hexdigest() == expected


@pytest.mark.parametrize("version,model", [("3.12.1", "cutoff"), ("3.12", "consequential")])
def test_unreviewed_contexts_still_refuse(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROWS[0]["source"], -1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


def test_without_exact_permission_the_waste_market_still_refuses():
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROWS[0]["source"], -1), context=contexts("ecoinvent", "3.12")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(False))
