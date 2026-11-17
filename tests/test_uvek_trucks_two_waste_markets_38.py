"""Keep source waste shares while using explicit complete treatment proxies."""

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

REVIEW = json.loads((DATA_DIR / "export/uvek_trucks_two_waste_markets_38_review.json").read_text())
ROWS = REVIEW["records"]


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
def test_negative_waste_references_keep_the_original_treatment_mix(amount, uncertainty, release):
    data = consumer(ROWS[0]["source"], amount, uncertainty)
    data[0]["exchanges"].append(consumer(ROWS[1]["source"], amount, uncertainty)[0]["exchanges"][1])
    original = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.8")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == original
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
    for edge, old, row in zip(result.data[0]["exchanges"][1:], original[0]["exchanges"][1:], ROWS, strict=True):
        assert edge["amount"] == -amount and edge["formula"] == "(dose) * (-1.0)"
        assert edge["loc"] == (old["loc"] if uncertainty == 2 else -old["loc"])
        assert edge["scale"] == old["scale"]
        if uncertainty == 2:
            assert edge["negative"] is not old["negative"]
        else:
            assert edge["minimum"] == -old["maximum"] and edge["maximum"] == -old["minimum"]
        root = helpers[row["target_recipe"]]
        assert edge["name"] == root["name"] and not root["brightpath_conversion"].get("source_input_omissions")
        inputs = root["exchanges"][1:]
        assert len(inputs) == len(REVIEW["recipes"][row["target_recipe"]]["inputs"])
        assert len([e for e in inputs if e["unit"] == "ton kilometer"]) == 7
        expected = []
        for raw in REVIEW["recipes"][row["target_recipe"]]["inputs"]:
            converted = deepcopy(raw)
            for control in ("approved_omission_if_unavailable", "omission_rationale", "biosphere_targets"):
                converted.pop(control, None)
            recipe = converted.pop("recipe", None)
            if recipe:
                converted.update({k: helpers[recipe][k] for k in ("name", "reference product", "unit", "location")})
            expected.append(converted)
        for actual, raw in zip(inputs, expected, strict=True):
            assert all(actual[k] == value for k, value in raw.items())
        fractions = [binding for binding in row["source_component_bindings"] if binding["conversion_factor"] == -1]
        assert len(fractions) == 12 and -sum(b["source_statistics"]["amount"] for b in fractions) == pytest.approx(1)
        for binding in fractions:
            if "target_recipe" in binding:
                adopted = helpers[binding["target_recipe"]]
                assert len(adopted["exchanges"]) > 60
                matching = [e for e in inputs if e["name"] == adopted["name"]]
                assert len(matching) == 1 and matching[0]["amount"] == -binding["source_statistics"]["amount"]
        assert all(p["source_and_adopted_inventory_are_not_claimed_equal"] for p in row["component_proxy_sources"])
    wood = helpers["trucks-38-waste-wood-market"]
    assert wood["exchanges"][1]["formula"] == "0.1932*0.1"


def test_old_selected_rules_and_reused_recipe_bodies_are_unchanged():
    resource = load_uvek_technosphere_resource()
    rows = {r["rule_id"]: r for r in ROWS}
    for op in REVIEW["preserved_prior_approval_operations"]:
        for scope in op["scopes"]:
            profile = TechnosphereProfile(**scope["source"])
            actual = next(
                r for r in select_profile_sign_rules(resource, profile)["replace"] if r["id"] == op["rule_id"]
            )
            parent = {**resource, "replace": [rows[op["rule_id"]]["original_rule"]]}
            assert actual == select_profile_sign_rules(parent, profile)["replace"][0]
    for recipe, expected in REVIEW["recipe_dependency_sha256"].items():
        raw = json.dumps(resource["recipes"][recipe], sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        assert hashlib.sha256(raw.encode()).hexdigest() == expected
    assert REVIEW["new_helpers"] == 2 and REVIEW["new_foreground_omissions"] == []


@pytest.mark.parametrize("version,model", [("3.9.1", "cutoff"), ("3.8", "consequential")])
def test_other_contexts_do_not_gain_permission(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROWS[0]["source"], -1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


def test_missing_permission_still_refuses_the_new_context():
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROWS[0]["source"], -1), context=contexts("ecoinvent", "3.8")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(False))
