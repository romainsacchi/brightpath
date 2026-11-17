"""Source 3.8 delivery markets retain signs, statistics and energy bases."""

import hashlib
import json
import math
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_trucks_five_markets_38_review.json").read_text())
ROWS = REVIEW["records"]
CHANGED = {r["rule_id"]: r for r in ROWS}
FIELDS = ("name", "reference product", "location", "unit")


def policy(approved=True):
    resource = load_uvek_technosphere_resource()
    rules = {r["id"]: r for r in resource["replace"]}
    return MigrationPolicy(
        approved_compatibility_rules=(
            tuple(
                compatibility_rule_fingerprint(
                    resource,
                    "replace",
                    rules[row["rule_id"]],
                    TechnosphereProfile(**REVIEW["source_profile"]),
                    TechnosphereProfile(**REVIEW["target_profile"]),
                )
                for row in ROWS
            )
            if approved
            else ()
        ),
        on_information_loss=PolicyAction.WARN,
    )


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_signed_market_demands_and_source_statistics(amount, uncertainty, release):
    data = consumer(ROWS[0]["source"], amount, uncertainty)
    data[0]["exchanges"].extend(consumer(r["source"], amount, uncertainty)[0]["exchanges"][1] for r in ROWS[1:])
    original = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.8")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == original and len(result.data) == 6
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
    assert set(helpers) == {r["target_recipe"] for r in ROWS}
    resource = load_uvek_technosphere_resource()
    for edge, old, row in zip(result.data[0]["exchanges"][1:], original[0]["exchanges"][1:], ROWS, strict=True):
        factor = row["conversion_factor"]
        assert edge["amount"] == old["amount"] * factor
        assert edge["loc"] == (old["loc"] if uncertainty == 2 else old["loc"] * factor)
        assert edge["scale"] == old["scale"]
        assert edge["formula"] == ("dose" if factor == 1 else "(dose) * (-1.0)")
        if uncertainty == 2:
            assert edge["negative"] is (old["negative"] != (factor < 0))
        else:
            assert edge["minimum"] == (old["minimum"] if factor == 1 else -old["maximum"])
            assert edge["maximum"] == (old["maximum"] if factor == 1 else -old["minimum"])
        helper = helpers[row["target_recipe"]]
        assert all(edge[k] == helper[k] for k in FIELDS)
        expected = resource["recipes"][row["target_recipe"]]["inputs"]
        assert len(helper["exchanges"]) == len(expected) + 1
        for actual, raw in zip(helper["exchanges"][1:], expected, strict=True):
            assert all(actual[k] == value for k, value in raw.items())
        assert not helper["brightpath_conversion"].get("source_input_omissions")
    gas = helpers["trucks-38-gas-net39"]["exchanges"][1:]
    assert len(gas) == 1 and gas[0]["amount"] == 39 and gas[0]["unit"] == "megajoule"
    assert gas[0]["name"] == "Natural gas, high pressure, at consumer"
    koh = helpers["trucks-38-koh-delivery"]["exchanges"][1:]
    assert len(koh) == 6
    assert sorted(e["amount"] for e in koh if e["unit"] == "kilogram") == [0.174, 0.826]
    weee = helpers["trucks-38-weee-collection"]["exchanges"][1:]
    assert [e["amount"] for e in weee] == [0.0112, 0.01932, 1]
    assert weee[-1]["name"] == "Shredding, electrical and electronic scrap"


def test_prior_selected_rules_and_recipe_closures_are_unchanged():
    resource = load_uvek_technosphere_resource()
    for operation in REVIEW["preserved_prior_approval_operations"]:
        rid = operation["rule_id"]
        for scope in operation["scopes"]:
            profile = TechnosphereProfile(**scope["source"])
            actual = next(r for r in select_profile_sign_rules(resource, profile)["replace"] if r["id"] == rid)
            parent = {**resource, "replace": [CHANGED[rid]["original_rule"]]}
            assert actual == select_profile_sign_rules(parent, profile)["replace"][0]
    for rid, expected in REVIEW["recipe_dependency_sha256"].items():
        raw = json.dumps(resource["recipes"][rid], sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        assert hashlib.sha256(raw.encode()).hexdigest() == expected
    gas39 = resource["recipes"]["deferred-delivered-gas-dce31adcf468d41bd94d"]["inputs"]
    assert len(gas39) == 1 and gas39[0]["amount"] == 36
    assert REVIEW["new_helpers"] == 4 and REVIEW["new_foreground_omissions"] == []
    for rounding in REVIEW["wafer_recipe_rounding_equivalence"]:
        assert math.nextafter(rounding["source38"], math.inf) == rounding["reused_recipe"]


@pytest.mark.parametrize("version,model", [("3.9.1", "cutoff"), ("3.8", "consequential")])
def test_new_approvals_do_not_authorize_other_contexts(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROWS[0]["source"], -1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


def test_missing_scoped_approval_refuses_the_reviewed_source():
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROWS[0]["source"], -1), context=contexts("ecoinvent", "3.8")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(False))
