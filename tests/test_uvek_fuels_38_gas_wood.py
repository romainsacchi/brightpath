"""Gas/wood profile extension keeps units, routes and older decisions."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_fuels_38_gas_wood import apply_fuels_38_gas_wood_review

REVIEW = json.loads((DATA_DIR / "export/uvek_fuels_38_gas_wood_review.json").read_text())
RECORDS = {r["rule_id"]: r for r in REVIEW["records"]}


@pytest.mark.parametrize("rid", RECORDS)
@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_gas_wood_keeps_caller_statistics_and_original_helper(rid, amount, uncertainty):
    row = RECORDS[rid]
    source = BrightwayInventory.from_data(
        consumer(row["original_rule"]["source"], amount, uncertainty),
        context=contexts("ecoinvent", "3.8", "cutoff"),
    )
    before = deepcopy(source.data)
    result = source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    edge = result.data[0]["exchanges"][1]
    assert edge["amount"] == amount and edge["formula"] == "dose"
    assert edge["uncertainty type"] == uncertainty and edge["scale"] == 0.2
    if uncertainty == 2:
        assert edge["negative"] is (amount < 0)
    else:
        assert edge["loc"] == amount
    target = row["new_profile"].get("target", row["compiled_rule"]["target"])
    if "recipe_ref" in target:
        assert len(result.data) == 2
        expected = REVIEW["recipes"][target["recipe_ref"]]["inputs"]
        actual = [e for e in result.data[1]["exchanges"] if e["type"] != "production"]
        assert len(actual) == len(expected) == 2
        for a, b in zip(actual, expected, strict=True):
            assert all(a.get(k) == v for k, v in b.items())
    else:
        assert len(result.data) == 1
        assert all(edge[k] == v for k, v in target.items())
    assert source.data == before


@pytest.mark.parametrize("rid", RECORDS)
def test_uninspected_37_does_not_borrow_new_sign_decision(rid):
    source = BrightwayInventory.from_data(
        consumer(RECORDS[rid]["original_rule"]["source"], 1, 2), context=contexts("ecoinvent", "3.7", "cutoff")
    )
    with pytest.raises(MigrationError) as caught:
        source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert "migration.reference_flow_sign_unresolved" in json.dumps(caught.value.report.to_dict())


def test_old_profiles_and_helper_closure_unchanged_and_replay_idempotent():
    resource = deepcopy(load_uvek_technosphere_resource())
    before = deepcopy(resource)
    apply_fuels_38_gas_wood_review(resource)
    assert resource == before
    for row in REVIEW["records"]:
        for proof in row["preserved_profiles"]:
            pr = TechnosphereProfile(**proof["source_profile"])
            assert (
                select_profile_sign_rules({"replace": [row["original_rule"]]}, pr)["replace"]
                == select_profile_sign_rules({"replace": [row["compiled_rule"]]}, pr)["replace"]
            )
    key = next(iter(REVIEW["recipes"]))
    resource["recipes"][key]["inputs"][0]["amount"] *= 2
    with pytest.raises(ValueError, match="helper changed"):
        apply_fuels_38_gas_wood_review(resource)


def test_wood_delivery_and_formula_boundary_are_explicit():
    proof = REVIEW["wood_delivery_proof"]
    assert proof["statistics"]["amount"] == 0.105
    assert proof["statistics"]["uncertainty type"] == 2
    assert proof["source_formula_present"] and not proof["source_formula_transplanted"]
    assert len(proof["source_formula_sha256"]) == 64
    assert all(amount == 0 for amount in proof["native_direct_delivery_amounts"])
    assert "parameter context" in RECORDS["bp-84bd9cf473cdeee9"]["limitations"][0]
    assert "inlet pressure" in RECORDS["bp-745c517f85a78ba7"]["limitations"][0]
