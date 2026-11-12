"""Steel prerequisites retain fuel supply and physical waste-treatment demand."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_steel_39_first import apply_steel_39_first_review

REVIEW = json.loads((DATA_DIR / "export/uvek_steel_39_first_review.json").read_text())


def policy():
    resource = load_uvek_technosphere_resource()
    rules = {r["id"]: r for r in resource["replace"]}
    scopes = tuple(
        compatibility_rule_fingerprint(
            resource,
            "replace",
            rules[row["rule_id"]],
            TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
            TechnosphereProfile("uvek", "2025", "cutoff"),
        )
        for row in REVIEW["records"]
    )
    return MigrationPolicy(approved_compatibility_rules=scopes, on_information_loss=PolicyAction.WARN)


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda row: row["rule_id"])
@pytest.mark.parametrize("amount", [-2, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_signed_demand_uncertainty_and_foreground_are_preserved(row, amount, uncertainty):
    data = consumer(row["source_witness"]["identity"], amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", "2026").background, policy=policy()
    )
    assert data == before
    edge = result.data[0]["exchanges"][1]
    factor = row["conversion_factor"]
    assert edge["amount"] == amount * factor
    assert edge["uncertainty type"] == uncertainty and edge["scale"] == 0.2
    if uncertainty == 2:
        assert edge["loc"] == before[0]["exchanges"][1]["loc"]
        assert edge["negative"] is (edge["amount"] < 0)
    else:
        assert edge["loc"] == amount * factor
        assert edge["minimum"] == min((amount - 1) * factor, (amount + 1) * factor)
        assert edge["maximum"] == max((amount - 1) * factor, (amount + 1) * factor)
    assert edge["formula"] == ("dose" if factor == 1 else "(dose) * (-1.0)")
    if row["rule_id"] == "bp-e5c0f91e3ad85dad":
        helper = next(
            a
            for a in result.data
            if a.get("brightpath_conversion", {}).get("recipe_id") == "steel-39-global-delivered-gas-nominal-energy"
        )
        (supply,) = [e for e in helper["exchanges"] if e["type"] == "technosphere"]
        assert supply["name"] == "Natural gas, high pressure, at consumer"
        assert supply["unit"] == "megajoule" and supply["amount"] == 36
        assert not any("vented" in a["name"].lower() for a in result.data)
    if row["rule_id"] == "bp-6e92a90be37a3d27":
        helpers = {a.get("brightpath_conversion", {}).get("recipe_id"): a for a in result.data}
        market = helpers["steel-39-zeolite-market-with-freight"]
        inputs = [e for e in market["exchanges"] if e["type"] == "technosphere"]
        freight = [e for e in inputs if e["unit"] == "ton kilometer"]
        assert len(inputs) == 10 and len(freight) == 9
        expected = REVIEW["recipes"]["steel-39-zeolite-market-with-freight"]["inputs"]
        assert [e["amount"] for e in freight] == [e["amount"] for e in expected if e.get("unit") == "ton kilometer"]
        operation = helpers["steel-39-zeolite-generic-inert-treatment"]
        assert sorted(e["amount"] for e in operation["exchanges"] if e["type"] == "technosphere") == [1.48e-9, 1]
        assert not any(e["type"] == "biosphere" for e in operation["exchanges"])
        assert not any("Disposal, zeolite, 5%" in a["name"] for a in result.data)


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda row: row["rule_id"])
def test_exact_approval_does_not_authorize_another_source_release(row):
    data = consumer(row["source_witness"]["identity"], -2)
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.11")).migrate_background(
            contexts("uvek", "2026").background, policy=policy()
        )


@pytest.mark.parametrize("drift", ["none", "parent", "recipe"])
def test_review_replay_is_idempotent_and_refuses_drift_atomically(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    if drift == "parent":
        next(r for r in resource["replace"] if r["id"] == REVIEW["records"][0]["rule_id"])["confidence"] = 0.1
    elif drift == "recipe":
        resource["recipes"]["steel-39-global-delivered-gas-nominal-energy"]["inputs"][0]["amount"] = 35
    before = deepcopy(resource)
    if drift == "none":
        apply_steel_39_first_review(resource)
    else:
        with pytest.raises(ValueError):
            apply_steel_39_first_review(resource)
    assert resource == before
