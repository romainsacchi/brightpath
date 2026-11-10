"""Waste signs and fresh source-profile audits cannot borrow older helper data."""

import json
from copy import deepcopy

import pytest
from test_uvek_pilot_hazardous_paint import hazardous_paint_authoring_stage_resource
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_heat_310_waste import apply_heat_310_waste_review
from scripts.review_uvek_pilot_hazardous_paint import apply_pilot_hazardous_paint_review
from scripts.review_uvek_pilot_nitrate_hdpe import apply_pilot_nitrate_hdpe_review

REVIEW = json.loads((DATA_DIR / "export/uvek_heat_310_waste_review.json").read_text())


@pytest.fixture(scope="module")
def policy():
    resource = load_uvek_technosphere_resource()
    indexed = {r["id"]: r for r in resource["replace"]}
    scopes = tuple(
        compatibility_rule_fingerprint(
            resource,
            "replace",
            indexed[row["rule_id"]],
            TechnosphereProfile("ecoinvent", "3.10", "cutoff"),
            TechnosphereProfile("uvek", "2025", "cutoff"),
        )
        for row in REVIEW["records"]
    )
    return MigrationPolicy(approved_compatibility_rules=scopes, on_information_loss=PolicyAction.WARN)


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("amount,uncertainty", [(-2, 2), (2, 2), (-2, 3), (2, 3)])
def test_reference_supply_coordinates_uncertainty_and_fresh_helper_inputs(row, amount, uncertainty, policy):
    data = consumer(row["source"], amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.10")).migrate_background(
        contexts("uvek", "2026").background,
        policy=policy,
    )
    assert data == before
    edge = result.data[0]["exchanges"][1]
    old = before[0]["exchanges"][1]
    factor = row["conversion_factor"]
    assert edge["amount"] == amount * factor
    assert edge["amount"] / row["target_reference_amount"] == amount / row["source_reference_amount"]
    assert edge["scale"] == old["scale"]
    if factor == 1:
        assert edge["formula"] == old["formula"]
        assert edge["loc"] == old["loc"]
    else:
        assert row["previous_rule"]["conversion_factor"] == -1  # Already correct before this qualification.
        assert edge["formula"] == "(dose) * (-1.0)"
        if uncertainty == 2:
            assert edge["loc"] == old["loc"] and edge["negative"] is not old["negative"]
        else:
            assert edge["loc"] == -old["loc"]
            assert edge["minimum"] == -old["maximum"] and edge["maximum"] == -old["minimum"]
    losses = [
        loss for loss in result.last_migration_report.losses if loss.code == "migration.recipe_source_input_omission"
    ]
    if not row["new_recipes"]:
        assert not losses
        assert all(edge[k] == v for k, v in row["target"].items())
        return
    recipe_id = row["target"]["recipe_ref"]
    recipe = row["new_recipes"][recipe_id]
    helper = next(a for a in result.data if a.get("brightpath_conversion", {}).get("recipe_id") == recipe_id)
    actual = [e for e in helper["exchanges"] if e["type"] != "production"]
    assert len(actual) == len(recipe["inputs"])
    for exchange, expected in zip(actual, recipe["inputs"], strict=True):
        for field in (
            "name",
            "unit",
            "amount",
            "uncertainty type",
            "loc",
            "scale",
            "scale without pedigree",
            "pedigree",
        ):
            if field in expected:
                assert exchange[field] == expected[field]
        if exchange["unit"] == "ton kilometer":
            assert exchange["scale"] == 0.44944410108488464
            assert exchange["scale without pedigree"] == 0.34641016151377546
    omissions = recipe.get("source_input_omissions", [])
    assert [loss.to_dict()["details"]["omission"] for loss in losses] == omissions
    if omissions:
        assert len(omissions) == 4
        assert all(o["source_profile"]["version"] == "3.10" for o in omissions)
        assert sum(e["amount"] for e in actual if e["unit"] == "kilogram") == row["retained_virgin_mass"]
        assert not row["renormalized"]
    else:
        waste = [e for e in actual if e["unit"] == "kilogram"]
        assert [e["amount"] for e in waste] == [0.5, 0.5]


def test_earlier_effective_mappings_and_omission_helpers_are_unchanged():
    resource = load_uvek_technosphere_resource()
    for row in REVIEW["records"]:
        current = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        for scope in row["previous_scopes"]:
            source = TechnosphereProfile(**scope["source"])
            old = select_profile_sign_rules({"replace": [row["previous_rule"]]}, source)["replace"][0]
            new = select_profile_sign_rules({"replace": [current]}, source)["replace"][0]
            assert new == old


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
def test_uninspected_version_does_not_inherit_waste_or_omission_approval(row, policy):
    data = consumer(row["source"], -1)
    before = deepcopy(data)
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.11")).migrate_background(
            contexts("uvek", "2026").background,
            policy=policy,
        )
    assert data == before


@pytest.mark.parametrize("case,author", [(0, apply_pilot_nitrate_hdpe_review), (1, apply_pilot_hazardous_paint_review)])
@pytest.mark.parametrize("drift", ["extra_profile", "fresh_helper"])
def test_older_authoring_stage_refuses_unqualified_extensions(case, author, drift):
    resource = hazardous_paint_authoring_stage_resource() if case == 1 else deepcopy(load_uvek_technosphere_resource())
    row = REVIEW["records"][case]
    if drift == "extra_profile":
        rule = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        forged = deepcopy(rule["reference_flow_sign_review"]["profiles"][-1])
        forged["source_profile"]["version"] = "3.11"
        rule["reference_flow_sign_review"]["profiles"].append(forged)
    else:
        resource["recipes"][row["target"]["recipe_ref"]]["inputs"][0]["amount"] *= 2
    before = deepcopy(resource)
    with pytest.raises(ValueError):
        author(resource)
    assert resource == before


@pytest.mark.parametrize("drift", [False, True])
def test_new_authoring_replay_is_idempotent_and_atomic(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    if drift:
        resource["recipes"][REVIEW["records"][0]["target"]["recipe_ref"]]["source_input_omissions"][0][
            "source_profile"
        ]["version"] = "3.9"
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_heat_310_waste_review(resource)
    else:
        apply_heat_310_waste_review(resource)
    assert resource == before
