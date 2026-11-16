"""Dolomite delivery keeps source-version boundaries and native raw mineral."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_battery_dolomite_39_review.json").read_text())
ROWS = REVIEW["records"]


def policy(version="3.9", approved=True):
    resource = load_uvek_technosphere_resource()
    ids = {row["rule_id"] for row in ROWS}
    scopes = tuple(
        compatibility_rule_fingerprint(
            resource,
            "replace",
            rule,
            TechnosphereProfile("ecoinvent", version, "cutoff"),
            TechnosphereProfile(**REVIEW["target_profile"]),
        )
        for rule in resource["replace"]
        if rule["id"] in ids
    )
    return MigrationPolicy(
        approved_compatibility_rules=scopes if approved else (), on_information_loss=PolicyAction.WARN
    )


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_source39_keeps_every_delivery_entry_and_signed_caller_coordinates(amount, uncertainty, release):
    data = consumer(ROWS[0]["compiled_rule"]["source"], amount, uncertainty)
    data[0]["exchanges"].append(consumer(ROWS[1]["compiled_rule"]["source"], amount, uncertainty)[0]["exchanges"][1])
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == before and len(result.data) == 2
    for actual, old in zip(result.data[0]["exchanges"][1:], before[0]["exchanges"][1:], strict=True):
        for key in ("amount", "loc", "scale", "negative", "formula", "uncertainty type"):
            if key in old:
                assert actual[key] == old[key]
    assert result.data[0]["exchanges"][2]["name"] == "Dolomite, at plant"
    helper = result.data[1]
    assert helper["brightpath_conversion"]["recipe_id"] == "battery-39-dolomite-delivery"
    edges = helper["exchanges"][1:]
    assert len(edges) == 10 and sum(e["unit"] == "ton kilometer" for e in edges) == 9
    assert edges[0]["name"] == "Dolomite, at plant" and edges[0]["amount"] == 1
    expected = REVIEW["recipes"]["battery-39-dolomite-delivery"]["inputs"]
    for actual, wanted in zip(edges, expected, strict=True):
        for key in (
            "name",
            "reference product",
            "location",
            "unit",
            "amount",
            "uncertainty type",
            "loc",
            "scale",
            "pedigree",
        ):
            if key in wanted:
                assert actual[key] == wanted[key]
    assert all("calcined" not in e["name"].lower() and "lime" not in e["name"].lower() for e in edges)


@pytest.mark.parametrize("amount", [-2.5, 2.5])
def test_source38_still_uses_its_unchanged_original_helper(amount):
    data = consumer(ROWS[0]["compiled_rule"]["source"], amount)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.8")).migrate_background(
        contexts("uvek", "2026").background, policy=policy("3.8")
    )
    helper = result.data[1]
    assert helper["brightpath_conversion"]["recipe_id"] == "ships-38-dolomite-market"
    old = load_uvek_technosphere_resource()["recipes"]["ships-38-dolomite-market"]["inputs"]
    for actual, expected in zip(helper["exchanges"][1:], old, strict=True):
        assert all(actual[key] == value for key, value in expected.items() if key != "type")
    assert ROWS[0]["compiled_rule"]["reference_flow_sign_review"]["profiles"][0] == REVIEW["preserved_source38_profile"]


@pytest.mark.parametrize(
    "version,model,approved",
    [("3.9", "cutoff", False), ("3.9.1", "cutoff", True), ("3.10", "cutoff", True), ("3.9", "consequential", True)],
)
def test_no_approval_or_an_uninspected_source_profile_still_refuses(version, model, approved):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROWS[0]["compiled_rule"]["source"], 1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(approved=approved))


def test_new_producer_permission_does_not_extend_to_source38_even_with_permissive_policy():
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROWS[1]["compiled_rule"]["source"], 1), context=contexts("ecoinvent", "3.8")
        ).migrate_background(contexts("uvek", "2026").background, policy=MigrationPolicy.permissive())
