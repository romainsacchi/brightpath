"""Market profiles retain treatment signs, source shares and prior recipes."""

import json
from copy import deepcopy
from dataclasses import replace

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_fuels_37_sand_hour import apply_fuels_37_sand_hour_review, digest

REVIEW = json.loads((DATA_DIR / "export/uvek_fuels_37_sand_hour_review.json").read_text())
RECORDS = {r["rule_id"]: r for r in REVIEW["records"]}


@pytest.mark.parametrize("rid", RECORDS)
@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_signed_market_demand_keeps_statistics_and_exact_recipe(rid, amount, uncertainty):
    row = RECORDS[rid]
    inventory = BrightwayInventory.from_data(
        consumer(row["original_rule"]["source"], amount, uncertainty), context=contexts("ecoinvent", "3.7", "cutoff")
    )
    before = deepcopy(inventory.data)
    policy = replace(
        MigrationPolicy.permissive(),
        on_invalid_source=PolicyAction.ERROR,
        on_invalid_target=PolicyAction.ERROR,
        on_unresolved_link=PolicyAction.ERROR,
        minimum_coverage=1,
    )
    result = inventory.migrate_background(target_context("2026"), policy=policy)
    edge = result.data[0]["exchanges"][1]
    factor = row["conversion_factor"]
    assert edge["amount"] == amount * factor
    assert edge["formula"] == ("dose" if factor == 1 else "(dose) * (-1.0)")
    assert edge["uncertainty type"] == uncertainty and edge["scale"] == 0.2
    if uncertainty == 2:
        assert edge["negative"] is (amount * factor < 0)
    else:
        assert edge["loc"] == amount * factor
    recipe_id = row["new_profile"]["target"]["recipe_ref"]
    helper = next(a for a in result.data if a.get("brightpath_conversion", {}).get("recipe_id") == recipe_id)
    assert edge["name"] == helper["name"]
    recipe = load_uvek_technosphere_resource()["recipes"][recipe_id]
    actual = [e for e in helper["exchanges"] if e["type"] != "production"]
    assert [e["amount"] for e in actual] == [e["amount"] for e in recipe["inputs"]]
    assert inventory.data == before


def test_scoped_author_preserves_prior_decisions_and_all_old_recipes():
    resource = deepcopy(load_uvek_technosphere_resource())
    before = deepcopy(resource)
    apply_fuels_37_sand_hour_review(resource)
    assert resource == before
    for row in RECORDS.values():
        for proof in row["preserved_profiles"]:
            selected = select_profile_sign_rules(
                {"replace": [row["compiled_rule"]]}, TechnosphereProfile(**proof["source_profile"])
            )["replace"][0]
            assert digest(selected) == proof["selected_rule_sha256"]
        for proof in row["preserved_physical_bindings"]:
            selected = select_profile_sign_rules(
                {"replace": [row["compiled_rule"]]}, TechnosphereProfile(**proof["source_profile"])
            )["replace"][0]
            assert selected["target"] == proof["target"]
            assert selected["source"] == proof["source"]
            assert selected.get("conversion_factor", 1) == proof["conversion_factor"]
        assert row["source_profile"]["version"] == "3.7"
        assert row["source_witness"]["inspected_profile"]["version"] == "3.7.1"


def test_sand_delivery_and_hour_limitations_keep_physical_basis():
    inputs = REVIEW["recipes"]["fuels-37-delivered-swiss-sand"]["inputs"]
    assert [e["amount"] for e in inputs] == [0.00372, 0.02026, 1]
    assert all(e["uncertainty type"] == 2 for e in inputs[:2])
    hour = RECORDS["bp-ab9eb6c9ccfd6290"]
    assert "26.4 kg/hour" in hour["limitations"][0]
    assert "182.6 kg/hour" in hour["limitations"][0]
    assert hour["quantity_basis"] == "hour" and hour["conversion_factor"] == 1


@pytest.mark.parametrize("version", ["3.8", "3.11"])
def test_sand_uninspected_profiles_remain_unresolved(version):
    row = RECORDS["bp-714b90acb435b476"]
    selected = select_profile_sign_rules(
        {"replace": [row["compiled_rule"]]}, TechnosphereProfile("ecoinvent", version, "cutoff")
    )["replace"][0]
    assert selected["reference_flow_sign_review"]["status"] == "unresolved"
