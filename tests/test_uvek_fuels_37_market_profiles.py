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
from scripts.review_uvek_fuels_37_market_profiles import apply_fuels_37_market_profiles_review, digest

REVIEW = json.loads((DATA_DIR / "export/uvek_fuels_37_market_profiles_review.json").read_text())
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
    apply_fuels_37_market_profiles_review(resource)
    assert resource == before
    for row in RECORDS.values():
        for witness in row["existing_recipe_witnesses"]:
            assert digest(resource["recipes"][witness["recipe_id"]]) == witness["sha256"]
        for proof in row["preserved_profiles"]:
            selected = select_profile_sign_rules(
                {"replace": [row["compiled_rule"]]}, TechnosphereProfile(**proof["source_profile"])
            )["replace"][0]
            assert digest(selected) == proof["selected_rule_sha256"]
        assert row["source_profile"]["version"] == "3.7"
        assert row["source_witness"]["inspected_profile"]["version"] == "3.7.1"


def test_cast_share_correction_keeps_freight_uncertainty_and_no_rolling():
    recipe = REVIEW["recipes"]["fuels-37-cast-steel-route-mix"]
    converter, electric, *freight = recipe["inputs"]
    assert converter["amount"] == 0.241030009635716
    assert electric["amount"] == 0.7589699903642834
    assert len(freight) == 4 and all(e["uncertainty type"] == 2 for e in freight)
    assert not any("rolling" in e.get("name", "").lower() for e in recipe["inputs"])
