"""Dry-active chemical references, same-mode freight and exact reaction emissions."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_deferred_naocl import apply_deferred_naocl_review

REVIEW = json.loads((DATA_DIR / "export/uvek_deferred_naocl_review.json").read_text())
ROW = REVIEW["records"][0]


@pytest.mark.parametrize("profile", ROW["profiles"], ids=lambda p: p["label"])
@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_active_kg_and_uncertainty_survive_with_exact_reactants_emissions_and_freight(profile, amount, uncertainty):
    data = consumer(ROW["source"], amount, uncertainty)
    before = deepcopy(data)
    version = profile["source_profile"]["version"]
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version)).migrate_background(
        target_context("2026"), policy=MigrationPolicy.permissive()
    )
    assert data == before and len(result.data) == 3
    x = result.data[0]["exchanges"][1]
    assert all(x[k] == v for k, v in before[0]["exchanges"][1].items() if k not in ROW["source"])
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
    market = helpers[ROW["generated_recipe_ids"][version]]
    producer_id = profile["inputs"][0]["recipe"]
    producer = helpers[producer_id]
    assert len(producer["exchanges"]) == 10
    for expected in REVIEW["auxiliary_recipes"][producer_id]["inputs"]:
        (actual,) = [
            e for e in producer["exchanges"] if e["type"] == expected["type"] and e["name"] == expected["name"]
        ]
        assert all(actual[k] == v for k, v in expected.items() if k != "biosphere_targets")
    for expected in profile["inputs"][1:]:
        (actual,) = [e for e in market["exchanges"] if e.get("name") == expected["name"]]
        assert all(actual[k] == v for k, v in expected.items())
    assert {e["name"] for e in market["exchanges"] if e["type"] == "technosphere"} >= {"Transport, freight, rail"}
    assert not any(e.get("name") == "Transport, aircraft, freight" for a in result.data for e in a["exchanges"])
    assert not any(i.code.endswith("omission") for i in result.last_migration_report.issues)


@pytest.mark.parametrize("version", ["3.7", "3.12"])
def test_other_profiles_cannot_inherit_the_chemical_reference_review(version):
    inv = BrightwayInventory.from_data(consumer(ROW["source"], 1), context=contexts("ecoinvent", version))
    before = deepcopy(inv.data)
    with pytest.raises(MigrationError):
        inv.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert inv.data == before


@pytest.mark.parametrize("drift", ["none", "parent", "producer", "market", "shared_review"])
def test_replay_is_atomic_for_parent_and_both_helper_levels(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    rule = next(r for r in resource["replace"] if r["id"] == ROW["rule_id"])
    if drift == "parent":
        rule["confidence"] = 0.123
    elif drift == "producer":
        resource["recipes"][next(iter(REVIEW["auxiliary_recipes"]))]["inputs"][-1]["amount"] += 1
    elif drift == "market":
        resource["recipes"][ROW["generated_recipe_ids"]["3.9"]]["inputs"][-1]["amount"] += 1
    elif drift == "shared_review":
        resource["reviews"][rule["review_ref"]]["rationale"] += " changed"
    before = deepcopy(resource)
    if drift == "none":
        apply_deferred_naocl_review(resource)
    else:
        with pytest.raises(ValueError):
            apply_deferred_naocl_review(resource)
    assert resource == before
