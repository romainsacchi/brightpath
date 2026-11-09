"""Delivered gas preserves source reference volume while using explicit energy bases."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_deferred_delivered_gas import apply_deferred_delivered_gas_review

REVIEW = json.loads((DATA_DIR / "export/uvek_deferred_delivered_gas_review.json").read_text())


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["source_profile"]["version"])
@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_gas_keeps_caller_volume_and_uncertainty_and_supplies_the_declared_energy(row, amount, uncertainty):
    data = consumer(row["source"], amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(
        data, context=contexts("ecoinvent", row["source_profile"]["version"])
    ).migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert data == before and len(result.data) == 2
    x = result.data[0]["exchanges"][1]
    assert all(x[k] == v for k, v in before[0]["exchanges"][1].items() if k not in row["source"])
    assert x["unit"] == "cubic meter"
    helper = next(
        a for a in result.data if a.get("brightpath_conversion", {}).get("recipe_id") == row["generated_recipe_id"]
    )
    (y,) = [e for e in helper["exchanges"] if e["type"] != "production"]
    expected = row["profiles"][0]["inputs"][0]
    assert all(y[k] == v for k, v in expected.items())
    assert y["name"] == "Natural gas, high pressure, at consumer" and y["unit"] == "megajoule"
    assert not any(i.code.endswith("omission") for i in result.last_migration_report.issues)


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["source_profile"]["version"])
@pytest.mark.parametrize("version", ["3.7", "3.12"])
def test_gas_uninspected_source_releases_remain_refused(row, version):
    inventory = BrightwayInventory.from_data(consumer(row["source"], 1), context=contexts("ecoinvent", version))
    before = deepcopy(inventory.data)
    with pytest.raises(MigrationError):
        inventory.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert inventory.data == before


@pytest.mark.parametrize("drift", ["none", "late_parent", "helper", "shared_review"])
def test_replay_checks_both_parent_rules_and_energy_coefficients_atomically(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    if drift == "late_parent":
        next(r for r in resource["replace"] if r["id"] == REVIEW["records"][-1]["rule_id"])["confidence"] = 0.123
    elif drift == "helper":
        resource["recipes"][REVIEW["records"][-1]["generated_recipe_id"]]["inputs"][0]["amount"] += 1
    elif drift == "shared_review":
        r = next(r for r in resource["replace"] if r["id"] == REVIEW["records"][-1]["rule_id"])
        resource["reviews"][r["review_ref"]]["rationale"] += " altered"
    before = deepcopy(resource)
    if drift == "none":
        apply_deferred_delivered_gas_review(resource)
    else:
        with pytest.raises(ValueError):
            apply_deferred_delivered_gas_review(resource)
    assert resource == before


def test_legacy_estimate_is_explicitly_distinct_from_verified_modern_ncv():
    modern = next(r for r in REVIEW["records"] if r["source_profile"]["version"] == "3.9")
    legacy = next(r for r in REVIEW["records"] if r["source_profile"]["version"] == "3.6")
    assert modern["source_net_energy_MJ_per_reference_m3"] == 36
    assert (
        legacy["estimated_net_energy_MJ_per_reference_m3"]
        == legacy["source_mass_kg_per_reference_m3"] * legacy["assumed_legacy_NCV_MJ_per_kg"]
    )
    assert legacy["estimated_net_energy_MJ_per_reference_m3"] != 36
    assert "not measured" in legacy["energy_basis_kind"]
    assert legacy["property_evidence"]["url"].startswith("https://support.ecoinvent.org/")
