"""Historic CHP parts and helicopter phases retain their own reference units."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pilot_legacy_components import apply_pilot_legacy_components_review

REVIEW = json.loads((DATA_DIR / "export/uvek_pilot_legacy_components_review.json").read_text())


@pytest.mark.parametrize("row", REVIEW["rules"], ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_component_unit_and_signed_caller_quantity_remain_unchanged(row, amount, uncertainty):
    data = consumer(row["source"], amount, uncertainty)
    before = deepcopy(data)
    source = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.6"))
    result = source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert source.data == data == before
    assert len(result.data) == row["expected_foreground_records"]
    x = result.data[0]["exchanges"][1]
    target = row.get("effective_target", row["target"])
    assert all(x[k] == v for k, v in target.items() if k != "name")
    if "generated_recipe_id" in row:
        assert x["name"].startswith(target["name"] + " [BrightPath UVEK conversion ")
    else:
        assert x["name"] == target["name"]
    for field in ("amount", "formula", "uncertainty type", "loc", "scale", "negative", "minimum", "maximum"):
        if field in before[0]["exchanges"][1]:
            assert x[field] == before[0]["exchanges"][1][field]
    if "generated_recipe_id" in row:
        helper = next(
            a for a in result.data if a.get("brightpath_conversion", {}).get("recipe_id") == row["generated_recipe_id"]
        )
        inputs = [e for e in helper["exchanges"] if e["type"] != "production"]
        expected = REVIEW["auxiliary_recipes"][row["generated_recipe_id"]]["inputs"]
        assert len(inputs) == row["native_hour_copy"]["copied_nonproduction_coordinates"]
        for actual, original in zip(inputs, expected, strict=True):
            assert all(actual[k] == v for k, v in original.items() if k != "biosphere_targets")
    assert not any(i.code == "migration.recipe_input_omission" for i in result.last_migration_report.issues)


@pytest.mark.parametrize("row", REVIEW["rules"], ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("version", ["3.9", "3.12"])
def test_uninspected_releases_cannot_inherit_the_component_correspondence(row, version):
    source = BrightwayInventory.from_data(consumer(row["source"], 1), context=contexts("ecoinvent", version))
    before = deepcopy(source.data)
    with pytest.raises(MigrationError):
        source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert source.data == before


@pytest.mark.parametrize("change", ["none", "late_rule", "shared_review", "native_copy"])
def test_authoring_is_idempotent_and_checks_all_rules_before_mutation(change):
    resource = deepcopy(load_uvek_technosphere_resource())
    indexed = {r["id"]: r for r in resource["replace"]}
    if change == "late_rule":
        for row in REVIEW["rules"][:-1]:
            indexed[row["rule_id"]].clear()
            indexed[row["rule_id"]].update(deepcopy(row["previous_rule"]))
            if "review_ref" in row["previous_rule"]:
                resource["reviews"][row["previous_rule"]["review_ref"]] = deepcopy(row["previous_review"])
        indexed[REVIEW["rules"][-1]["rule_id"]]["confidence"] = 0.123
    elif change == "shared_review":
        ref = indexed[REVIEW["rules"][-1]["rule_id"]]["review_ref"]
        resource["reviews"][ref]["rationale"] += " changed"
    elif change == "native_copy":
        rid = next(iter(REVIEW["auxiliary_recipes"]))
        resource["recipes"][rid]["inputs"][0]["amount"] *= 2
    before = deepcopy(resource)
    if change == "none":
        apply_pilot_legacy_components_review(resource)
    else:
        with pytest.raises(ValueError):
            apply_pilot_legacy_components_review(resource)
    assert resource == before


def test_functional_parts_and_phases_do_not_become_whole_plant_or_combined_flights():
    indexed = {r["rule_id"]: r for r in REVIEW["rules"]}
    assert indexed["bp-17d12aa909c440e8"]["target"]["name"] == "Heat exchanger of cogen unit 160kWe"
    hour = indexed["bp-ab9eb6c9ccfd6290"]
    assert hour["target"]["name"] == "Transport, passenger, helicopter, single-engine, without LTO"
    assert hour["source"]["unit"] == hour["target"]["unit"] == "hour"
    assert indexed["bp-b62924eca33d09c2"]["target"]["name"] == "Transport, helicopter, twin-engine, LTO cycle"
    assert all(
        r["conversion_factor"] == r["source_reference_amount"] == r["target_reference_amount"] == 1
        for r in indexed.values()
    )
    assert "182.6" in hour["limitations"] and "26.4" in hour["limitations"]
    assert REVIEW["legacy_2025_unit_discrepancies"] and REVIEW["new_helper_count"] == 1
    assert not REVIEW["new_source_omissions_authorized"]
