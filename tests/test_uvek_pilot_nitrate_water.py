"""Approved complete water utilities preserve signed caller data and disclose internal substitutions."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pilot_nitrate_water import apply_pilot_nitrate_water_review

REVIEW = json.loads((DATA_DIR / "export/uvek_pilot_nitrate_water_review.json").read_text())


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["source"]["location"])
@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_water_preserves_signed_caller_uncertainty_and_native_utility_inputs(row, amount, uncertainty):
    data = consumer(row["source"], amount, uncertainty)
    emission = {
        "name": "Carbon dioxide, fossil",
        "categories": ["air"],
        "unit": "kilogram",
        "type": "biosphere",
        "amount": 7,
    }
    data[0]["exchanges"].append(emission)
    before = deepcopy(data)
    source = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9"))
    result = source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert source.data == data == before
    records = result.data
    assert len(records) == row["expected_foreground_records"]
    assert records[0]["exchanges"][-1] == emission
    actual = records[0]["exchanges"][1]
    original = before[0]["exchanges"][1]
    assert actual["amount"] == amount and actual["formula"] == "dose"
    assert actual["scale"] == original["scale"]
    if uncertainty == 2:
        assert actual["loc"] == original["loc"] and actual["negative"] is original["negative"]
    else:
        assert actual["loc"] == original["loc"]
        assert actual["minimum"] == original["minimum"] and actual["maximum"] == original["maximum"]
    root = next(r for r in records if r.get("code") == actual["input"][1])
    inputs = [x for x in root["exchanges"] if x["type"] != "production"]
    assert len(inputs) == len(row["profiles"][0]["inputs"])
    assert all(
        all(x[k] == v for k, v in expected.items() if k != "recipe")
        for x, expected in zip(inputs, row["profiles"][0]["inputs"], strict=True)
    )
    helpers = {r.get("brightpath_conversion", {}).get("recipe_id"): r for r in records[1:]}
    coefficient_fields = (
        "amount",
        "uncertainty type",
        "loc",
        "scale",
        "minimum",
        "maximum",
        "negative",
        "pedigree",
        "scale without pedigree",
    )
    for recipe_id, recipe in REVIEW["auxiliary_recipes"].items():
        if recipe_id not in helpers:
            continue
        actual_inputs = [x for x in helpers[recipe_id]["exchanges"] if x["type"] != "production"]
        assert len(actual_inputs) == len(recipe["inputs"])
        for x, expected in zip(actual_inputs, recipe["inputs"], strict=True):
            assert {k: x[k] for k in coefficient_fields if k in x} == {
                k: expected[k] for k in coefficient_fields if k in expected
            }
            if "recipe" not in expected:
                assert x["name"] == expected["name"] and x["unit"] == expected["unit"]
            if expected.get("type") == "biosphere":
                assert tuple(x["categories"]) == tuple(expected["categories"])
    assert (
        sum(x["type"] == "biosphere" for r in records[1:] for x in r["exchanges"])
        == row["expected_foreground_biosphere_exchanges"]
    )
    assert not any(i.code == "migration.recipe_input_omission" for i in result.last_migration_report.issues)
    assert not result.last_migration_report.losses or all(
        loss.code != "migration.recipe_source_input_omission" for loss in result.last_migration_report.losses
    )


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["source"]["location"])
@pytest.mark.parametrize("version", ["3.11", "3.12"])
def test_water_refuses_uninspected_source_versions(row, version):
    source = BrightwayInventory.from_data(consumer(row["source"], -1), context=contexts("ecoinvent", version))
    before = deepcopy(source.data)
    if row["source"]["location"] == "ZA" and version == "3.12":
        actual = source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
        assert len(actual.data) == 1
        assert actual.data[0]["exchanges"][1]["location"] == "CH"
        assert source.data == before
        return
    with pytest.raises(MigrationError):
        source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert source.data == before


@pytest.mark.parametrize("change", ["none", "late_parent", "new_helper"])
def test_water_authoring_replay_is_atomic_and_freezes_all_helpers(change):
    resource = deepcopy(load_uvek_technosphere_resource())
    if change == "late_parent":
        next(r for r in resource["replace"] if r["id"] == REVIEW["records"][-1]["rule_id"])["confidence"] = 0.123
    elif change == "new_helper":
        rid = next(
            p["target"]["recipe_ref"]
            for r in resource["replace"]
            if r["id"] == REVIEW["records"][-1]["rule_id"]
            for p in r["reference_flow_sign_review"]["profiles"]
            if p["source_profile"] == REVIEW["records"][-1]["source_profile"]
        )
        resource["recipes"][rid]["inputs"][0]["amount"] += 1
    before = deepcopy(resource)
    if change == "none":
        apply_pilot_nitrate_water_review(resource)
        assert resource == before
    else:
        with pytest.raises(ValueError):
            apply_pilot_nitrate_water_review(resource)
        assert resource == before


def test_water_substitution_keeps_european_mix_and_caller_accounting_contract():
    assert len(REVIEW["records"]) == 7
    (group,) = [r for r in REVIEW["records"] if r["source"]["location"] == "RER"]
    assert [x["amount"] for x in group["profiles"][0]["inputs"]] == [0.002087020385985745, 0.9979129796140143]
    assert [x["location"] for x in group["profiles"][0]["inputs"]] == ["CH", "RER"]
    for row in REVIEW["records"]:
        assert row["caller_signed_quantity_and_uncertainty_retained"]
        assert not row["source_internal_treatment_losses_network_and_emissions_reconstructed"]
        if row is not group:
            (x,) = row["profiles"][0]["inputs"]
            assert x["amount"] == 1 and x["location"] == "RER"
        assert "scoreeffects remain unquantified" in row["limitations"]


def test_water_authoring_rejects_tampered_review_atomically(tmp_path):
    from scripts.review_uvek_pilot_nitrate_water import REVIEW_PATH

    path = tmp_path / REVIEW_PATH
    path.parent.mkdir(parents=True)
    path.write_text("{}")
    resource = deepcopy(load_uvek_technosphere_resource())
    before = deepcopy(resource)
    with pytest.raises(ValueError):
        apply_pilot_nitrate_water_review(resource, root=tmp_path)
    assert resource == before


def test_water_preserves_prior_312_south_african_runtime_graph(monkeypatch):
    version = "3.12"
    from brightpath.background import execution, migration, uvek_import

    resource = load_uvek_technosphere_resource()
    row = next(r for r in REVIEW["records"] if "preserved_profile" in r)
    previous = deepcopy(resource)
    position = next(i for i, r in enumerate(previous["replace"]) if r["id"] == row["rule_id"])
    previous["replace"][position] = deepcopy(row["previous_rule"])
    data = consumer(row["source"], -2, 2)
    with monkeypatch.context() as patch:
        for module in (execution, migration, uvek_import):
            patch.setattr(module, "load_uvek_technosphere_resource", lambda: previous)
        old = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version)).migrate_background(
            target_context("2026"), policy=MigrationPolicy.permissive()
        )
    actual = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version)).migrate_background(
        target_context("2026"), policy=MigrationPolicy.permissive()
    )
    old_data, actual_data = deepcopy(old.data), deepcopy(actual.data)
    # The raw-operation warning changes to explain the 3.9 refinement; every
    # other result field, including generated suppliers and uncertainty, stays.
    old_data[0]["exchanges"][1].pop("comment", None)
    actual_data[0]["exchanges"][1].pop("comment", None)
    assert actual_data == old_data
