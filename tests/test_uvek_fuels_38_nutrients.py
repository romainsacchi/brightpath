"""Nutrient references and pesticide proxies retain caller quantities and releases."""

import json
from copy import deepcopy
from dataclasses import replace

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_fuels_38_nutrients import apply_fuels_38_nutrients_review

REVIEW = json.loads((DATA_DIR / "export/uvek_fuels_38_nutrients_review.json").read_text())
RECORDS = {r["rule_id"]: r for r in REVIEW["records"]}


def migrate(rid, amount=1, uncertainty=3, version="3.8", model="cutoff", strict=False):
    source = BrightwayInventory.from_data(
        consumer(RECORDS[rid]["original_rule"]["source"], amount, uncertainty),
        context=contexts("ecoinvent", version, model),
    )
    before = deepcopy(source.data)
    policy = MigrationPolicy.permissive()
    if strict:
        policy = replace(
            policy,
            on_invalid_source=PolicyAction.ERROR,
            on_invalid_target=PolicyAction.ERROR,
            on_unresolved_link=PolicyAction.ERROR,
            minimum_coverage=1.0,
        )
    result = source.migrate_background(target_context("2026"), policy=policy)
    assert source.data == before
    return result


def helper(result):
    edge = result.data[0]["exchanges"][1]
    return next(a for a in result.data if a.get("code") == edge["input"][1])


@pytest.mark.parametrize("rid", RECORDS)
@pytest.mark.parametrize("amount", [-2, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_caller_stock_mass_uncertainty_and_formula_survive(rid, amount, uncertainty):
    result = migrate(rid, amount, uncertainty)
    edge = result.data[0]["exchanges"][1]
    factor = RECORDS[rid]["source_witness"]["reference_amounts"][0]
    assert edge["amount"] == amount * factor and edge["uncertainty type"] == uncertainty
    assert edge["scale"] == 0.2
    assert edge["formula"] == ("dose" if factor == 1 else "(dose) * (-1.0)")
    if uncertainty == 2:
        assert edge["negative"] is (amount * factor < 0)
    else:
        assert edge["loc"] == amount * factor


@pytest.mark.parametrize("rid", [rid for rid, row in RECORDS.items() if row["resource_action"] != "unchanged_rule"])
@pytest.mark.parametrize("version,model", [("3.10", "cutoff"), ("3.8", "consequential")])
def test_unreviewed_profiles_are_not_inferred(rid, version, model):
    with pytest.raises(MigrationError):
        migrate(rid, version=version, model=model)


@pytest.mark.parametrize("rid", RECORDS)
def test_strict_native_target(rid):
    result = migrate(rid, strict=True)
    row = RECORDS[rid]
    edge = result.data[0]["exchanges"][1]
    assert all(edge[k] == value for k, value in row["new_target"].items())
    assert len(result.data) == 1


def test_nine_existing_rules_remain_unchanged():
    resource = load_uvek_technosphere_resource()
    unchanged = [r for r in REVIEW["records"] if r["resource_action"] == "unchanged_rule"]
    assert len(unchanged) == 9
    for row in unchanged:
        current = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        assert current == row["original_rule"] == row["compiled_rule"]


@pytest.mark.parametrize("rid", ["bp-debda812c6313c05", "bp-bd7ba1d16a722d39"])
def test_nitrogen_does_not_become_phosphorus_or_waste_disposal(rid):
    edge = migrate(rid, amount=3, strict=True).data[0]["exchanges"][1]
    assert edge["name"] == "Average mineral fertiliser, as N, at regional storehouse"
    assert edge["amount"] == 3 and edge["unit"] == "kilogram"


def test_pyrethroid_proxy_is_explicitly_generic_insecticide_and_preserves_field_release():
    rid = "bp-89125291fb94d47e"
    inventory = consumer(RECORDS[rid]["original_rule"]["source"], 1, 3)
    release = {
        "type": "biosphere",
        "name": "Cypermethrin",
        "categories": ("soil", "agricultural"),
        "unit": "kilogram",
        "amount": 0.001,
    }
    inventory[0]["exchanges"].append(release)
    source = BrightwayInventory.from_data(inventory, context=contexts("ecoinvent", "3.8", "cutoff"))
    result = source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    edge = result.data[0]["exchanges"][1]
    assert edge["name"] == "xx Insecticides, at regional storehouse" and edge["amount"] == 1
    assert any(
        e["type"] == "biosphere" and e["name"] == "Cypermethrin" and e["amount"] == 0.001
        for e in result.data[0]["exchanges"]
    )
    assert "not that ingredient class" in RECORDS[rid]["limitations"][0]


def test_native_nutrient_mix_consistency_limitations_are_not_silently_normalized():
    evidence = REVIEW["nutrient_reference_evidence"]
    assert evidence["N"]["native_nutrient_carrier_input_sum"] == pytest.approx(0.973)
    assert evidence["P2O5"]["native_nutrient_carrier_input_sum"] == pytest.approx(1.223)
    assert evidence["K2O"]["native_nutrient_carrier_input_sum"] == pytest.approx(1.0013)
    assert all(not row["native_inventory_renormalized"] for row in evidence.values())


def test_replay_idempotence_and_changed_parent_rejection():
    resource = load_uvek_technosphere_resource()
    apply_fuels_38_nutrients_review(resource)
    before = deepcopy(resource)
    apply_fuels_38_nutrients_review(resource)
    assert resource == before
    next(r for r in resource["replace"] if r["id"] == "bp-debda812c6313c05")["conversion_factor"] = 2
    with pytest.raises(ValueError, match="parent"):
        apply_fuels_38_nutrients_review(resource)
