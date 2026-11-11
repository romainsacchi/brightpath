"""Harvested oilseed and propagation seed retain different physical purposes."""

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
from scripts.review_uvek_fuels_38_crops import apply_fuels_38_crops_review

REVIEW = json.loads((DATA_DIR / "export/uvek_fuels_38_crops_review.json").read_text())
ROWS = {r["rule_id"]: r for r in REVIEW["records"]}
RAPE, SUGAR = REVIEW["records"]


def migrate(rid, amount=1, uncertainty=3, version="3.8", model="cutoff"):
    source = BrightwayInventory.from_data(
        consumer(ROWS[rid]["original_rule"]["source"], amount, uncertainty),
        context=contexts("ecoinvent", version, model),
    )
    before = deepcopy(source.data)
    policy = replace(
        MigrationPolicy.permissive(),
        on_invalid_source=PolicyAction.ERROR,
        on_invalid_target=PolicyAction.ERROR,
        on_unresolved_link=PolicyAction.ERROR,
        minimum_coverage=1,
    )
    result = source.migrate_background(target_context("2026"), policy=policy)
    assert source.data == before
    return result, result.data[0]["exchanges"][1]


@pytest.mark.parametrize("rid", ROWS)
@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_fresh_kg_caller_quantity_uncertainty_and_formula(rid, amount, uncertainty):
    _, edge = migrate(rid, amount, uncertainty)
    assert edge["amount"] == amount and edge["formula"] == "dose"
    assert edge["uncertainty type"] == uncertainty and edge["scale"] == 0.2
    if uncertainty == 2:
        assert edge["negative"] is (amount < 0)
    else:
        assert edge["loc"] == amount


def test_six_original_crop_entries_and_all_final_freight_survive():
    result, edge = migrate(RAPE["rule_id"])
    market = next(a for a in result.data if a.get("code") == edge["input"][1])
    entries = [e for e in market["exchanges"] if e["type"] != "production"]
    assert len(entries) == 10
    assert len([e for e in entries if e["unit"] == "kilogram"]) == 6
    assert len([e for e in entries if e["unit"] == "ton kilometer"]) == 4
    for actual, witness in zip(entries, RAPE["retained_rows"], strict=True):
        for field, value in witness["statistics"].items():
            assert actual[field] == value
        assert all(actual[k] == v for k, v in witness["target"].items())
    producers = RAPE["source_producer_witnesses"]
    assert {p["source_water_in_wet_mass"] for p in producers} >= {0.06, 0.09, 0.1}
    assert sum(p["geographic_proxy"] for p in producers) == 3
    by_country = {p["identity"]["location"]: p["target"]["location"] for p in producers}
    assert by_country == {"DE": "DE", "FR": "FR", "US": "US", "Canada without Quebec": "US", "CA-QC": "US", "RoW": "US"}
    assert not any(e["unit"] == "megajoule" for e in entries)


def test_sowing_seed_uses_complete_processing_stock_once():
    result, edge = migrate(SUGAR["rule_id"])
    assert len(result.data) == 1
    assert edge["name"] == "Sugar beet seed IP, at regional storehouse"
    assert edge["unit"] == "kilogram" and edge["amount"] == 1
    current = next(r for r in load_uvek_technosphere_resource()["replace"] if r["id"] == SUGAR["rule_id"])
    assert current == SUGAR["original_rule"] == SUGAR["compiled_rule"]


@pytest.mark.parametrize("version,model", [("3.9", "cutoff"), ("3.8", "consequential")])
def test_harvested_crop_recipe_does_not_infer_other_profiles(version, model):
    with pytest.raises(MigrationError):
        migrate(RAPE["rule_id"], version=version, model=model)


def test_crop_review_replays_without_mutating_unrelated_rules():
    resource = deepcopy(load_uvek_technosphere_resource())
    before = deepcopy(resource)
    apply_fuels_38_crops_review(resource)
    assert resource == before
    next(r for r in resource["replace"] if r["id"] == RAPE["rule_id"])["conversion_factor"] = 2
    changed = deepcopy(resource)
    with pytest.raises(ValueError, match="Crop parent"):
        apply_fuels_38_crops_review(resource)
    assert resource == changed
