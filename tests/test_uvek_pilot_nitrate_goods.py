"""Nitrate material markets retain producer shares, delivery and reference bases."""

import hashlib
import json
from collections import Counter
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pilot_nitrate_goods import apply_pilot_nitrate_goods_review

REVIEW = json.loads((DATA_DIR / "export/uvek_pilot_nitrate_goods_review.json").read_text())
ROWS = REVIEW["records"] + REVIEW["direct_records"]


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_markets_keep_signed_quantities_uncertainty_and_original_delivery(row, amount, uncertainty):
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
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9"))
    result = inventory.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert inventory.data == data == before
    graph = result.data
    caller = graph[0]["exchanges"][1]
    for field in ("amount", "formula", "uncertainty type", "loc", "scale", "negative", "minimum", "maximum"):
        if field in before[0]["exchanges"][1]:
            assert caller[field] == before[0]["exchanges"][1][field]
    assert graph[0]["exchanges"][-1] == emission
    if row in REVIEW["direct_records"]:
        assert len(graph) == 1
        assert all(caller[k] == v for k, v in row["target"].items())
        assert row["thermal_MJ_reference_explicit"] and row["conversion_factor"] == 1
        assert row["source_diesel_kg_per_MJ"] == 0.0234 and row["native_diesel_kg_per_MJ"] == 0.02336
    else:
        assert len(graph) == 2
        helper = graph[1]
        assert caller["input"][1] == helper["code"]
        assert helper["reference product"] == row["source"]["reference product"]
        assert helper["unit"] == row["source"]["unit"]
        (production,) = [x for x in helper["exchanges"] if x["type"] == "production"]
        assert production["amount"] == 1
        inputs = [x for x in helper["exchanges"] if x["type"] != "production"]
        originals = row["profiles"][0]["inputs"]
        assert len(inputs) == len(originals)
        for actual, original in zip(inputs, originals, strict=True):
            assert all(actual[k] == v for k, v in original.items())
        freight = [x for x in inputs if x["unit"] == "ton kilometer"]
        assert len(freight) == row["matching_product_evidence"]["original_freight_entries"]
        assert len(inputs) - len(freight) == len(row["source_producer_witnesses"])
        assert not any("coal freight" in x["name"] for x in freight)
    assert not any(i.code == "migration.recipe_input_omission" for i in result.last_migration_report.issues)


def test_review_keeps_all_supply_and_freight_entries_in_their_original_units():
    assert len(REVIEW["records"]) == 9 and len(REVIEW["direct_records"]) == 1
    assert sum(len(r["source_producer_witnesses"]) for r in REVIEW["records"]) == 14
    assert sum(len(r["freight_witnesses"]) for r in REVIEW["records"]) == 41
    assert Counter(r["source"]["unit"] for r in REVIEW["records"]) == {
        "kilogram": 6,
        "cubic meter": 2,
        "square meter": 1,
    }
    assert set(REVIEW["source_freight_modes"]) == {"rail", "heavy", "light", "barge", "bulk", "container"}
    for authority in REVIEW["authorization_evidence"]:
        assert hashlib.sha256((DATA_DIR / authority["path"]).read_bytes()).hexdigest() == authority["sha256"]


@pytest.mark.parametrize("version", ["3.6", "3.12"])
@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
def test_material_compositions_refuse_uninspected_releases(version, row):
    inventory = BrightwayInventory.from_data(consumer(row["source"], 1), context=contexts("ecoinvent", version))
    before = deepcopy(inventory.data)
    with pytest.raises(MigrationError):
        inventory.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert inventory.data == before


def goods_stage_resource():
    """Restore frozen parents superseded by independently reviewed later stages."""
    resource = deepcopy(load_uvek_technosphere_resource())
    later = json.loads((DATA_DIR / "export/uvek_metals_38_operating_services_review.json").read_text())
    indexed = {r["id"]: r for r in resource["replace"]}
    with pytest.raises(ValueError, match="generator basis changed"):
        apply_pilot_nitrate_goods_review(resource)
    for overlay in later["records"]:
        rule = indexed[overlay["rule_id"]]
        assert rule == overlay["compiled_rule"]
        if overlay["kind"] == "burned_diesel_generator_service":
            digest = hashlib.sha256(json.dumps(overlay["original_rule"], sort_keys=True).encode()).hexdigest()
            assert digest == REVIEW["direct_records"][0]["raw_rule_sha256"]
        rule.clear()
        rule.update(deepcopy(overlay["original_rule"]))
    for filename, rule_id in (
        ("uvek_pulp_heat_so2_sulfite_review.json", "bp-46ffc4892be8fcb1"),
        ("uvek_steel_39_product_corrections_review.json", "bp-8e691fdb86f7b1c6"),
    ):
        authority = json.loads((DATA_DIR / "export" / filename).read_text())
        row = next(r for r in authority["records"] if r["rule_id"] == rule_id)
        rule = indexed[rule_id]
        assert rule == row["compiled_rule"]
        rule.clear()
        rule.update(deepcopy(row["original_rule"]))
    resource["coverage"]["mapping_methods"] = dict(
        sorted(Counter(r["mapping_method"] for r in resource["replace"]).items())
    )
    resource["coverage"]["mean_confidence"] = round(
        sum(r["confidence"] for r in resource["replace"]) / len(resource["replace"]), 6
    )
    dispositions = Counter(
        "recipe" if "recipe_ref" in r["target"] else r.get("classification", "unclassified")
        for r in resource["replace"]
    )
    dispositions["omitted"] = len(resource["delete"])
    if resource["coverage"].get("unresolved_identities"):
        dispositions["unresolved"] = resource["coverage"]["unresolved_identities"]
    resource["coverage"]["dispositions"] = dict(dispositions)
    return resource


def test_composition_authoring_is_atomic_and_idempotent():
    resource = goods_stage_resource()
    expected = deepcopy(resource)
    byid = {r["id"]: r for r in resource["replace"]}
    for row in REVIEW["records"]:
        byid[row["rule_id"]].clear()
        byid[row["rule_id"]].update(deepcopy(row["previous_rule"]))
    apply_pilot_nitrate_goods_review(resource)
    assert resource == expected
    apply_pilot_nitrate_goods_review(resource)
    assert resource == expected


@pytest.mark.parametrize("change", ["last_parent", "delivery", "freight_rule", "generator"])
def test_drift_refuses_before_mutating_any_market(change):
    resource = goods_stage_resource()
    byid = {r["id"]: r for r in resource["replace"]}
    if change == "last_parent":
        byid[REVIEW["records"][-1]["rule_id"]]["source"]["unit"] = "unit"
    elif change == "delivery":
        recipe = byid[REVIEW["records"][0]["rule_id"]]["target"]["recipe_ref"]
        resource["recipes"][recipe]["inputs"][-1]["amount"] = 999
    elif change == "generator":
        byid[REVIEW["direct_records"][0]["rule_id"]]["conversion_factor"] = 2.853881278538814
    else:
        binding = REVIEW["records"][0]["profiles"][0]["bindings"][-1]
        rule = next(
            r
            for r in resource["replace"]
            if [r["source"][k] for k in ("name", "reference product", "location", "unit")] == binding["source"]
        )
        rule["confidence"] = 0.123
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="changed"):
        apply_pilot_nitrate_goods_review(resource)
    assert resource == before
