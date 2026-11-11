"""Negative waste references become positive service demands without extra credits."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_fuels_38_construction_waste_review.json").read_text())
RECORDS = {r["rule_id"]: r for r in REVIEW["records"]}


@pytest.mark.parametrize("rid", RECORDS)
@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_signed_waste_demand_and_statistics(rid, amount, uncertainty):
    row = RECORDS[rid]
    source = BrightwayInventory.from_data(
        consumer(row["rule"]["source"], amount, uncertainty), context=contexts("ecoinvent", "3.8", "cutoff")
    )
    before = deepcopy(source.data)
    result = source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert len(result.data) == 1
    edge = result.data[0]["exchanges"][1]
    assert all(edge[k] == v for k, v in row["rule"]["target"].items())
    assert edge["amount"] == -amount and edge["formula"] == "(dose) * (-1.0)"
    assert edge["uncertainty type"] == uncertainty and edge["scale"] == 0.2
    if uncertainty == 2:
        assert edge["negative"] is (amount > 0)
        assert edge["loc"] == before[0]["exchanges"][1]["loc"]
    else:
        assert edge["loc"] == -amount
        assert edge["minimum"] == -(amount + 1)
        assert edge["maximum"] == -(amount - 1)
    assert source.data == before


def test_existing_rules_and_native_service_references_unchanged():
    resource = load_uvek_technosphere_resource()
    assert len(RECORDS) == 2
    for rid, row in RECORDS.items():
        assert next(r for r in resource["replace"] if r["id"] == rid) == row["rule"]
        assert row["source_witness"]["reference_amounts"] == [-1]
        assert row["native_2026_witness"]["reference_amount"] == 1
        assert row["rule"]["conversion_factor"] == row["conversion_factor"] == -1
        assert not row["native_inventory_modified"]
    assert all(
        r["native_activity_supply"] > 0 and r["net_fulfilled_demand"] == pytest.approx(1) and r["finite_inventory"]
        for r in REVIEW["native_lci_checks"]
    )


def test_actual_source_and_native_demands_both_require_positive_service_supply():
    assert len(REVIEW["source_lci_checks"]) == len(REVIEW["native_lci_checks"]) == 2
    for source, native in zip(REVIEW["source_lci_checks"], REVIEW["native_lci_checks"], strict=True):
        assert source["rule_id"] == native["rule_id"]
        assert source["source_demand"] == -1 and native["native_demand"] == 1
        assert source["source_activity_supply"] > 0 and native["native_activity_supply"] > 0
        assert source["net_fulfilled_demand"] == pytest.approx(-1)
        assert native["net_fulfilled_demand"] == pytest.approx(1)
        assert source["finite_supply"] and source["finite_inventory"]
        assert native["finite_supply"] and native["finite_inventory"]
