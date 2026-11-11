"""Complete-stock/service proxies keep requested context and patch evidence distinct."""

import json
from copy import deepcopy
from dataclasses import replace

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_fuels_37_stocks_review.json").read_text())
RECORDS = {r["rule_id"]: r for r in REVIEW["records"]}


@pytest.mark.parametrize("rid", RECORDS)
@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_complete_native_stock_keeps_signed_caller_statistics(rid, amount, uncertainty):
    row = RECORDS[rid]
    source = BrightwayInventory.from_data(
        consumer(row["rule"]["source"], amount, uncertainty),
        context=contexts("ecoinvent", "3.7", "cutoff"),
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
    assert len(result.data) == 1
    edge = result.data[0]["exchanges"][1]
    assert all(edge[k] == v for k, v in row["rule"]["target"].items())
    assert edge["amount"] == amount and edge["formula"] == "dose"
    assert edge["uncertainty type"] == uncertainty and edge["scale"] == 0.2
    if uncertainty == 2:
        assert edge["negative"] is (amount < 0)
    else:
        assert edge["loc"] == amount


def test_reviews_do_not_change_any_of_the_six_existing_rules():
    assert len(RECORDS) == 6
    resource = load_uvek_technosphere_resource()
    for rid, row in RECORDS.items():
        assert next(r for r in resource["replace"] if r["id"] == rid) == row["rule"]
        assert row["source_witness"]["reference_amounts"] == [1]
        assert row["native_2026_witness"]["reference_amount"] == 1
        assert not row["source_market_internal_shares_and_freight_reproduced"]
        assert not row["native_inventory_modified"]
    assert all(
        r["finite_supply"] and r["finite_inventory"] for packet in (REVIEW,) for r in packet["native_lci_checks"]
    )


def test_patch_reference_evidence_remains_distinct_from_requested_context():
    decision = REVIEW["patch_evidence_decision"]
    assert decision["requested_version"] == "3.7"
    assert decision["inspected_version"] == "3.7.1"
    for row in RECORDS.values():
        assert row["source_profile"]["version"] == "3.7"
        assert row["source_witness"]["inspected_profile"]["version"] == "3.7.1"
        assert row["source_witness"]["requested_catalog_activity_id"] != row["source_witness"]["inspected_activity_id"]
        assert "does not relabel" in row["limitations"][-1]
    assert "exclude the steel material" in RECORDS["bp-1d72298e464032e1"]["limitations"][0]
    assert RECORDS["bp-24eb26e899c2f905"]["quantity_basis"] == "square meter"
    assert RECORDS["bp-1fb529c7535e435f"]["quantity_basis"] == "cubic meter"
