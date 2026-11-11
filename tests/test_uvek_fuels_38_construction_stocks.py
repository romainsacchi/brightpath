"""Finished steel and concrete proxies preserve signed declared quantities."""

import json
from copy import deepcopy
from dataclasses import replace

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_fuels_38_construction_stocks_review.json").read_text())
RECORDS = {r["rule_id"]: r for r in REVIEW["records"]}


@pytest.mark.parametrize("rid", RECORDS)
@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_complete_native_stock_keeps_signed_caller_statistics(rid, amount, uncertainty):
    row = RECORDS[rid]
    source = BrightwayInventory.from_data(
        consumer(row["rule"]["source"], amount, uncertainty),
        context=contexts("ecoinvent", "3.8", "cutoff"),
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


def test_reviews_do_not_change_any_of_the_three_existing_rules():
    assert len(RECORDS) == 3
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


def test_rolling_and_concrete_volume_bases_are_explicit():
    routes = REVIEW["reinforcing_route_rolling_checks"]
    assert len(routes) == 2
    assert sorted(r["market_share"] for r in routes) == [0.3, 0.7]
    assert all(r["hot_rolling_kg"] == 1 for r in routes)
    row = RECORDS["bp-6b60ae27d55196e6"]
    assert row["quantity_basis"] == "cubic meter"
    assert row["conversion_factor"] == 1
    assert "355 kg" in row["limitations"][0] and "325 kg" in row["limitations"][0]
    assert "CEM III/B" in row["limitations"][0]
