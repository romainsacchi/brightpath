"""Complete native installations retain caller basis; unqualified water stays refused."""

import json
from copy import deepcopy
from dataclasses import replace

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_fuels_38_infrastructure_water_review.json").read_text())
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
        r["finite_supply"] and r["finite_inventory"]
        for packet in (REVIEW,)
        for r in packet["native_lci_checks"]
    )


def test_complete_installation_and_water_bases_are_explicit():
    by_id = RECORDS
    assert "90000" in by_id["bp-4c51b8491a9aebe9"]["limitations"][0]
    assert "16000" in by_id["bp-c291034cfcb83f1c"]["limitations"][0]
    assert "5 MW" in by_id["bp-23f6df653910092e"]["limitations"][0]
    assert "ultrapure" in REVIEW["pending_water_case"]["record"]["limitations"][0]
    assert all(row["conversion_factor"] == 1 for row in by_id.values())


def test_pending_water_profile_is_not_implicitly_qualified():
    from brightpath.exceptions import MigrationError

    row = REVIEW["pending_water_case"]["record"]
    source = BrightwayInventory.from_data(
        consumer(row["rule"]["source"], 1, 2),
        context=contexts("ecoinvent", "3.8", "cutoff"),
    )
    with pytest.raises(MigrationError):
        source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
