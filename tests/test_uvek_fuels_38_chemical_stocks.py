"""Inspected native chemical stocks keep caller mass without dilution factors."""

import json
from copy import deepcopy
from dataclasses import replace

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_fuels_38_chemical_stocks_review.json").read_text())
ADDITIONAL = json.loads((DATA_DIR / "export/uvek_fuels_38_additional_chemicals_review.json").read_text())
RECORDS = {r["rule_id"]: r for packet in (REVIEW, ADDITIONAL) for r in packet["records"]}


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


def test_reviews_keep_the_seven_existing_physical_suppliers_and_product_bases():
    assert len(RECORDS) == 7
    resource = load_uvek_technosphere_resource()
    for rid, row in RECORDS.items():
        current = next(r for r in resource["replace"] if r["id"] == rid)
        extension = json.loads((DATA_DIR / "export/uvek_solvent_supply_markets_39_review.json").read_text())
        extended = next((r for r in extension["records"] if r["rule_id"] == rid), None)
        if extended:
            assert extended["original_rule"] == row["rule"]
            assert current == extended["compiled_rule"]
            selected = next(
                r
                for r in select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", "3.8", "cutoff"))[
                    "replace"
                ]
                if r["id"] == rid
            )
            assert selected["source"] == row["rule"]["source"]
            assert selected["target"] == row["rule"]["target"]
            assert selected.get("conversion_factor", 1) == row["rule"].get("conversion_factor", 1)
        else:
            assert current == row["rule"]
        assert row["source_witness"]["reference_amounts"] == [1]
        assert row["native_2026_witness"]["reference_amount"] == 1
        assert not row["source_market_internal_shares_and_freight_reproduced"]
        assert not row["native_inventory_modified"]
    assert all(
        r["finite_supply"] and r["finite_inventory"]
        for packet in (REVIEW, ADDITIONAL)
        for r in packet["native_lci_checks"]
    )
    assert "Sodium methoxide" in REVIEW["excluded_case"]
    assert "screening proxy" in ADDITIONAL["prior_methoxide_disposition"]["resolution"]
