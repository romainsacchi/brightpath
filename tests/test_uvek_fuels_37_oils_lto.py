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

REVIEW = json.loads((DATA_DIR / "export/uvek_fuels_37_oils_lto_review.json").read_text())
RECORDS = {r["rule_id"]: r for r in REVIEW["records"]}


@pytest.mark.parametrize("rid", RECORDS)
@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_complete_native_stock_keeps_signed_caller_statistics(rid, amount, uncertainty):
    row = RECORDS[rid]
    source = BrightwayInventory.from_data(
        consumer(row["compiled_rule"]["source"], amount, uncertainty),
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
    assert all(edge[k] == v for k, v in row["compiled_rule"]["target"].items())
    assert edge["amount"] == amount and edge["formula"] == "dose"
    assert edge["uncertainty type"] == uncertainty and edge["scale"] == 0.2
    if uncertainty == 2:
        assert edge["negative"] is (amount < 0)
    else:
        assert edge["loc"] == amount


def test_reviews_do_not_change_any_of_the_scoped_extensions_preserve_previous_profiles():
    assert len(RECORDS) == 2
    resource = load_uvek_technosphere_resource()
    for rid, row in RECORDS.items():
        assert next(r for r in resource["replace"] if r["id"] == rid) == row["compiled_rule"]
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
    for row in RECORDS.values():
        assert row["quantity_basis"] in ("kilogram", "unit")
        assert row["conversion_factor"] == 1
        assert row["preserved_profiles"]


def test_extension_author_is_idempotent_and_preserves_helpers():
    from brightpath import TechnosphereProfile
    from brightpath.background.reference_signs import select_profile_sign_rules
    from scripts.review_uvek_fuels_37_oils_lto import apply_fuels_37_oils_lto_review, digest

    resource = deepcopy(load_uvek_technosphere_resource())
    before = deepcopy(resource)
    apply_fuels_37_oils_lto_review(resource)
    assert resource == before
    for row in RECORDS.values():
        for proof in row["preserved_profiles"]:
            selected = select_profile_sign_rules(
                {"replace": [row["compiled_rule"]]}, TechnosphereProfile(**proof["source_profile"])
            )["replace"][0]
            assert digest(selected) == proof["selected_rule_sha256"]
