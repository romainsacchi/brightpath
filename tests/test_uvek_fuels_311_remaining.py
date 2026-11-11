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

REVIEW = json.loads((DATA_DIR / "export/uvek_fuels_311_remaining_review.json").read_text())
RECORDS = {r["rule_id"]: r for r in REVIEW["records"]}


@pytest.mark.parametrize("rid", RECORDS)
@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_complete_native_stock_keeps_signed_caller_statistics(rid, amount, uncertainty):
    row = RECORDS[rid]
    source = BrightwayInventory.from_data(
        consumer(row["compiled_rule"]["source"], amount, uncertainty),
        context=contexts("ecoinvent", "3.11", "cutoff"),
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
    assert len(RECORDS) == 4
    resource = load_uvek_technosphere_resource()
    for rid, row in RECORDS.items():
        assert next(r for r in resource["replace"] if r["id"] == rid) == row["compiled_rule"]
        assert row["source_witness"]["reference_amounts"] == [1]
        assert row["native_2026_witness"]["reference_amount"] == 1
        assert row["compiled_rule"]["source"] == row["original_rule"]["source"]
        assert row["compiled_rule"]["target"] == row["original_rule"]["target"]
    assert all(
        r["finite_supply"] and r["finite_inventory"] for packet in (REVIEW,) for r in packet["native_lci_checks"]
    )


def test_exact_source_and_screening_limitations_are_explicit():
    for row in RECORDS.values():
        assert row["source_profile"]["version"] == "3.11"
        assert row["source_witness"]["inspected_profile"]["version"] == "3.11"
        assert row["quantity_basis"] == "kilogram"
    assert "25 bar" in RECORDS["bp-cf83bb724223e5b7"]["limitations"][0]
    assert "99.5" in RECORDS["bp-1104956dff1c701e"]["limitations"][0]
    assert "second rolling" in RECORDS["bp-263c185cdbca7a8c"]["limitations"][0]


def test_extension_author_is_idempotent_and_preserves_helpers():
    from brightpath import TechnosphereProfile
    from brightpath.background.reference_signs import select_profile_sign_rules
    from scripts.review_uvek_fuels_311_remaining import apply_fuels_311_remaining_review, digest

    resource = deepcopy(load_uvek_technosphere_resource())
    before = deepcopy(resource)
    apply_fuels_311_remaining_review(resource)
    assert resource == before
    for row in RECORDS.values():
        for proof in row["preserved_profiles"]:
            selected = select_profile_sign_rules(
                {"replace": [row["compiled_rule"]]}, TechnosphereProfile(**proof["source_profile"])
            )["replace"][0]
            assert digest(selected) == proof["selected_rule_sha256"]
