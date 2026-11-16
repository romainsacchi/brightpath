"""Reviewed car services preserve treatment demand, transport mode and old profiles."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.migrations.resources import load_uvek_technosphere_resource
from brightpath.migrations.scaling import scale_exchange
from scripts.review_uvek_passenger_car_services_312 import apply_passenger_car_services_312_review

REVIEW = json.loads((DATA_DIR / "export/uvek_passenger_car_services_312_review.json").read_text())


@pytest.mark.parametrize("amount,uncertainty", [(-2, 2), (2, 3)])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_services_preserve_signed_reference_demand_and_uncertainty(amount, uncertainty, release):
    resource = load_uvek_technosphere_resource()
    indexed = {r["id"]: r for r in resource["replace"]}
    rows = REVIEW["records"]
    policy = MigrationPolicy(
        approved_compatibility_rules=tuple(
            compatibility_rule_fingerprint(
                resource,
                "replace",
                indexed[row["rule_id"]],
                TechnosphereProfile(**REVIEW["source_profile"]),
                TechnosphereProfile(**resource["target_profile"]),
            )
            for row in rows
        ),
        on_information_loss=PolicyAction.WARN,
    )
    data = consumer(rows[0]["source"], amount, uncertainty)
    data[0]["exchanges"].extend(consumer(r["source"], amount, uncertainty)[0]["exchanges"][1] for r in rows[1:])
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        contexts("uvek", release).background, policy=policy
    )
    assert data == before and len(result.data) == 1
    for row, original, edge in zip(rows, before[0]["exchanges"][1:], result.data[0]["exchanges"][1:], strict=True):
        expected = deepcopy(original)
        scale_exchange(expected, row["conversion_factor"])
        assert all(edge[key] == value for key, value in row["target"].items())
        assert edge["amount"] / row["native_witnesses"][release]["reference_amount"] == (
            original["amount"] / row["source_witness"]["reference_amount"]
        )
        for key in ("amount", "loc", "scale", "negative", "formula", "uncertainty type"):
            if key in expected:
                assert edge[key] == expected[key]


def test_old_profile_decisions_and_existing_treatment_signs_are_unchanged():
    rows = REVIEW["records"]
    for row in rows:
        if row["conversion_factor"] == -1:
            assert row["compiled_rule"] == row["original_rule"]
    for version in ("3.9", "3.10", "3.11"):
        for row in rows:
            if not row["original_rule"].get("reference_flow_sign_review", {}).get("profiles"):
                continue
            before = {"replace": [row["original_rule"]]}
            after = {"replace": [row["compiled_rule"]]}
            source = TechnosphereProfile("ecoinvent", version, "cutoff")
            assert select_profile_sign_rules(before, source) == select_profile_sign_rules(after, source)
    indexed = {r["rule_id"]: r for r in rows}
    assert indexed["bp-7b0f21cdcfdbf2d9"]["target"]["name"] == "Transport, freight, rail"
    assert "EURO-VI" in indexed["bp-ff69d56618594d0b"]["target"]["name"]


def test_replay_is_atomic_and_idempotent():
    resource = deepcopy(load_uvek_technosphere_resource())
    apply_passenger_car_services_312_review(resource)
    once = deepcopy(resource)
    apply_passenger_car_services_312_review(resource)
    assert resource == once
    parent = next(r for r in resource["replace"] if r["id"] == REVIEW["records"][-1]["rule_id"])
    parent["conversion_factor"] = 2
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="parent changed"):
        apply_passenger_car_services_312_review(resource)
    assert resource == before
