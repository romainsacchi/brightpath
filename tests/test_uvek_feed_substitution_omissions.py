"""Nutritional feed credits and product supply retain independent profile decisions."""

import json
from copy import deepcopy
from pathlib import Path

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from uvek_review_helpers import historical_forward_resource

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy
from scripts.review_uvek_reference_signs import (
    APPROVED_FEED_SUBSTITUTION_OMISSIONS,
    model_specific_review,
    profile_omission_profiles,
)


def feed_data(amount, *, markets_only=False):
    data = None
    for source, approval in APPROVED_FEED_SUBSTITUTION_OMISSIONS.items():
        if markets_only and approval["kind"] != "market":
            continue
        for kind, uncertainty in (("technosphere", 2), ("substitution", 3)):
            batch = consumer(dict(zip(FIELDS, source, strict=True)), amount, uncertainty, kind)
            row = batch[0]["exchanges"][1]
            row["audit_note"] = "original feed substitution"
            if data is None:
                data = batch
            else:
                data[0]["exchanges"].append(row)
    data[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "amount": 0.5,
            "type": "biosphere",
        }
    )
    return data


def test_feed_approvals_keep_70_omissions_with_35_reviewed_and_no_pending_product_profiles():
    resource = historical_forward_resource()
    ledger = json.loads((DATA_DIR / "export/uvek_reference_flow_sign_review.json").read_text())
    rules = [
        r for r in resource["replace"] if tuple(r["source"][f] for f in FIELDS) in APPROVED_FEED_SUBSTITUTION_OMISSIONS
    ]
    assert len(rules) == 10
    assert sum(a["approval_scope"] == "explicit" for a in APPROVED_FEED_SUBSTITUTION_OMISSIONS.values()) == 5
    omitted = pending = replaced = 0
    for rule in rules:
        approval = APPROVED_FEED_SUBSTITUTION_OMISSIONS[tuple(rule["source"][f] for f in FIELDS)]
        decision = ledger["resources"][resource["name"]][rule["id"]]
        if "recipe_ref" not in rule["target"]:
            assert tuple(rule["target"][f] for f in FIELDS) == approval["previous_target"]
        assert decision["source_observations"] == [list(row) for row in approval["observations"]]
        assert decision["original_factor"] == decision["factor"] == 1
        assert decision.get("original_native_flow_type", decision["uvek_native_flow_type"]) == "PRODUCT_FLOW"
        for entry in rule["reference_flow_sign_review"]["profiles"]:
            if entry["source_profile"]["system_model"] == "consequential":
                assert "conversion_factor" not in entry
                assert entry["operation"] == "delete" and entry["approved_omission"]
                assert resource["reviews"][entry["review_ref"]] == approval["review"]
                omitted += 1
            elif entry["review"]["status"] == "verified":
                assert entry["conversion_factor"] == 1 and "recipe_ref" in entry["target"]
                replaced += 1
            else:
                assert entry["review"]["status"] == "unresolved" and "operation" not in entry
                assert "conversion_factor" not in entry
                pending += 1
    assert (omitted, pending, replaced) == (70, 0, 35)


@pytest.mark.parametrize("version", ["3.6", "3.12"])
def test_cutoff_feed_markets_use_separately_approved_product_helpers_without_omission(version):
    data = feed_data(-10, markets_only=True)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, "cutoff"))
    result = inventory.migrate_background(contexts("uvek", "2025").background, policy=MigrationPolicy.permissive())
    assert len(result.data) == 6
    assert all(e["amount"] == -10 for e in result.data[0]["exchanges"][1:-1])
    assert not any(i.code == "migration.technosphere_omission" for i in result.last_migration_report.issues)
    assert inventory.data == data == before


def test_independent_prior_product_approval_survives_omission_reinspection():
    source, approval = next((s, a) for s, a in APPROVED_FEED_SUBSTITUTION_OMISSIONS.items() if a["kind"] == "market")
    previous = model_specific_review(approval["observations"], ["Synthetic evidence"])
    entry = next(e for e in previous if e["review"]["source_sign"] == 1)
    entry["review"].update(status="verified", reason="Independent synthetic product approval")
    entry["conversion_factor"] = 1
    before = deepcopy(previous)
    result = profile_omission_profiles(source, approval["observations"], ["Synthetic evidence"], previous)
    assert next(e for e in result if e["source_profile"] == entry["source_profile"]) == entry
    assert previous == before


def test_review_queue_distinguishes_partial_feed_market_approvals():
    queue = json.loads((Path(__file__).resolve().parents[1] / "docs/uvek_forward_review_queue.json").read_text())
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    resource = load_uvek_technosphere_resource()
    from scripts.review_uvek_cutoff_feed_products import MARKETS as PHYSICAL_FEED_MARKETS

    market_ids = {
        r["id"]
        for r in resource["replace"]
        if tuple(r["source"][f] for f in FIELDS) in APPROVED_FEED_SUBSTITUTION_OMISSIONS
        and APPROVED_FEED_SUBSTITUTION_OMISSIONS[tuple(r["source"][f] for f in FIELDS)]["kind"] == "market"
        and tuple(r["source"][f] for f in FIELDS) not in PHYSICAL_FEED_MARKETS
    }
    from scripts.review_uvek_additional_cutoff_waste_markets import SOURCES as ADDITIONAL_MARKETS
    from scripts.review_uvek_industrial_waste_markets import SOURCES as INDUSTRIAL_MARKETS
    from scripts.review_uvek_market_compositions import COMPOSITIONS
    from scripts.review_uvek_natural_gas_flares import FLARES as NATURAL_GAS_FLARES
    from scripts.review_uvek_pet_waste_markets import SOURCES as PET_MARKETS
    from scripts.review_uvek_processing_compositions import PROCESSING
    from scripts.review_uvek_resolved_child_markets import SOURCES as RESOLVED_CHILD_MARKETS
    from scripts.review_uvek_steel_residue_markets import SOURCES as STEEL_MARKETS

    assemblies = (
        COMPOSITIONS
        | PROCESSING
        | RESOLVED_CHILD_MARKETS
        | STEEL_MARKETS
        | INDUSTRIAL_MARKETS
        | ADDITIONAL_MARKETS
        | PET_MARKETS
    )

    def still_pending(rule, label):
        family, version, system_model = label.split("-")
        profile = {"family": family, "version": version, "system_model": system_model}
        inspected = [
            p for p in rule.get("reference_flow_sign_review", {}).get("profiles", []) if p["source_profile"] == profile
        ]
        # Later exact-profile reviews can resolve an earlier assembly frontier.
        # Missing or unverified evidence must still count as pending.
        return len(inspected) != 1 or inspected[0]["review"]["status"] != "verified"

    partial_assemblies = {
        r["id"]
        for r in resource["replace"]
        if any(
            p["label"].endswith("-cutoff") and still_pending(r, p["label"])
            for p in assemblies.get(tuple(r["source"][f] for f in FIELDS), {}).get("pending_profiles", [])
        )
    }
    partial_flares = {
        r["id"]
        for r in resource["replace"]
        if tuple(r["source"][f] for f in FIELDS) in NATURAL_GAS_FLARES
        and any(
            p["review"]["status"] != "verified"
            for p in r["reference_flow_sign_review"].get("profiles", [])
            if p["source_profile"]["system_model"] == "cutoff"
        )
    }
    assert len(market_ids) == 0 and len(partial_assemblies) == 21 and len(partial_flares) == 0
    assert {
        r["id"] for r in queue["rules"] if r["review_state"] == "partially_approved"
    } == market_ids | partial_assemblies | partial_flares
    assert queue["partially_approved_identities"] == len(market_ids | partial_assemblies | partial_flares)
    assert queue["fully_pending_identities"] == len(queue["rules"]) - queue["partially_approved_identities"]
    assert queue["pending_identities"] == len(queue["rules"])
    assert set(queue["families"]) == {row["family"] for row in queue["rules"]}
