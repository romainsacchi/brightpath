"""Primary-ingot delivery and tonne-km preserve their distinct physical boundaries."""

import hashlib
import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_aluminium_europe_delivery_39_review.json").read_text())
ROWS = REVIEW["records"]
ALUMINIUM, BARGE = ROWS


def policy(approved=True, version="3.9"):
    resource = load_uvek_technosphere_resource()
    ids = {row["rule_id"] for row in ROWS}
    fingerprints = tuple(
        compatibility_rule_fingerprint(
            resource,
            "replace",
            rule,
            TechnosphereProfile("ecoinvent", version, "cutoff"),
            TechnosphereProfile("uvek", "2025", "cutoff"),
        )
        for rule in resource["replace"]
        if rule["id"] in ids
    )
    return MigrationPolicy(
        approved_compatibility_rules=fingerprints if approved else (), on_information_loss=PolicyAction.WARN
    )


@pytest.mark.parametrize("row", ROWS, ids=["delivered-ingot", "tank-barge"])
@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_signed_uncertain_product_demand_keeps_native_inventory_and_final_freight(row, amount, uncertainty, release):
    data = consumer(row["original_rule"]["source"], amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == before
    edge = result.data[0]["exchanges"][1]
    for key in ("amount", "uncertainty type", "loc", "scale", "negative", "formula", "minimum", "maximum"):
        if key in before[0]["exchanges"][1]:
            assert edge[key] == before[0]["exchanges"][1][key]
    if row["recipe_id"]:
        assert len(result.data) == 2
        helper = result.data[1]
        assert all(edge[k] == helper[k] for k in ["name", "reference product", "location", "unit"])
        actual = [e for e in helper["exchanges"] if e["type"] != "production"]
        expected = REVIEW["recipes"][row["recipe_id"]]["inputs"]
        assert len(actual) == len(expected) == 10
        assert all(all(e[k] == v for k, v in d.items()) for e, d in zip(actual, expected, strict=True))
        assert not helper["brightpath_conversion"].get("source_input_omissions")
    else:
        assert len(result.data) == 1
        assert all(edge[k] == v for k, v in row["original_rule"]["target"].items())


def test_import_handoffs_and_final_delivery_do_not_create_an_extra_casting_or_loading_factor():
    entries = REVIEW["recipes"][ALUMINIUM["recipe_id"]]["inputs"]
    assert all(e["name"] == "Aluminium, primary, at plant" and e["amount"] > 0 for e in entries[:8])
    assert sum(e["amount"] for e in entries[:8]) == pytest.approx(1, abs=1e-12)
    assert [e["amount"] for e in entries[8:]] == [0.088, 2.4332]
    assert entries[8]["name"] == "Transport, barge tanker"
    assert ALUMINIUM["boundary_checks"]["source_import_handoffs_have_no_extra_inventory"]
    assert ALUMINIUM["boundary_checks"]["source_regional_smelter_mix_reproduced"] is False
    assert ALUMINIUM["conversion_factor"] == BARGE["conversion_factor"] == 1
    assert all(r["source_witness"]["reference_amounts"] == [1] for r in ROWS)
    assert all(w["reference_amounts"] == [1] for r in ROWS for w in r["native_witnesses"].values())
    assert REVIEW["new_source_omissions"] == [] and REVIEW["new_helpers"] == 1
    authority = REVIEW["prior_primary_metal_proxy_authority"]
    assert hashlib.sha256((DATA_DIR / authority["path"]).read_bytes()).hexdigest() == authority["sha256"]


def test_earlier_310_keeps_its_exact_direct_stock_binding_and_frozen_authority():
    (old,) = REVIEW["preserved_existing_scopes"]
    assert (
        hashlib.sha256((DATA_DIR / old["previous_review_path"]).read_bytes()).hexdigest()
        == old["previous_review_sha256"]
    )
    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == ALUMINIUM["rule_id"])
    source = TechnosphereProfile(**old["source_profile"])
    target = TechnosphereProfile(**old["target_profile"])
    assert compatibility_rule_fingerprint(resource, "replace", rule, source, target) == old["rebound_fingerprint"]
    original = deepcopy(resource)
    parent = next(r for r in original["replace"] if r["id"] == ALUMINIUM["rule_id"])
    parent.clear()
    parent.update(deepcopy(ALUMINIUM["original_rule"]))
    assert compatibility_rule_fingerprint(original, "replace", parent, source, target) == old["previous_fingerprint"]
    result = BrightwayInventory.from_data(
        consumer(ALUMINIUM["original_rule"]["source"], -2.5), context=contexts("ecoinvent", "3.10")
    ).migrate_background(contexts("uvek", "2026").background, policy=policy(version="3.10"))
    assert len(result.data) == 1 and result.data[0]["exchanges"][1]["amount"] == -2.5
    assert all(result.data[0]["exchanges"][1][k] == v for k, v in old["unchanged_physical_binding"]["target"].items())


@pytest.mark.parametrize("row", ROWS)
def test_new_source_profiles_need_their_own_permission(row):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], 1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(False))


@pytest.mark.parametrize(
    "version,model", [("3.8", "cutoff"), ("3.9.1", "cutoff"), ("3.11", "cutoff"), ("3.9", "consequential")]
)
def test_new_39_permission_does_not_widen_profiles(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ALUMINIUM["original_rule"]["source"], 1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


@pytest.mark.parametrize("drift", [None, "parent", "delivery", "product"])
def test_frozen_replay_is_idempotent_and_refuses_unreviewed_drift_atomically(drift):
    from scripts.review_uvek_aluminium_europe_delivery_39 import apply_aluminium_europe_delivery_39_review

    resource = deepcopy(load_uvek_technosphere_resource())
    if drift == "parent":
        next(r for r in resource["replace"] if r["id"] == BARGE["rule_id"])["confidence"] = 0.001
    elif drift == "delivery":
        resource["recipes"][ALUMINIUM["recipe_id"]]["inputs"][-1]["amount"] *= 2
    elif drift == "product":
        resource["recipes"][ALUMINIUM["recipe_id"]]["inputs"][0]["name"] = "unreviewed secondary alloy"
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_aluminium_europe_delivery_39_review(resource)
    else:
        apply_aluminium_europe_delivery_39_review(resource)
    assert resource == before
