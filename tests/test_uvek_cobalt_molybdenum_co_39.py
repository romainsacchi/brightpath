"""Complete cobalt, molybdenum and CO supplies retain distinct market boundaries."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_cobalt_molybdenum_co_39_review.json").read_text())
ROWS = REVIEW["records"]
COBALT, MOLYBDENUM, CO = ROWS


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


@pytest.mark.parametrize("row", ROWS, ids=["cobalt", "molybdenum", "carbon-monoxide"])
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
        assert len(actual) == len(expected) == 8
        assert all(all(e[k] == v for k, v in d.items()) for e, d in zip(actual, expected, strict=True))
        assert not helper["brightpath_conversion"].get("source_input_omissions")
    else:
        assert len(result.data) == 1
        assert all(edge[k] == v for k, v in row["original_rule"]["target"].items())


def test_recovered_cobalt_is_a_positive_product_and_native_delivery_boundaries_are_explicit():
    entries = REVIEW["recipes"][COBALT["recipe_id"]]["inputs"]
    assert all(e["name"] == "Cobalt, at plant" and e["amount"] > 0 for e in entries[:4])
    assert sum(e["amount"] for e in entries[:4]) == pytest.approx(1, abs=1e-12)
    assert entries[1]["amount"] + entries[3]["amount"] == pytest.approx(0.00571625801376486)
    assert [e["amount"] for e in entries[4:]] == [0.3861, 0.3272, 0.1739, 1.699]
    assert all(w["reference_amounts"] == [1] for w in COBALT["source_provider_witnesses"])
    assert MOLYBDENUM["recipe_id"] is None and CO["recipe_id"] is None
    assert MOLYBDENUM["boundary_checks"]["native_distribution_model_replaces_source_market"]
    assert CO["boundary_checks"]["source_CO_purchased_oxygen_absent_from_native"]
    assert all(row["conversion_factor"] == 1 for row in ROWS)
    assert all(w["reference_amounts"] == [1] for row in ROWS for w in row["native_witnesses"].values())
    assert REVIEW["new_source_omissions"] == [] and REVIEW["new_helpers"] == 1
    limits = " ".join(CO["limitations"])
    assert "1.142857143kg liquid oxygen" in limits and "water exchanges absent" in limits
    assert "-1.9028kg concentrate substitution" in limits
    assert "lacks further-treatment emissions" in limits


@pytest.mark.parametrize("row", ROWS)
def test_new_source_profiles_need_their_own_permission(row):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], 1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(False))


@pytest.mark.parametrize(
    "version,model", [("3.8", "cutoff"), ("3.9.1", "cutoff"), ("3.10", "cutoff"), ("3.9", "consequential")]
)
def test_new_39_permission_does_not_widen_profiles(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(COBALT["original_rule"]["source"], 1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


@pytest.mark.parametrize("drift", [None, "parent", "delivery", "product"])
def test_frozen_replay_is_idempotent_and_refuses_unreviewed_drift_atomically(drift):
    from scripts.review_uvek_cobalt_molybdenum_co_39 import apply_cobalt_molybdenum_co_39_review

    resource = deepcopy(load_uvek_technosphere_resource())
    if drift == "parent":
        next(r for r in resource["replace"] if r["id"] == MOLYBDENUM["rule_id"])["confidence"] = 0.001
    elif drift == "delivery":
        resource["recipes"][COBALT["recipe_id"]]["inputs"][-1]["amount"] *= 2
    elif drift == "product":
        resource["recipes"][COBALT["recipe_id"]]["inputs"][0]["name"] = "unreviewed secondary alloy"
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_cobalt_molybdenum_co_39_review(resource)
    else:
        apply_cobalt_molybdenum_co_39_review(resource)
    assert resource == before
