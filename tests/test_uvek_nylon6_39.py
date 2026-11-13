"""Nylon6 keeps its polymer kg basis, source3.9 delivery and earlier at-plant profiles."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_nylon6_39_review.json").read_text())
ROWS = REVIEW["records"]


def policy(row, approved=True):
    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
    fingerprint = compatibility_rule_fingerprint(
        resource,
        "replace",
        rule,
        TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    )
    return MigrationPolicy(
        approved_compatibility_rules=(fingerprint,) if approved else (),
        on_information_loss=PolicyAction.WARN,
    )


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_complete_material_or_freight_preserves_caller_and_uncertainty(row, amount, uncertainty, release):
    data = consumer(row["original_rule"]["source"], amount, uncertainty)
    emission = {
        "name": "Carbon dioxide, fossil",
        "categories": ["air"],
        "unit": "kilogram",
        "type": "biosphere",
        "amount": 0.5,
    }
    data[0]["exchanges"].append(emission)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy(row)
    )
    assert data == before and len(result.data) == (2 if row["recipe_id"] else 1)
    assert len(result.data[0]["exchanges"]) == 3
    actual = result.data[0]["exchanges"][1]
    for field in ("amount", "uncertainty type", "loc", "scale", "negative", "formula", "minimum", "maximum"):
        if field in before[0]["exchanges"][1]:
            assert actual[field] == before[0]["exchanges"][1][field]
    if row["recipe_id"]:
        helper = result.data[1]
        assert helper["brightpath_conversion"]["recipe_id"] == row["recipe_id"]
        edges = [e for e in helper["exchanges"] if e["type"] != "production"]
        assert len(edges) == 4
        assert [e["amount"] for e in edges] == [1, 0.0438, 0.0219, 0.1713]
        for edge, trace in zip(edges, REVIEW["retained_market_interfaces"][row["recipe_id"]], strict=True):
            assert all(edge[k] == v for k, v in trace["statistics"].items())
            assert all(edge[k] == v for k, v in trace["target"].items())
    else:
        assert all(actual[k] == v for k, v in row["compiled_rule"]["target"].items())
    assert result.data[0]["exchanges"][-1] == emission


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
def test_material_or_freight_needs_its_own_exact_approval(row):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], 1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(row, False))


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
@pytest.mark.parametrize(
    "version,model", [("3.8", "cutoff"), ("3.9.1", "cutoff"), ("3.10", "cutoff"), ("3.9", "consequential")]
)
def test_new_scope_does_not_authorize_other_profiles(row, version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], 1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(row))


@pytest.mark.parametrize("drift", [None, 0, 1, "delivery"])
def test_frozen_replay_is_atomic_and_idempotent(drift):
    from scripts.review_uvek_nylon6_39 import apply_nylon6_39_review

    resource = deepcopy(load_uvek_technosphere_resource())
    if drift == "delivery":
        resource["recipes"][ROWS[0]["recipe_id"]]["inputs"][2]["amount"] *= 2
    elif drift is not None:
        next(r for r in resource["replace"] if r["id"] == ROWS[drift]["rule_id"])["confidence"] = 0.01
    before = deepcopy(resource)
    if drift is None:
        apply_nylon6_39_review(resource)
    else:
        with pytest.raises(ValueError):
            apply_nylon6_39_review(resource)
    assert resource == before


def test_polymer_and_embedded_energy_limits_are_explicit():
    assert REVIEW["new_source_omissions"] == [] and REVIEW["new_helpers"] == 1
    assert len(ROWS) == 2
    for row in ROWS:
        assert row["source_witness"]["reference_amounts"] == [1]
        assert row["conversion_factor"] == 1
        assert row["original_rule"]["target"] == row["compiled_rule"]["target"]
        assert row["boundary_checks"]["embedded_energy_updatable_by_supplier_relinking"] is False
        assert row["boundary_checks"]["native_explicit_energy_purchases"] == 0
    assert ROWS[1]["original_rule"] == ROWS[1]["compiled_rule"]
    for prior in REVIEW["prior_approval_refinements"]:
        assert all(prior["previous_primary_hash_checks"].values())
    assert {p["source_profile"]["version"] for p in REVIEW["prior_approval_refinements"]} == {"3.8", "3.12"}


@pytest.mark.parametrize("prior", REVIEW["prior_approval_refinements"], ids=lambda p: p["source_profile"]["version"])
def test_prior_profiles_keep_the_at_plant_supplier_without_new_delivery(prior):
    row = ROWS[0]
    source = prior["source_profile"]
    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
    fp = compatibility_rule_fingerprint(
        resource, "replace", rule, TechnosphereProfile(**source), TechnosphereProfile("uvek", "2025", "cutoff")
    )
    old_policy = MigrationPolicy(approved_compatibility_rules=(fp,), on_information_loss=PolicyAction.WARN)
    data = consumer(row["original_rule"]["source"], -2.5, 2)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", source["version"])).migrate_background(
        contexts("uvek", "2026").background, policy=old_policy
    )
    assert data == before and len(result.data) == 1
    edge = result.data[0]["exchanges"][1]
    assert all(edge[k] == v for k, v in row["original_rule"]["target"].items())
    assert edge["amount"] == -2.5 and edge["negative"] is True


def test_nylon6_permission_does_not_authorize_nylon66():
    source = {"name": "nylon 6-6 production", "reference product": "nylon 6-6", "location": "RER", "unit": "kilogram"}
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(consumer(source, 1), context=contexts("ecoinvent", "3.9")).migrate_background(
            contexts("uvek", "2026").background, policy=policy(ROWS[0])
        )
