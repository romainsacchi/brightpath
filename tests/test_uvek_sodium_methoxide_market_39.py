"""Reuse the approved same-product proxy without dropping delivery or changing old permissions."""

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
from brightpath.migrations.scaling import scale_exchange

REVIEW = json.loads((DATA_DIR / "export/uvek_sodium_methoxide_market_39_review.json").read_text())
ROW = REVIEW["records"][0]
RECIPE = REVIEW["recipes"][ROW["recipe_id"]]


def policy(approved=True):
    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == ROW["rule_id"])
    fp = compatibility_rule_fingerprint(
        resource,
        "replace",
        rule,
        TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    )
    return MigrationPolicy(
        approved_compatibility_rules=(fp,) if approved else (), on_information_loss=PolicyAction.WARN
    )


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_named_product_kg_and_original_freight_survive_signed_uncertain_demands(amount, uncertainty, release):
    data = consumer(ROW["original_rule"]["source"], amount, uncertainty)
    data[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.5,
        }
    )
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == before and len(result.data) == 2
    edge = result.data[0]["exchanges"][1]
    expected = deepcopy(before[0]["exchanges"][1])
    scale_exchange(expected, 1)
    for key in ("amount", "uncertainty type", "loc", "scale", "negative", "formula", "minimum", "maximum"):
        if key in expected:
            assert edge[key] == expected[key]
    helper = result.data[1]
    assert all(edge[k] == helper[k] for k in ["name", "reference product", "location", "unit"])
    entries = [e for e in helper["exchanges"] if e["type"] != "production"]
    assert len(entries) == 6
    assert all(
        all(e[k] == v for k, v in definition.items()) for e, definition in zip(entries, RECIPE["inputs"], strict=True)
    )
    assert [e["amount"] for e in entries[:2]] == [0.2, 0.8]
    assert sum(e["amount"] for e in entries if e["unit"] == "kilogram") == 1
    assert result.data[0]["exchanges"][-1] == before[0]["exchanges"][-1]
    assert not helper["brightpath_conversion"].get("source_input_omissions")


def test_39_needs_its_own_permission():
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROW["original_rule"]["source"], -1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(False))


@pytest.mark.parametrize(
    "version,model", [("3.8", "cutoff"), ("3.9.1", "cutoff"), ("3.10", "cutoff"), ("3.9", "consequential")]
)
def test_new_permission_does_not_widen_source_profiles(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROW["original_rule"]["source"], -1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


def test_reference_product_proxy_and_allocation_limit_are_explicit():
    facts = ROW["boundary_checks"]
    assert facts["source_CAS"] == "124-41-4" and facts["source_formula"] == "CH3NaO"
    assert ROW["source_witness"]["reference_amounts"] == [1]
    assert all(w["reference_amounts"] == [1] for w in ROW["native_witnesses"].values())
    assert facts["prior_explicit_38_same_proxy_reused"] is True
    assert facts["source_route_technology_and_allocation_reproduced"] is False
    assert facts["new_allocation_yield_or_concentration_factor"] is False
    assert facts["source_producer_internals_overlaid"] is False
    assert all(e["name"] == "Sodium methoxide, at plant" for e in RECIPE["inputs"][:2])
    assert [e["amount"] for e in RECIPE["inputs"][2:]] == [0.3091, 0.0246, 0.2088, 0.599]
    assert REVIEW["new_source_omissions"] == [] and REVIEW["new_helpers"] == 1
    authority = REVIEW["prior_explicit_same_proxy_authority"]
    assert hashlib.sha256((DATA_DIR / authority["path"]).read_bytes()).hexdigest() == authority["sha256"]


def test_older_38_binding_authority_and_direct_stock_are_preserved():
    prior = REVIEW["preserved_existing_scopes"][0]
    resource = load_uvek_technosphere_resource()
    parent = next(r for r in resource["replace"] if r["id"] == ROW["rule_id"])
    source = TechnosphereProfile(**prior["source_profile"])
    target = TechnosphereProfile(**prior["target_profile"])
    assert compatibility_rule_fingerprint(resource, "replace", parent, source, target) == prior["rebound_fingerprint"]
    baseline = deepcopy(resource)
    original = next(r for r in baseline["replace"] if r["id"] == ROW["rule_id"])
    original.clear()
    original.update(deepcopy(ROW["original_rule"]))
    assert (
        compatibility_rule_fingerprint(baseline, "replace", original, source, target) == prior["previous_fingerprint"]
    )
    allowed = MigrationPolicy(
        approved_compatibility_rules=(prior["rebound_fingerprint"],), on_information_loss=PolicyAction.WARN
    )
    result = BrightwayInventory.from_data(
        consumer(ROW["original_rule"]["source"], -2.5), context=contexts("ecoinvent", "3.8")
    ).migrate_background(contexts("uvek", "2026").background, policy=allowed)
    assert len(result.data) == 1 and result.data[0]["exchanges"][1]["amount"] == -2.5
    assert all(result.data[0]["exchanges"][1][k] == v for k, v in prior["unchanged_physical_binding"]["target"].items())


@pytest.mark.parametrize("drift", [None, "parent", "freight", "product"])
def test_replay_refuses_unreviewed_supplier_or_quantity_drift_atomically(drift):
    from scripts.review_uvek_sodium_methoxide_market_39 import apply_sodium_methoxide_market_39_review

    resource = deepcopy(load_uvek_technosphere_resource())
    if drift == "parent":
        next(r for r in resource["replace"] if r["id"] == ROW["rule_id"])["confidence"] = 0.001
    elif drift == "freight":
        resource["recipes"][ROW["recipe_id"]]["inputs"][-1]["amount"] *= 2
    elif drift == "product":
        resource["recipes"][ROW["recipe_id"]]["inputs"][1]["name"] = "unreviewed product"
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_sodium_methoxide_market_39_review(resource)
    else:
        apply_sodium_methoxide_market_39_review(resource)
    assert resource == before
