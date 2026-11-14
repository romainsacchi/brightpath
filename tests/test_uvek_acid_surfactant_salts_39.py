"""Active acids and AE3 routes preserve quantities, delivery and older chemical bindings."""

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

REVIEW = json.loads((DATA_DIR / "export/uvek_acid_surfactant_salts_39_review.json").read_text())
ROWS = REVIEW["records"]
HCL, AE3, BICARBONATE, NITRIC = ROWS


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


@pytest.mark.parametrize("row", ROWS, ids=["HCl", "AE3", "bicarbonate", "nitric"])
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
        assert len(actual) == len(expected)
        assert all(all(e[k] == v for k, v in d.items()) for e, d in zip(actual, expected, strict=True))
        assert not helper["brightpath_conversion"].get("source_input_omissions")
    else:
        assert len(result.data) == 1
        assert all(edge[k] == v for k, v in row["original_rule"]["target"].items())


def test_active_basis_feedstock_shares_and_final_delivery_are_not_rescaled():
    assert all(r["conversion_factor"] == 1 for r in ROWS)
    assert all(w["reference_amounts"] == [1] for r in ROWS for w in r["native_witnesses"].values())
    hcl = REVIEW["recipes"][HCL["recipe_id"]]["inputs"]
    assert len(hcl) == 11 and [e["amount"] for e in hcl[8:]] == [0.004381967, 0.0021869035, 0.0171298966]
    assert hcl[2]["name"] == "Hydrochloric acid, from Mannheim process, at plant"
    assert hcl[3]["name"] == "Hydrochloric acid, from the reaction of hydrogen with chlorine, at plant"
    aggregate = HCL["original_rule"]["target"]["name"]
    assert sum(e["amount"] for e in hcl[:8] if e["name"] == aggregate) == pytest.approx(0.2573411416440952)
    ae3 = REVIEW["recipes"][AE3["recipe_id"]]["inputs"]
    assert [e["amount"] for e in ae3[:3]] == [0.917431192660551, 0.0275229357798165, 0.055045871559633]
    assert [e["name"] for e in ae3[:3]] == [
        "Ethoxylated alcohols (AE3), petrochemical, at plant",
        "Ethoxylated alcohols (AE3), coconut oil, at plant",
        "Ethoxylated alcohols (AE3), palm kernel oil, at plant",
    ]
    salt = REVIEW["recipes"][BICARBONATE["recipe_id"]]["inputs"]
    assert len(salt) == 11 and salt[0]["amount"] == 1
    assert salt[5]["name"] == "Transport, freight, rail, diesel" and salt[5]["location"] == "US"
    assert salt[5]["amount"] == 0.07882579040030561
    nitric = REVIEW["recipes"][NITRIC["recipe_id"]]["inputs"]
    assert len(nitric) == 3 and nitric[0]["amount"] == 1
    assert REVIEW["retained_original_producer_entries"] == 13
    assert REVIEW["retained_original_final_freight_entries"] == 18
    assert REVIEW["new_source_omissions"] == [] and REVIEW["new_helpers"] == 4


@pytest.mark.parametrize("prior", REVIEW["preserved_existing_scopes"])
@pytest.mark.parametrize("amount,uncertainty", [(-2.5, 2), (2.5, 2), (-2.5, 3), (2.5, 3)])
def test_older_HCl_authority_and_direct_stock_remain_physically_unchanged(prior, amount, uncertainty):
    assert (
        hashlib.sha256((DATA_DIR / prior["previous_review_path"]).read_bytes()).hexdigest()
        == prior["previous_review_sha256"]
    )
    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == HCL["rule_id"])
    source = TechnosphereProfile(**prior["source_profile"])
    target = TechnosphereProfile(**prior["target_profile"])
    assert compatibility_rule_fingerprint(resource, "replace", rule, source, target) == prior["rebound_fingerprint"]
    baseline = deepcopy(resource)
    original = next(r for r in baseline["replace"] if r["id"] == HCL["rule_id"])
    original.clear()
    original.update(deepcopy(HCL["original_rule"]))
    assert (
        compatibility_rule_fingerprint(baseline, "replace", original, source, target) == prior["previous_fingerprint"]
    )
    result = BrightwayInventory.from_data(
        consumer(HCL["original_rule"]["source"], amount, uncertainty), context=contexts("ecoinvent", source.version)
    ).migrate_background(contexts("uvek", "2026").background, policy=policy(version=source.version))
    assert len(result.data) == 1 and result.data[0]["exchanges"][1]["amount"] == amount
    assert all(result.data[0]["exchanges"][1][k] == v for k, v in prior["unchanged_physical_binding"]["target"].items())


@pytest.mark.parametrize("row", ROWS)
def test_new_source_profiles_need_their_own_permission(row):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], 1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(False))


@pytest.mark.parametrize(
    "version,model", [("3.7", "cutoff"), ("3.9.1", "cutoff"), ("3.11", "cutoff"), ("3.9", "consequential")]
)
def test_new_39_permission_does_not_widen_profiles(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(HCL["original_rule"]["source"], 1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


@pytest.mark.parametrize("drift", [None, "parent", "delivery", "product"])
def test_frozen_replay_is_idempotent_and_refuses_unreviewed_drift_atomically(drift):
    from scripts.review_uvek_acid_surfactant_salts_39 import apply_acid_surfactant_salts_39_review

    resource = deepcopy(load_uvek_technosphere_resource())
    if drift == "parent":
        next(r for r in resource["replace"] if r["id"] == AE3["rule_id"])["confidence"] = 0.001
    elif drift == "delivery":
        resource["recipes"][HCL["recipe_id"]]["inputs"][-1]["amount"] *= 2
    elif drift == "product":
        resource["recipes"][HCL["recipe_id"]]["inputs"][0]["name"] = "unreviewed secondary alloy"
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_acid_surfactant_salts_39_review(resource)
    else:
        apply_acid_surfactant_salts_39_review(resource)
    assert resource == before
