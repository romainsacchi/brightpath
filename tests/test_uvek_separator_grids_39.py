"""Forty delivered MV supplies retain units, caller fields and earlier permissions."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_separator_grids_39_review.json").read_text())
ROWS = REVIEW["records"]


def policy(omit=None):
    resource = load_uvek_technosphere_resource()
    fps = tuple(
        compatibility_rule_fingerprint(
            resource,
            "replace",
            next(r for r in resource["replace"] if r["id"] == row["rule_id"]),
            TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
            TechnosphereProfile("uvek", "2025", "cutoff"),
        )
        for row in ROWS
        if row["rule_id"] != omit
    )
    return MigrationPolicy(approved_compatibility_rules=fps, on_information_loss=PolicyAction.WARN)


def basket(amount, uncertainty):
    data = consumer(ROWS[0]["original_rule"]["source"], amount, uncertainty)
    for row in ROWS[1:]:
        data[0]["exchanges"].append(
            deepcopy(consumer(row["original_rule"]["source"], amount, uncertainty)[0]["exchanges"][1])
        )
    data[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.5,
        }
    )
    return data


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_all40_targets_preserve_signed_delivered_kWh_uncertainty_and_emissions(amount, uncertainty, release):
    source = basket(amount, uncertainty)
    before = deepcopy(source)
    result = BrightwayInventory.from_data(source, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert source == before and len(result.data) == 1 and len(result.data[0]["exchanges"]) == 42
    for row, old, new in zip(ROWS, before[0]["exchanges"][1:41], result.data[0]["exchanges"][1:41], strict=True):
        assert all(new[k] == v for k, v in row["compiled_rule"]["target"].items())
        for key in ("amount", "uncertainty type", "loc", "scale", "negative", "minimum", "maximum", "formula"):
            if key in old:
                assert new[key] == old[key]
        assert new["unit"] == "kilowatt hour"
        expected_geo = (
            "GLO"
            if row["boundary_checks"]["match_kind"] == "global_geographic_proxy"
            else row["original_rule"]["source"]["location"]
        )
        assert new["location"] == expected_geo
    assert result.data[0]["exchanges"][-1] == before[0]["exchanges"][-1]


@pytest.mark.parametrize(
    "rid",
    [
        ROWS[0]["rule_id"],
        next(r["rule_id"] for r in ROWS if r["boundary_checks"]["match_kind"] == "same_native_geography"),
    ],
)
def test_approval_withdrawal_refuses_the_actual_source_grid(rid):
    row = next(r for r in ROWS if r["rule_id"] == rid)
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], 1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(rid))


@pytest.mark.parametrize("prior", REVIEW["preserved_existing_operations"], ids=lambda p: p["operation"]["rule_id"])
def test_all_five_earlier_operations_and_native_targets_remain_operational(prior):
    resource = load_uvek_technosphere_resource()
    op = prior["operation"]
    rule = next(r for r in resource["replace"] if r["id"] == op["rule_id"])
    row = next(r for r in ROWS if r["rule_id"] == op["rule_id"])
    assert rule == row["original_rule"] == row["compiled_rule"]
    for scope in op["scopes"]:
        fp = compatibility_rule_fingerprint(
            resource, "replace", rule, TechnosphereProfile(**scope["source"]), TechnosphereProfile(**scope["target"])
        )
        assert fp == scope["fingerprint"]
        oldpolicy = MigrationPolicy(approved_compatibility_rules=(fp,), on_information_loss=PolicyAction.WARN)
        result = BrightwayInventory.from_data(
            consumer(rule["source"], 2.5), context=contexts("ecoinvent", scope["source"]["version"])
        ).migrate_background(contexts("uvek", "2026").background, policy=oldpolicy)
        assert result.data[0]["exchanges"][1]["amount"] == 2.5
        assert all(result.data[0]["exchanges"][1][k] == v for k, v in rule["target"].items())


@pytest.mark.parametrize("version,model", [("3.8", "cutoff"), ("3.9.1", "cutoff"), ("3.9", "consequential")])
def test_new_global_proxy_does_not_authorize_other_profiles(version, model):
    row = next(r for r in ROWS if not r["boundary_checks"]["raw_rule_unchanged"])
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], 1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


def test_primary_reference_basis_and_no_extra_grid_or_country_helpers():
    resource = load_uvek_technosphere_resource()
    assert (
        REVIEW["same_native_geographies"],
        REVIEW["matching_national_supplies"],
        REVIEW["matching_regional_supplies"],
        REVIEW["explicit_global_geographic_proxies"],
    ) == (18, 17, 1, 22)
    assert len(REVIEW["changed_rule_ids"]) == 22 and REVIEW["recipes"] == {}
    for row in ROWS:
        assert row["source_witness"]["reference_amounts"] == [1]
        assert all(w["reference_amounts"] == [1] for w in row["native_witnesses"].values())
        assert row["boundary_checks"]["new_loss_or_generation_multiplier"] is False
        assert row["boundary_checks"]["new_helper"] is False
        assert next(r for r in resource["replace"] if r["id"] == row["rule_id"]) == row["compiled_rule"]
    assert REVIEW["new_source_omissions"] == []


@pytest.mark.parametrize("drift", [None, "parent"])
def test_replay_is_atomic_without_expanding_scope(drift):
    from scripts.review_uvek_separator_grids_39 import apply_separator_grids_39_review

    resource = deepcopy(load_uvek_technosphere_resource())
    if drift:
        next(r for r in resource["replace"] if r["id"] == ROWS[0]["rule_id"])["confidence"] = 0.01
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_separator_grids_39_review(resource)
    else:
        apply_separator_grids_39_review(resource)
    assert resource == before
