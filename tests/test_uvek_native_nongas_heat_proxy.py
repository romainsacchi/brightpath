"""Use an existing useful-heat supply without recreating its upstream network."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_native_nongas_heat_proxy import apply_native_nongas_heat_proxy_review

REVIEW = json.loads((DATA_DIR / "export/uvek_native_nongas_heat_proxy_review.json").read_text())
ROW = REVIEW["records"][0]


def policy(rule_id=None):
    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == (rule_id or ROW["rule_id"]))
    return MigrationPolicy(
        approved_compatibility_rules=(
            compatibility_rule_fingerprint(
                resource,
                "replace",
                rule,
                TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
                TechnosphereProfile("uvek", "2025", "cutoff"),
            ),
        ),
        on_information_loss=PolicyAction.WARN,
    )


@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_useful_heat_quantities_and_uncertainty_reach_native_supply_once(amount, uncertainty, release):
    data = consumer(ROW["source_witness"]["identity"], amount, uncertainty)
    original = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == original and len(result.data) == 1
    actual = result.data[0]["exchanges"][1]
    assert all(actual[k] == v for k, v in ROW["compiled_rule"]["target"].items())
    for key in ("amount", "uncertainty type", "loc", "scale", "negative", "minimum", "maximum", "formula"):
        if key in original[0]["exchanges"][1]:
            assert actual[key] == original[0]["exchanges"][1][key]


def test_different_rule_and_other_profiles_remain_unapproved():
    data = consumer(ROW["source_witness"]["identity"], 1)
    original = deepcopy(data)
    with pytest.raises(MigrationError) as caught:
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
            contexts("uvek", "2026").background, policy=policy("bp-5484396eb5e79c5d")
        )
    codes = {i["code"] for s in caught.value.report.to_dict()["stages"] for i in s.get("issues", [])}
    assert "migration.unapproved_correspondence" in codes
    for version, model in (("3.10", "cutoff"), ("3.12", "cutoff"), ("3.9", "consequential")):
        with pytest.raises(MigrationError):
            BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model)).migrate_background(
                contexts("uvek", "2026").background, policy=policy()
            )
    assert data == original


def test_proxy_uses_heat_instead_of_fuel_and_records_the_changed_mix():
    assert ROW["conversion_factor"] == 1
    assert ROW["source_witness"]["reference_amounts"] == ROW["native_witness"]["reference_amounts"] == [1]
    assert ROW["compiled_rule"]["target"]["name"] == "Heat, at hard coal industrial furnace 1-10MW"
    assert REVIEW["native_net_efficiency"] * REVIEW["native_fuel_MJ_per_useful_heat_MJ"] == 1
    assert 0.69 < REVIEW["source_main_coal_heat_share"] < 0.70
    assert REVIEW["source_pulp_heat_share"] < 0.0004
    assert REVIEW["new_source_omissions"] == [] and REVIEW["new_helpers"] == 0
    assert REVIEW["recipes"] == REVIEW["recipe_dependencies"] == {}
    assert "not retained as a mix" in ROW["limitations"][0]


@pytest.mark.parametrize("drift", [None, "parent"])
def test_replay_is_idempotent_or_refuses_atomically(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    if drift:
        next(r for r in resource["replace"] if r["id"] == ROW["rule_id"])["confidence"] = 0.1
    before = deepcopy(resource)
    if drift is None:
        apply_native_nongas_heat_proxy_review(resource)
    else:
        with pytest.raises(ValueError):
            apply_native_nongas_heat_proxy_review(resource)
    assert resource == before
