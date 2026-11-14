"""Cooling demand remains cooling MJ without repeating chiller heat or efficiency."""

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

REVIEW = json.loads((DATA_DIR / "export/uvek_cooling_energy_39_review.json").read_text())
ROWS = REVIEW["records"]


def policy(exclude=None):
    resource = load_uvek_technosphere_resource()
    fingerprints = tuple(
        compatibility_rule_fingerprint(
            resource,
            "replace",
            next(r for r in resource["replace"] if r["id"] == row["rule_id"]),
            TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
            TechnosphereProfile("uvek", "2025", "cutoff"),
        )
        for row in ROWS
        if row["rule_id"] != exclude
    )
    return MigrationPolicy(approved_compatibility_rules=fingerprints, on_information_loss=PolicyAction.WARN)


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_same_service_preserves_signed_cooling_mj_without_double_heat(amount, uncertainty, release):
    data = []
    for row in ROWS:
        node = consumer(row["original_rule"]["source"], amount, uncertainty)[0]
        node["name"] += row["rule_id"]
        node["exchanges"].append(
            {
                "name": "Carbon dioxide, fossil",
                "categories": ["air"],
                "unit": "kilogram",
                "type": "biosphere",
                "amount": 0.5,
            }
        )
        data.append(node)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == before and len(result.data) == 3
    for actual, original, row in zip(result.data, before, ROWS, strict=True):
        assert len(actual["exchanges"]) == 3
        edge = actual["exchanges"][1]
        expected = deepcopy(original["exchanges"][1])
        scale_exchange(expected, 1)
        assert all(edge[k] == v for k, v in row["compiled_rule"]["target"].items())
        for key in ("amount", "uncertainty type", "loc", "scale", "negative", "minimum", "maximum", "formula"):
            if key in expected:
                assert edge[key] == expected[key]
        assert actual["exchanges"][-1] == original["exchanges"][-1]


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
def test_each_exact_39_service_requires_its_own_permission(row):
    data = consumer(row["original_rule"]["source"], -1)
    before = deepcopy(data)
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
            contexts("uvek", "2026").background, policy=policy(exclude=row["rule_id"])
        )
    assert data == before


@pytest.mark.parametrize(
    "version,model", [("3.8", "cutoff"), ("3.9.1", "cutoff"), ("3.10", "cutoff"), ("3.9", "consequential")]
)
def test_permissions_do_not_expand_source_release_or_system_model(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROWS[0]["original_rule"]["source"], -1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


def test_complete_native_chiller_and_water_boundary_are_documented_without_new_recipes():
    rules = {r["id"]: r for r in load_uvek_technosphere_resource()["replace"]}
    assert REVIEW["new_helpers"] == 0 and REVIEW["new_source_omissions"] == [] and REVIEW["recipes"] == {}
    for row in ROWS:
        assert rules[row["rule_id"]] == row["original_rule"] == row["compiled_rule"]
        assert row["source_witness"]["reference_amounts"] == [1]
        assert all(w["reference_amounts"] == [1] for w in row["native_witnesses"].values())
        facts = row["boundary_checks"]
        assert facts["reference_basis"] == "MJ cooling output"
        assert facts["driving_heat_MJ_per_MJ_cooling"] == 1.67
        assert facts["electricity_kWh_per_MJ_cooling"] == 0.02
        assert facts["seasonal_performance_factor_already_internal"] == 0.6
        assert facts["extra_efficiency_or_heat_multiplier"] is False
        assert facts["source_direct_water_returns_present"] is True
        assert facts["native_direct_water_returns_present"] is False
        assert facts["market_producer_shares_sum"] == 1
        assert facts["market_has_freight_or_additional_losses"] is False
        assert facts["new_helper_required"] is False
        assert any("water-return" in limit for limit in row["limitations"])
