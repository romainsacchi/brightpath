"""Ordinary Swiss MSW incineration keeps the negative-waste treatment convention."""

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

REVIEW = json.loads((DATA_DIR / "export/uvek_msw_incineration_39_review.json").read_text())
ROW = REVIEW["records"][0]


def policy(approved=True):
    r = load_uvek_technosphere_resource()
    rule = next(x for x in r["replace"] if x["id"] == ROW["rule_id"])
    fp = compatibility_rule_fingerprint(
        r,
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
def test_whole_service_keeps_signed_demand_uncertainty_and_separate_emissions(amount, uncertainty, release):
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
    assert data == before and len(result.data) == 1
    actual = result.data[0]["exchanges"][1]
    expected = deepcopy(before[0]["exchanges"][1])
    scale_exchange(expected, -1)
    for k in ("amount", "uncertainty type", "loc", "scale", "negative", "formula", "minimum", "maximum"):
        if k in expected:
            assert actual[k] == expected[k]
    assert all(actual[k] == value for k, value in ROW["original_rule"]["target"].items())
    assert result.data[0]["exchanges"][-1] == before[0]["exchanges"][-1]


def test_exact_39_service_requires_its_own_approval():
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROW["original_rule"]["source"], -1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(False))


@pytest.mark.parametrize(
    "version,model", [("3.8", "cutoff"), ("3.9.1", "cutoff"), ("3.10", "cutoff"), ("3.9", "consequential")]
)
def test_39_scope_does_not_expand_to_other_profiles(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROW["original_rule"]["source"], -1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


def test_direct_coverage_and_stock_boundary_differences_are_explicit():
    r = load_uvek_technosphere_resource()
    assert next(x for x in r["replace"] if x["id"] == ROW["rule_id"]) == ROW["original_rule"]
    assert ROW["source_witness"]["reference_amounts"] == [-1]
    assert all(w["reference_amounts"] == [1] for w in ROW["native_witnesses"].values())
    assert all(f["match"] and f["source"] == f["native"] for f in REVIEW["composition_and_residue_checks"])
    bio = REVIEW["biosphere_comparison_2026"]
    assert (bio["source_exchange_count"], bio["native_exchange_count"], bio["exact_match_count"]) == (132, 129, 127)
    assert sorted(x["name"] for x in bio["source_differences"]) == [
        "Manganese II",
        "Manganese II",
        "Oxygen",
        "Water",
        "Water",
    ]
    assert len(bio["native_generic_manganese_flows"]) == 2
    assert len(REVIEW["source_recovery_stock_witnesses"]) == 3
    assert all(
        w["nonproduction_count"] == 0 and w["reference_amounts"] == [1]
        for w in REVIEW["source_recovery_stock_witnesses"]
    )
    assert REVIEW["new_caller_exchange_omissions"] == []
