"""Freight services preserve tonne-km with explicit fleet and geography limitations."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_coal_ft_background_review.json").read_text())
FIELDS = ("name", "reference product", "location", "unit")


def graph(amount, uncertainty=0):
    data = consumer(REVIEW["records"][0]["compiled_rule"]["source"], amount, uncertainty)
    data[0]["exchanges"] = data[0]["exchanges"][:1] + [
        consumer(r["compiled_rule"]["source"], amount, uncertainty)[0]["exchanges"][1] for r in REVIEW["records"]
    ]
    return data


def policy(version):
    resource = load_uvek_technosphere_resource()
    rules = {r["id"]: r for r in resource["replace"]}
    scopes = tuple(
        compatibility_rule_fingerprint(
            resource,
            "replace",
            rules[r["rule_id"]],
            TechnosphereProfile("ecoinvent", version, "cutoff"),
            TechnosphereProfile("uvek", "2025", "cutoff"),
        )
        for r in REVIEW["records"]
    )
    return MigrationPolicy(approved_compatibility_rules=scopes, on_information_loss=PolicyAction.WARN)


@pytest.mark.parametrize("version", ["3.9"])
@pytest.mark.parametrize("amount", [-2, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_complete_freight_proxy_preserves_callers_and_creates_no_helpers(version, amount, uncertainty):
    data = graph(amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version)).migrate_background(
        contexts("uvek", "2026").background, policy=policy(version)
    )
    assert data == before and len(result.data) == 1
    for row, actual, original in zip(
        REVIEW["records"], result.data[0]["exchanges"][1:], before[0]["exchanges"][1:], strict=True
    ):
        assert {k: actual[k] for k in FIELDS} == row["native_2026_witness"]["identity"]
        for key in ("amount", "uncertainty type", "loc", "scale", "minimum", "maximum", "negative", "formula"):
            if key in original:
                assert actual[key] == original[key]


@pytest.mark.parametrize("version,model", [("3.6", "cutoff"), ("3.9", "consequential")])
def test_uninspected_profiles_do_not_inherit_proxy_approval(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(graph(1), context=contexts("ecoinvent", version, model)).migrate_background(
            contexts("uvek", "2026").background, policy=policy("3.9")
        )


def test_grid_losses_and_capital_reference_are_verified_without_parent_edits():
    rules = {r["id"]: r for r in load_uvek_technosphere_resource()["replace"]}
    for row in REVIEW["records"]:
        assert rules[row["rule_id"]] == row["compiled_rule"]
        assert row["source_witness"]["reference_amounts"] == [1]
        assert row["native_2026_witness"]["reference_amounts"] == [1]
        facts = row["reference_basis_facts"]
        if row["source_witness"]["identity"]["unit"] == "kilowatt hour":
            assert facts["native_high_voltage_kWh_per_delivered_MV_kWh"] > 1
            assert facts["network_and_SF6_retained"]
        else:
            assert facts["source_and_native_annual_crude_capacity_tonnes"] == 1000000
            assert facts["source_and_native_lifetime_years"] == 30
    assert REVIEW["new_helpers"] == 0 and REVIEW["new_source_omissions"] == []
