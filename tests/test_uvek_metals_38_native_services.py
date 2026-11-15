"""Six exact source-3.8 services use complete native supplies without helpers."""

import json
from copy import deepcopy

import pytest

from brightpath import (
    DATA_DIR,
    BackgroundContext,
    BiosphereProfile,
    BrightwayInventory,
    FormatProfile,
    InventoryContext,
    TechnosphereProfile,
)
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_metals_38_native_services_review.json").read_text())


def context(family, version):
    return BackgroundContext(TechnosphereProfile(family, version, "cutoff"), BiosphereProfile("ecoinvent", "3.10"))


def policy(row, version="3.8"):
    resource = load_uvek_technosphere_resource()
    return MigrationPolicy(
        approved_compatibility_rules=(
            compatibility_rule_fingerprint(
                resource,
                "replace",
                row["rule"],
                TechnosphereProfile("ecoinvent", version, "cutoff"),
                TechnosphereProfile(**REVIEW["target_rule_profile"]),
            ),
        ),
        on_information_loss=PolicyAction.WARN,
    )


def consumer(row, amount):
    foreground = {"name": "Synthetic service buyer", "reference product": "service", "location": "CH", "unit": "unit"}
    return [
        {
            **foreground,
            "exchanges": [
                {**foreground, "type": "production", "amount": 1},
                {
                    **row["rule"]["source"],
                    "type": "technosphere",
                    "amount": amount,
                    "uncertainty type": 3,
                    "loc": amount,
                    "scale": 0.2,
                    "formula": "purchased_service",
                },
                {
                    "type": "biosphere",
                    "name": "Carbon dioxide, fossil",
                    "categories": ["air"],
                    "unit": "kilogram",
                    "amount": 0.5,
                },
            ],
        }
    ]


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_complete_services_preserve_signed_caller_and_emissions(row, amount, release):
    data = consumer(row, amount)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(
        data, context=InventoryContext(FormatProfile("brightway_excel"), context("ecoinvent", "3.8"))
    ).migrate_background(context("uvek", release), policy=policy(row))
    assert data == before and len(result.data) == 1
    edges = result.data[0]["exchanges"]
    assert len(edges) == 3
    assert all(edges[1][key] == value for key, value in row["rule"]["target"].items())
    for key in ("amount", "uncertainty type", "loc", "scale", "formula"):
        assert edges[1][key] == before[0]["exchanges"][1][key]
    assert edges[2] == before[0]["exchanges"][2]


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
def test_other_profile_approval_cannot_authorize_38(row):
    data = consumer(row, -1)
    before = deepcopy(data)
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            data, context=InventoryContext(FormatProfile("brightway_excel"), context("ecoinvent", "3.8"))
        ).migrate_background(context("uvek", "2026"), policy=policy(row, version="3.10"))
    assert data == before


def test_review_matches_exact_packaged_rules_and_reference_bases():
    resource = load_uvek_technosphere_resource()
    rules = {r["id"]: r for r in resource["replace"]}
    assert len(REVIEW["records"]) == 6 and REVIEW["new_helpers"] == 0
    assert REVIEW["changed_rules"] == ["bp-94e99d669f988a94"]
    assert REVIEW["prior_source39_selected_rule_unchanged"]
    assert not REVIEW["new_source_omissions"]
    for row in REVIEW["records"]:
        assert rules[row["rule_id"]] == row["rule"]
        assert row["conversion_factor"] == 1
        assert row["source_witness"]["reference_amounts"] == [1]
        assert row["source_witness"]["source_database"] == "ecoinvent-3.8-cutoff"
        assert row["source_witness"]["source_project"] == "ecoinvent-3.8-cutoff"
        assert row["source_witness"]["activity_code"]
        assert row["source_witness"]["identity"] == row["rule"]["source"]
        assert set(row["native_witnesses"]) == {"2025", "2026"}
        for witness in row["native_witnesses"].values():
            assert witness["reference_amounts"] == [1]
            assert witness["identity"] == row["rule"]["target"]
            assert witness["nonproduction_exchange_count"] > 0
        assert row["limitations"]


def test_late_reference_profile_replay_is_atomic_and_keeps_39_physics():
    from brightpath.background.reference_signs import select_profile_sign_rules
    from scripts.review_uvek_metals_38_native_services import apply_metals_38_native_services_review

    resource = deepcopy(load_uvek_technosphere_resource())
    before = deepcopy(resource)
    apply_metals_38_native_services_review(resource)
    assert resource == before
    row = next(r for r in REVIEW["records"] if "original_rule" in r)
    selected = TechnosphereProfile("ecoinvent", "3.9", "cutoff")
    old = select_profile_sign_rules({"replace": [row["original_rule"]]}, selected)
    new = select_profile_sign_rules({"replace": [row["rule"]]}, selected)
    assert old == new
    parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
    parent.clear()
    parent.update(deepcopy(row["original_rule"]))
    apply_metals_38_native_services_review(resource)
    assert resource == before
    parent["target"]["name"] = "wrong fuel"
    changed = deepcopy(resource)
    with pytest.raises(ValueError):
        apply_metals_38_native_services_review(resource)
    assert resource == changed
