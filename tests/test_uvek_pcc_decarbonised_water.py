"""Decarbonised water retains kg-water references and existing 3.6 behavior."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_pcc_decarbonised_water_review.json").read_text())
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


@pytest.mark.parametrize("version", ["3.6", "3.9"])
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


@pytest.mark.parametrize("version,model", [("3.12", "cutoff"), ("3.9", "consequential")])
def test_uninspected_profiles_do_not_inherit_proxy_approval(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(graph(1), context=contexts("ecoinvent", version, model)).migrate_background(
            contexts("uvek", "2026").background, policy=policy("3.9")
        )


def test_every_source_and_native_reference_is_positive_and_nonempty():
    assert len(REVIEW["records"]) == 6 and REVIEW["new_helpers"] == 0
    for row in REVIEW["records"]:
        assert row["source_witness"]["reference_amounts"] == [1]
        assert row["source_witness"]["nonproduction_count"] == 1
        assert row["source_producer_witness"]["reference_amounts"] == [1]
        assert row["source_water_mass_per_reference_kg"] == 1
        assert row["source_dry_mass_per_reference_kg"] == 0
        assert row["native_2026_witness"]["reference_amounts"] == [1]
        assert row["conversion_factor"] == 1


def test_parents_remain_unchanged_and_nonempty_ether_coproduct_is_excluded():
    rules = {r["id"]: r for r in load_uvek_technosphere_resource()["replace"]}
    for row in REVIEW["records"]:
        assert row["compiled_rule"] == rules[row["rule_id"]]
        assert row["parent_unchanged"]
    assert {r["source_witness"]["identity"]["location"] for r in REVIEW["records"]} == {
        "IN",
        "US",
        "CN",
        "CA",
        "BR",
        "ZA",
    }
    assert REVIEW["excluded_RoW_case"]["co_product_nonzero_nonproduction_rows"] == 20
    assert REVIEW["new_source_omissions"] == []
