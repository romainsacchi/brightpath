"""Native delivered-gas references use the inspected net heating value."""

import json
import math
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_ferrosilicon_gas import apply_ferrosilicon_gas_review

REVIEW = json.loads((DATA_DIR / "export/uvek_ferrosilicon_gas_review.json").read_text())
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
def test_complete_delivered_grid_proxy_preserves_callers_and_creates_no_helpers(version, amount, uncertainty):
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
        assert actual["amount"] == original["amount"] * 36
        assert actual["uncertainty type"] == original["uncertainty type"]
        assert actual["formula"] == "(dose) * (36.0)"
        if uncertainty == 2:
            assert actual["loc"] == pytest.approx(original["loc"] + math.log(36))
            assert actual["scale"] == original["scale"]
            assert actual["negative"] == original["negative"]
        else:
            assert actual["loc"] == original["loc"] * 36
            assert actual["scale"] == pytest.approx(original["scale"] * 36)
            assert actual["minimum"] == original["minimum"] * 36
            assert actual["maximum"] == original["maximum"] * 36


@pytest.mark.parametrize("version,model", [("3.6", "cutoff"), ("3.12", "cutoff"), ("3.9", "consequential")])
def test_uninspected_profiles_do_not_inherit_proxy_approval(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(graph(1), context=contexts("ecoinvent", version, model)).migrate_background(
            contexts("uvek", "2026").background, policy=policy("3.9")
        )


def test_every_source_and_native_reference_is_positive_and_nonempty():
    assert len(REVIEW["records"]) == 13 and REVIEW["source_profile_count"] == 13
    assert REVIEW["new_helpers"] == 0 and len(REVIEW["native_2026_witnesses"]) == 13
    assert sum(r["geographic_proxy"] for r in REVIEW["records"]) == 0
    for row in REVIEW["records"]:
        assert len(row["source_profiles"]) == 1
        for p in row["source_profiles"]:
            assert p["witness"]["reference_amounts"] == [1] and p["witness"]["nonproduction_count"] > 0
        assert row["native_2026_witness"]["reference_amounts"] == [1]
        assert row["native_2026_witness"]["nonproduction_count"] > 0
        assert row["compiled_rule"]["conversion_factor"] == 36
        assert row["compiled_rule"]["target"]["name"] == "Natural gas, low pressure, at consumer"


@pytest.mark.parametrize("drift", [None, 0, -1])
def test_replay_is_atomic_and_rejects_parent_drift(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    if drift is not None:
        rid = REVIEW["records"][drift]["rule_id"]
        next(r for r in resource["replace"] if r["id"] == rid)["confidence"] = 0.123
    before = deepcopy(resource)
    if drift is None:
        apply_ferrosilicon_gas_review(resource)
    else:
        with pytest.raises(ValueError, match="parent changed"):
            apply_ferrosilicon_gas_review(resource)
    assert resource == before
