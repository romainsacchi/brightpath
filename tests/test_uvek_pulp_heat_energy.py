"""Complete pulp heat energy supplies preserve quantity bases and old electricity rules."""

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
from scripts.review_uvek_pulp_heat_energy import apply_pulp_heat_energy_review

REVIEW = json.loads((DATA_DIR / "export/uvek_pulp_heat_energy_review.json").read_text())
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
        factor = row["compiled_rule"].get("conversion_factor", 1)
        assert actual["amount"] == original["amount"] * factor
        assert actual["uncertainty type"] == original["uncertainty type"]
        assert actual["formula"] == ("dose" if factor == 1 else "(dose) * (36.0)")
        if uncertainty == 2:
            assert actual["loc"] == pytest.approx(original["loc"] + math.log(factor))
            assert actual["scale"] == original["scale"]
            assert actual["negative"] == original["negative"]
        else:
            assert actual["loc"] == original["loc"] * factor
            assert actual["scale"] == pytest.approx(original["scale"] * factor)
            assert actual["minimum"] == original["minimum"] * factor
            assert actual["maximum"] == original["maximum"] * factor


@pytest.mark.parametrize("version,model", [("3.6", "cutoff"), ("3.12", "cutoff"), ("3.9", "consequential")])
def test_uninspected_profiles_do_not_inherit_proxy_approval(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(graph(1), context=contexts("ecoinvent", version, model)).migrate_background(
            contexts("uvek", "2026").background, policy=policy("3.9")
        )


def test_every_source_and_native_reference_is_positive_and_nonempty():
    assert len(REVIEW["records"]) == 51 and REVIEW["source_profile_count"] == 51
    assert REVIEW["new_helpers"] == 0 and len(REVIEW["native_2026_witnesses"]) == 39
    assert sum(r["geographic_proxy"] for r in REVIEW["records"]) == 16
    for row in REVIEW["records"]:
        assert len(row["source_profiles"]) == 1
        for p in row["source_profiles"]:
            assert p["witness"]["reference_amounts"] == [1] and p["witness"]["nonproduction_count"] > 0
        assert row["native_2026_witness"]["reference_amounts"] == [1]
        assert row["native_2026_witness"]["nonproduction_count"] > 0
        assert row["source_profiles"][0]["conversion_factor"] in (1, 36)
        assert row["compiled_rule"]["target"]["name"] in {
            "Natural gas, high pressure, at consumer",
            "Electricity, high voltage, at grid",
            "Electricity, medium voltage, at grid",
            "Diesel, at regional storage",
            "Light fuel oil, at regional storage",
        }
        if row["historical_parent_unchanged"]:
            assert row["compiled_rule"] == row["original_rule"]


@pytest.mark.parametrize("drift", [None, 0, -1])
def test_replay_is_atomic_and_rejects_parent_drift(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    if drift is not None:
        rid = REVIEW["records"][drift]["rule_id"]
        next(r for r in resource["replace"] if r["id"] == rid)["confidence"] = 0.123
    before = deepcopy(resource)
    if drift is None:
        apply_pulp_heat_energy_review(resource)
    else:
        with pytest.raises(ValueError, match="parent changed"):
            apply_pulp_heat_energy_review(resource)
    assert resource == before
