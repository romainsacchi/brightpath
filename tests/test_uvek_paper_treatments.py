"""Approved paper disposal must select signs by exact version and system model."""

import json
import math
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
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_reference_signs import APPROVED_PROFILED_TREATMENTS, profiled_treatment_review

FIELDS = ("name", "reference product", "location", "unit")
CASES = [
    (source, profile, sign)
    for source, approval in APPROVED_PROFILED_TREATMENTS.items()
    for profile, sign in approval["observations"]
    if profile.endswith("-cutoff")
]


def context(family, version, model="cutoff"):
    return InventoryContext(
        FormatProfile("brightway_excel"),
        BackgroundContext(TechnosphereProfile(family, version, model), BiosphereProfile("ecoinvent", "3.10")),
    )


def consumer(source, amount):
    output = {"name": "Paper treatment consumer", "reference product": "output", "location": "CH", "unit": "unit"}
    supplier = dict(zip(FIELDS, source, strict=True))
    return [
        {
            **output,
            "exchanges": [
                {**output, "type": "production", "amount": 1},
                {
                    **supplier,
                    "type": "technosphere",
                    "amount": amount,
                    "uncertainty type": 2,
                    "loc": math.log(abs(amount)) if amount else 0,
                    "negative": amount < 0,
                    "scale": 0.2,
                    "formula": "dose",
                },
                {
                    **supplier,
                    "type": "substitution",
                    "amount": amount,
                    "uncertainty type": 3,
                    "loc": amount,
                    "scale": 0.2,
                    "minimum": amount - 1,
                    "maximum": amount + 1,
                    "formula": "dose",
                },
            ],
        }
    ]


@pytest.mark.parametrize("source,profile,sign", CASES)
@pytest.mark.parametrize("release", ["2025", "2026"])
@pytest.mark.parametrize("amount", [-10, 0, 10])
def test_paper_services_apply_exact_profile_factors_and_disclose_treatment_losses(
    source, profile, sign, release, amount
):
    _, version, model = profile.split("-")
    data = consumer(source, amount)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=context("ecoinvent", version, model))
    result = inventory.migrate_background(context("uvek", release).background, policy=MigrationPolicy.permissive())
    assert result.data[0]["exchanges"][0] == before[0]["exchanges"][0]
    assert len(result.data[0]["exchanges"]) == 3
    for original, exchange in zip(before[0]["exchanges"][1:], result.data[0]["exchanges"][1:], strict=True):
        assert tuple(exchange[field] for field in FIELDS) == APPROVED_PROFILED_TREATMENTS[source]["target"]
        assert exchange["type"] == original["type"]
        assert exchange["amount"] == sign * amount
        assert exchange["scale"] == original["scale"]
        assert exchange["formula"] == ("dose" if sign == 1 else "(dose) * (-1.0)")
        if original["uncertainty type"] == 2:
            assert exchange["loc"] == original["loc"]
            if amount:
                assert exchange["negative"] is (sign * amount < 0)
        else:
            assert exchange["loc"] == sign * amount
            assert exchange["minimum"] == min(sign * original["minimum"], sign * original["maximum"])
            assert exchange["maximum"] == max(sign * original["minimum"], sign * original["maximum"])
    warnings = [i for i in result.last_migration_report.issues if i.code == "migration.technosphere_information_loss"]
    assert len(warnings) == 2 and all(i.severity.value == "warning" for i in warnings)
    assert all("LCIA equivalence is not established" in i.message for i in warnings)
    limitation = "electricity and heat substitution credits" if "landfill" in source[0] else "Energy recovery"
    assert all(limitation in i.message for i in warnings)
    assert not any(i.code == "migration.reference_flow_sign_unresolved" for i in result.last_migration_report.issues)
    assert inventory.data == data == before


@pytest.mark.parametrize("source", APPROVED_PROFILED_TREATMENTS)
def test_paper_treatment_approval_has_no_uninspected_profile_fallback(source):
    resource = load_uvek_technosphere_resource()
    before = deepcopy(resource)
    for profile in (
        TechnosphereProfile("ecoinvent", "3.10", "cutoff"),
        TechnosphereProfile("ecoinvent", "3.9.1", "cutoff"),
    ):
        selected = select_profile_sign_rules(resource, profile)
        rule = next(r for r in selected["replace"] if tuple(r["source"][f] for f in FIELDS) == source)
        assert rule["reference_flow_sign_review"]["status"] == "unresolved"
        assert "No reference-flow sign decision" in rule["reference_flow_sign_review"]["reason"]
    assert resource == before


@pytest.mark.parametrize("source", APPROVED_PROFILED_TREATMENTS)
@pytest.mark.parametrize("change", ["sign", "missing", "extra"])
def test_paper_treatment_reinspection_rejects_changed_profile_evidence(source, change):
    approval = APPROVED_PROFILED_TREATMENTS[source]
    observations = deepcopy(approval["observations"])
    if change == "sign":
        label, sign = observations[0]
        observations[0] = (label, -sign)
    elif change == "missing":
        observations.pop()
    else:
        observations.append(("ecoinvent-3.12-cutoff", 1))
    with pytest.raises(ValueError, match="observations changed"):
        profiled_treatment_review(approval, observations, ["Synthetic changed evidence"])


def test_paper_treatment_information_loss_still_requires_policy_acceptance():
    source = next(iter(APPROVED_PROFILED_TREATMENTS))
    data = consumer(source, -10)
    inventory = BrightwayInventory.from_data(data, context=context("ecoinvent", "3.9", "cutoff"))
    policy = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.ERROR)
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(context("uvek", "2025").background, policy=policy)
    assert "migration.technosphere_information_loss" in {i.code for i in caught.value.report.issues}
    assert inventory.data == data


def test_paper_treatment_audit_counts_approved_profiles_separately_from_pending_suppliers():
    ledger = json.loads((DATA_DIR / "export/uvek_reference_flow_sign_review.json").read_text())
    assert ledger["summary"]["ecoinvent-to-uvek-2025.json"]["approved_by_profile"] == 3
    assert sum(len(approval["observations"]) for approval in APPROVED_PROFILED_TREATMENTS.values()) == 18
