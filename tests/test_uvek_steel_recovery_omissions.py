"""Steelmaking recovery handoffs retain their exact profile and accounting boundaries."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from uvek_review_helpers import historical_forward_resource

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_complete_patch_boundaries import CASES as COMPLETION_CASES
from scripts.review_uvek_exact_patch_boundaries import CASES as PATCH_CASES
from scripts.review_uvek_reference_signs import APPROVED_STEEL_RECOVERY_OMISSIONS, profile_omission_profiles

PROFILES = [
    profile
    for profile in [(v, m) for v in ("3.7", "3.8", "3.9", "3.10", "3.11", "3.12") for m in ("cutoff", "consequential")]
    if profile[1] == "cutoff"
]


def inventory_data(amount):
    data = None
    for source in APPROVED_STEEL_RECOVERY_OMISSIONS:
        supplier = dict(zip(FIELDS, source, strict=True))
        for kind, uncertainty in (("technosphere", 2), ("substitution", 3)):
            batch = consumer(supplier, amount, uncertainty, kind)
            row = batch[0]["exchanges"][1]
            row["audit_note"] = "original steel recovery input"
            if data is None:
                data = batch
            else:
                data[0]["exchanges"].append(row)
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


@pytest.mark.parametrize("version,model", PROFILES)
@pytest.mark.parametrize("release", ["2025", "2026"])
@pytest.mark.parametrize("amount", [-10, 0, 10])
def test_steel_recovery_omissions_preserve_original_data_and_warn_about_lost_credits(version, model, release, amount):
    data = inventory_data(amount)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
    result = inventory.migrate_background(
        contexts("uvek", release).background,
        policy=MigrationPolicy(
            on_heuristic_mapping=PolicyAction.WARN,
            on_deletion=PolicyAction.ERROR,
            on_information_loss=PolicyAction.ERROR,
        ),
    )
    assert result.data[0]["exchanges"] == [before[0]["exchanges"][0], before[0]["exchanges"][-1]]
    report = result.last_migration_report
    warnings = [i for i in report.issues if i.code == "migration.technosphere_omission"]
    losses = [i for i in report.losses if i.code == "migration.technosphere_omission"]
    assert len(warnings) == len(losses) == 32
    assert all(i.severity.value == "warning" for i in warnings)
    assert all("recovery credits, which are excluded" in i.message for i in warnings)
    assert all("method-dependent and unquantified" in i.message for i in warnings)
    assert all("empty recycled-content boundary" in i.message for i in warnings)
    originals = {f"datasets[0].exchanges[{i}]": row for i, row in enumerate(before[0]["exchanges"])}
    assert all(loss.details["source_exchange"] == originals[loss.path] for loss in losses)
    assert not any(i.code == "migration.reference_flow_sign_unresolved" for i in report.issues)
    assert inventory.data == data == before
    assert result.migrate_background(contexts("uvek", release).background).data == result.data


def test_historical_steel_recovery_approval_retains_exact_profiles_original_targets_and_review_evidence():
    resource = historical_forward_resource()
    ledger = json.loads((DATA_DIR / "export/uvek_reference_flow_sign_review.json").read_text())
    rules = [
        r for r in resource["replace"] if tuple(r["source"][f] for f in FIELDS) in APPROVED_STEEL_RECOVERY_OMISSIONS
    ]
    assert len(rules) == 16
    assert sum(a["approval_scope"] == "explicit" for a in APPROVED_STEEL_RECOVERY_OMISSIONS.values()) == 2
    observations = 0
    for rule in rules:
        approval = APPROVED_STEEL_RECOVERY_OMISSIONS[tuple(rule["source"][f] for f in FIELDS)]
        decision = ledger["resources"][resource["name"]][rule["id"]]
        assert tuple(rule["target"][f] for f in FIELDS) == approval["previous_target"]
        assert decision["factor"] == decision["original_factor"] == 1
        assert decision["uvek_native_flow_type"] == "PRODUCT_FLOW"
        assert decision["source_observations"] == [list(row) for row in approval["observations"]]
        assert decision["profile_omission_review"] == approval["review"]
        entries = rule["reference_flow_sign_review"]["profiles"]
        added = sum(
            p["kind"] == "handoff" and p["source"] == rule["source"]
            for p in [*PATCH_CASES.values(), *COMPLETION_CASES.values()]
        )
        assert len(entries) == 12 + added
        assert all(
            e["operation"] == "delete" and e["approved_omission"] and "conversion_factor" not in e for e in entries
        )
        assert all(resource["reviews"][e["review_ref"]] == approval["review"] for e in entries)
        observations += len(entries)
    assert observations == 192 + 11 + 21


def test_steel_omission_selection_preserves_local_suppliers_and_cached_resource():
    resource = load_uvek_technosphere_resource()
    before = deepcopy(resource)
    selected = select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", "3.12", "cutoff"))
    added = [
        r
        for r in selected["delete"]
        if r["id"] not in {x["id"] for x in resource["delete"]}
        and tuple(r["source"][f] for f in FIELDS) in APPROVED_STEEL_RECOVERY_OMISSIONS
    ]
    assert {tuple(r["source"][f] for f in FIELDS) for r in added} == set(APPROVED_STEEL_RECOVERY_OMISSIONS)
    assert not any(
        tuple(r["source"][f] for f in FIELDS) in APPROVED_STEEL_RECOVERY_OMISSIONS for r in selected["replace"]
    )
    assert resource == before
    data = inventory_data(-10)
    for source in APPROVED_STEEL_RECOVERY_OMISSIONS:
        supplier = dict(zip(FIELDS, source, strict=True))
        data.append({**supplier, "exchanges": [{**supplier, "type": "production", "amount": -1}]})
    original = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        contexts("uvek", "2025").background, policy=MigrationPolicy.permissive()
    )
    assert result.data == data == original


@pytest.mark.parametrize("version,model", [("3.6", "cutoff"), ("3.6", "consequential"), ("3.9.2", "cutoff")])
def test_steel_recovery_omissions_do_not_fall_back_for_unreviewed_profiles(version, model):
    data = inventory_data(-10)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(contexts("uvek", "2025").background, policy=MigrationPolicy.permissive())
    expected = (
        "migration.technosphere_cross_system_model_unavailable"
        if model == "consequential"
        else "migration.reference_flow_sign_unresolved"
    )
    assert any(i.code == expected for i in caught.value.report.issues)
    assert not any(i.code == "migration.technosphere_omission" for i in caught.value.report.issues)
    assert inventory.data == data == before


def test_unrelated_unapproved_treatment_still_rolls_back_steel_recovery_omissions(unreviewed_uvek_correspondence):
    data = inventory_data(-10)
    data[0]["exchanges"].append({**unreviewed_uvek_correspondence, "amount": 1, "type": "technosphere"})
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    with pytest.raises(MigrationError):
        inventory.migrate_background(contexts("uvek", "2025").background, policy=MigrationPolicy.permissive())
    assert inventory.data == data == before


@pytest.mark.parametrize("problem", ["missing", "sign", "version"])
def test_changed_steel_recovery_profile_evidence_requires_a_new_review(problem):
    source, approval = next(iter(APPROVED_STEEL_RECOVERY_OMISSIONS.items()))
    observations = list(approval["observations"])
    if problem == "missing":
        observations.pop()
    elif problem == "sign":
        observations[0] = (observations[0][0], 1)
    else:
        observations[0] = ("ecoinvent-3.6-cutoff", -1)
    with pytest.raises(ValueError, match="coverage changed"):
        profile_omission_profiles(source, observations, ["Synthetic evidence"])
