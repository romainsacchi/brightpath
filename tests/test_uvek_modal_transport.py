"""Same-mode freight corrections retain signed tkm and exact component-review guards."""

from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from test_uvek_refrigerant_disposal import target_context
from uvek_review_helpers import historical_forward_resource

from brightpath import BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_release_resources, load_uvek_technosphere_resource
from scripts.review_uvek_market_compositions import validate_composition_bindings
from scripts.review_uvek_modal_transport import (
    TARGETS,
    TRANSPORTS,
    approved_transport_rule,
    transport_decision,
)
from scripts.review_uvek_processing_compositions import validate_processing_bindings
from scripts.review_uvek_reference_signs import rule_binding

POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN)
LABELS = [
    label
    for label in sorted({p["label"] for row in TRANSPORTS.values() for p in row["profiles"]})
    if label.endswith("-cutoff")
]


def batch(amount):
    data = None
    for source in TRANSPORTS:
        for kind, uncertainty in (("technosphere", 2), ("substitution", 3)):
            row = consumer(dict(zip(FIELDS, source, strict=True)), amount, uncertainty, kind)
            if data is None:
                data = row
            else:
                data[0]["exchanges"].append(row[0]["exchanges"][1])
    return data


@pytest.mark.parametrize("label", LABELS)
@pytest.mark.parametrize("amount", [-10, 0, 10])
@pytest.mark.parametrize("release", ["2025", "native2026"])
def test_modal_proxies_preserve_signed_tkm_uncertainty_formulas_and_modes(label, amount, release):
    _, version, model = label.split("-")
    data = batch(amount)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
    result = inventory.migrate_background(target_context(release), policy=POLICY)
    assert len(result.data) == 1 and result.data[0]["exchanges"][0] == before[0]["exchanges"][0]
    bridge = (
        next(r for r in load_uvek_release_resources("technosphere") if r["source_profiles"][0]["version"] == "2025")
        if release != "2025"
        else None
    )
    for old, new in zip(before[0]["exchanges"][1:], result.data[0]["exchanges"][1:], strict=True):
        row = TRANSPORTS[tuple(old[f] for f in FIELDS)]
        target = row["target"]
        if bridge:
            target = next(r["target"] for r in bridge["replace"] if r["source"] == target)
        assert {f: new[f] for f in FIELDS} == target
        for f in ("amount", "type", "uncertainty type", "formula", "loc", "scale", "negative", "minimum", "maximum"):
            assert new.get(f) == old.get(f)
        assert "lorry" not in new["name"].lower()
    warnings = [i for i in result.last_migration_report.issues if i.code == "migration.technosphere_information_loss"]
    assert len(warnings) == 6 and all(
        "same-mode freight" in i.message and "Global fleet" in i.message for i in warnings
    )
    assert inventory.data == data == before
    assert result.migrate_background(target_context(release)).data == result.data


def test_modal_proxy_policy_and_all_dependent_bindings_remain_enforced():
    data = batch(2)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    with pytest.raises(MigrationError):
        inventory.migrate_background(
            target_context("2025"),
            policy=MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.ERROR),
        )
    assert inventory.data == data
    resource = historical_forward_resource()
    validate_composition_bindings(resource)
    validate_processing_bindings(resource)
    broken = deepcopy(resource)
    row = next(r for r in broken["replace"] if tuple(r["source"][f] for f in FIELDS) in TRANSPORTS)
    row["target"]["name"] = "Unreviewed lorry substitution"
    with pytest.raises(ValueError, match="component binding changed"):
        validate_composition_bindings(broken)


@pytest.mark.parametrize("version", ["3.11.1", "3.12.1"])
def test_modal_approvals_are_not_inferred_for_patch_profiles(version):
    resource = select_profile_sign_rules(
        load_uvek_technosphere_resource(), TechnosphereProfile("ecoinvent", version, "cutoff")
    )
    selected = [r for r in resource["replace"] if tuple(r["source"][f] for f in FIELDS) in TRANSPORTS]
    assert len(selected) == 3 and all(r["reference_flow_sign_review"]["status"] == "unresolved" for r in selected)


@pytest.mark.parametrize("mutation", ["source_hash", "target_hash", "profile", "sign", "endpoint"])
def test_modal_authoring_rejects_stale_or_incompatible_evidence(mutation):
    inventories = {s: [(p["label"], p["inventory_sha256"]) for p in row["profiles"]] for s, row in TRANSPORTS.items()}
    inventories.update({t: [("uvek-2025-cutoff", row["inventory_sha256"])] for t, row in TARGETS.items()})
    source, row = next(iter(TRANSPORTS.items()))
    rule = approved_transport_rule({"source": row["source"], "target": row["previous_target"]})
    observations = [(p["label"], 1) for p in row["profiles"]]
    assert transport_decision(rule, observations, inventories, rule_binding(rule), None)
    if mutation in ("source_hash", "target_hash"):
        key = source if mutation == "source_hash" else tuple(row["target"][f] for f in FIELDS)
        inventories[key][0] = (inventories[key][0][0], "0" * 64)
    elif mutation == "profile":
        observations.pop()
    elif mutation == "sign":
        observations[0] = (observations[0][0], -1)
    else:
        rule["target"] = row["previous_target"]
    with pytest.raises(ValueError, match="repeat the scientific review"):
        transport_decision(rule, observations, inventories, rule_binding(rule), None)
