"""Reviewed inorganic industrial WWT residues follow the established omission policy."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_wwt_residues import APPROVAL, APPROVED_WWT_RESIDUE_OMISSIONS, validate_wwt_residue_inventories

LABELS = [
    label
    for label in sorted({p["label"] for a in APPROVED_WWT_RESIDUE_OMISSIONS.values() for p in a["profiles"]})
    if label.endswith("-cutoff")
]
POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN)


def batch(amount, label="ecoinvent-3.12-cutoff"):
    data = None
    for source, approval in APPROVED_WWT_RESIDUE_OMISSIONS.items():
        if label not in {p["label"] for p in approval["profiles"]}:
            continue
        for kind, uncertainty in (("technosphere", 2), ("substitution", 3)):
            row = consumer(dict(zip(FIELDS, source, strict=True)), amount, uncertainty, kind)
            row[0]["exchanges"][1]["caller_metadata"] = {"retain": [1, 2]}
            if data is None:
                data = row
            else:
                data[0]["exchanges"].append(row[0]["exchanges"][1])
    data[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "type": "biosphere",
            "unit": "kilogram",
            "amount": 0.5,
        }
    )
    return data


@pytest.mark.parametrize("label", LABELS)
@pytest.mark.parametrize("target_kind", ["2025", "2026", "native2026"])
@pytest.mark.parametrize("amount", [-10, 0, 10])
def test_all_industrial_residue_profiles_omit_with_full_signed_exchange_evidence(label, target_kind, amount):
    _, version, model = label.split("-")
    data = batch(amount, label)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
    result = inventory.migrate_background(target_context(target_kind), policy=POLICY)
    assert len(result.data) == 1 and len(result.data[0]["exchanges"]) == 2
    assert result.data[0]["exchanges"][0] == before[0]["exchanges"][0]
    assert (
        result.data[0]["exchanges"][-1]["name"] == "Carbon dioxide, fossil"
        and result.data[0]["exchanges"][-1]["amount"] == 0.5
    )
    warnings = {i.path: i for i in result.last_migration_report.issues if i.code == "migration.technosphere_omission"}
    losses = {
        loss.path: loss
        for loss in result.last_migration_report.losses
        if loss.code == "migration.technosphere_omission"
    }
    encoded = {
        loss["path"]: loss
        for stage in json.loads(result.last_migration_report.to_json())["stages"]
        for loss in stage["losses"]
        if loss["code"] == "migration.technosphere_omission"
    }
    assert len(warnings) == len(losses) == len(encoded) == len(before[0]["exchanges"]) - 2
    for index, old in enumerate(before[0]["exchanges"][1:-1], start=1):
        path = f"datasets[0].exchanges[{index}]"
        assert warnings[path].severity.value == "warning"
        assert all(
            s in warnings[path].message
            for s in ("0% biogenic carbon", "credits", "not represented in the target dataset")
        )
        assert (
            losses[path].to_dict()["details"]["source_exchange"] == encoded[path]["details"]["source_exchange"] == old
        )
        assert losses[path].details["approved_omission"] is True
    assert inventory.data == data == before


def test_wwt_omission_scope_and_prior_suppliers_remain_explicit():
    resource = load_uvek_technosphere_resource()
    ledger = json.loads((DATA_DIR / "export/uvek_reference_flow_sign_review.json").read_text())
    rules = [r for r in resource["delete"] if tuple(r["source"][f] for f in FIELDS) in APPROVED_WWT_RESIDUE_OMISSIONS]
    assert len(rules) == APPROVAL["source_count"] == 226
    assert (
        sum(len(a["profiles"]) for a in APPROVED_WWT_RESIDUE_OMISSIONS.values())
        == APPROVAL["source_profile_count"]
        == 1704
    )
    for rule in rules:
        decision = ledger["resources"][resource["name"]][rule["id"]]
        approval = APPROVED_WWT_RESIDUE_OMISSIONS[tuple(rule["source"][f] for f in FIELDS)]
        assert (
            decision["operation"] == "delete"
            and tuple(decision["original_rule"]["target"][f] for f in FIELDS) == approval["previous_target"]
        )
        assert len(decision["source_observations"]) == len(approval["profiles"]) and all(
            sign == -1 for _, sign in decision["source_observations"]
        )
        assert not any(r["source"] == rule["source"] for r in resource["replace"])


@pytest.mark.parametrize("problem", ["hash", "sign", "missing", "profile"])
def test_unreviewed_industrial_residue_evidence_cannot_inherit_an_omission(problem):
    source, approval = next(iter(APPROVED_WWT_RESIDUE_OMISSIONS.items()))
    obs = [(p["label"], -1) for p in approval["profiles"]]
    inventories = [(p["label"], p["inventory_sha256"]) for p in approval["profiles"]]
    if problem == "hash":
        inventories[0] = (inventories[0][0], "0" * 64)
    elif problem == "sign":
        obs[0] = (obs[0][0], 1)
    elif problem == "missing":
        inventories.pop()
    else:
        obs[0] = ("ecoinvent-3.9.1-cutoff", -1)
    with pytest.raises(ValueError, match="repeat the scientific review"):
        validate_wwt_residue_inventories(source, obs, inventories)


def test_wwt_residue_omissions_preserve_foreground_suppliers_and_repeat_calls():
    data = batch(-1)
    for source in APPROVED_WWT_RESIDUE_OMISSIONS:
        supplier = dict(zip(FIELDS, source, strict=True))
        data.append({**supplier, "exchanges": [{**supplier, "type": "production", "amount": -1}]})
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context("2025"), policy=POLICY
    )
    assert result.data == data == before
    external = BrightwayInventory.from_data(batch(-1), context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context("2025"), policy=POLICY
    )
    again = external.migrate_background(target_context("2025"), policy=POLICY)
    assert external.data == again.data and not any(
        i.code == "migration.technosphere_omission" for i in again.last_migration_report.issues
    )


def test_wwt_omission_rollback_is_independent_of_later_mapping_approvals(unreviewed_uvek_correspondence):
    data = batch(-1)
    data[0]["exchanges"].append({**unreviewed_uvek_correspondence, "amount": 1, "type": "technosphere"})
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    with pytest.raises(MigrationError):
        inventory.migrate_background(target_context("2025"), policy=POLICY)
    assert inventory.data == data == before
