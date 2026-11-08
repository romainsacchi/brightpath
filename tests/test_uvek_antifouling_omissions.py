"""Unsupported marine paint emissions are omitted, with full exchange provenance."""

import hashlib
import json
import pickle
import sqlite3
from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory, SimaProInventory
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_antifouling import (
    APPROVAL,
    APPROVED_ANTIFOULING_OMISSIONS,
    validate_antifouling_inventories,
)
from scripts.review_uvek_reference_signs import collect_reference_profiles

LABELS = [
    label
    for label in sorted(p["label"] for p in next(iter(APPROVED_ANTIFOULING_OMISSIONS.values()))["profiles"])
    if label.endswith("-cutoff")
]
POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN)


def batch(amount):
    data = None
    for source in APPROVED_ANTIFOULING_OMISSIONS:
        for kind in ("technosphere", "substitution"):
            for uncertainty in (2, 3, 4, 5):
                row = consumer(dict(zip(FIELDS, source, strict=True)), amount, uncertainty, kind)
                row[0]["exchanges"][1]["caller_metadata"] = {"retain": [1, 2]}
                if data is None:
                    data = row
                else:
                    data[0]["exchanges"].append(row[0]["exchanges"][1])
    data[0]["comment"] = "Synthetic antifouling-emission omission fixture."
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


@pytest.mark.parametrize("label", LABELS)
@pytest.mark.parametrize("target_kind", ["2025", "2026", "native2026"])
@pytest.mark.parametrize("amount", [-10, 0, 10])
def test_all_antifouling_omissions_warn_preserve_original_signed_exchanges_and_foreground_emissions(
    label, target_kind, amount
):
    _, version, model = label.split("-")
    data = batch(amount)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
    target = target_context(target_kind)
    result = inventory.migrate_background(target, policy=POLICY)
    assert len(result.data) == 1 and len(result.data[0]["exchanges"]) == 2
    assert result.data[0]["exchanges"][0] == before[0]["exchanges"][0]
    assert result.data[0]["exchanges"][-1]["name"] == "Carbon dioxide, fossil"
    assert result.data[0]["exchanges"][-1]["amount"] == 0.5
    report = result.last_migration_report
    warnings = [i for i in report.issues if i.code == "migration.technosphere_omission"]
    losses = [loss for loss in report.losses if loss.code == "migration.technosphere_omission"]
    assert len(warnings) == len(losses) == 40
    serialized = [
        loss
        for stage in json.loads(report.to_json())["stages"]
        for loss in stage["losses"]
        if loss["code"] == "migration.technosphere_omission"
    ]
    assert len(serialized) == 40
    warnings_by_path = {warning.path: warning for warning in warnings}
    losses_by_path = {loss.path: loss for loss in losses}
    encoded_by_path = {loss["path"]: loss for loss in serialized}
    for index, old in enumerate(before[0]["exchanges"][1:-1], start=1):
        path = f"datasets[0].exchanges[{index}]"
        warning, loss, encoded = warnings_by_path[path], losses_by_path[path], encoded_by_path[path]
        assert warning.severity.value == "warning"
        assert warning.path == loss.path == f"datasets[0].exchanges[{index}]"
        assert all(
            s in warning.message
            for s in ("marine", "organotin", "ecotoxicity", "not represented in the target dataset")
        )
        assert loss.details["approved_omission"] is True
        assert loss.to_dict()["details"]["source_exchange"] == encoded["details"]["source_exchange"] == old
    assert inventory.data == data == before
    assert not any(i.code == "migration.reference_flow_sign_unresolved" for i in report.issues)


@pytest.mark.parametrize("on_deletion", list(PolicyAction))
@pytest.mark.parametrize("on_information_loss", list(PolicyAction))
def test_antifouling_approval_continues_with_warning_under_all_generic_loss_policies(on_deletion, on_information_loss):
    result = BrightwayInventory.from_data(batch(-1), context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context("2025"),
        policy=MigrationPolicy(
            on_heuristic_mapping=PolicyAction.WARN, on_deletion=on_deletion, on_information_loss=on_information_loss
        ),
    )
    assert len(result.data[0]["exchanges"]) == 2
    assert (
        len(
            [
                i
                for i in result.last_migration_report.issues
                if i.code == "migration.technosphere_omission" and i.severity.value == "warning"
            ]
        )
        == 40
    )


def test_antifouling_foreground_suppliers_and_production_are_preserved_and_migration_is_idempotent():
    data = batch(-1)
    for source in APPROVED_ANTIFOULING_OMISSIONS:
        supplier = dict(zip(FIELDS, source, strict=True))
        data.append({**supplier, "exchanges": [{**supplier, "amount": -1, "type": "production"}]})
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context("2025"), policy=POLICY
    )
    assert result.data == data == before
    assert not any(i.code == "migration.technosphere_omission" for i in result.last_migration_report.issues)
    external = BrightwayInventory.from_data(batch(-1), context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context("2025"), policy=POLICY
    )
    again = external.migrate_background(target_context("2025"), policy=POLICY)
    assert again.data == external.data and not any(
        i.code == "migration.technosphere_omission" for i in again.last_migration_report.issues
    )


@pytest.mark.parametrize("model", ["cutoff"])
def test_unrelated_mapping_failure_rolls_back_every_antifouling_omission(model, unreviewed_uvek_correspondence):
    data = batch(-1)
    data[0]["exchanges"].append({**unreviewed_uvek_correspondence, "amount": 1, "type": "technosphere"})
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12", model))
    with pytest.raises(MigrationError):
        inventory.migrate_background(target_context("2025"), policy=POLICY)
    assert inventory.data == data == before


@pytest.mark.parametrize("problem", ["hash", "missing_inventory", "profile", "sign"])
def test_antifouling_inventory_and_profile_changes_require_a_new_review(problem):
    source, approval = next(iter(APPROVED_ANTIFOULING_OMISSIONS.items()))
    observations = [(p["label"], -1) for p in approval["profiles"]]
    inventories = [(p["label"], p["inventory_sha256"]) for p in approval["profiles"]]
    if problem == "hash":
        inventories[0] = (inventories[0][0], "0" * 64)
    elif problem == "missing_inventory":
        inventories.pop()
    elif problem == "profile":
        observations[0] = ("ecoinvent-3.9.1-cutoff", -1)
    else:
        observations[0] = (observations[0][0], 1)
    with pytest.raises(ValueError, match="repeat the scientific review"):
        validate_antifouling_inventories(source, observations, inventories)


def test_all_five_historical_misassignments_and_70_inventory_fingerprints_remain_auditable():
    resource = load_uvek_technosphere_resource()
    ledger = json.loads((DATA_DIR / "export/uvek_reference_flow_sign_review.json").read_text())
    decisions = ledger["resources"][resource["name"]]
    omissions = [
        r for r in resource["delete"] if tuple(r["source"][f] for f in FIELDS) in APPROVED_ANTIFOULING_OMISSIONS
    ]
    assert len(omissions) == APPROVAL["source_count"] == 5
    assert sum(len(row["profiles"]) for row in APPROVAL["rules"]) == APPROVAL["source_profile_count"] == 70
    for rule in omissions:
        source = tuple(rule["source"][f] for f in FIELDS)
        row = next(row for row in APPROVAL["rules"] if row["source"] == rule["source"])
        decision = decisions[rule["id"]]
        assert decision["operation"] == "delete"
        assert decision["original_rule"]["target"] == row["previous_target"]
        assert decision["original_rule"].get("conversion_factor", 1) == row["previous_factor"]
        assert decision["original_rule"]["reference_flow_sign_review"]["status"] == row["previous_sign_status"]
        assert decision["source_observations"] == sorted([[p["label"], -1] for p in row["profiles"]])
        assert decision["uvek_native_flow_type"] == row["previous_native_flow_type"]
        assert not any(r["source"] == rule["source"] for r in resource["replace"])
        assert all(p["inventory_sha256"] for p in APPROVED_ANTIFOULING_OMISSIONS[source]["profiles"])


def test_antifouling_inventory_collector_reads_without_mutating_sqlite(tmp_path):
    source = next(iter(APPROVED_ANTIFOULING_OMISSIONS))
    activity = {**dict(zip(FIELDS, source, strict=True)), "code": "antifouling"}
    raw = pickle.dumps({"input": ("background", "antifouling"), "amount": -1})
    path = tmp_path / "reference-project/lci/databases.db"
    path.parent.mkdir(parents=True)
    with sqlite3.connect(path) as c:
        c.execute("CREATE TABLE activitydataset (data BLOB, database TEXT)")
        c.execute("CREATE TABLE exchangedataset (output_code TEXT, data BLOB, output_database TEXT, type TEXT)")
        c.execute("INSERT INTO activitydataset VALUES (?, ?)", (pickle.dumps(activity), "background"))
        c.execute("INSERT INTO exchangedataset VALUES (?, ?, ?, ?)", ("antifouling", raw, "background", "production"))
    before = path.read_bytes()
    actual = collect_reference_profiles(
        tmp_path,
        [
            {
                "profile": ["ecoinvent", "3.12", "cutoff"],
                "project": "reference-project",
                "database": "background",
                "reference_count": 1,
            }
        ],
    )
    assert actual[0]["antifouling_inventories"] == [
        {"source": list(source), "inventory_sha256": hashlib.sha256(raw).hexdigest()}
    ]
    assert path.read_bytes() == before


@pytest.mark.parametrize("model", ["cutoff"])
@pytest.mark.parametrize("target_kind", ["2025", "native2026"])
def test_paint_manufacturing_survives_and_only_retained_inventory_is_exported(tmp_path, model, target_kind):
    resource = load_uvek_technosphere_resource()
    paint = next(
        r
        for r in resource["replace"]
        if r["source"]["name"] == "market for alkyd paint, white, without solvent, in 60% solution state"
        and r["source"]["location"] == "RER"
    )
    data = batch(-1)
    data[0]["exchanges"].append({**paint["source"], "amount": 2, "type": "technosphere"})
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12", model)).migrate_background(
        target_context(target_kind), policy=POLICY
    )
    retained = [e for e in result.data[0]["exchanges"] if e["type"] == "technosphere"]
    assert len(retained) == 1 and retained[0]["amount"] == 2
    assert "paint" in retained[0]["name"].lower()
    excel = result.write_excel(tmp_path / "antifouling.xlsx")
    restored = BrightwayInventory.from_excel(excel)
    assert not restored.validate().has_errors and len(restored.data) == 1
    categorized = result.data
    categorized[0]["exchanges"][0]["simapro category"] = "Materials/Paints"
    csv = (
        BrightwayInventory.from_data(categorized, context=result.context)
        .to_simapro()
        .write_csv(tmp_path / "antifouling.csv")
    )
    loaded = SimaProInventory.from_csv(
        csv, background_profile=result.background_profile, biosphere_profile=result.context.background.biosphere
    ).to_brightway()
    assert not loaded.validate().has_errors and len(loaded.data) == 1
    for inventory in (restored, loaded):
        inputs = [e for e in inventory.data[0]["exchanges"] if e["type"] == "technosphere"]
        assert len(inputs) == 1 and inputs[0]["amount"] == 2
        assert not any("antifouling" in e["name"].lower() for e in inventory.data[0]["exchanges"])
