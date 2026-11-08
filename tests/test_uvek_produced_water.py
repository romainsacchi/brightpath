"""Produced-water proxies preserve discharge mass and disclose inventory substitution."""

import hashlib
import json
import pickle
import sqlite3
from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from test_uvek_refrigerant_disposal import target_context
from uvek_review_helpers import historical_forward_resource

from brightpath import DATA_DIR, BrightwayInventory, SimaProInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_release_resources, load_uvek_technosphere_resource
from scripts.review_uvek_produced_water import (
    APPROVAL,
    APPROVED_DISCHARGE_TREATMENTS,
    DISCHARGE_TARGETS,
    discharge_profiles,
)
from scripts.review_uvek_reference_signs import collect_reference_profiles

LABELS = [
    label
    for label in sorted(p["label"] for p in next(iter(APPROVED_DISCHARGE_TREATMENTS.values()))["profiles"])
    if label.endswith("-cutoff")
]
POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN)


def batch(amount):
    data = None
    for source in APPROVED_DISCHARGE_TREATMENTS:
        for kind in ("technosphere", "substitution"):
            for uncertainty in (2, 3, 4, 5):
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
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.5,
        }
    )
    return data


@pytest.mark.parametrize("label", LABELS)
@pytest.mark.parametrize("target_kind", ["2025", "2026", "native2026"])
@pytest.mark.parametrize("amount", [-10, 0, 10])
def test_discharge_mass_direction_uncertainty_formulas_emissions_and_provenance(label, target_kind, amount):
    _, version, model = label.split("-")
    data = batch(amount)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
    result = inventory.migrate_background(target_context(target_kind), policy=POLICY)
    assert len(result.data) == 1
    assert result.data[0]["exchanges"][0] == before[0]["exchanges"][0]
    assert result.data[0]["exchanges"][-1]["name"] == "Carbon dioxide, fossil"
    assert result.data[0]["exchanges"][-1]["amount"] == 0.5
    release = (
        next(r for r in load_uvek_release_resources("technosphere") if r["source_profiles"][0]["version"] == "2025")
        if target_kind != "2025"
        else None
    )
    report = result.last_migration_report
    warnings = [i for i in report.issues if i.code == "migration.technosphere_information_loss"]
    losses = [loss for loss in report.losses if loss.code == "migration.technosphere_information_loss"]
    assert len(warnings) == len(losses) == 32
    warnings_by_path = {i.path: i for i in warnings}
    losses_by_path = {loss.path: loss for loss in losses}
    for index, (old, new) in enumerate(
        zip(before[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True), start=1
    ):
        path = f"datasets[0].exchanges[{index}]"
        warning, loss = warnings_by_path[path], losses_by_path[path]
        approval = APPROVED_DISCHARGE_TREATMENTS[tuple(old[f] for f in FIELDS)]
        target = dict(zip(FIELDS, approval["target"], strict=True))
        if release:
            target = next(r["target"] for r in release["replace"] if r["source"] == target)
        assert {f: new[f] for f in FIELDS} == target
        assert new["amount"] == -amount and new["type"] == old["type"]
        assert new["caller_metadata"] == old["caller_metadata"]
        assert new["formula"] == "(dose) * (-1.0)"
        if old["uncertainty type"] == 2:
            assert new["loc"] == old["loc"] and new["scale"] == old["scale"]
            assert new["negative"] is (amount >= 0)
        else:
            assert new["loc"] == -amount
            assert new["minimum"] == -amount - 1 and new["maximum"] == -amount + 1
            if "scale" in old:
                assert new["scale"] == old["scale"]
        assert warning.severity.value == "warning" and approval["limitations"] in warning.message
        assert all(
            s in warning.message for s in ("0.02003", "0.000295", "chromium VI", "chromium III", "water-balance")
        )
        assert loss.to_dict()["details"]["source"] == {f: old[f] for f in FIELDS}
        assert loss.to_dict()["details"]["target"] == dict(zip(FIELDS, approval["target"], strict=True))
    assert inventory.data == data == before
    assert not any(i.code == "migration.reference_flow_sign_unresolved" for i in report.issues)


@pytest.mark.parametrize("version", ["3.9.1", "3.10.1"])
@pytest.mark.parametrize("model", ["cutoff"])
def test_discharge_patch_profiles_require_independent_review(version, model):
    data = batch(-10)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(target_context("2025"), policy=POLICY)
    assert caught.value.report.has_errors
    resource = load_uvek_technosphere_resource()
    selected = select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", version, model))
    rules = [r for r in selected["replace"] if tuple(r["source"][f] for f in FIELDS) in APPROVED_DISCHARGE_TREATMENTS]
    assert len(rules) == 4
    assert all(r["reference_flow_sign_review"]["status"] == "unresolved" for r in rules)
    assert inventory.data == data == before


def test_discharge_information_loss_policy_foreground_protection_and_idempotence():
    data = batch(-10)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(
            target_context("2025"),
            policy=MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.ERROR),
        )
    assert any(i.code == "migration.technosphere_information_loss" for i in caught.value.report.issues)
    assert inventory.data == data == before
    result = inventory.migrate_background(target_context("2025"), policy=POLICY)
    assert result.migrate_background(target_context("2025")).data == result.data
    for source in APPROVED_DISCHARGE_TREATMENTS:
        supplier = dict(zip(FIELDS, source, strict=True))
        data.append({**supplier, "exchanges": [{**supplier, "amount": -1, "type": "production"}]})
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context("2025"), policy=POLICY
    )
    assert result.data == data == before
    assert not any(i.code == "migration.technosphere_information_loss" for i in result.last_migration_report.issues)


def test_unrelated_mapping_error_rolls_back_discharge_substitutions(unreviewed_uvek_correspondence):
    data = batch(-10)
    data[0]["exchanges"].append({**unreviewed_uvek_correspondence, "amount": 1, "type": "technosphere"})
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    with pytest.raises(MigrationError):
        inventory.migrate_background(target_context("2025"), policy=POLICY)
    assert inventory.data == data == before


def evidence_inventories():
    return {
        **{
            s: [(p["label"], p["inventory_sha256"]) for p in a["profiles"]]
            for s, a in APPROVED_DISCHARGE_TREATMENTS.items()
        },
        **{t: [("uvek-2025-cutoff", row["inventory_sha256"])] for t, row in DISCHARGE_TARGETS.items()},
    }


@pytest.mark.parametrize(
    "problem", ["source_hash", "target_hash", "missing_source", "missing_target", "profile", "sign"]
)
def test_changed_discharge_inventories_or_profiles_cannot_reuse_approval(problem):
    source, approval = next(iter(APPROVED_DISCHARGE_TREATMENTS.items()))
    inventories = evidence_inventories()
    observations = [(p["label"], -1) for p in approval["profiles"]]
    if problem in ("source_hash", "target_hash"):
        key = source if problem == "source_hash" else approval["target"]
        inventories[key][0] = (inventories[key][0][0], "0" * 64)
    elif problem in ("missing_source", "missing_target"):
        inventories[source if problem == "missing_source" else approval["target"]] = []
    elif problem == "profile":
        observations[0] = ("ecoinvent-3.9.1-cutoff", -1)
    else:
        observations[0] = (observations[0][0], 1)
    with pytest.raises(ValueError, match="repeat the scientific review"):
        discharge_profiles(source, observations, inventories, ["Synthetic test evidence"])


def test_historical_all_56_discharge_decisions_retain_prior_mappings_and_sparse_inventory_evidence():
    resource = historical_forward_resource()
    ledger = json.loads((DATA_DIR / "export/uvek_reference_flow_sign_review.json").read_text())
    rules = [r for r in resource["replace"] if tuple(r["source"][f] for f in FIELDS) in APPROVED_DISCHARGE_TREATMENTS]
    assert len(rules) == APPROVAL["source_count"] == 4
    assert (
        sum(len(r["reference_flow_sign_review"]["profiles"]) for r in rules) == APPROVAL["source_profile_count"] == 56
    )
    for r in rules:
        approval = APPROVED_DISCHARGE_TREATMENTS[tuple(r["source"][f] for f in FIELDS)]
        decision = ledger["resources"][resource["name"]][r["id"]]
        assert decision["original_target"] == r["target"]
        assert decision["original_factor"] == 1 and decision["factor"] == -1
        assert decision["uvek_native_flow_type"] == "PRODUCT_FLOW"
        assert r["reference_flow_sign_review"]["status"] == "unresolved"
        assert r["reference_flow_sign_review"]["profiles"] == discharge_profiles(
            tuple(r["source"][f] for f in FIELDS),
            [(p["label"], -1) for p in approval["profiles"]],
            evidence_inventories(),
            decision["review"]["evidence"],
        )
    assert ledger["summary"]["ecoinvent-to-uvek-2025.json"]["approved_discharge_proxy"] == 4


def test_discharge_fingerprint_collector_is_read_only(tmp_path):
    source = next(iter(APPROVED_DISCHARGE_TREATMENTS))
    activity = {**dict(zip(FIELDS, source, strict=True)), "code": "discharge"}
    path = tmp_path / "reference-project/lci/databases.db"
    path.parent.mkdir(parents=True)
    exchange = {"input": ("background", "discharge"), "amount": -1}
    raw = pickle.dumps(exchange)
    with sqlite3.connect(path) as c:
        c.execute("CREATE TABLE activitydataset (data BLOB, database TEXT)")
        c.execute("CREATE TABLE exchangedataset (output_code TEXT, data BLOB, output_database TEXT, type TEXT)")
        c.execute("INSERT INTO activitydataset VALUES (?, ?)", (pickle.dumps(activity), "background"))
        c.execute("INSERT INTO exchangedataset VALUES (?, ?, ?, ?)", ("discharge", raw, "background", "production"))
    before = path.read_bytes()
    profiles = collect_reference_profiles(
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
    assert profiles[0]["discharge_inventories"] == [
        {"source": list(source), "inventory_sha256": hashlib.sha256(raw).hexdigest()}
    ]
    assert path.read_bytes() == before


@pytest.mark.parametrize("model", ["cutoff"])
@pytest.mark.parametrize("target_kind", ["2025", "native2026"])
def test_discharge_links_round_trip_through_brightway_and_simapro(tmp_path, model, target_kind):
    data = batch(-1)
    data[0]["comment"] = "Synthetic produced-water discharge export fixture."
    for e in data[0]["exchanges"][1:-1]:
        e.pop("formula")
        e["type"] = "technosphere"
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12", model)).migrate_background(
        target_context(target_kind), policy=POLICY
    )
    excel = result.write_excel(tmp_path / "discharge.xlsx")
    restored = BrightwayInventory.from_excel(excel)
    assert not restored.validate().has_errors and len(restored.data) == 1
    categorized = result.data
    categorized[0]["exchanges"][0]["simapro category"] = "Materials/Water"
    csv = (
        BrightwayInventory.from_data(categorized, context=result.context)
        .to_simapro()
        .write_csv(tmp_path / "discharge.csv")
    )
    loaded = SimaProInventory.from_csv(
        csv, background_profile=result.background_profile, biosphere_profile=result.context.background.biosphere
    ).to_brightway()
    assert not loaded.validate().has_errors and len(loaded.data) == 1
    for inventory in (restored, loaded):
        inputs = [e for e in inventory.data[0]["exchanges"] if e["type"] == "technosphere"]
        assert len(inputs) == 32 and all(e["amount"] == 1 for e in inputs)
