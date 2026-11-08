"""Reviewed metal recovery credits retain their signed product-equivalent quantities."""

import json
import pickle
import sqlite3
from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from uvek_review_helpers import historical_forward_resource

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from brightpath.migrations.schema import validate_migration_resource
from scripts.review_uvek_reference_signs import (
    APPROVED_RECOVERY_CREDITS,
    collect_reference_profiles,
    recovery_credit_profiles,
)


def credit_rules(resource):
    return [r for r in resource["replace"] if tuple(r["source"][f] for f in FIELDS) in APPROVED_RECOVERY_CREDITS]


def credit_data(amount, version=None):
    data = None
    for source in APPROVED_RECOVERY_CREDITS:
        if (
            version is not None
            and (f"ecoinvent-{version}-consequential", -1) not in APPROVED_RECOVERY_CREDITS[source]["observations"]
        ):
            continue
        for kind in ("technosphere", "substitution"):
            for uncertainty in (2, 3, 4, 5):
                batch = consumer(dict(zip(FIELDS, source, strict=True)), amount, uncertainty, kind)
                batch[0]["exchanges"][1]["audit_note"] = "original recovery credit"
                if data is None:
                    data = batch
                else:
                    data[0]["exchanges"].append(batch[0]["exchanges"][1])
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


def test_recovery_credit_evidence_retains_all_25_quantities_and_prior_mappings():
    resource = historical_forward_resource()
    ledger = json.loads((DATA_DIR / "export/uvek_reference_flow_sign_review.json").read_text())
    rules = credit_rules(resource)
    assert len(rules) == 4
    assert sum(a["approval_scope"] == "explicit" for a in APPROVED_RECOVERY_CREDITS.values()) == 1
    observations = 0
    for rule in rules:
        approval = APPROVED_RECOVERY_CREDITS[tuple(rule["source"][f] for f in FIELDS)]
        decision = ledger["resources"][resource["name"]][rule["id"]]
        assert tuple(decision["original_target"][f] for f in FIELDS) == approval["previous_target"]
        assert decision["original_factor"] == 1
        assert decision["uvek_native_flow_type"] == "PRODUCT_FLOW"
        assert len(decision["recovery_credit_observations"]) == len(approval["observations"])
        for record in decision["recovery_credit_observations"]:
            assert record["credited_amount"] == -approval["quantity"]
            assert record["source_reference_amount"] == -1 and record["supplier_reference_amount"] == 1
        entries = rule["reference_flow_sign_review"]["profiles"]
        assert all(e["review"]["role_conversion"] == "recovery_credit" for e in entries)
        assert all(e["conversion_factor"] == approval["quantity"] for e in entries)
        observations += len(entries)
    assert observations == 25


@pytest.mark.parametrize("version,model", [("3.12", "cutoff"), ("3.9.1", "consequential")])
def test_recovery_only_sources_are_not_admitted_as_cutoff_links_or_cross_model_routes(version, model):
    data = credit_data(-10)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
    policy = (
        MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN) if model == "cutoff" else MigrationPolicy.permissive()
    )
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(contexts("uvek", "2025").background, policy=policy)
    expected = (
        "background.technosphere_link_unresolved"
        if model == "cutoff"
        else "migration.technosphere_cross_system_model_unavailable"
    )
    assert any(i.code == expected for i in caught.value.report.issues)
    assert not credit_rules(load_uvek_technosphere_resource())
    assert inventory.data == data == before


@pytest.mark.parametrize(
    "problem", ["role", "sign", "source_sign", "target_sign", "pending", "missing_review", "empty_evidence", "omission"]
)
def test_invalid_recovery_credit_decisions_fail_schema_validation(problem):
    resource = deepcopy(historical_forward_resource())
    entry = credit_rules(resource)[0]["reference_flow_sign_review"]["profiles"][0]
    if problem == "role":
        del entry["review"]["role_conversion"]
    elif problem == "sign":
        entry["conversion_factor"] *= -1
    elif problem in ("source_sign", "target_sign"):
        entry["review"][problem] *= -1
    elif problem == "pending":
        entry["review"]["status"] = "unresolved"
    elif problem == "missing_review":
        entry["review_ref"] = "unknown"
    elif problem == "empty_evidence":
        resource["reviews"][entry["review_ref"]]["evidence"] = []
    else:
        entry.update(operation="delete", approved_omission=True)
        del entry["conversion_factor"]
    with pytest.raises(MigrationError):
        validate_migration_resource(resource)


@pytest.mark.parametrize(
    "problem", ["quantity", "supplier", "source_reference_amount", "supplier_reference_amount", "missing"]
)
def test_recovery_quantity_or_supplier_drift_requires_new_scientific_review(problem):
    approval = next(iter(APPROVED_RECOVERY_CREDITS.values()))
    quantities = [
        (
            label,
            {
                "supplier": list(approval["credited_supplier"]),
                "source_reference_amount": -1,
                "supplier_reference_amount": 1,
                "credited_amount": -approval["quantity"],
            },
        )
        for label, _ in approval["observations"]
    ]
    if problem == "quantity":
        quantities[0][1]["credited_amount"] *= 2
    elif problem == "supplier":
        quantities[0][1]["supplier"][0] = "unapproved supplier"
    elif problem == "missing":
        quantities.pop()
    else:
        quantities[0][1][problem] *= 2
    with pytest.raises(ValueError, match="changed"):
        recovery_credit_profiles(approval, approval["observations"], quantities, ["Synthetic evidence"])


@pytest.mark.parametrize("extra", [None, "technosphere", "biosphere"])
def test_reference_collector_rejects_processing_burdens_in_a_credit_only_source(tmp_path, extra):
    source, approval = next(iter(APPROVED_RECOVERY_CREDITS.items()))
    directory = tmp_path / "credit-project/lci"
    directory.mkdir(parents=True)
    path = directory / "databases.db"
    database = "credit-test"
    suppliers = {
        "source": {**dict(zip(FIELDS, source, strict=True)), "code": "source", "database": database},
        "market": {
            **dict(zip(FIELDS, approval["credited_supplier"], strict=True)),
            "code": "market",
            "database": database,
        },
    }
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE activitydataset (data BLOB, database TEXT)")
        connection.execute(
            "CREATE TABLE exchangedataset (output_code TEXT, output_database TEXT, type TEXT, data BLOB)"
        )
        for code, activity in suppliers.items():
            connection.execute("INSERT INTO activitydataset VALUES (?,?)", (pickle.dumps(activity), database))
            production = {"type": "production", "amount": -1 if code == "source" else 1, "input": (database, code)}
            connection.execute(
                "INSERT INTO exchangedataset VALUES (?,?,?,?)", (code, database, "production", pickle.dumps(production))
            )
        credit = {"type": "technosphere", "amount": -approval["quantity"], "input": (database, "market")}
        connection.execute(
            "INSERT INTO exchangedataset VALUES (?,?,?,?)", ("source", database, "technosphere", pickle.dumps(credit))
        )
        if extra:
            exchange = {"type": extra, "amount": 1, "input": (database, "market")}
            connection.execute(
                "INSERT INTO exchangedataset VALUES (?,?,?,?)", ("source", database, extra, pickle.dumps(exchange))
            )
    before = path.read_bytes()
    provenance = [
        {
            "project": "credit-project",
            "database": database,
            "profile": ["ecoinvent", "3.12", "consequential"],
            "reference_count": 2,
        }
    ]
    if extra:
        with pytest.raises(ValueError, match="additional processing or biosphere"):
            collect_reference_profiles(tmp_path, provenance)
    else:
        records = collect_reference_profiles(tmp_path, provenance)[0]["recovery_credits"]
        assert len(records) == 1 and records[0]["credited_amount"] == -approval["quantity"]
    assert path.read_bytes() == before
