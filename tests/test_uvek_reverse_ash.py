"""Independent ash-treatment signs, preserved uncertainty and reversible service pairs."""

from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BrightwayInventory, FormatProfile, InventoryContext
from brightpath.background.uvek_export import load_uvek_export_resource
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from scripts.review_uvek_reverse_ash import (
    APPROVAL,
    METHOD,
    apply_reverse_ash,
    identity,
    legacy_ash_projection,
    validate_reverse_ash_inventories,
)

POLICY = MigrationPolicy.permissive()


def batch(amount, field="source"):
    data = None
    for row in APPROVAL["rules"]:
        for kind, uncertainty in (("technosphere", 2), ("substitution", 3)):
            current = consumer(row[field], amount, uncertainty, kind)
            current[0]["exchanges"][1]["caller_metadata"] = {"keep": [1, 2]}
            if data is None:
                data = current
            else:
                data[0]["exchanges"].append(current[0]["exchanges"][1])
    data[0]["comment"] = "Synthetic same-region dry-ash treatment fixture."
    return data


@pytest.mark.parametrize("kind", ["2025", "2026", "native2026"])
@pytest.mark.parametrize("amount", [-10, 0, 10])
def test_all_23_ash_services_reverse_sign_without_losing_uncertainty_or_original_changes(kind, amount):
    data = batch(amount)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(
        data, context=InventoryContext(FormatProfile("brightway_excel"), target_context(kind))
    )
    result = inventory.migrate_background(contexts("ecoinvent", "3.12").background, policy=POLICY)
    assert len(result.data) == 1
    changes = {c.path: c for c in result.last_migration_report.changes if c.code == "migration.uvek_supplier"}
    assert len(changes) == 46
    for index, (old, new) in enumerate(
        zip(before[0]["exchanges"][1:], result.data[0]["exchanges"][1:], strict=True), 1
    ):
        row = APPROVAL["rules"][(index - 1) // 2]
        assert identity(new) == identity(row["target"])
        assert (
            new["amount"] == -amount and new["type"] == old["type"] and new["caller_metadata"] == old["caller_metadata"]
        )
        assert new["formula"] == "(dose) * (-1.0)" and new["scale"] == old["scale"]
        if old["uncertainty type"] == 2:
            assert new["loc"] == old["loc"] and new["negative"] is (amount >= 0)
        else:
            assert new["loc"] == -amount and new["minimum"] == -amount - 1 and new["maximum"] == -amount + 1
        recorded = changes[f"datasets[0].exchanges[{index}]"].to_dict()["before"]
        assert {k: recorded[k] for k in old} == old
    warnings = [i for i in result.last_migration_report.issues if i.code == "migration.documented_proxy"]
    assert len(warnings) == 46 and all("without taking an absolute value" in i.message for i in warnings)
    assert inventory.data == data == before


@pytest.mark.parametrize("kind", ["2025", "native2026"])
@pytest.mark.parametrize("amount", [-10, 0, 10])
def test_independently_matched_ash_services_round_trip_from_negative_ecoinvent_references(kind, amount):
    data = batch(amount, "target")
    forward = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context(kind), policy=POLICY
    )
    back = forward.migrate_background(contexts("ecoinvent", "3.12").background, policy=POLICY)
    assert len(back.data) == 1
    for old, new in zip(data[0]["exchanges"][1:], back.data[0]["exchanges"][1:], strict=True):
        assert identity(new) == identity(old)
        for field in (
            "amount",
            "type",
            "uncertainty type",
            "loc",
            "scale",
            "minimum",
            "maximum",
            "negative",
            "caller_metadata",
        ):
            assert new.get(field) == old.get(field)
        assert new["formula"] == "((dose) * (-1.0)) * (-1.0)"


def test_reverse_ash_policy_foreground_protection_and_existing_baseline():
    data = batch(-1)
    inventory = BrightwayInventory.from_data(data, context=contexts("uvek", "2025"))
    with pytest.raises(MigrationError):
        inventory.migrate_background(contexts("ecoinvent", "3.12").background)
    assert inventory.data == data
    for row in APPROVAL["rules"]:
        s = row["source"]
        data.append({**s, "exchanges": [{**s, "type": "production", "amount": 1}]})
    result = BrightwayInventory.from_data(data, context=contexts("uvek", "2025")).migrate_background(
        contexts("ecoinvent", "3.12").background
    )
    assert result.data == data
    resource = load_uvek_export_resource()
    assert len(resource["replace"]) - len(legacy_ash_projection(resource)["replace"]) == APPROVAL["source_count"] == 23


@pytest.mark.parametrize("change", ["source_hash", "target_hash", "profile"])
def test_reverse_ash_changed_reference_inventory_requires_a_new_review(change):
    inventories = {
        identity(r["source"]): [("uvek-2025-cutoff", r["source_inventory_sha256"])] for r in APPROVAL["rules"]
    }
    inventories.update(
        {identity(r["target"]): [("ecoinvent-3.12-cutoff", r["target_inventory_sha256"])] for r in APPROVAL["rules"]}
    )
    row = APPROVAL["rules"][0]
    key = identity(row["source"] if change != "target_hash" else row["target"])
    label, digest = inventories[key][0]
    inventories[key] = [
        ("uvek-2026-cutoff" if change == "profile" else label, digest if change == "profile" else "0" * 64)
    ]
    with pytest.raises(ValueError, match="repeat the independent review"):
        validate_reverse_ash_inventories(inventories)


@pytest.mark.parametrize("change", ["factor", "target", "missing", "review", "outside_scope"])
def test_reverse_ash_partial_or_changed_batches_cannot_bypass_the_publisher_fingerprint(change):
    resource = deepcopy(load_uvek_export_resource())
    rule = next(r for r in resource["replace"] if r.get("mapping_method") == METHOD)
    if change == "factor":
        rule["conversion_factor"] = 1
    elif change == "target":
        rule["target"]["location"] = "Unreviewed"
    elif change == "missing":
        resource["replace"].remove(rule)
    elif change == "review":
        resource["reviews"][rule["review_ref"]]["rationale"] = "Unreviewed"
    else:
        rule["source"]["name"] = "Unreviewed"
    with pytest.raises(ValueError):
        legacy_ash_projection(resource)


def test_reverse_ash_missing_catalog_endpoint_is_not_a_similarity_fallback():
    resource = legacy_ash_projection(load_uvek_export_resource())
    sources = {identity(r["source"]) for r in APPROVAL["rules"]}
    targets = {identity(r["target"]) for r in APPROVAL["rules"]}
    with pytest.raises(ValueError, match="missing from the exact catalog"):
        apply_reverse_ash(deepcopy(resource), [], sources, targets - {identity(APPROVAL["rules"][0]["target"])})
