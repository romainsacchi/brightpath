"""Inspected material proxies restore caller quantities without widening profiles."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context
from uvek_review_helpers import compiled_authoring_stage_resource

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from scripts.review_uvek_deferred_materials import apply_deferred_materials_review

REVIEW = json.loads((DATA_DIR / "export/uvek_deferred_materials_review.json").read_text())
CASES = [(r, p["source_profile"]["version"]) for r in REVIEW["rules"] for p in r["profiles"]]


@pytest.mark.parametrize("row,version", CASES, ids=[r["rule_id"] + "-" + v for r, v in CASES])
@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_restored_material_keeps_signed_quantity_and_uncertainty(row, version, amount, uncertainty):
    data = consumer(row["source"], amount, uncertainty)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = inventory.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert inventory.data == data == before
    assert len(result.data) == 1
    exchange = result.data[0]["exchanges"][1]
    assert all(exchange[k] == v for k, v in row["target"].items())
    assert all(exchange[k] == v for k, v in before[0]["exchanges"][1].items() if k not in row["source"])
    assert "unquantified" in row["limitations"]
    assert not any(i.code.endswith("omission") for i in result.last_migration_report.issues)


@pytest.mark.parametrize("row", REVIEW["rules"], ids=lambda r: r["rule_id"])
def test_other_source_releases_do_not_inherit_review(row):
    # Nickel 3.11 has its own later review; 3.10 remains unapproved.
    version = "3.10" if row["rule_id"] == "bp-1104956dff1c701e" else "3.11"
    inventory = BrightwayInventory.from_data(consumer(row["source"], 1), context=contexts("ecoinvent", version))
    before = deepcopy(inventory.data)
    with pytest.raises(MigrationError):
        inventory.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert inventory.data == before


@pytest.mark.parametrize("drift", [False, True])
def test_batch_replay_is_idempotent_and_rejects_late_drift_atomically(drift):
    resource = compiled_authoring_stage_resource("review_uvek_deferred_materials", "apply_deferred_materials_review")
    if drift:
        rid = REVIEW["rules"][-1]["rule_id"]
        next(r for r in resource["replace"] if r["id"] == rid)["confidence"] = 0.123
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_deferred_materials_review(resource)
    else:
        apply_deferred_materials_review(resource)
    assert resource == before
