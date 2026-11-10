"""Inspected material proxies restore caller quantities without widening profiles."""

import hashlib
import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
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
    inventory = BrightwayInventory.from_data(consumer(row["source"], 1), context=contexts("ecoinvent", "3.11"))
    before = deepcopy(inventory.data)
    with pytest.raises(MigrationError):
        inventory.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert inventory.data == before


def deferred_materials_authoring_stage_resource():
    """Restore the earlier epoxy stage; keep the later concrete extension visible."""
    resource = deepcopy(load_uvek_technosphere_resource())
    path = DATA_DIR / "export/uvek_heat_profile_extension_review.json"
    assert (
        hashlib.sha256(path.read_bytes()).hexdigest()
        == "ceb5b782aa15161a5680d1901cffa0851e0b384ead67dcdc4deae1f183231b6b"
    )
    row = next(r for r in json.loads(path.read_text())["records"] if r["rule_id"] == "bp-08f2432b62895931")
    current = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
    from brightpath import TechnosphereProfile
    from brightpath.background.reference_signs import select_profile_sign_rules

    profile = TechnosphereProfile("ecoinvent", "3.12", "cutoff")
    assert (
        select_profile_sign_rules({"replace": [current]}, profile)["replace"]
        == select_profile_sign_rules({"replace": [row["previous_rule"]]}, profile)["replace"]
    )
    current.clear()
    current.update(deepcopy(row["previous_rule"]))
    return resource


@pytest.mark.parametrize("drift", [False, True])
def test_batch_replay_is_idempotent_and_rejects_late_drift_atomically(drift):
    resource = deferred_materials_authoring_stage_resource()
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
