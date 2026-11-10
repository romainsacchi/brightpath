"""Cement preparation retains real treatment burdens and exact profile approvals."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from scripts.review_uvek_cement_treatment_39 import REVIEW_PATH, apply_cement_treatment_39_review

REVIEW = json.loads((DATA_DIR / "export/uvek_cement_treatment_39_review.json").read_text())


@pytest.mark.parametrize("label", ["polyethylene", "concrete"])
@pytest.mark.parametrize("amount", [-2, 0, 2])
def test_inspected_treatment_reference_sign_and_real_service(label, amount):
    record = REVIEW[label]
    data = consumer(record["original_rule"]["source"], amount, 3)
    before = deepcopy(data)
    source = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9"))
    result = source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert data == source.data == before
    edge = result.data[0]["exchanges"][1]
    assert edge["amount"] == -amount
    assert edge["scale"] == before[0]["exchanges"][1]["scale"]
    if label == "polyethylene":
        assert all(edge[k] == v for k, v in record["compiled_rule"]["target"].items())
    else:
        helper = next(a for a in result.data if a.get("code") == edge["input"][1])
        inputs = [e for e in helper["exchanges"] if e["type"] != "production"]
        assert len(inputs) == 2
        for actual, expected in zip(inputs, record["recipe"]["inputs"], strict=True):
            assert all(actual[k] == v for k, v in expected.items())
        assert {e["unit"] for e in inputs} == {"unit", "kilogram"}
        assert all(e["uncertainty type"] == 2 and e["scale"] > 0 for e in inputs)
        assert not any("Disposal, concrete" in e["name"] for e in inputs)
    assert any(loss.code == "migration.technosphere_information_loss" for loss in result.last_migration_report.losses)


@pytest.mark.parametrize("label", ["polyethylene", "concrete"])
@pytest.mark.parametrize("version", ["3.9.1", "3.10"])
def test_uninspected_treatment_profiles_remain_refused(label, version):
    data = consumer(REVIEW[label]["original_rule"]["source"], -1)
    source = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    with pytest.raises(MigrationError):
        source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert source.data == data


def minimal_resource():
    return {
        "replace": [deepcopy(REVIEW[label]["original_rule"]) for label in ("polyethylene", "concrete")],
        "delete": [],
        "recipes": {},
        "reviews": {},
        "coverage": {},
    }


def test_replay_is_idempotent_and_preserves_original_polyethylene_profile():
    resource = minimal_resource()
    apply_cement_treatment_39_review(resource)
    before = deepcopy(resource)
    apply_cement_treatment_39_review(resource)
    assert resource == before
    pe = resource["replace"][0]
    original = REVIEW["polyethylene"]["original_rule"]
    assert pe["target"] == original["target"]
    assert pe["conversion_factor"] == original["conversion_factor"]
    assert pe["reference_flow_sign_review"]["profiles"][:-1] == original["reference_flow_sign_review"]["profiles"]


@pytest.mark.parametrize("change", ["parent", "helper", "evidence"])
def test_review_drift_is_refused_before_any_mutation(tmp_path, change):
    resource = minimal_resource()
    if change == "parent":
        resource["replace"][1]["confidence"] = 0
    elif change == "helper":
        concrete = REVIEW["concrete"]
        resource["recipes"][concrete["recipe_id"]] = deepcopy(concrete["recipe"])
        resource["recipes"][concrete["recipe_id"]]["inputs"][0]["amount"] *= 2
    else:
        path = tmp_path / REVIEW_PATH
        path.parent.mkdir(parents=True)
        path.write_text("{}")
    before = deepcopy(resource)
    with pytest.raises(ValueError):
        apply_cement_treatment_39_review(resource, **({"root": tmp_path} if change == "evidence" else {}))
    assert resource == before
