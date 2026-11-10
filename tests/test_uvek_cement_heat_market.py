"""European cement heat uses plant-gate services, not a district-delivery proxy."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from scripts.review_uvek_cement_heat_market import REVIEW_PATH, apply_cement_heat_market_review

REVIEW = json.loads((DATA_DIR / "export/uvek_cement_european_ng_heat_review.json").read_text())
SOURCE = REVIEW["record"]["source"]
EXPECTED = next(iter(REVIEW["decision"]["recipes"].values()))["inputs"]


@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_39_heat_keeps_caller_basis_and_all_mapped_technology_quantities(amount, uncertainty):
    data = consumer(SOURCE, amount, uncertainty)
    original = deepcopy(data)
    source = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9"))
    result = source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert data == source.data == original
    edge = result.data[0]["exchanges"][1]
    for field in ("amount", "formula", "loc", "scale", "negative", "minimum", "maximum"):
        if field in original[0]["exchanges"][1]:
            assert edge[field] == original[0]["exchanges"][1][field]
    helper = next(row for row in result.data if row.get("code") == edge["input"][1])
    actual = [row for row in helper["exchanges"] if row["type"] != "production"]
    assert len(actual) == 4
    for actual_row, expected in zip(actual, EXPECTED, strict=True):
        assert all(actual_row[key] == value for key, value in expected.items())
    assert sum(row["amount"] for row in actual) == pytest.approx(REVIEW["source_quantity_sum"], abs=1e-14)
    assert not any(row["name"].startswith(("District heat,", "Natural gas,")) for row in actual)
    assert any(loss.code == "migration.technosphere_information_loss" for loss in result.last_migration_report.losses)


@pytest.mark.parametrize("version", ["3.8", "3.9.1", "3.10", "3.12"])
def test_cement_heat_does_not_inherit_uninspected_profiles(version):
    data = consumer(SOURCE, 1)
    source = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    with pytest.raises(MigrationError):
        source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert source.data == data


def minimal_resource():
    return {
        "replace": [deepcopy(REVIEW["compiled_rule"])],
        "delete": [],
        "recipes": deepcopy(REVIEW["decision"]["recipes"]),
        "reviews": {},
        "coverage": {},
    }


def test_cement_heat_review_replays_idempotently():
    resource = minimal_resource()
    apply_cement_heat_market_review(resource)
    original = deepcopy(resource)
    apply_cement_heat_market_review(resource)
    assert resource == original


@pytest.mark.parametrize("change", ["parent", "helper", "evidence"])
def test_cement_heat_drift_is_rejected_before_mutation(tmp_path, change):
    resource = minimal_resource()
    if change == "parent":
        resource["replace"][0]["confidence"] = 0.1
    elif change == "helper":
        next(iter(resource["recipes"].values()))["inputs"][0]["amount"] *= 2
    else:
        evidence = tmp_path / REVIEW_PATH
        evidence.parent.mkdir(parents=True)
        evidence.write_text("{}")
    original = deepcopy(resource)
    with pytest.raises(ValueError):
        apply_cement_heat_market_review(resource, **({"root": tmp_path} if change == "evidence" else {}))
    assert resource == original
