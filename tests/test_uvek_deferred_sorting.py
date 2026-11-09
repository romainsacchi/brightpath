"""A negative-reference concrete waste becomes a positive service purchase."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError

ROW = json.loads((DATA_DIR / "export/uvek_deferred_sorting_review.json").read_text())["rules"][0]


@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_sorting_changes_reference_sign_once_and_keeps_uncertainty(amount, uncertainty):
    data = consumer(ROW["source"], amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.6")).migrate_background(
        target_context("2026"), policy=MigrationPolicy.permissive()
    )
    assert data == before
    exchange = result.data[0]["exchanges"][1]
    assert len(result.data) == 1
    assert all(exchange[k] == v for k, v in ROW["target"].items())
    assert exchange["amount"] == -amount
    original = before[0]["exchanges"][1]
    assert exchange["formula"] == "(dose) * (-1.0)"
    assert exchange["scale"] == original["scale"]
    if uncertainty == 2:
        assert exchange["loc"] == original["loc"]
        assert exchange.get("negative", False) is (not original.get("negative", amount < 0))
    else:
        assert exchange["loc"] == -original["loc"]
        assert exchange["minimum"] == -original["maximum"]
        assert exchange["maximum"] == -original["minimum"]


def test_sorting_other_release_remains_refused_without_mutation():
    inventory = BrightwayInventory.from_data(consumer(ROW["source"], -1), context=contexts("ecoinvent", "3.9"))
    before = deepcopy(inventory.data)
    with pytest.raises(MigrationError):
        inventory.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert inventory.data == before
