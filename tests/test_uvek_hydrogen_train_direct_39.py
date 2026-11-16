"""The required rail suppliers keep signed quantities and their declared units."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_hydrogen_train_direct_39 import apply_hydrogen_train_direct_39_review


@pytest.mark.parametrize("amount", [-2, 2])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_direct_train_suppliers_keep_reference_quantities_and_uncertainty(amount, release):
    review = json.loads((DATA_DIR / "export/uvek_hydrogen_train_direct_39_review.json").read_text())
    resource = load_uvek_technosphere_resource()
    by_id = {r["id"]: r for r in resource["replace"]}
    source = TechnosphereProfile("ecoinvent", "3.9", "cutoff")
    bridge = TechnosphereProfile("uvek", "2025", "cutoff")
    policy = MigrationPolicy(
        approved_compatibility_rules=tuple(
            compatibility_rule_fingerprint(resource, "replace", by_id[row["rule_id"]], source, bridge)
            for row in review["rules"]
        ),
        on_information_loss=PolicyAction.WARN,
    )
    data = consumer(review["rules"][0]["source"], amount, 3)
    data[0]["exchanges"].extend(consumer(row["source"], amount, 3)[0]["exchanges"][1] for row in review["rules"][1:])
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9"))
    converted = inventory.migrate_background(contexts("uvek", release).background, policy=policy)
    assert data == inventory.data == original
    assert len(converted.data) == 1
    for row, edge in zip(review["rules"], converted.data[0]["exchanges"][1:], strict=True):
        assert all(edge[k] == v for k, v in row["target"].items())
        assert edge["amount"] == edge["loc"] == amount
        assert edge["scale"] == 0.2 and edge["formula"] == "dose"


def test_review_replay_is_atomic_idempotent_and_keeps_eleven_rules_unchanged():
    resource = deepcopy(load_uvek_technosphere_resource())
    before = deepcopy(resource)
    apply_hydrogen_train_direct_39_review(resource)
    assert resource == before
    review = json.loads((DATA_DIR / "export/uvek_hydrogen_train_direct_39_review.json").read_text())
    assert sum(row["original_rule"] != row["compiled_rule"] for row in review["rules"]) == 1
    parent = next(r for r in resource["replace"] if r["id"] == review["rules"][-1]["rule_id"])
    parent["confidence"] = 0.123
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="parent changed"):
        apply_hydrogen_train_direct_39_review(resource)
    assert resource == before
