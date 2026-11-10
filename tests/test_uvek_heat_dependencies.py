"""Qualified heat dependencies preserve product quantities and treatment coordinates."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_heat_dependencies import apply_heat_dependency_review

REVIEW = json.loads((DATA_DIR / "export/uvek_heat_dependency_batch_review.json").read_text())


@pytest.fixture(scope="module")
def scoped_policy():
    resource = load_uvek_technosphere_resource()
    indexed = {r["id"]: r for r in resource["replace"]}
    source = TechnosphereProfile("ecoinvent", "3.9", "cutoff")
    target = TechnosphereProfile("uvek", "2025", "cutoff")
    fingerprints = tuple(
        compatibility_rule_fingerprint(resource, "replace", indexed[r["rule_id"]], source, target)
        for r in REVIEW["records"]
    )
    return MigrationPolicy(approved_compatibility_rules=fingerprints, on_information_loss=PolicyAction.WARN)


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["source"]["name"])
@pytest.mark.parametrize("amount,uncertainty", [(-2, 2), (2, 2), (-2, 3), (2, 3), (0, 3)])
def test_quantities_uncertainty_and_reference_supply_coordinate(row, amount, uncertainty, scoped_policy):
    data = consumer(row["source"], amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", "2026").background, policy=scoped_policy
    )
    edge = result.data[0]["exchanges"][1]
    factor = row["conversion_factor"]
    assert edge["amount"] == amount * factor
    assert edge["amount"] / row["target_reference_amount"] == amount / row["source_reference_amount"]
    assert all(edge[k] == v for k, v in row["target"].items())
    old = before[0]["exchanges"][1]
    assert edge["scale"] == old["scale"]
    if factor == 1:
        for field in ("amount", "uncertainty type", "loc", "scale", "minimum", "maximum", "negative", "formula"):
            if field in old:
                assert edge[field] == old[field]
    else:
        assert row["previous_rule"]["conversion_factor"] == -1  # Already corrected before this review.
        assert edge["formula"] == "(dose) * (-1.0)"
        if uncertainty == 2:
            assert edge["loc"] == old["loc"]
            assert edge["negative"] is (amount >= 0)
        else:
            assert edge["loc"] == -old["loc"]
            assert edge["minimum"] == -old["maximum"]
            assert edge["maximum"] == -old["minimum"]
    assert data == before
    assert any("screening" in str(s).lower() for s in result.last_migration_report.to_dict()["stages"])


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
def test_uninspected_release_cannot_borrow_exact_scope(row, scoped_policy):
    resource = load_uvek_technosphere_resource()
    projected = select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", "3.10", "cutoff"))
    rule = next(r for r in projected["replace"] if r["id"] == row["rule_id"])
    assert rule["reference_flow_sign_review"]["status"] == "unresolved"
    inventory = BrightwayInventory.from_data(consumer(row["source"], -2), context=contexts("ecoinvent", "3.10"))
    before = inventory.data
    with pytest.raises(MigrationError):
        inventory.migrate_background(contexts("uvek", "2026").background, policy=scoped_policy)
    assert inventory.data == before


def test_batch_replay_is_idempotent_and_failed_validation_is_atomic():
    resource = {
        "replace": [deepcopy(row["previous_rule"]) for row in REVIEW["records"]],
        "source_profiles": [REVIEW["records"][0]["source_profile"]],
        "reviews": {},
        "coverage": {},
    }
    apply_heat_dependency_review(resource)
    first = deepcopy(resource)
    apply_heat_dependency_review(resource)
    assert resource == first
    resource["replace"][-1]["target"]["location"] = "unreviewed"
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="identity, reference coordinate or exact scope"):
        apply_heat_dependency_review(resource)
    assert resource == before


def test_cutoff_approval_does_not_authorize_consequential(scoped_policy):
    row = REVIEW["records"][0]
    inventory = BrightwayInventory.from_data(
        consumer(row["source"], 1), context=contexts("ecoinvent", "3.9", "consequential")
    )
    before = inventory.data
    with pytest.raises(MigrationError):
        inventory.migrate_background(contexts("uvek", "2026").background, policy=scoped_policy)
    assert inventory.data == before
