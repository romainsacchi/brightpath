"""Complete heat infrastructure proxies preserve physical caller references."""

import hashlib
import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW_PATH = DATA_DIR / "export/uvek_heat_310_infrastructure_review.json"
REVIEW = json.loads(REVIEW_PATH.read_text())


@pytest.fixture(scope="module")
def policy():
    resource = load_uvek_technosphere_resource()
    indexed = {r["id"]: r for r in resource["replace"]}
    scopes = tuple(
        compatibility_rule_fingerprint(
            resource,
            "replace",
            indexed[row["rule_id"]],
            TechnosphereProfile("ecoinvent", "3.10", "cutoff"),
            TechnosphereProfile("uvek", "2025", "cutoff"),
        )
        for row in REVIEW["records"]
    )
    return MigrationPolicy(approved_compatibility_rules=scopes, on_information_loss=PolicyAction.WARN)


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("amount,uncertainty", [(-2, 2), (2, 2), (-2, 3), (2, 3)])
def test_complete_infrastructure_preserves_signed_quantity_and_uncertainty(row, amount, uncertainty, policy):
    data = consumer(row["source"], amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.10")).migrate_background(
        contexts("uvek", "2026").background,
        policy=policy,
    )
    assert data == before
    assert len(result.data) == 1
    assert len(result.data[0]["exchanges"]) == 2
    edge = result.data[0]["exchanges"][1]
    assert all(edge[k] == v for k, v in row["target"].items())
    original = before[0]["exchanges"][1]
    assert all(edge[k] == v for k, v in original.items() if k not in row["source"])
    # A physical metre is not annualized using the differing prose lifetimes.
    assert edge["amount"] == amount


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("version", ["3.9", "3.11"])
def test_permission_does_not_extend_to_other_versions(row, version, policy):
    data = consumer(row["source"], 1)
    before = deepcopy(data)
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", version)).migrate_background(
            contexts("uvek", "2026").background,
            policy=policy,
        )
    assert data == before


def test_review_is_pinned_and_does_not_change_underlying_rules():
    assert (
        hashlib.sha256(REVIEW_PATH.read_bytes()).hexdigest()
        == "87aa35eb7d9e7acc11787c1df4e3b76f1cd77b5ad324699a8720bba24679859f"
    )
    indexed = {r["id"]: r for r in load_uvek_technosphere_resource()["replace"]}
    for row in REVIEW["records"]:
        assert indexed[row["rule_id"]] == row["previous_rule"]
        assert row["source_reference_amount"] == row["target_reference_amount"] == row["conversion_factor"] == 1
        assert len(row["source_producer_witnesses"]) == 2
