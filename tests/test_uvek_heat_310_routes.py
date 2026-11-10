"""Native route corrections preserve quantities and exact-profile refusal."""

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
from scripts.review_uvek_heat_310_routes import apply_heat_310_routes_review, compiled_route_rule

REVIEW_PATH = DATA_DIR / "export/uvek_heat_310_routes_review.json"
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
def test_native_route_correction_preserves_signed_quantity_and_uncertainty(row, amount, uncertainty, policy):
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
    # Correct the complete supplier without physical quantity rescaling.
    assert edge["amount"] == amount


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("version,model", [("3.9", "cutoff"), ("3.11", "cutoff"), ("3.10", "consequential")])
def test_permission_does_not_extend_to_other_profiles(row, version, model, policy):
    data = consumer(row["source"], 1)
    before = deepcopy(data)
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model)).migrate_background(
            contexts("uvek", "2026").background,
            policy=policy,
        )
    assert data == before


def test_review_pins_corrected_rules_and_rejected_candidates():
    assert (
        hashlib.sha256(REVIEW_PATH.read_bytes()).hexdigest()
        == "11e0a2208e869ae67dabc994377a71572ed3fb27feb05709de67db32339c4f42"
    )
    indexed = {r["id"]: r for r in load_uvek_technosphere_resource()["replace"]}
    for row in REVIEW["records"]:
        assert indexed[row["rule_id"]] == compiled_route_rule(row)
        assert row["source_reference_amount"] == row["target_reference_amount"] == row["conversion_factor"] == 1
        assert row["target"] != row["previous_rule"]["target"]


@pytest.mark.parametrize("drift", [False, True])
def test_authoring_stage_is_atomic_and_replayable(drift):
    active = load_uvek_technosphere_resource()
    resource = {**active, "replace": [deepcopy(r["previous_rule"]) for r in REVIEW["records"]]}
    apply_heat_310_routes_review(resource)
    if drift:
        resource["replace"][-1]["target"]["location"] = "unreviewed"
        resource["replace"][0] = deepcopy(REVIEW["records"][0]["previous_rule"])
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError, match="earlier rule changed"):
            apply_heat_310_routes_review(resource)
    else:
        apply_heat_310_routes_review(resource)
    assert resource == before
