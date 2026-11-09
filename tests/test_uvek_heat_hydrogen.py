"""Exact-profile fossil H2 proxy keeps mass/signs and does not inherit electrolysis."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BackgroundContext, BiosphereProfile, BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_heat_hydrogen import apply_heat_hydrogen_review

REVIEW = json.loads((DATA_DIR / "export/uvek_heat_hydrogen_market_review.json").read_text())


def scoped_policy():
    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == REVIEW["rule_id"])
    fp = compatibility_rule_fingerprint(
        resource,
        "replace",
        rule,
        TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    )
    return MigrationPolicy(approved_compatibility_rules=(fp,), on_information_loss=PolicyAction.WARN)


@pytest.mark.parametrize("amount", [-2, 0, 2])
def test_inspected_market_uses_fossil_smr_preserving_signed_kg_and_uncertainty(amount):
    data = consumer(REVIEW["source"], amount, uncertainty=3)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9"))
    result = inventory.migrate_background(
        BackgroundContext(TechnosphereProfile("uvek", "2026", "cutoff"), BiosphereProfile("ecoinvent", "3.10")),
        policy=scoped_policy(),
    )
    exchange = result.data[0]["exchanges"][1]
    assert all(exchange[k] == v for k, v in REVIEW["target"].items())
    for field in ("amount", "uncertainty type", "loc", "scale", "formula", "minimum", "maximum"):
        if field in before[0]["exchanges"][1]:
            assert exchange[field] == before[0]["exchanges"][1][field]
    assert data == before and inventory.data == before
    assert "electrolysis" not in exchange["name"]
    report = result.last_migration_report.to_dict()
    assert any("Refinery integration" in str(stage) for stage in report["stages"])


@pytest.mark.parametrize("version,model", [("3.8", "cutoff"), ("3.10", "cutoff"), ("3.9", "consequential")])
def test_uninspected_or_cross_model_route_does_not_borrow_scope(version, model):
    resource = load_uvek_technosphere_resource()
    if model == "cutoff":
        selected = select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", version, model))
        rule = next(r for r in selected["replace"] if r["id"] == REVIEW["rule_id"])
        assert rule["reference_flow_sign_review"]["status"] == "unresolved"
    data = consumer(REVIEW["source"], 1)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
    with pytest.raises(MigrationError):
        inventory.migrate_background(contexts("uvek", "2026").background, policy=scoped_policy())
    assert inventory.data == before


def test_generator_is_idempotent_and_rejects_changed_source_binding():
    base = {
        "replace": [
            {
                "id": REVIEW["rule_id"],
                "source": REVIEW["source"],
                "target": REVIEW["previous_target"],
                "mapping_method": "heuristic_similarity",
                "confidence": 0.3,
            }
        ],
        "reviews": {},
        "coverage": {},
        "source_profiles": [REVIEW["source_profile"]],
    }
    apply_heat_hydrogen_review(base)
    once = deepcopy(base)
    apply_heat_hydrogen_review(base)
    assert base == once
    base["replace"][0]["source"] = {**REVIEW["source"], "unit": "cubic meter"}
    with pytest.raises(ValueError, match="identity, reference or scope"):
        apply_heat_hydrogen_review(base)


def test_inverse_request_cannot_borrow_the_forward_scope():
    data = consumer(REVIEW["target"], 1)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("uvek", "2026"))
    with pytest.raises(MigrationError):
        inventory.migrate_background(contexts("ecoinvent", "3.9").background, policy=scoped_policy())
    assert inventory.data == before
