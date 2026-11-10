"""Delivered-air proxies preserve caller quantities and both original mix entries."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_heat_compressed_air import apply_heat_compressed_air_review

REVIEW = json.loads((DATA_DIR / "export/uvek_heat_compressed_air_review.json").read_text())


@pytest.fixture(scope="module")
def policy():
    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == REVIEW["rule_id"])
    fingerprint = compatibility_rule_fingerprint(
        resource,
        "replace",
        rule,
        TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    )
    return MigrationPolicy(approved_compatibility_rules=(fingerprint,), on_information_loss=PolicyAction.WARN)


@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_delivered_air_market_keeps_sign_uncertainty_and_two_complete_services(amount, uncertainty, policy):
    data = consumer(REVIEW["source"], amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", "2026").background,
        policy=policy,
    )
    assert data == before
    assert len(result.data) == 2
    edge = result.data[0]["exchanges"][1]
    old = before[0]["exchanges"][1]
    for field in ("amount", "uncertainty type", "loc", "scale", "minimum", "maximum", "negative", "formula"):
        if field in old:
            assert edge[field] == old[field]
    helper = result.data[1]
    assert helper["brightpath_conversion"]["recipe_id"] == REVIEW["recipe_ref"]
    assert helper["unit"] == "cubic meter"
    assert [e["amount"] for e in helper["exchanges"] if e["type"] == "production"] == [1]
    inputs = [e for e in helper["exchanges"] if e["type"] == "technosphere"]
    assert len(inputs) == 2
    for role, exchange in zip(REVIEW["roles"], inputs, strict=True):
        assert exchange["amount"] == role["source_market_share"] == 0.5
        assert all(exchange[k] == v for k, v in role["target"].items())
    assert not any(e["type"] == "biosphere" for e in helper["exchanges"])
    assert "xx" in str(inputs)  # Preserve the original label and its explicit quality warning.
    assert "unexplained" in str(result.last_migration_report.to_dict())


@pytest.mark.parametrize("version,model", [("3.8", "cutoff"), ("3.10", "cutoff"), ("3.9", "consequential")])
def test_other_profiles_cannot_borrow_the_inspected_mix(version, model, policy):
    data = consumer(REVIEW["source"], -2)
    before = deepcopy(data)
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model)).migrate_background(
            contexts("uvek", "2026").background,
            policy=policy,
        )
    assert data == before


@pytest.mark.parametrize("drift", [None, "rule", "recipe"])
def test_replay_idempotence_and_atomic_drift_refusals(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    if drift == "rule":
        next(r for r in resource["replace"] if r["id"] == REVIEW["rule_id"])["conversion_factor"] = 2
    elif drift == "recipe":
        resource["recipes"][REVIEW["recipe_ref"]]["inputs"][0]["amount"] = 0.9
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError, match="reference, scope, rule or helper changed"):
            apply_heat_compressed_air_review(resource)
    else:
        apply_heat_compressed_air_review(resource)
    assert resource == before
