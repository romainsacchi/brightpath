"""Unsorted scrap retains the observed allocation boundary and collection burden."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_unsorted_iron_scrap_39_review.json").read_text())
ROW = REVIEW["records"][0]


def policy(approved=True):
    r = load_uvek_technosphere_resource()
    rule = next(x for x in r["replace"] if x["id"] == ROW["rule_id"])
    fp = compatibility_rule_fingerprint(
        r,
        "replace",
        rule,
        TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    )
    return MigrationPolicy(
        approved_compatibility_rules=(fp,) if approved else (), on_information_loss=PolicyAction.WARN
    )


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_market_preserves_stock_transport_uncertainty_and_caller(amount, uncertainty, release):
    data = consumer(ROW["original_rule"]["source"], amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == before and len(result.data) == 3
    actual = result.data[0]["exchanges"][1]
    for k in ("amount", "uncertainty type", "loc", "scale", "negative", "formula", "minimum", "maximum"):
        if k in before[0]["exchanges"][1]:
            assert actual[k] == before[0]["exchanges"][1][k]
    market = next(a for a in result.data[1:] if a["brightpath_conversion"]["recipe_id"] == ROW["recipe_id"])
    stock = next(a for a in result.data[1:] if a["brightpath_conversion"].get("zero_burden"))
    assert len(stock["exchanges"]) == 1 and stock["exchanges"][0]["type"] == "production"
    edges = [e for e in market["exchanges"] if e["type"] != "production"]
    assert [e["amount"] for e in edges] == [1, 0.0112, 0.01932]
    assert all(edges[0][key] == stock[key] for key in ("name", "reference product", "location", "unit"))
    assert [e["name"] for e in edges[1:]] == ["Transport, freight, rail", "Transport, freight, lorry, fleet average"]
    assert all("Iron scrap, at plant" != e["name"] for a in result.data for e in a["exchanges"])


def test_market_requires_its_own_approval():
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROW["original_rule"]["source"], 1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(False))


@pytest.mark.parametrize(
    "version,model", [("3.8", "cutoff"), ("3.9.1", "cutoff"), ("3.10", "cutoff"), ("3.9", "consequential")]
)
def test_stock_approval_does_not_expand_market_profile(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROW["original_rule"]["source"], 1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


@pytest.mark.parametrize("drift", [None, "parent", "stock_rule", "stock_recipe"])
def test_frozen_replay_is_atomic_and_checks_existing_stock(drift):
    from scripts.review_uvek_unsorted_iron_scrap_39 import apply_unsorted_iron_scrap_39_review

    r = deepcopy(load_uvek_technosphere_resource())
    if drift in ("parent", "stock_rule"):
        rid = ROW["rule_id"] if drift == "parent" else next(iter(REVIEW["source_rule_dependencies"]))
        next(x for x in r["replace"] if x["id"] == rid)["confidence"] = 0.01
    elif drift == "stock_recipe":
        r["recipes"][next(iter(REVIEW["recipe_dependencies"]))]["zero_burden"] = False
    before = deepcopy(r)
    if drift:
        with pytest.raises(ValueError):
            apply_unsorted_iron_scrap_39_review(r)
    else:
        apply_unsorted_iron_scrap_39_review(r)
    assert r == before
