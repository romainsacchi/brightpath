"""Sorted scrap keeps processing; new aluminium keeps its exact cut-off allocation."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_scrap_delivery_39_review.json").read_text())
ROWS = REVIEW["records"]
STATS = (
    "amount",
    "uncertainty type",
    "loc",
    "scale",
    "shape",
    "minimum",
    "maximum",
    "negative",
    "pedigree",
    "scale without pedigree",
    "formula",
)


def policy(row, approved=True):
    r = load_uvek_technosphere_resource()
    rule = next(x for x in r["replace"] if x["id"] == row["rule_id"])
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


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_scrap_market_preserves_all_delivery_statistics_and_caller(row, amount, uncertainty, release):
    data = consumer(row["original_rule"]["source"], amount, uncertainty)
    data[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.5,
        }
    )
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy(row)
    )
    assert data == before
    actual = result.data[0]["exchanges"][1]
    for field in STATS:
        if field in before[0]["exchanges"][1]:
            assert actual[field] == before[0]["exchanges"][1][field]
    assert result.data[0]["exchanges"][-1] == before[0]["exchanges"][-1]
    root = next(a for a in result.data[1:] if a["brightpath_conversion"]["recipe_id"] == row["recipe_id"])
    edges = [e for e in root["exchanges"] if e["type"] != "production"]
    trace = REVIEW["retained_market_interfaces"][row["recipe_id"]]
    assert len(edges) == len(trace)
    for edge, original in zip(edges, trace, strict=True):
        for field, value in original["statistics"].items():
            assert edge[field] == value
    assert all("coal freight" not in edge["name"].lower() for edge in edges)
    stocks = [a for a in result.data[1:] if a["brightpath_conversion"].get("zero_burden")]
    if "sorted-iron" in row["recipe_id"]:
        assert len(result.data) == 2 and not stocks
        assert [e["name"] for e in edges[:3]] == ["Iron scrap, at plant"] * 3
        assert all(e["location"] == "RER" for e in edges[:3])
        assert sum(e["amount"] for e in edges[:3]) == pytest.approx(1)
    else:
        assert len(result.data) == 3 and len(stocks) == 1
        stock = stocks[0]
        assert len(stock["exchanges"]) == 1 and stock["exchanges"][0]["amount"] == 1
        assert edges[0]["amount"] == 1
        assert all(edges[0][k] == stock[k] for k in ("name", "reference product", "location", "unit"))
    assert not any(
        loss.code == "migration.recipe_source_input_omission" for loss in result.last_migration_report.losses
    )


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
def test_market_needs_its_own_scope(row):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], 1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(row, False))


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
@pytest.mark.parametrize(
    "version,model", [("3.8", "cutoff"), ("3.9.1", "cutoff"), ("3.10", "cutoff"), ("3.9", "consequential")]
)
def test_new_profile_grants_do_not_cover_other_source_profiles(row, version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], 1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(row))


@pytest.mark.parametrize("drift", [None, "parent", "stock_rule", "stock_recipe", "delivery"])
def test_replay_rejects_dependency_or_delivery_drift_atomically(drift):
    from scripts.review_uvek_scrap_delivery_39 import apply_scrap_delivery_39_review

    r = deepcopy(load_uvek_technosphere_resource())
    if drift in ("parent", "stock_rule"):
        rid = ROWS[0]["rule_id"] if drift == "parent" else next(iter(REVIEW["source_rule_dependencies"]))
        next(a for a in r["replace"] if a["id"] == rid)["confidence"] = 0.01
    elif drift == "stock_recipe":
        r["recipes"][next(iter(REVIEW["recipe_dependencies"]))]["zero_burden"] = False
    elif drift == "delivery":
        r["recipes"][ROWS[1]["recipe_id"]]["inputs"][2]["amount"] *= 2
    before = deepcopy(r)
    if drift:
        with pytest.raises(ValueError):
            apply_scrap_delivery_39_review(r)
    else:
        apply_scrap_delivery_39_review(r)
    assert r == before


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
def test_other_profile_entries_and_default_targets_are_unchanged(row):
    old, new = row["original_rule"], row["compiled_rule"]
    assert old["target"] == new["target"]
    for entry in old.get("reference_flow_sign_review", {}).get("profiles", []):
        if entry["source_profile"] != row["source_profile"]:
            assert entry in new["reference_flow_sign_review"]["profiles"]
