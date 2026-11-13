"""Sodium-sulfate production and finishing retain their separate reference bases."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_sodium_sulfate_39_review.json").read_text())
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
    if row["recipe_id"]:
        helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
        assert set(helpers) == set(REVIEW["recipes"]) | set(REVIEW["recipe_dependencies"])
        root = helpers[row["recipe_id"]]
        edges = [e for e in root["exchanges"] if e["type"] != "production"]
        trace = REVIEW["retained_market_interfaces"][row["recipe_id"]]
        assert len(edges) == len(trace) == 7
        for edge, original in zip(edges, trace, strict=True):
            assert all(edge[k] == v for k, v in original["statistics"].items())
        assert [e["amount"] for e in edges] == [
            0.333,
            1.9,
            0.930563075257174,
            0.0694369247428263,
            0.3091,
            0.0246,
            0.2088,
        ]
        assert edges[2]["name"] == "Sodium sulphate, from Mannheim process, at plant"
        assert edges[3]["name"] == "Sodium sulphate, from natural sources, at plant"
        assert all("production mix" not in e["name"] for e in edges)
        assert not any(a["brightpath_conversion"].get("zero_burden") for a in helpers.values())
    else:
        assert len(result.data) == 1
        target = row["original_rule"]["target"]
        assert all(actual[k] == v for k, v in target.items())
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


@pytest.mark.parametrize("drift", [None, "parent", "dependency_rule", "dependency_recipe", "delivery"])
def test_replay_rejects_dependency_or_delivery_drift_atomically(drift):
    from scripts.review_uvek_sodium_sulfate_39 import apply_sodium_sulfate_39_review

    r = deepcopy(load_uvek_technosphere_resource())
    if drift in ("parent", "dependency_rule"):
        rid = ROWS[0]["rule_id"] if drift == "parent" else REVIEW["source_rule_dependency_scopes"][0]["rule_id"]
        next(a for a in r["replace"] if a["id"] == rid)["confidence"] = 0.01
    elif drift == "dependency_recipe":
        r["recipes"][next(iter(REVIEW["recipe_dependencies"]))]["inputs"][0]["amount"] *= 2
    elif drift == "delivery":
        r["recipes"][ROWS[0]["recipe_id"]]["inputs"][2]["amount"] *= 2
    before = deepcopy(r)
    if drift:
        with pytest.raises(ValueError):
            apply_sodium_sulfate_39_review(r)
    else:
        apply_sodium_sulfate_39_review(r)
    assert r == before


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
def test_other_profile_entries_and_default_targets_are_unchanged(row):
    old, new = row["original_rule"], row["compiled_rule"]
    assert old["target"] == new["target"]
    for entry in old.get("reference_flow_sign_review", {}).get("profiles", []):
        if entry["source_profile"] != row["source_profile"]:
            assert entry in new["reference_flow_sign_review"]["profiles"]
