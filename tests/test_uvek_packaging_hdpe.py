"""HDPE profiles keep delivery and distinguish audited 3.8 losses from intact 3.11."""

import json
from copy import deepcopy
from dataclasses import replace

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_packaging_hdpe import apply_packaging_hdpe_review, digest

REVIEW = json.loads((DATA_DIR / "export/uvek_packaging_hdpe_review.json").read_text())
ROWS = {row["source_profile"]["version"]: row for row in REVIEW["records"]}


def migrate(version, amount=1, uncertainty=3):
    source = BrightwayInventory.from_data(
        consumer(REVIEW["original_rule"]["source"], amount, uncertainty),
        context=contexts("ecoinvent", version, "cutoff"),
    )
    before = deepcopy(source.data)
    policy = replace(
        MigrationPolicy.permissive(),
        on_invalid_source=PolicyAction.ERROR,
        on_invalid_target=PolicyAction.ERROR,
        on_unresolved_link=PolicyAction.ERROR,
        minimum_coverage=1,
    )
    result = source.migrate_background(target_context("2026"), policy=policy)
    assert source.data == before
    edge = result.data[0]["exchanges"][1]
    helper = next(ds for ds in result.data if ds.get("code") == edge["input"][1])
    return result, edge, helper


@pytest.mark.parametrize("version", ROWS)
@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_signed_caller_stock_quantity_uncertainty_and_formula(version, amount, uncertainty):
    _, edge, _ = migrate(version, amount, uncertainty)
    assert edge["amount"] == amount and edge["formula"] == "dose"
    assert edge["uncertainty type"] == uncertainty and edge["scale"] == 0.2
    if uncertainty == 2:
        assert edge["negative"] is (amount < 0)
    else:
        assert edge["loc"] == amount


@pytest.mark.parametrize("version", ROWS)
def test_both_producers_and_all_freight_are_separate_without_renormalization(version):
    _, _, helper = migrate(version)
    inputs = [e for e in helper["exchanges"] if e["type"] != "production"]
    expected = REVIEW["recipes"][ROWS[version]["new_recipe_id"]]["inputs"]
    assert len(inputs) == 5
    producers = [e for e in inputs if e["unit"] == "kilogram"]
    assert [e["amount"] for e in producers] == [e["amount"] for e in expected if e["unit"] == "kilogram"]
    assert sorted(e["amount"] for e in producers) == [0.190635451504994, 0.809364548494888]
    assert all(e["name"] == "Polyethylene, HDPE, granulate, at plant" for e in producers)
    assert len([e for e in inputs if e["unit"] == "ton kilometer"]) == 3
    for actual, reference in zip(inputs, expected, strict=True):
        for field in (
            "amount",
            "uncertainty type",
            "loc",
            "scale",
            "shape",
            "negative",
            "minimum",
            "maximum",
            "formula",
        ):
            if field in reference:
                assert actual[field] == reference[field]


def test_only_exact_38_recycled_entries_have_omission_audits():
    result, _, helper = migrate("3.8")
    losses = [
        loss for loss in result.last_migration_report.losses if loss.code == "migration.recipe_source_input_omission"
    ]
    assert len(losses) == 4
    omissions = helper["brightpath_conversion"]["source_input_omissions"]
    assert omissions == ROWS["3.8"]["source_omissions"]
    assert sum(o["source_exchange"]["amount"] for o in omissions) == pytest.approx(1.173141240030732e-13)
    assert all(o["source_profile"] == ROWS["3.8"]["source_profile"] for o in omissions)
    assert all(o["source_exchange"]["amount"] > 0 for o in omissions)


def test_311_has_no_source_omission_despite_inherited_historical_39_description():
    result, _, helper = migrate("3.11")
    assert not ROWS["3.11"]["source_omissions"]
    assert not helper.get("brightpath_conversion", {}).get("source_input_omissions")
    assert not any(
        loss.code == "migration.recipe_source_input_omission" for loss in result.last_migration_report.losses
    )


def test_old_selected_rules_shared_evidence_and_helper_closures_are_unchanged():
    current = load_uvek_technosphere_resource()
    for proof in REVIEW["preserved_profiles"]:
        selected = next(
            r
            for r in select_profile_sign_rules(current, TechnosphereProfile(**proof["source_profile"]))["replace"]
            if r["id"] == REVIEW["rule_id"]
        )
        assert digest(selected) == proof["selected_rule_sha256"]
        assert digest(current["reviews"][selected["review_ref"]]) == proof["shared_review_sha256"]
    assert all(digest(current["recipes"][rid]) == expected for rid, expected in REVIEW["recipe_dependencies"].items())


def test_replay_is_idempotent_and_rejects_parent_drift():
    resource = deepcopy(load_uvek_technosphere_resource())
    apply_packaging_hdpe_review(resource)
    before = deepcopy(resource)
    apply_packaging_hdpe_review(resource)
    assert resource == before
    next(r for r in resource["replace"] if r["id"] == REVIEW["rule_id"])["confidence"] = 0
    with pytest.raises(ValueError, match="parent"):
        apply_packaging_hdpe_review(resource)
