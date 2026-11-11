"""Lime delivery keeps separate grinding, allocated mining and freight burdens."""

import json
from copy import deepcopy
from dataclasses import replace

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_fuels_38_lime import apply_fuels_38_lime_review

REVIEW = json.loads((DATA_DIR / "export/uvek_fuels_38_lime_review.json").read_text())


def migrate(amount=1, uncertainty=3, version="3.8", model="cutoff"):
    source = BrightwayInventory.from_data(
        consumer(REVIEW["original_rule"]["source"], amount, uncertainty),
        context=contexts("ecoinvent", version, model),
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
    delivery = next(a for a in result.data if a.get("code") == edge["input"][1])
    return result, edge, delivery


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_caller_mass_uncertainty_and_formula(amount, uncertainty):
    _, edge, _ = migrate(amount, uncertainty)
    assert edge["amount"] == amount and edge["formula"] == "dose"
    assert edge["uncertainty type"] == uncertainty and edge["scale"] == 0.2
    if uncertainty == 2:
        assert edge["negative"] is (amount < 0)
    else:
        assert edge["loc"] == amount


def test_three_separate_producer_shares_and_three_freight_rows_survive():
    result, _, delivery = migrate()
    rows = [e for e in delivery["exchanges"] if e["type"] != "production"]
    assert len(rows) == 6
    for actual, retained in zip(rows, REVIEW["retained_rows"], strict=True):
        for field, value in retained["statistics"].items():
            assert actual[field] == value
    grinding = [e for e in rows if e["name"] == "Limestone, milled, loose, at plant"]
    assert len(grinding) == 2
    assert len([e for e in rows if e["unit"] == "ton kilometer"]) == 3
    mining_edge = next(e for e in rows if e["unit"] == "kilogram" and e not in grinding)
    mining = next(
        a
        for a in result.data
        if all(a.get(k) == mining_edge.get(k) for k in ("name", "reference product", "location", "unit"))
    )
    burden = [e for e in mining["exchanges"] if e["type"] != "production"]
    assert len(burden) == 1
    assert burden[0]["name"] == "Zinc, from combined metal production, at beneficiation"
    assert burden[0]["amount"] == 0.59 * (0.117 / 0.38)
    assert all(e["amount"] > 0 for e in rows + burden)
    assert not delivery["brightpath_conversion"].get("source_input_omissions")


@pytest.mark.parametrize("version,model", [("3.9", "cutoff"), ("3.8", "consequential")])
def test_uninspected_profiles_refuse(version, model):
    with pytest.raises(MigrationError):
        migrate(version=version, model=model)


def test_replay_is_idempotent_and_rejects_changed_parent_without_mutation():
    resource = deepcopy(load_uvek_technosphere_resource())
    apply_fuels_38_lime_review(resource)
    before = deepcopy(resource)
    apply_fuels_38_lime_review(resource)
    assert resource == before
    next(r for r in resource["replace"] if r["id"] == REVIEW["rule_id"])["conversion_factor"] = 2
    changed = deepcopy(resource)
    with pytest.raises(ValueError, match="scope"):
        apply_fuels_38_lime_review(resource)
    assert resource == changed
