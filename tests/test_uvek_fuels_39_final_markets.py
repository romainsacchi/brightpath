"""Active chemical references and allocated silica preserve market delivery."""

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
from scripts.review_uvek_fuels_39_final_markets import apply_fuels_39_final_markets_review

REVIEW = json.loads((DATA_DIR / "export/uvek_fuels_39_final_markets_review.json").read_text())
RECORDS = {r["rule_id"]: r for r in REVIEW["records"]}


def migrate(rid, amount=1, uncertainty=3, version="3.9", model="cutoff", strict=False):
    source = BrightwayInventory.from_data(
        consumer(RECORDS[rid]["original_rule"]["source"], amount, uncertainty),
        context=contexts("ecoinvent", version, model),
    )
    before = deepcopy(source.data)
    policy = MigrationPolicy.permissive()
    if strict:
        policy = replace(
            policy,
            on_invalid_source=PolicyAction.ERROR,
            on_invalid_target=PolicyAction.ERROR,
            on_unresolved_link=PolicyAction.ERROR,
            minimum_coverage=1.0,
        )
    result = source.migrate_background(target_context("2026"), policy=policy)
    assert source.data == before
    return result


def helper(result):
    edge = result.data[0]["exchanges"][1]
    return next(a for a in result.data if a.get("code") == edge["input"][1])


@pytest.mark.parametrize("rid", RECORDS)
@pytest.mark.parametrize("amount", [-2, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_caller_stock_mass_uncertainty_and_formula_survive(rid, amount, uncertainty):
    result = migrate(rid, amount, uncertainty)
    edge = result.data[0]["exchanges"][1]
    assert edge["amount"] == amount and edge["uncertainty type"] == uncertainty
    assert edge["scale"] == 0.2 and edge["formula"] == "dose"
    if uncertainty == 2:
        assert edge["negative"] is (amount < 0)
    else:
        assert edge["loc"] == amount
    assert any(loss.code == "migration.technosphere_information_loss" for loss in result.last_migration_report.losses)


@pytest.mark.parametrize("rid", RECORDS)
@pytest.mark.parametrize("version,model", [("3.10", "cutoff"), ("3.9", "consequential")])
def test_unreviewed_profiles_are_not_inferred(rid, version, model):
    with pytest.raises(MigrationError):
        migrate(rid, version=version, model=model)


@pytest.mark.parametrize("rid", RECORDS)
def test_strict_conversion_keeps_all_producer_and_freight_rows(rid):
    result = migrate(rid, strict=True)
    ds = helper(result)
    row = RECORDS[rid]
    recipe = REVIEW["recipes"][row["new_recipe_id"]]
    inputs = [e for e in ds["exchanges"] if e["type"] != "production"]
    assert len(inputs) == row["retention"]["source_nonproduction_rows"]
    assert len([e for e in inputs if e["unit"] == "kilogram"]) == row["retention"]["source_producer_rows"]
    assert len([e for e in inputs if e["unit"] == "ton kilometer"]) == row["retention"]["source_freight_rows"]
    if rid != "bp-dbd744f486420f56":
        assert sum(e["amount"] for e in inputs if e["unit"] == "kilogram") == pytest.approx(1)
    for actual, expected in zip(inputs, recipe["inputs"], strict=True):
        for key in (
            "amount",
            "uncertainty type",
            "loc",
            "scale",
            "shape",
            "negative",
            "minimum",
            "maximum",
            "pedigree",
            "scale without pedigree",
            "formula",
        ):
            if key in expected:
                assert actual[key] == expected[key]
    assert not any(
        loss.code == "migration.recipe_source_input_omission" for loss in result.last_migration_report.losses
    )


def test_silica_coproduct_retains_allocated_chemical_burden_not_quarry_stock():
    ds = helper(migrate("bp-dbd744f486420f56", strict=True))
    inputs = [e for e in ds["exchanges"] if e["type"] != "production"]
    assert "Cryolite, at plant" in inputs[0]["name"]
    assert inputs[0]["amount"] == pytest.approx(0.000275161844289697 * 0.0312)
    assert all("Silica sand, at plant" in e["name"] for e in inputs[1:3])
    proof = REVIEW["silica_allocation_comparison"]
    assert proof["all_nonproduction_rows"] == 12
    assert proof["all_nonproduction_ratio"] == 0.0312
    assert proof["nonproduction_vectors_same_keys"]


def test_ethanol_preserves_eleven_entries_and_country_specific_suppliers():
    ds = helper(migrate("bp-d262805ddd8a5f1e", strict=True))
    inputs = [e for e in ds["exchanges"] if e["type"] != "production"]
    assert len(inputs) == 11
    assert {e["location"] for e in inputs} == {"CH", "BR", "US", "RER"}
    assert sum(e["amount"] for e in inputs) == pytest.approx(1)
    assert REVIEW["active_basis_reference"]["section"] == "5.1.11"


def test_frozen_replay_is_idempotent_and_rejects_changed_parent():
    resource = deepcopy(load_uvek_technosphere_resource())
    apply_fuels_39_final_markets_review(resource)
    once = deepcopy(resource)
    apply_fuels_39_final_markets_review(resource)
    assert resource == once
    next(r for r in resource["replace"] if r["id"] in RECORDS)["conversion_factor"] = 2
    with pytest.raises(ValueError, match="parent"):
        apply_fuels_39_final_markets_review(resource)
