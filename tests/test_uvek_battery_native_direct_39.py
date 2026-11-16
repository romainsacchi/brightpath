"""Reviewed native supplies preserve caller coordinates without extra inventories."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_battery_native_direct_39_review.json").read_text())
ROWS = REVIEW["records"]


def scoped_policy(approved=True):
    resource = load_uvek_technosphere_resource()
    source = TechnosphereProfile("ecoinvent", "3.9", "cutoff")
    target = TechnosphereProfile("uvek", "2025", "cutoff")
    return MigrationPolicy(
        approved_compatibility_rules=(
            tuple(compatibility_rule_fingerprint(resource, "replace", row["rule"], source, target) for row in ROWS)
            if approved
            else ()
        ),
        on_information_loss=PolicyAction.WARN,
    )


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_fourteen_supplies_keep_signed_statistics_formula_and_single_supply(amount, uncertainty, release):
    source = consumer(ROWS[0]["source"], amount, uncertainty)
    source[0]["exchanges"] = [source[0]["exchanges"][0]] + [
        consumer(row["source"], amount, uncertainty)[0]["exchanges"][1] for row in ROWS
    ]
    original = deepcopy(source)
    result = BrightwayInventory.from_data(source, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=scoped_policy()
    )
    assert source == original and len(result.data) == 1
    assert len(result.data[0]["exchanges"]) == 15
    for edge, before, row in zip(result.data[0]["exchanges"][1:], original[0]["exchanges"][1:], ROWS, strict=True):
        assert all(edge[k] == value for k, value in row["target"].items())
        for key in ("amount", "loc", "scale", "negative", "minimum", "maximum", "formula", "uncertainty type"):
            if key in before:
                assert edge[key] == before[key]


@pytest.mark.parametrize(
    "version,model,approved", [("3.9", "cutoff", False), ("3.8", "cutoff", True), ("3.9", "consequential", True)]
)
def test_approval_is_required_and_does_not_cover_another_source_profile(version, model, approved):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROWS[0]["source"], 1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=scoped_policy(approved))


def test_review_keeps_chemical_and_service_boundaries_explicit():
    resource = load_uvek_technosphere_resource()
    rules = {rule["id"]: rule for rule in resource["replace"]}
    assert len(ROWS) == 14 and REVIEW["new_helpers"] == 0
    assert REVIEW["new_foreground_omissions"] == []
    for row in ROWS:
        assert row["rule"] == rules[row["rule_id"]]
        assert row["source_witness"]["reference_amount"] == 1
        assert row["actual_nonzero_foreground_uses"] > 0
        assert all(w["reference_amount"] == 1 for w in row["native_witnesses"].values())
        assert row["limitations"] and row["functional_basis"]
    indexed = {row["category"]: row for row in ROWS}
    assert "excluding the steel" in indexed["hot rolling service"]["functional_basis"]
    assert "38%" in indexed["activated bentonite mixture"]["functional_basis"]
    assert "fuel input" in indexed["natural-gas turbine combustion"]["functional_basis"]
    assert "ferrochromium" in indexed["chromium metal supply"]["limitations"][0]
    assert "AlLi" in indexed["AlMg3 alloy supply"]["limitations"][0]
    assert "tetraethyl orthosilicate" in indexed["generic organic solvent mixture"]["limitations"][0]
