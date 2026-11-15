"""Shipping dependencies retain waste signs, market delivery and native product boundaries."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from brightpath.migrations.scaling import scale_exchange

REVIEW = json.loads((DATA_DIR / "export/uvek_ships_39_market_boundaries_review.json").read_text())
ROWS = REVIEW["records"]


def policy(approved=True):
    resource = load_uvek_technosphere_resource()
    source = TechnosphereProfile("ecoinvent", "3.9", "cutoff")
    target = TechnosphereProfile("uvek", "2025", "cutoff")
    fingerprints = tuple(
        compatibility_rule_fingerprint(
            resource, "replace", next(r for r in resource["replace"] if r["id"] == row["rule_id"]), source, target
        )
        for row in ROWS
    )
    return MigrationPolicy(
        approved_compatibility_rules=fingerprints if approved else (), on_information_loss=PolicyAction.WARN
    )


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_supplies_and_treatment_preserve_signed_quantities_and_source(amount, uncertainty, release):
    source = consumer(ROWS[0]["original_rule"]["source"], amount, uncertainty)
    source[0]["exchanges"] = [source[0]["exchanges"][0]] + [
        consumer(row["original_rule"]["source"], amount, uncertainty)[0]["exchanges"][1] for row in ROWS
    ]
    before = deepcopy(source)
    result = BrightwayInventory.from_data(source, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert source == before and len(result.data) == 4
    helpers = {a.get("brightpath_conversion", {}).get("recipe_id"): a for a in result.data[1:]}
    recipes = {**REVIEW["recipe_dependencies"], **REVIEW["recipes"]}
    for i, row in enumerate(ROWS, 1):
        expected = deepcopy(before[0]["exchanges"][i])
        scale_exchange(expected, row["compiled_rule"]["conversion_factor"])
        edge = result.data[0]["exchanges"][i]
        for key in ("amount", "uncertainty type", "loc", "scale", "negative", "minimum", "maximum", "formula"):
            if key in expected:
                assert edge[key] == expected[key]
        inputs = [e for e in helpers[row["recipe_id"]]["exchanges"] if e["type"] != "production"]
        assert len(inputs) == row["boundary_checks"]["input_count"]
        for got, want in zip(inputs, recipes[row["recipe_id"]]["inputs"], strict=True):
            for key in (
                "name",
                "reference product",
                "location",
                "unit",
                "amount",
                "uncertainty type",
                "loc",
                "scale",
                "pedigree",
            ):
                if key in want:
                    assert got[key] == want[key]
    waste = [e for e in helpers[ROWS[0]["recipe_id"]]["exchanges"] if e["type"] != "production"]
    assert len(waste) == 2 and {e["unit"] for e in waste} == {"kilogram", "unit"}
    assert not any(e["type"] == "biosphere" or e["unit"] == "ton kilometer" for e in waste)
    lime = helpers[ROWS[1]["recipe_id"]]["exchanges"]
    products = [e["name"] for e in lime if e["type"] == "technosphere" and e["unit"] == "kilogram"]
    assert products.count("Limestone, milled, loose, at plant") == 2
    assert products.count("xx Lime, algae, at regional storehouse") == 1
    assert not any("Quicklime" in name or "hydraulic" in name for name in products)


@pytest.mark.parametrize("row", ROWS, ids=lambda row: row["rule_id"])
@pytest.mark.parametrize(
    "version,model,approved",
    [("3.9", "cutoff", False), ("3.8", "cutoff", True), ("3.9.1", "cutoff", True), ("3.9", "consequential", True)],
)
def test_approval_does_not_cover_other_profiles_or_omit_the_exchange(row, version, model, approved):
    source = consumer(row["original_rule"]["source"], -1, 2)
    before = deepcopy(source)
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(source, context=contexts("ecoinvent", version, model)).migrate_background(
            contexts("uvek", "2026").background, policy=policy(approved)
        )
    assert source == before
