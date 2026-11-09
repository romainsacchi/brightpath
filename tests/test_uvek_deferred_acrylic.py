"""Wet source recipes bypass unresolved legacy bases and preserve treatment routes."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_deferred_acrylic import apply_deferred_acrylic_review

REVIEW = json.loads((DATA_DIR / "export/uvek_deferred_acrylic_review.json").read_text())
ROW = REVIEW["records"][0]


@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_wet_quantities_reactants_isomer_emissions_and_waste_fates_survive(amount, uncertainty):
    data = consumer(ROW["source"], amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        target_context("2026"), policy=MigrationPolicy.permissive()
    )
    assert data == before and len(result.data) == ROW["expected_foreground_records"]
    x = result.data[0]["exchanges"][1]
    assert all(x[k] == v for k, v in before[0]["exchanges"][1].items() if k not in ROW["source"])
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
    quantities = (
        "amount",
        "uncertainty type",
        "loc",
        "scale",
        "negative",
        "minimum",
        "maximum",
        "formula",
        "pedigree",
        "scale without pedigree",
    )
    for rid, recipe in REVIEW["auxiliary_recipes"].items():
        inputs = [e for e in helpers[rid]["exchanges"] if e["type"] != "production"]
        assert len(inputs) == len(recipe["inputs"])
        for actual, expected in zip(inputs, recipe["inputs"], strict=True):
            assert {k: actual[k] for k in quantities if k in actual} == {
                k: expected[k] for k in quantities if k in expected
            }
            if "recipe" not in expected:
                assert actual["name"] == expected["name"] and actual["unit"] == expected["unit"]
            if expected.get("type") == "biosphere":
                assert tuple(actual["categories"]) == tuple(expected["categories"])
    (binder,) = [a for a in helpers.values() if "acrylic binder production" in a["name"]]
    for name, amount_expected in [
        ("Butyl acrylate, at plant", 0.0901),
        ("Esters of versatic acid, at plant", 0.152),
        ("Vinyl acetate, at plant", 0.329),
    ]:
        (e,) = [e for e in binder["exchanges"] if e.get("name") == name]
        assert e["amount"] == amount_expected
    (dispersion,) = [a for a in helpers.values() if "acrylic dispersion production" in a["name"]]
    (butanol,) = [e for e in dispersion["exchanges"] if "butanol" in e.get("name", "")]
    assert butanol["name"] == "1-butanol, propylene hydroformylation, at plant" and butanol["amount"] == 0.005161702
    assert not any(
        e.get("name", "").startswith(("Acrylic binder,", "Acrylic dispersion,", "2-butanol,"))
        for a in result.data
        for e in a["exchanges"]
    )
    bios = [e for a in result.data[1:] for e in a["exchanges"] if e["type"] == "biosphere"]
    assert len(bios) == 9
    assert [(e["amount"], e["unit"]) for e in bios if e["name"] == "Dissolved solids"] == [(0.00036, "kilogram")]
    assert [(e["amount"], e["unit"]) for e in bios if e["name"] == "Suspended solids, unspecified"] == [
        (2.1e-5, "kilogram")
    ]
    assert sorted(e["amount"] for e in bios if e["name"] == "Water, unspecified natural origin") == [0.000184, 0.00046]
    waste = [a for a in helpers.values() if "market for waste emulsion paint" in a["name"]]
    assert len(waste) == 2
    for a in waste:
        inputs = [e for e in a["exchanges"] if e["type"] == "technosphere"]
        assert all(e["amount"] > 0 for e in inputs)
        names = {e["name"] for e in inputs}
        assert "Disposal, emulsion paint, 0% water, to municipal incineration" in names
        assert "Disposal, emulsion paint remains, 0% water, to hazardous waste incineration" in names
        if a["location"] == "Europe without Switzerland":
            assert "Disposal, emulsion paint, 0% water, to sanitary landfill" in names
            assert "Disposal, emulsion paint, 0% water, to construction waste landfill" in names
        else:
            assert not any("landfill" in name for name in names)
    losses = [i for i in result.last_migration_report.losses if i.code == "migration.recipe_source_input_omission"]
    assert len(losses) == 2
    assert all(
        i.to_dict()["details"]["omission"]
        in ROW["approved_recipe_source_omissions"][i.to_dict()["details"]["recipe_id"]]
        for i in losses
    )
    assert not any(i.code == "migration.recipe_input_omission" for i in result.last_migration_report.issues)


@pytest.mark.parametrize("version", ["3.6", "3.11", "3.12"])
def test_uninspected_releases_cannot_inherit_wet_recipe_qualification(version):
    source = BrightwayInventory.from_data(consumer(ROW["source"], 1), context=contexts("ecoinvent", version))
    before = deepcopy(source.data)
    with pytest.raises(MigrationError):
        source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert source.data == before


@pytest.mark.parametrize("drift", ["none", "parent", "wet_binder", "waste_market", "dependency"])
def test_replay_freezes_physical_recipe_and_inherited_audits_atomically(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    if drift == "parent":
        next(r for r in resource["replace"] if r["id"] == ROW["rule_id"])["confidence"] = 0.123
    elif drift in ("wet_binder", "waste_market"):
        marker = "acrylic binder production" if drift == "wet_binder" else "market for waste emulsion paint"
        rid = next(k for k, a in REVIEW["auxiliary_recipes"].items() if marker in a["name"])
        resource["recipes"][rid]["inputs"][0]["amount"] += 1
    elif drift == "dependency":
        rid = next(iter(ROW["approved_recipe_source_omissions"]))
        resource["recipes"][rid]["source_input_omissions"][0]["rationale"] += " changed"
    before = deepcopy(resource)
    if drift == "none":
        apply_deferred_acrylic_review(resource)
    else:
        with pytest.raises(ValueError):
            apply_deferred_acrylic_review(resource)
    assert resource == before
