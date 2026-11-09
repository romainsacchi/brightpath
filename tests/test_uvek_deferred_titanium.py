"""Metal production preserves allocated Mg/Cl recovery rather than using TiO2."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_deferred_titanium import apply_deferred_titanium_review

REVIEW = json.loads((DATA_DIR / "export/uvek_deferred_titanium_review.json").read_text())
ROW = REVIEW["records"][0]


@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_metal_recovery_references_waste_signs_and_country_shares(amount, uncertainty):
    data = consumer(ROW["source"], amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        target_context("2026"), policy=MigrationPolicy.permissive()
    )
    assert data == before and len(result.data) == ROW["expected_foreground_records"]
    x = result.data[0]["exchanges"][1]
    assert all(x[k] == v for k, v in before[0]["exchanges"][1].items() if k not in ROW["source"])
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
    fields = (
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
            assert {k: actual[k] for k in fields if k in actual} == {k: expected[k] for k in fields if k in expected}
            if "recipe" not in expected:
                assert actual["name"] == expected["name"] and actual["unit"] == expected["unit"]
            if expected.get("type") == "biosphere":
                assert tuple(actual["categories"]) == tuple(expected["categories"])
    stages = list(helpers.values())

    def find(stage, product, location):
        (a,) = [
            a
            for a in stages
            if a["name"].startswith("Reconstructed titanium source-chain stage, " + stage)
            and a["reference product"] == product
            and a["location"] == location
        ]
        return a

    def tech(a):
        return [e for e in a["exchanges"] if e["type"] == "technosphere"]

    sponge_market = find("market for titanium sponge", "titanium sponge", "GLO")
    production = [e for e in tech(sponge_market) if e["reference product"] == "titanium sponge"]
    assert {e["location"]: e["amount"] for e in production} == {"CN": 0.425, "RoW": 0.33, "JP": 0.245}
    assert len(tech(sponge_market)) == 7
    (stock,) = [
        a
        for a in stages
        if a["reference product"] == "magnesium chloride, from titanium sponge production"
        and a.get("brightpath_conversion", {}).get("zero_burden")
    ]
    assert len(stock["exchanges"]) == 1 and stock["exchanges"][0]["amount"] == 1
    for location in ("CN", "RoW", "JP"):
        sponge = find("titanium sponge production, from titanium tetrachloride", "titanium sponge", location)
        (recovered_stock,) = [
            e
            for e in tech(sponge)
            if all(e.get(k) == stock[k] for k in ("name", "reference product", "location", "unit"))
        ]
        assert recovered_stock["amount"] == -3.885
        assert all(e["amount"] > 0 for e in tech(sponge) if e["reference product"] == "inert waste")
        magnesium = find(
            "electrolysis of magnesium chloride, from titanium sponge production",
            "magnesium, for reuse in titanium sponge production",
            location,
        )
        chlorine = find(
            "electrolysis of magnesium chloride, from titanium sponge production",
            "chlorine, for reuse in titanium tetrachloride production",
            location,
        )
        assert magnesium["code"] != chlorine["code"]
        (mg_feed,) = [
            e
            for e in tech(magnesium)
            if e["reference product"] == "magnesium chloride, from titanium sponge production"
        ]
        (cl_feed,) = [
            e for e in tech(chlorine) if e["reference product"] == "magnesium chloride, from titanium sponge production"
        ]
        assert mg_feed["amount"] == 3.5793965975378907
        assert mg_feed["amount"] != cl_feed["amount"] and cl_feed["amount"] > 0
    slag = find("market for titania slag, 94% titanium dioxide", "titania slag, 94% titanium dioxide", "GLO")
    (feed,) = [e for e in tech(slag) if e["name"] == "Rutile, 95% titanium dioxide, at plant"]
    assert feed["amount"] == 94 / 95 and feed["location"] == "AU" and len(tech(slag)) == 5
    cn = find("titanium tetrachloride production", "titanium tetrachloride", "CN")
    row = find("titanium tetrachloride production", "titanium tetrachloride", "RoW")
    (cn_feed,) = [e for e in tech(cn) if e["reference product"] == "titania slag, 94% titanium dioxide"]
    (row_feed,) = [e for e in tech(row) if e["reference product"] == "rutile, 95% titanium dioxide"]
    assert cn_feed["amount"] == 0.4465479174261463 and row_feed["amount"] == 0.44184741303218666
    assert cn_feed["amount"] * 94 == pytest.approx(row_feed["amount"] * 95)
    assert not any(
        e.get("name") == "Titanium dioxide, chloride process, at plant" for a in result.data for e in a["exchanges"]
    )
    other_heat = find(
        "market group for heat, district or industrial, other than natural gas",
        "heat, district or industrial, other than natural gas",
        "GLO",
    )
    assert not any(e["name"].startswith("District heat, at consumer, natural gas") for e in tech(other_heat))
    assert sum(e["amount"] for e in tech(other_heat)) == pytest.approx(1)
    losses = [
        i.to_dict()["details"]
        for i in result.last_migration_report.losses
        if i.code == "migration.recipe_source_input_omission"
    ]
    assert len(losses) == 6
    assert all(i["omission"] in ROW["approved_recipe_source_omissions"][i["recipe_id"]] for i in losses)
    assert not any(i.code == "migration.recipe_input_omission" for i in result.last_migration_report.issues)


@pytest.mark.parametrize("version", ["3.6", "3.10", "3.12"])
def test_uninspected_releases_are_refused_without_mutation(version):
    source = BrightwayInventory.from_data(consumer(ROW["source"], 1), context=contexts("ecoinvent", version))
    before = deepcopy(source.data)
    with pytest.raises(MigrationError):
        source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert source.data == before


@pytest.mark.parametrize("drift", ["none", "parent", "allocated_stage", "stock_dependency", "literal_handover"])
def test_authoring_freezes_recovery_and_allocated_boundaries_atomically(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    if drift == "parent":
        next(r for r in resource["replace"] if r["id"] == ROW["rule_id"])["confidence"] = 0.123
    elif drift == "allocated_stage":
        rid = next(
            k
            for k, a in REVIEW["auxiliary_recipes"].items()
            if a["reference product"] == "chlorine, for reuse in titanium tetrachloride production"
        )
        resource["recipes"][rid]["inputs"][0]["amount"] += 1
    elif drift == "stock_dependency":
        rid = next(k for k in REVIEW["recipe_dependencies"] if resource["recipes"][k].get("zero_burden"))
        resource["recipes"][rid]["zero_burden_reason"] += " changed"
    elif drift == "literal_handover":
        rid = REVIEW["records"][1]["generated_recipe_id"]
        resource["recipes"][rid]["source_input_omissions"][0]["source_exchange"]["amount"] += 1
    before = deepcopy(resource)
    if drift == "none":
        apply_deferred_titanium_review(resource)
    else:
        with pytest.raises(ValueError):
            apply_deferred_titanium_review(resource)
    assert resource == before
