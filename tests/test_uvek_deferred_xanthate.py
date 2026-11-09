"""Active chemical synthesis retains reactants, water compartments and waste demand."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_deferred_xanthate import apply_deferred_xanthate_review

REVIEW = json.loads((DATA_DIR / "export/uvek_deferred_xanthate_review.json").read_text())
ROW = REVIEW["records"][0]


@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_pure_kg_and_source_metadata_survive_with_complete_synthesis(amount, uncertainty):
    data = consumer(ROW["source"], amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context("2026"), policy=MigrationPolicy.permissive()
    )
    assert data == before and len(result.data) == ROW["expected_foreground_records"]
    x = result.data[0]["exchanges"][1]
    assert all(x[k] == v for k, v in before[0]["exchanges"][1].items() if k not in ROW["source"])
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
    market = helpers[ROW["generated_recipe_id"]]
    coefficients = (
        "amount",
        "uncertainty type",
        "loc",
        "scale",
        "negative",
        "minimum",
        "maximum",
        "pedigree",
        "scale without pedigree",
    )
    for recipe_id, recipe in REVIEW["auxiliary_recipes"].items():
        inputs = [e for e in helpers[recipe_id]["exchanges"] if e["type"] != "production"]
        assert len(inputs) == len(recipe["inputs"])
        for actual, expected in zip(inputs, recipe["inputs"], strict=True):
            assert {k: actual[k] for k in coefficients if k in actual} == {
                k: expected[k] for k in coefficients if k in expected
            }
            if "recipe" not in expected:
                assert actual["name"] == expected["name"] and actual["unit"] == expected["unit"]
            if expected.get("type") == "biosphere":
                assert tuple(actual["categories"]) == tuple(expected["categories"])

        def total(name, inputs=inputs):
            return sum(e["amount"] for e in inputs if e.get("name") == name)

        assert total("Carbon disulfide, at plant") == 0.53
        assert total("Ethanol from ethylene, at plant") == pytest.approx(0.32, rel=1e-14)
        assert total("Sodium hydroxide, 50% in H2O, production mix, at plant") == pytest.approx(0.28, rel=1e-14)
        assert total("Nitrogen, liquid, at plant") == pytest.approx(0.02563, rel=1e-14)
        assert total("Electricity, medium voltage, at grid") == pytest.approx(0.164, rel=1e-14)
        assert total("Treatment, sewage, unpolluted, to wastewater treatment, class 3") == pytest.approx(
            2.18e-7, rel=1e-14
        )
        assert all(e["amount"] > 0 for e in inputs if e["type"] == "technosphere")
        direct = [e for e in inputs if e["type"] == "biosphere"]
        assert len(direct) == 4 and sum(e["name"] == "Water" for e in direct) == 2
    assert not any("from biomass" in e.get("name", "") for a in result.data for e in a["exchanges"])
    market_inputs = [e for e in market["exchanges"] if e["type"] != "production"]
    assert all(
        all(x[k] == v for k, v in expected.items() if k != "recipe")
        for x, expected in zip(market_inputs, ROW["profiles"][0]["inputs"], strict=True)
    )
    assert sorted(e["amount"] for e in market_inputs[:2]) == [0.00409999999999999, 0.9958999999999999]
    assert sum(e["type"] == "biosphere" for a in result.data[1:] for e in a["exchanges"]) == 8
    assert not any(i.code.endswith("omission") for i in result.last_migration_report.issues)


@pytest.mark.parametrize("version", ["3.6", "3.9", "3.11"])
def test_other_versions_cannot_inherit_synthesis_and_purity_assumptions(version):
    inventory = BrightwayInventory.from_data(consumer(ROW["source"], 1), context=contexts("ecoinvent", version))
    before = deepcopy(inventory.data)
    with pytest.raises(MigrationError):
        inventory.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert inventory.data == before


@pytest.mark.parametrize("drift", ["none", "parent", "producer", "market", "dependency"])
def test_replay_freezes_parent_native_graph_and_reviewed_waste_markets_atomically(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    if drift == "parent":
        next(r for r in resource["replace"] if r["id"] == ROW["rule_id"])["confidence"] = 0.123
    elif drift == "producer":
        resource["recipes"][next(iter(REVIEW["auxiliary_recipes"]))]["inputs"][0]["amount"] += 1
    elif drift == "market":
        resource["recipes"][ROW["generated_recipe_id"]]["inputs"][-1]["amount"] += 1
    elif drift == "dependency":
        resource["recipes"][next(iter(REVIEW["recipe_dependencies"]))]["inputs"][0]["amount"] += 1
    before = deepcopy(resource)
    if drift == "none":
        apply_deferred_xanthate_review(resource)
    else:
        with pytest.raises(ValueError):
            apply_deferred_xanthate_review(resource)
    assert resource == before
