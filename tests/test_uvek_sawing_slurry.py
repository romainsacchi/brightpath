"""Slurry treatment uses litre helpers and preserves genuine recovery credits."""

from copy import deepcopy

import pytest
from test_uvek_market_compositions import assert_composition_quantities, batch
from test_uvek_reference_signs import FIELDS, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BrightwayInventory, SimaProInventory
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from scripts.review_uvek_market_compositions import COMPOSITIONS
from scripts.review_uvek_processing_compositions import PROCESSING, processing_decision, processing_recipe

SLURRY = {key: row for key, row in PROCESSING.items() if key[1] == "spent sawing slurry from Si-wafer cutting"}
POLICY = MigrationPolicy.permissive()


@pytest.mark.parametrize("model", ["cutoff"])
@pytest.mark.parametrize("amount", [-10, 0, 10])
def test_slurry_litre_demand_and_credits_keep_sign_uncertainty_formula_and_components(model, amount):
    label = f"ecoinvent-3.6-{model}"
    assert_composition_quantities(
        label,
        amount,
        definitions=SLURRY,
        recipe_factory=processing_recipe,
        warning_fragment="Independently inspected ecoinvent 3.6 sawing-slurry treatment",
    )
    for source, row in SLURRY.items():
        profile = next(p for p in row["profiles"] if p["label"] == label)
        _, recipe = processing_recipe(source, profile)
        assert recipe["unit"] == "litre"
        electricity = sum(e["amount"] for e in recipe["inputs"] if e.get("unit") == "kilowatt hour")
        assert electricity == pytest.approx(0.992263592263592)
        credits = [
            e
            for e in recipe["inputs"]
            if e.get("name") in {"Silicon carbide, at plant", "Triethylene glycol, at plant"}
        ]
        if model == "consequential":
            assert len(credits) == 2
            assert [e["amount"] for e in credits] == pytest.approx([-0.63000063000063] * 2)
        else:
            assert not credits
        assert any(e["amount"] > 0 and "Disposal" in e.get("name", "") for e in recipe["inputs"])


@pytest.mark.parametrize("model", ["cutoff"])
@pytest.mark.parametrize("version", ["3.7", "3.8", "3.9", "3.10", "3.11", "3.12", "3.6.1"])
def test_newer_and_uninspected_slurry_profiles_remain_blocked_transactionally(model, version):
    data = batch(f"ecoinvent-3.6-{model}", -1, definitions=SLURRY)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
    with pytest.raises(MigrationError):
        inventory.migrate_background(target_context("2025"), policy=POLICY)
    assert inventory.data == data == before


@pytest.mark.parametrize("model", ["cutoff"])
@pytest.mark.parametrize("kind", ["2025", "native2026"])
def test_slurry_helpers_round_trip_through_excel_and_simapro_with_separate_emissions(tmp_path, model, kind):
    data = batch(f"ecoinvent-3.6-{model}", -1, definitions=SLURRY)
    data[0]["comment"] = "Consumer used to verify complete slurry-helper export and reimport."
    for exchange in data[0]["exchanges"][1:]:
        exchange.pop("formula")
        exchange["type"] = "technosphere"
    data[0]["exchanges"].append(
        {"name": "Carbon dioxide, fossil", "categories": ["air"], "unit": "kilogram", "type": "biosphere", "amount": 7}
    )
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.6", model))
    migrated = inventory.migrate_background(target_context(kind), policy=POLICY)
    assert inventory.data == original
    assert migrated.data[0]["exchanges"][-1]["amount"] == 7
    assert all(e["amount"] == 1 for e in migrated.data[0]["exchanges"][1:-1])
    excel = BrightwayInventory.from_excel(migrated.write_excel(tmp_path / "slurry.xlsx"))
    categorized = migrated.data
    for dataset in categorized:
        dataset["exchanges"][0]["simapro category"] = "Materials/Waste treatment"
    csv = (
        BrightwayInventory.from_data(categorized, context=migrated.context)
        .to_simapro()
        .write_csv(tmp_path / "slurry.csv")
    )
    simapro = SimaProInventory.from_csv(
        csv, background_profile=migrated.background_profile, biosphere_profile=migrated.context.background.biosphere
    ).to_brightway()
    for restored in (excel, simapro):
        assert len(restored.data) == len(migrated.data)
        assert not restored.validate().has_errors
        parents = [dataset for dataset in restored.data if dataset.get("unit") == "litre"]
        assert len(parents) == 2
        for helper in parents:
            credits = [
                e
                for e in helper["exchanges"]
                if "carbide" in e.get("name", "").lower() or "triethylene" in e.get("name", "").lower()
            ]
            assert len(credits) == (2 if model == "consequential" else 0)
            assert all(e["amount"] < 0 for e in credits)


@pytest.mark.parametrize("change", ["prior_factor", "inventory", "profile"])
def test_slurry_replaces_only_the_exact_inspected_wrong_spreading_correspondence(change):
    source, row = next(iter(SLURRY.items()))
    rule = {"source": row["source"], "target": row["previous_target"], "conversion_factor": 0.001}
    profiles = [*row["profiles"], *row["pending_profiles"]]
    observations = [(p["label"], p["source_sign"]) for p in profiles]
    inventories = [(p["label"], p["inventory_sha256"]) for p in profiles]
    decision = processing_decision(rule, observations, inventories, "synthetic", None)
    assert decision["original_factor"] == 0.001 and decision["factor"] == -1
    assert len(decision["recipes"]) == 2 and len(decision["review"]["profiles"]) == 14
    if change == "prior_factor":
        rule["conversion_factor"] = 1
    elif change == "inventory":
        inventories[0] = (inventories[0][0], "0" * 64)
    else:
        observations[0] = ("ecoinvent-3.6.1-cutoff", -1)
    with pytest.raises(ValueError, match="repeat the scientific review"):
        processing_decision(rule, observations, inventories, "synthetic", None)


def test_slurry_foreground_suppliers_are_preserved():
    data = batch("ecoinvent-3.6-cutoff", -1, definitions=SLURRY)
    for source in SLURRY:
        supplier = dict(zip(FIELDS, source, strict=True))
        data.append({**supplier, "exchanges": [{**supplier, "type": "production", "amount": -1}]})
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.6", "cutoff")).migrate_background(
        target_context("2025"), policy=POLICY
    )
    assert result.data == data == before


@pytest.mark.parametrize("model", ["cutoff"])
@pytest.mark.parametrize("kind", ["2025", "native2026"])
def test_slurry_market_retains_both_unrounded_regional_treatment_shares(model, kind):
    definitions = {
        key: row for key, row in COMPOSITIONS.items() if key[1] == "spent sawing slurry from Si-wafer cutting"
    }
    data = batch(f"ecoinvent-3.6-{model}", -1, definitions=definitions)
    original = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.6", model)).migrate_background(
        target_context(kind), policy=POLICY
    )
    assert data == original
    assert all(e["amount"] == 1 for e in result.data[0]["exchanges"][1:])
    markets = [d for d in result.data[1:] if d["name"].startswith("market for spent sawing slurry")]
    assert len(markets) == 1 and markets[0]["unit"] == "litre"
    inputs = markets[0]["exchanges"][1:]
    assert sorted(e["amount"] for e in inputs) == [0.499999999999999, 0.500000000000001]
    assert all(e["unit"] == "litre" and e["location"] in {"RER", "RoW"} for e in inputs)


@pytest.mark.parametrize("model", ["cutoff"])
@pytest.mark.parametrize("kind", ["2025", "native2026"])
def test_slurry_helper_reverse_requires_independent_component_coverage_and_rolls_back(model, kind):
    forward = BrightwayInventory.from_data(
        batch(f"ecoinvent-3.6-{model}", -1, definitions=SLURRY), context=contexts("ecoinvent", "3.6", model)
    ).migrate_background(target_context(kind), policy=POLICY)
    before = deepcopy(forward.data)
    with pytest.raises(MigrationError) as caught:
        forward.migrate_background(contexts("ecoinvent", "3.12").background, policy=POLICY)
    assert any(issue.code == "migration.uvek_supplier_unresolved" for issue in caught.value.report.issues)
    assert forward.data == before
