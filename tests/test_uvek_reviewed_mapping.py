import copy
import json

import pytest

from brightpath import DATA_DIR
from scripts.generate_uvek_migration_resources import apply_reviewed_overrides
from scripts.review_uvek_mapping import identity, select


def rule(product="lithium", old="Lithium chloride, at plant"):
    return {
        "source": {
            "name": product + " production",
            "reference product": product,
            "location": "GLO",
            "unit": "kilogram",
        },
        "target": {"name": old, "reference product": old, "location": "GLO", "unit": "kilogram"},
        "mapping_method": "heuristic_similarity",
        "confidence": 0.2,
    }


def candidate(target="Lithium, at plant"):
    return {
        "target": {"name": target, "reference product": target, "location": "GLO", "unit": "kilogram"},
        "method": "review",
        "confidence": 1.0,
    }


def test_accepts_material_correction_but_preserves_specific_technology():
    assert select(rule(), candidate()) == "accept_product_match"
    steel = rule("steel, low-alloyed", "Steel, electric, low-alloyed, at plant")
    steel["source"]["name"] = "steel production, electric, low-alloyed"
    assert (
        select(
            steel,
            candidate("Steel, low-alloyed, at plant"),
        )
        == "retain_process_detail"
    )
    electricity = rule("electricity, low voltage", "Electricity, low voltage, PV")
    electricity["source"]["name"] = "electricity production, photovoltaic"
    assert (
        select(
            electricity,
            candidate("Electricity, low voltage, at grid"),
        )
        == "retain_energy_fuel"
    )


def test_rejects_production_disposal_and_coproduct_regressions():
    assert (
        select(rule("activated carbon", "Carbon, at plant"), candidate("Disposal, activated carbon"))
        == "reject_product_role"
    )
    assert (
        select(rule("sodium chloride", "Sodium chloride, powder, at plant"), candidate("Chemicals organic, at plant"))
        == "reject_chemical_class"
    )


def test_retains_curated_decisions_and_does_not_import_drops():
    current = rule()
    current["mapping_method"] = "curated_legacy"
    assert select(current, candidate()) == "retain_existing_curated"
    assert select(current, {"method": "dropped"}) == "review_drop"
    assert select(current, {}) == "review_unmapped"


def test_rejects_unit_changes():
    proposed = candidate()
    proposed["target"]["unit"] = "megajoule"
    assert select(rule(), proposed) == "review_unit_mismatch"


def test_packaged_overrides_are_idempotent_valid_and_preserve_unselected_rules():
    resource = json.loads((DATA_DIR / "migrations/uvek/ecoinvent-to-uvek-2025.json").read_text())
    overrides = json.loads((DATA_DIR / "export/uvek_reviewed_overrides.json").read_text())
    catalog = json.loads((DATA_DIR / "export/reference_catalogs/uvek__2025__cutoff.json").read_text())
    targets = {(row["name"], row["reference_product"], row["location"], row["unit"]) for row in catalog["technosphere"]}
    original = copy.deepcopy(resource)
    apply_reviewed_overrides(resource, overrides, targets)
    assert resource == original
    assert overrides["decisions"]
    indexed = {identity(row["source"]): row for row in resource["replace"]}
    for decision in overrides["decisions"]:
        current = indexed[identity(decision["source"])]
        assert current["target"] == decision["target"]
        assert current["mapping_method"] == decision["mapping_method"]


@pytest.mark.parametrize("problem", ["duplicate", "stale", "target", "unit"])
def test_invalid_overrides_fail_before_mutating_resource(problem):
    current = rule()
    proposed = candidate()["target"]
    decision = {
        "source": current["source"],
        "previous_target": current["target"],
        "target": proposed,
        "confidence": 0.9,
        "mapping_method": "clic_product_checked",
        "branch_method": "review",
        "reason": "test",
    }
    overrides = {"source_revision": "test", "decisions": [decision]}
    resource = {"replace": [current], "coverage": {}, "methodology": {}}
    fields = ("name", "reference product", "location", "unit")
    targets = {tuple(proposed[field] for field in fields)}
    if problem == "duplicate":
        overrides["decisions"].append(copy.deepcopy(decision))
    elif problem == "stale":
        decision["previous_target"] = candidate("unrelated")["target"]
    elif problem == "target":
        targets.clear()
    else:
        proposed["unit"] = "megajoule"
        targets = {tuple(proposed[field] for field in fields)}
    before = copy.deepcopy(resource)
    with pytest.raises(ValueError):
        apply_reviewed_overrides(resource, overrides, targets)
    assert resource == before


@pytest.mark.parametrize(
    "product,previous,proposed,accepted",
    [
        ("butane", "Butane-1,4-diol, at plant", "Chemicals organic, at plant", True),
        ("sodium chloride, powder", "Sodium chloride, powder, at plant", "Chemicals organic, at plant", False),
        ("hydroxylamine", "Aluminium hydroxide, at plant", "Chemicals organic, at plant", False),
        ("methallylchloride", "Ammonium chloride, at plant", "Chemicals inorganic, at plant", False),
        (
            "indium tin oxide powder, nanoscale, for sputtering target",
            "ITO powder, for target production, at plant",
            "Chemicals inorganic, at plant",
            False,
        ),
        (
            "neutralising agent, sodium hydroxide-equivalent",
            "Sodium hydroxide, 50% in H2O, at plant",
            "Chemicals inorganic, at plant",
            False,
        ),
        ("ammonium sulfate", "Cobalt sulfate", "Ammonium sulphate, as N, at regional storehouse", False),
        ("nylon 6-6, glass-filled", "Nylon 66, glass-filled, at plant", "xx Nylon 6, glass-filled, at plant", False),
        (
            "pitch",
            "Silver, from copper production, at refinery",
            "Pitch despergents, in paper production, at plant",
            False,
        ),
        ("soybean", "Soybean oil, at oil mill", "Soybeans, at farm", True),
        ("potato", "Potato grading", "Potatoes, at farm", True),
        ("refinery sludge", "Diesel, at refinery", "Refinery gas, at refinery", False),
        (
            "waste reinforced plasterboard",
            "Concrete, reinforced, in sorting plant",
            "Disposal, reinforced concrete, to sorting plant",
            False,
        ),
        (
            "waste polyethylene terephthalate",
            "Fleece production, polyethylene terephthalate",
            "Disposal, polyethylene, 0.4% water, to sanitary landfill",
            False,
        ),
        (
            "waste polyethylene",
            "Fleece, polyethylene, at plant",
            "Disposal, polyethylene/polypropylene products, to municipal waste incineration",
            True,
        ),
        ("sewage sludge, 97% water", "Heat pump", "Disposal, refinery sludge, 89.5% water, to landfarming", False),
        (
            "bottom ash, MSWI, waste plastic",
            "Plastic",
            "Disposal, ash horse dung and waste wood chips, to landfarming",
            False,
        ),
    ],
)
def test_pairwise_product_and_mass_basis_regressions(product, previous, proposed, accepted):
    assert select(rule(product, previous), candidate(proposed)).startswith("accept_") is accepted


@pytest.mark.parametrize(
    "source_name,previous,proposed,accepted",
    [
        (
            "electricity production, wind, onshore",
            "Electricity, high voltage, at grid",
            "Electricity, at wind farm",
            True,
        ),
        (
            "electricity production, nuclear, boiling water reactor",
            "Electricity, at grid",
            "Electricity, nuclear, at power plant pressure water reactor",
            False,
        ),
        ("electricity production, wind, offshore", "Electricity, at grid", "Electricity, wind, onshore", False),
        (
            "electricity production, photovoltaic, single-Si",
            "Electricity, at grid",
            "Electricity, photovoltaic, multi-Si",
            False,
        ),
        ("heat and power co-generation, natural gas", "Electricity, at grid", "Electricity, at cogen, wood", False),
        (
            "heat and power co-generation, wood",
            "Electricity, at grid",
            "xxx Electricity, at cogen, wood, allocation heat",
            False,
        ),
    ],
)
def test_energy_fuel_and_subtype_regressions(source_name, previous, proposed, accepted):
    current = rule("electricity, high voltage", previous)
    current["source"]["name"] = source_name
    current["source"]["unit"] = "kilowatt hour"
    current["target"]["unit"] = "kilowatt hour"
    alternative = candidate(proposed)
    alternative["target"]["unit"] = "kilowatt hour"
    assert select(current, alternative).startswith("accept_") is accepted


def test_waste_energy_coproduct_is_not_treated_as_waste_disposal():
    current = rule("electricity, medium voltage", "Electricity, medium voltage, at grid")
    current["source"]["name"] = "treatment of municipal solid waste, municipal waste incineration"
    assert (
        select(current, candidate("Electricity from waste, at municipal waste incineration plant"))
        == "accept_energy_fuel"
    )


def test_butane_coproduct_is_updated_in_packaged_resource():
    resource = json.loads((DATA_DIR / "migrations/uvek/ecoinvent-to-uvek-2025.json").read_text())
    selected = next(
        row
        for row in resource["replace"]
        if row["source"]
        == {
            "location": "RoW",
            "name": "2-butanol production by hydration of butene",
            "reference product": "butane",
            "unit": "kilogram",
        }
    )
    assert selected["target"]["name"] == "Chemicals organic, at plant"
    assert selected["confidence"] == 0.4
    assert selected["mapping_method"] == "clic_pairwise_review"
