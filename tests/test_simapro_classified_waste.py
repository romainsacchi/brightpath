from copy import deepcopy

import pytest
from test_simapro_waste_export import link, process, section_rows

from brightpath import BackgroundProfile, SimaProInventory
from brightpath.profiles.simapro_waste import resolve_classified_waste
from brightpath.units import normalize_unit


def classified(name="treatment of sludge", isic="3821", cpc="34659", amount=1):
    data = process(name, production=amount)
    data["exchanges"][0].pop("simapro category")
    data["classifications"] = [("ISIC rev.4 ecoinvent", isic), ("CPC", cpc)]
    return data


@pytest.mark.parametrize(
    "name,isic,cpc,amount,expected",
    [
        ("treatment of sludge", "3821", "34659", 1, None),
        ("treatment of waste", "3821", "17100", 1, None),
        ("treatment of waste", "3821", "39990", -1, True),
        ("dismantling", "3830", "39990", -1, True),
        ("treatment of x-ray film", "3830", "41310", 1, None),
        ("market for waste paper", "3821", "39240", 1, None),
        ("aluminium turning", "2592", "39363", 1, False),
        ("cement production", "2394", "39990", -1, None),
        ("treatment of waste", "2394", "39990", 1, None),
        ("manufacture", "2394", "94320", 1, None),
        ("collection", "3811", "39990", 1, None),
        ("unknown", "", "39990", -1, None),
        ("unknown", "38", "39990", 1, None),
    ],
)
def test_roles(name, isic, cpc, amount, expected):
    data = classified(name, isic, cpc, amount)
    original = deepcopy(data)
    assert resolve_classified_waste(data).waste is expected
    assert data == original


def test_explicit_category_and_multiple_codes():
    data = classified()
    data["classifications"].append(("ISIC rev.4", "2394"))
    assert resolve_classified_waste(data).waste is None
    data["exchanges"][0]["simapro category"] = "material/Native"
    assert resolve_classified_waste(data).waste is False


@pytest.mark.parametrize("amount", [-2, -0.5])
def test_inferred_supplier_controls_both_sections_and_signs(amount):
    supplier = classified(amount=amount)
    customer = process("consumer")
    customer["exchanges"].append(link(supplier, -3))
    original = deepcopy([supplier, customer])
    inventory = SimaProInventory.from_data(
        original, background_profile=BackgroundProfile("ecoinvent", "3.12", "cutoff")
    )
    result = inventory.render(category_mode="infer_classifications")
    assert not result.has_errors, result.issues
    assert float(section_rows(result.rows, "Waste treatment")[0][2]) == abs(amount)
    assert float(section_rows(result.rows, "Waste to treatment")[0][2]) == 3
    assert inventory.data == original
    assert inventory.render(category_mode="infer_classifications").rows == result.rows


def test_unresolved_blocks_export_and_external_supplier_does_not_use_keywords():
    data = classified(name="market for waste paper", cpc="39240")
    inventory = SimaProInventory.from_data([data], background_profile=BackgroundProfile("ecoinvent", "3.12", "cutoff"))
    result = inventory.render(category_mode="infer_classifications")
    assert result.has_errors and not result.rows
    assert any(i.code == "simapro_waste_unresolved" for i in result.issues)
    data = process("consumer")
    data["exchanges"].append(link(process("waste treatment"), -1))
    inventory = SimaProInventory.from_data([data], background_profile=BackgroundProfile("ecoinvent", "3.12", "cutoff"))
    result = inventory.render(category_mode="infer_classifications")
    assert result.has_errors and not result.rows


@pytest.mark.parametrize(
    "reference,unit,cpc",
    [
        ("electricity, medium voltage", "kilowatt hour", "17100"),
        ("heat, district or industrial", "MJ", "17300"),
        ("organic nitrogen fertiliser, as N", "kg", "34659"),
        ("organic phosphorus fertiliser, as P2O5", "kilogram", "34659"),
        ("organic potassium fertiliser, as K2O", "kilogram", "34659"),
        ("compost", "kilogram", "391"),
        ("biogas", "cubic meter", "17200"),
        ("poultry manure, dried", "kilogram", "34654"),
    ],
)
def test_positive_recovered_products_keep_input_sign(reference, unit, cpc):
    supplier = classified(cpc=cpc)
    supplier["reference product"] = reference
    supplier["unit"] = unit
    supplier["exchanges"][0].update({"reference product": reference, "unit": unit})
    assert resolve_classified_waste(supplier).waste is False
    supplier["unit"] = normalize_unit(unit)
    supplier["exchanges"][0]["unit"] = normalize_unit(unit)
    consumer = process("consumer")
    consumer["exchanges"].append(link(supplier, 3))
    inventory = SimaProInventory.from_data(
        [supplier, consumer], background_profile=BackgroundProfile("ecoinvent", "3.12", "cutoff")
    )
    original = inventory.data
    result = inventory.render(category_mode="infer_classifications")
    assert not result.has_errors, result.issues
    assert not section_rows(result.rows, "Waste treatment")
    assert not section_rows(result.rows, "Waste to treatment")
    inputs = section_rows(result.rows, "Materials/fuels") + section_rows(result.rows, "Electricity/heat")
    assert len(inputs) == 1 and float(inputs[0][2]) == 3
    assert inventory.data == original
    supplier["exchanges"][0]["amount"] = -1
    assert resolve_classified_waste(supplier).waste is None


@pytest.mark.parametrize(
    "reference,unit,cpc",
    [
        ("municipal solid waste", "kilogram", "17100"),
        ("municipal solid waste", "kilogram", "17300"),
        ("electricity, medium voltage", "kilogram", "17100"),
        ("heat", "cubic meter", "17300"),
        ("biogas", "kilogram", "17200"),
        ("compost", "cubic meter", "391"),
        ("organic nitrogen fertiliser, as N", "kilogram", "39990"),
        ("organic nitrogen fertiliser, as N", "cubic meter", "34659"),
        ("waste fertiliser", "kilogram", "34659"),
        ("treatment service", "kilogram", "94320"),
    ],
)
def test_conflicting_or_unreviewed_positive_products_require_review(reference, unit, cpc):
    data = classified(cpc=cpc)
    data["reference product"] = reference
    data["unit"] = unit
    data["exchanges"][0].update({"reference product": reference, "unit": unit})
    assert resolve_classified_waste(data).waste is None
    data["exchanges"][0]["simapro category"] = "waste treatment/Reviewed"
    assert resolve_classified_waste(data).waste is True


@pytest.mark.parametrize("field,value", [("reference product", "different product"), ("unit", "cubic meter")])
def test_inconsistent_reference_identity_requires_review(field, value):
    data = classified(amount=-1)
    data["exchanges"][0][field] = value
    assert resolve_classified_waste(data).rule == "reference_identity_unresolved"


@pytest.mark.parametrize("amount", [-1, 1])
def test_reviewed_effluent_treatment(amount):
    name = "treatment of effluent from nitrogen trifluoride production, wastewater treatment, class 3"
    reference = "effluent from nitrogen trifluoride production"
    supplier = classified(name=name, isic="3700", cpc="39990", amount=amount)
    supplier.update({"reference product": reference, "unit": "cubic meter", "location": "CH"})
    supplier["exchanges"][0].update({"reference product": reference, "unit": "cubic meter", "location": "CH"})
    resolution = resolve_classified_waste(supplier)
    assert resolution.waste is True
    assert resolution.rule == "reviewed_nf3_effluent_treatment"
    customer = process("consumer")
    customer["exchanges"].append(link(supplier, 3))
    inventory = SimaProInventory.from_data(
        [supplier, customer], background_profile=BackgroundProfile("ecoinvent", "3.12", "cutoff")
    )
    result = inventory.render(category_mode="infer_classifications")
    assert not result.has_errors, result.issues
    assert float(section_rows(result.rows, "Waste treatment")[0][2]) == 1
    assert float(section_rows(result.rows, "Waste to treatment")[0][2]) == -3
    supplier["exchanges"][0]["amount"] = 1
    supplier["location"] = "RER"
    assert resolve_classified_waste(supplier).waste is None
    supplier["location"] = "CH"
    supplier["exchanges"][0]["simapro category"] = "material/Explicit"
    assert resolve_classified_waste(supplier).waste is False


@pytest.mark.parametrize(
    "names",
    [
        ("market for energy feed", "market for energy feed, organic"),
        ("market for battery capacity, LFP", "market for battery capacity, NMC"),
    ],
)
def test_qualified_markets_and_links_keep_distinct_labels(names):
    suppliers = [process(name) for name in names]
    for supplier in suppliers:
        supplier["reference product"] = "shared product"
        supplier["exchanges"][0]["reference product"] = "shared product"
    customer = process("consumer")
    customer["exchanges"].extend(link(s, 1) for s in suppliers)
    inventory = SimaProInventory.from_data(
        [*suppliers, customer], background_profile=BackgroundProfile("ecoinvent", "3.12", "cutoff")
    )
    result = inventory.render()
    assert not result.has_errors, result.issues
    products = [r[0] for r in section_rows(result.rows, "Products")][:2]
    links = [r[0] for r in section_rows(result.rows, "Materials/fuels")]
    assert len(set(products)) == 2
    assert links == products


def test_supplier_label_collision_blocks_export():
    suppliers = [process("supplier"), process("Supplier")]
    for supplier in suppliers:
        supplier["reference product"] = "shared product"
        supplier["exchanges"][0]["reference product"] = "shared product"
    inventory = SimaProInventory.from_data(
        suppliers, background_profile=BackgroundProfile("ecoinvent", "3.12", "cutoff")
    )
    result = inventory.render()
    assert result.has_errors and not result.rows
    assert any("Ambiguous SimaPro supplier label" in i.message for i in result.issues)


@pytest.mark.parametrize("waste", [False, True])
def test_folder_path_is_independent_of_waste_status(waste):
    category = "waste treatment" if waste else "material"
    data = process("supplier", category=category + "/Old")
    data["simapro category path"] = "01 - Agriculture/011 - Crops/0111 - Cereals"
    before = deepcopy(data)
    inventory = SimaProInventory.from_data([data], background_profile=BackgroundProfile("ecoinvent", "3.12", "cutoff"))
    result = inventory.render()
    assert not result.has_errors, result.issues
    rows = section_rows(result.rows, "Waste treatment" if waste else "Products")
    assert rows[0][4 if waste else 5] == "01 - Agriculture\\011 - Crops\\0111 - Cereals"
    assert inventory.data == [before]


def test_folder_path_does_not_resolve_ambiguous_waste_status():
    data = classified(name="market for waste paper", cpc="39240")
    data["simapro category path"] = "38 - Waste/382 - Treatment"
    inventory = SimaProInventory.from_data([data], background_profile=BackgroundProfile("ecoinvent", "3.12", "cutoff"))
    result = inventory.render(category_mode="infer_classifications")
    assert result.has_errors and not result.rows
    assert any(i.code == "simapro_waste_unresolved" for i in result.issues)
