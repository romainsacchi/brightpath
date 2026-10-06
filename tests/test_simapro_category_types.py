from copy import deepcopy

import pytest
from test_simapro_classified_waste import classified
from test_simapro_inventory import row_after

from brightpath import BackgroundProfile, SimaProInventory


@pytest.mark.parametrize("version", ["3.8", "3.9.1", "3.12"])
@pytest.mark.parametrize("system_model", ["cutoff", "consequential"])
@pytest.mark.parametrize(
    "name,reference,unit,isic,cpc,expected",
    [
        ("electricity production", "electricity, high voltage", "kilowatt hour", "3510", "17100", "energy"),
        ("heat production", "heat, district or industrial", "megajoule", "3530", "17300", "energy"),
        ("transport service", "transport, freight, lorry", "ton kilometer", "4941", "65119", "transport"),
        ("steel rolling", "steel rolling", "kilogram", "2410", "88421", "processing"),
        ("machine operation", "operation, machine", "hour", "3312", "87159", "use"),
        ("metal production", "metal", "kilogram", "2410", "412", "material"),
    ],
)
def test_non_waste_category_types_are_not_collapsed(version, system_model, name, reference, unit, isic, cpc, expected):
    data = classified(name, isic, cpc)
    data["reference product"] = reference
    data["unit"] = unit
    data["exchanges"][0].update({"reference product": reference, "unit": unit})
    data["simapro category path"] = "ISIC/Folder"
    before = deepcopy(data)
    inv = SimaProInventory.from_data([data], background_profile=BackgroundProfile("ecoinvent", version, system_model))
    result = inv.render(category_mode="infer_classifications")
    assert not result.has_errors, result.issues
    assert row_after(result.rows, "Category type") == [expected]
    assert row_after(result.rows, "Products")[5] == "ISIC\\Folder"
    assert data == before


def test_non_waste_without_product_role_needs_review():
    data = classified("unclassified service", "9999", "")
    data["unit"] = "hour"
    data["exchanges"][0]["unit"] = "hour"
    result = SimaProInventory.from_data(
        [data], background_profile=BackgroundProfile("ecoinvent", "3.12", "cutoff")
    ).render(category_mode="infer_classifications")
    assert result.has_errors and not result.rows
    assert any(i.code == "simapro_category_type_unresolved" for i in result.issues)


def test_native_explicit_type_precedes_inference():
    data = classified("activity", "2410", "412")
    data["simapro metadata"] = {"Category type": "processing"}
    result = SimaProInventory.from_data(
        [data], background_profile=BackgroundProfile("ecoinvent", "3.12", "cutoff")
    ).render(category_mode="infer_classifications")
    assert not result.has_errors, result.issues
    assert row_after(result.rows, "Category type") == ["processing"]


@pytest.mark.parametrize(
    "cpc,amount,unit,expected",
    [
        ("39240", 1, "kilogram", "material"),
        ("39310", 1, "kilogram", "material"),
        ("39240", -1, "kilogram", "waste treatment"),
        ("39990", 1, "kilogram", None),
        ("39240", 1, "hour", None),
    ],
)
def test_recyclable_market_role_uses_product_class_and_sign(cpc, amount, unit, expected):
    data = classified("market for recyclable material", "3830", cpc, amount)
    data["unit"] = unit
    data["exchanges"][0]["unit"] = unit
    result = SimaProInventory.from_data(
        [data], background_profile=BackgroundProfile("ecoinvent", "3.12", "cutoff")
    ).render(category_mode="infer_classifications")
    if expected is None:
        assert result.has_errors and not result.rows
    else:
        assert not result.has_errors, result.issues
        assert row_after(result.rows, "Category type") == [expected]


@pytest.mark.parametrize("demand", [-3.0, 3.0])
def test_positive_recyclable_market_preserves_customer_sign(demand):
    from test_simapro_waste_export import link, process, section_rows

    supplier = classified("market for waste paperboard", "3830", "39240", 1)
    customer = process("paperboard user")
    customer["exchanges"].append(link(supplier, demand))
    result = SimaProInventory.from_data(
        [supplier, customer], background_profile=BackgroundProfile("ecoinvent", "3.12", "cutoff")
    ).render(category_mode="infer_classifications")
    assert not result.has_errors, result.issues
    assert not section_rows(result.rows, "Waste treatment")
    assert not section_rows(result.rows, "Waste to treatment")
    assert float(section_rows(result.rows, "Materials/fuels")[0][2]) == demand
