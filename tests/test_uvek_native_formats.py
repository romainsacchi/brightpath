"""Native UVEK compartments survive format boundaries without background migration."""

from copy import deepcopy

import pytest

from brightpath import BackgroundProfile, BiosphereProfile, BrightwayInventory, SimaProInventory
from brightpath.formats.openlca_jsonld import load_openlca_jsonld, write_openlca_jsonld
from brightpath.models import InventoryDocument
from brightpath.profiles.biosphere_categories import restore_native_categories


def native_inventory(root="emissions to air", name="Carbon dioxide, fossil"):
    foreground = {"name": "Synthetic foreground", "reference product": "product", "location": "CH", "unit": "kilogram"}
    return BrightwayInventory.from_data(
        [
            {
                **foreground,
                "comment": "Synthetic native UVEK format fixture.",
                "exchanges": [
                    {**foreground, "type": "production", "amount": 1, "simapro category": "Materials/Test"},
                    {
                        "name": name,
                        "unit": "kilogram",
                        "categories": (root, "unspecified"),
                        "type": "biosphere",
                        "amount": 2,
                    },
                ],
            }
        ],
        background_profile=BackgroundProfile("uvek", "2026", "cutoff"),
    )


@pytest.mark.parametrize("software", ["brightway", "simapro", "openlca"])
def test_native_compartments_validate_and_round_trip(tmp_path, software):
    inventory = native_inventory()
    before = deepcopy(inventory.data)
    assert not inventory.validate().has_errors
    if software == "brightway":
        path = inventory.write_excel(tmp_path / "native.xlsx")
        restored = BrightwayInventory.from_excel(path, background_profile=inventory.background_profile)
    elif software == "simapro":
        path = inventory.to_simapro().write_csv(tmp_path / "native.csv")
        restored = SimaProInventory.from_csv(path, background_profile=inventory.background_profile)
    else:
        path = write_openlca_jsonld(
            InventoryDocument(data=inventory.data, context=inventory.context), tmp_path / "native.zip"
        )
        restored = load_openlca_jsonld(path, background_profile=inventory.background_profile)
    flow = restored.data[0]["exchanges"][1]
    assert tuple(flow["categories"]) == ("emissions to air", "unspecified")
    assert (flow["name"], flow["unit"], flow["amount"]) == ("Carbon dioxide, fossil", "kilogram", 2)
    assert inventory.data == before


def test_exact_native_catalog_match_is_required_to_restore_wire_categories():
    data = [
        {
            "exchanges": [
                {"type": "biosphere", "name": "Carbon dioxide, fossil", "unit": "kilogram", "categories": ("air",)},
                {"type": "biosphere", "name": "Not a native flow", "unit": "kilogram", "categories": ("air",)},
            ]
        }
    ]
    original = deepcopy(data)
    restore_native_categories(data, BiosphereProfile("ecoinvent", "3.10"))
    assert data == original
    restore_native_categories(data, BiosphereProfile("uvek", "2026"))
    assert data[0]["exchanges"][0]["categories"] == ("emissions to air", "unspecified")
    assert data[0]["exchanges"][1] == original[0]["exchanges"][1]


def test_native_unknown_compartment_does_not_bypass_exact_catalog_validation():
    assert native_inventory(root="economic issues").validate().has_errors


def test_supplied_catalog_and_ambiguity_are_respected():
    data = [
        {
            "exchanges": [
                {"type": "biosphere", "name": "Custom flow", "categories": ("air",), "unit": "kilogram"},
            ]
        }
    ]
    profile = BiosphereProfile("uvek", "2026")
    identity = ("Custom flow", ("emissions to air", "unspecified"), "kilogram")
    before = deepcopy(data)
    with pytest.raises(ValueError, match="Ambiguous native"):
        restore_native_categories(
            data, profile, identities=[identity, (identity[0], ("emissions to air",), identity[2])]
        )
    assert data == before
    restore_native_categories(data, profile, identities=[identity])
    assert data[0]["exchanges"][0]["categories"] == identity[1]


def test_simapro_does_not_discard_unrepresentable_native_compartments(tmp_path):
    from brightpath.background import PackageCatalogProvider
    from brightpath.exceptions import InventoryValidationError, SimaProSerializationError

    identity = next(
        row
        for row in PackageCatalogProvider().load_biosphere(BiosphereProfile("uvek", "2026")).identities
        if row[1][0] == "economic issues"
    )
    inventory = native_inventory()
    data = inventory.data
    data[0]["exchanges"][1].update(name=identity[0], categories=identity[1], unit=identity[2])
    inventory = BrightwayInventory.from_data(data, background_profile=inventory.background_profile)
    assert not inventory.validate().has_errors
    with pytest.raises((InventoryValidationError, SimaProSerializationError, ValueError)):
        inventory.to_simapro().write_csv(tmp_path / "unsupported.csv")
    assert not (tmp_path / "unsupported.csv").exists()
