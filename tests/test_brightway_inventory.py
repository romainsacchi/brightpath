from copy import deepcopy

import pytest

import brightpath
from brightpath import BackgroundProfile, BrightwayInventory, InventoryFormat


def minimal_inventory(*extra_exchanges, **overrides):
    activity = {
        "name": "foreground service",
        "reference product": "service",
        "location": "GLO",
        "unit": "unit",
        "exchanges": [
            {
                "name": "foreground service",
                "reference product": "service",
                "product": "service",
                "location": "GLO",
                "unit": "unit",
                "amount": 1.0,
                "type": "production",
            },
            *extra_exchanges,
        ],
    }
    activity.update(overrides)
    return [activity]


def profile(version="3.10"):
    return BackgroundProfile("ecoinvent", version, "cutoff")


def test_v1_public_api_removes_converter_exports():
    assert brightpath.__version__ == (1, 0, 0)
    assert brightpath.BrightwayInventory is BrightwayInventory
    assert not hasattr(brightpath, "BrightwayConverter")
    assert not hasattr(brightpath, "SimaproConverter")
    assert InventoryFormat.OPENLCA_EXCEL.value == "openlca_excel"
    assert InventoryFormat.ECOSPOLD2.value == "ecospold2"


def test_background_profile_normalizes_legacy_aliases_and_patch_versions():
    assert BackgroundProfile("BAFU", "2025.0", "cut-off").normalized() == BackgroundProfile("uvek", "2025", "cutoff")
    assert BackgroundProfile("ECOINVENT", "3.10.1", "cut-off").normalized() == BackgroundProfile(
        "ecoinvent", "3.10", "cutoff"
    )


def test_normalize_is_copy_on_write():
    data = minimal_inventory()
    data[0]["product"] = data[0].pop("reference product")
    data[0]["exchanges"][0]["product"] = data[0]["exchanges"][0].pop("reference product")
    source = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, background_profile=profile())

    normalized = inventory.normalize()

    assert data == source
    assert "reference product" not in inventory.data[0]
    assert normalized.data[0]["reference product"] == "service"
    assert normalized.data[0]["exchanges"][0]["reference product"] == "service"


def test_validate_is_read_only_and_checks_catalog_links():
    exchange = {
        "name": "market for electricity, low voltage",
        "reference product": "electricity, low voltage",
        "location": "CH",
        "unit": "kilowatt hour",
        "amount": 2.0,
        "type": "technosphere",
    }
    inventory = BrightwayInventory.from_data(
        minimal_inventory(exchange),
        background_profile=profile(),
    )
    before = inventory.data

    report = inventory.validate()

    assert report.is_valid
    assert inventory.data == before


def test_validate_accepts_foreground_specific_units():
    data = minimal_inventory(unit="work-hours")
    data[0]["exchanges"][0]["unit"] = "work-hours"
    inventory = BrightwayInventory.from_data(data, background_profile=profile())

    report = inventory.validate()

    assert report.is_valid


def test_validate_reports_production_identity_mismatch_without_mutating():
    data = minimal_inventory()
    data[0]["exchanges"][0]["location"] = "CH"
    inventory = BrightwayInventory.from_data(data, background_profile=profile())

    report = inventory.validate()

    assert "production_identity_mismatch" in {issue.code for issue in report.issues}
    assert inventory.data[0]["exchanges"][0]["location"] == "CH"


def test_validate_reports_duplicate_dataset_identities():
    data = minimal_inventory() * 2
    inventory = BrightwayInventory.from_data(data, background_profile=profile())

    report = inventory.validate()

    assert "duplicate_dataset_identity" in {issue.code for issue in report.issues}


def test_excel_round_trip_preserves_profile_parameters_and_extra_fields(tmp_path):
    data = minimal_inventory(
        parameters=[
            {
                "name": "efficiency",
                "amount": 0.9,
                "group": "foreground",
                "provenance": {"source": "test"},
            }
        ],
    )
    data[0]["custom metadata"] = {"nested": [1, 2, 3]}
    data[0]["exchanges"][0]["custom_none"] = None
    data[0]["exchanges"][0]["custom_mapping"] = {"source": ["a", "b"]}
    inventory = BrightwayInventory.from_data(
        data,
        background_profile=BackgroundProfile("bafu", "2025", "cut-off"),
        database_name="round-trip",
        metadata={"owner": {"name": "BrightPath"}},
        database_parameters=[{"name": "db_parameter", "amount": 3.0}],
        project_parameters=[{"name": "project_parameter", "amount": 4.0}],
    )

    output = inventory.write_excel(tmp_path / "round-trip", validate=False)
    loaded = BrightwayInventory.from_excel(output)

    assert output == (tmp_path / "round-trip.xlsx").resolve()
    assert loaded.background_profile == BackgroundProfile("uvek", "2025", "cutoff")
    assert loaded.metadata["owner"] == {"name": "BrightPath"}
    assert loaded.data[0]["custom metadata"] == {"nested": [1, 2, 3]}
    assert loaded.data[0]["exchanges"][0]["custom_none"] is None
    assert loaded.data[0]["exchanges"][0]["custom_mapping"] == {"source": ["a", "b"]}
    assert loaded.data[0]["parameters"][0]["provenance"] == {"source": "test"}
    assert loaded.database_parameters[0]["name"] == "db_parameter"
    assert loaded.project_parameters[0]["name"] == "project_parameter"


def test_write_excel_rejects_non_xlsx_suffix(tmp_path):
    inventory = BrightwayInventory.from_data(
        minimal_inventory(),
        background_profile=profile(),
    )

    with pytest.raises(ValueError, match=r"\.xlsx"):
        inventory.write_excel(tmp_path / "inventory.csv", validate=False)
