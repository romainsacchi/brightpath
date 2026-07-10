import csv
from copy import deepcopy

import bw2io
import pytest

from brightpath import (
    BackgroundProfile,
    BrightwayInventory,
    InventoryFormat,
    SimaProInventory,
    SimaProSerializationError,
)
from brightpath.core import MigrationPolicy
from brightpath.formats.simapro_csv import normalize_simapro_import_data


def profile(version="3.9"):
    return BackgroundProfile("ecoinvent", version, "cutoff")


def minimal_activity(extra_exchanges=None, **overrides):
    activity = {
        "name": "test process",
        "reference product": "test product",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [
            {
                "type": "production",
                "name": "test process",
                "reference product": "test product",
                "product": "test product",
                "location": "GLO",
                "unit": "kilogram",
                "amount": 1.0,
                "simapro category": "Materials/Test",
            }
        ],
    }
    activity.update(overrides)
    if extra_exchanges:
        activity["exchanges"].extend(extra_exchanges)
    return activity


def flatten_rows(rows):
    return [cell for row in rows for cell in row]


def test_format_facades_share_a_copy_on_write_document():
    data = [minimal_activity()]
    source = deepcopy(data)
    brightway = BrightwayInventory.from_data(data, background_profile=profile())

    simapro = brightway.to_simapro()
    converted_back = simapro.to_brightway()

    assert isinstance(simapro, SimaProInventory)
    assert isinstance(converted_back, BrightwayInventory)
    assert brightway.inventory_format is InventoryFormat.BRIGHTWAY_EXCEL
    assert simapro.inventory_format is InventoryFormat.SIMAPRO_CSV
    assert converted_back.inventory_format is InventoryFormat.BRIGHTWAY_EXCEL
    assert simapro.background_profile == profile()
    assert simapro.data == source
    assert converted_back.data == source
    assert data == source


def test_from_data_preserves_metadata_and_parameters():
    inventory = SimaProInventory.from_data(
        [minimal_activity()],
        background_profile=profile(),
        database_name="test-db",
        metadata={"owner": "BrightPath"},
        database_parameters=[{"name": "database parameter", "amount": 2}],
        project_parameters=[{"name": "project parameter", "amount": 3}],
    )

    assert inventory.database_name == "test-db"
    assert inventory.metadata == {"owner": "BrightPath"}
    assert inventory.database_parameters == [{"name": "database parameter", "amount": 2}]
    assert inventory.project_parameters == [{"name": "project parameter", "amount": 3}]


def test_render_reports_format_specific_requirements():
    missing_category = minimal_activity()
    del missing_category["exchanges"][0]["simapro category"]
    unsupported_unit = minimal_activity()
    unsupported_unit["exchanges"][0]["unit"] = "unknown unit"

    category_result = SimaProInventory.from_data(
        [missing_category],
        background_profile=profile(),
    ).render()
    unit_result = SimaProInventory.from_data(
        [unsupported_unit],
        background_profile=profile(),
    ).render()

    assert category_result.rows == []
    assert {issue.code for issue in category_result.issues} == {"simapro_category_missing"}
    assert unit_result.rows == []
    assert {issue.code for issue in unit_result.issues} == {"simapro_unit_unsupported"}


def test_render_returns_structured_errors_for_malformed_inventory():
    malformed = SimaProInventory.from_data(
        [{}],
        background_profile=profile(),
        database_parameters=[{"name": "missing amount"}],
    )

    result = malformed.render()

    assert result.rows == []
    assert {issue.code for issue in result.issues} == {
        "simapro_inventory_invalid",
        "simapro_parameters_invalid",
    }


def test_render_includes_metadata_technosphere_and_biosphere_rows():
    activity = minimal_activity(
        extra_exchanges=[
            {
                "type": "technosphere",
                "name": "market for product",
                "reference product": "product",
                "location": "CH",
                "unit": "kilogram",
                "amount": 2.0,
                "uncertainty type": 3,
                "scale": 0.2,
                "min": 0.1,
                "max": 3.0,
                "comment": "input comment",
            },
            {
                "type": "technosphere",
                "name": "market for electricity",
                "reference product": "electricity",
                "location": "CH",
                "unit": "kilowatt hour",
                "amount": 3.0,
            },
            {
                "type": "biosphere",
                "name": "Water",
                "categories": ("air", "urban air close to ground"),
                "unit": "cubic meter",
                "amount": 2.0,
            },
        ]
    )
    inventory = SimaProInventory.from_data(
        [activity],
        background_profile=profile(),
        metadata={
            "system description": {
                "name": "Test system",
                "description": "System description text",
            },
            "literature reference": {
                "name": "Test reference",
                "comment": "Reference comment",
            },
        },
    )

    cells = flatten_rows(inventory.render().rows)

    assert "Test system" in cells
    assert "Test reference" in cells
    assert "System description text" in cells
    assert "Reference comment" in cells
    assert "Product {CH}| market for product | Cut-off, U" in cells
    assert "Electricity {CH}| market for electricity | Cut-off, U" in cells
    assert "Normal" in cells
    assert "4.000E-02" in cells
    assert "2.000E+03" in cells
    assert "kg" in cells


def test_render_is_non_mutating_and_reports_unused_exchanges():
    data = [
        minimal_activity(
            extra_exchanges=[
                {
                    "type": "biosphere",
                    "name": "Oxygen",
                    "categories": ("air", "urban air close to ground"),
                    "unit": "kilogram",
                    "amount": 1.0,
                }
            ]
        )
    ]
    source = deepcopy(data)
    inventory = SimaProInventory.from_data(data, background_profile=profile())

    result = inventory.render()

    assert data == source
    assert inventory.data == source
    assert [issue.code for issue in result.issues] == ["simapro_exchange_unused"]
    assert "Oxygen" in result.issues[0].message


def test_waste_treatment_activity_uses_waste_section():
    inventory = SimaProInventory.from_data(
        [minimal_activity(name="treatment of municipal waste", type="waste treatment")],
        background_profile=profile(),
    )

    cells = flatten_rows(inventory.render().rows)

    assert "Waste treatment" in cells
    assert "Products" not in cells
    assert "1.000E+00" in cells


def test_write_csv_adds_suffix_escapes_formula_text_and_overwrites(tmp_path):
    inventory = SimaProInventory.from_data(
        [minimal_activity(comment="@formula")],
        background_profile=profile(),
    )

    first = inventory.write_csv(tmp_path / "inventory", validate=False)
    second = inventory.write_csv(tmp_path / "inventory.csv", validate=False)

    assert first == (tmp_path / "inventory.csv").resolve()
    assert second == first
    with first.open(newline="", encoding="latin-1") as handle:
        cells = [cell for row in csv.reader(handle, delimiter=";") for cell in row]
    assert "'@formula " in cells


def test_write_csv_rejects_wrong_suffix_and_non_latin1_text(tmp_path):
    inventory = SimaProInventory.from_data(
        [minimal_activity(comment="unsupported snowman: \u2603")],
        background_profile=profile(),
    )

    with pytest.raises(ValueError, match=r"\.csv"):
        inventory.write_csv(tmp_path / "inventory.xlsx", validate=False)
    with pytest.raises(SimaProSerializationError, match="Latin-1"):
        inventory.write_csv(tmp_path / "inventory.csv", validate=False)


def test_ecoinvent_csv_round_trip_is_importable_and_preserves_identity(tmp_path):
    data = [
        minimal_activity(
            extra_exchanges=[
                {
                    "type": "technosphere",
                    "name": "market for product",
                    "reference product": "product",
                    "location": "CH",
                    "unit": "kilogram",
                    "amount": 2.0,
                    "uncertainty type": 3,
                    "scale": 0.2,
                    "minimum": 0.1,
                    "maximum": 3.0,
                }
            ]
        )
    ]
    source = SimaProInventory.from_data(data, background_profile=profile())
    path = source.write_csv(tmp_path / "round-trip.csv", validate=False)

    loaded = SimaProInventory.from_csv(
        path,
        background_profile=profile(),
        database_name="round-trip",
    )

    assert loaded.background_profile == profile()
    assert loaded.database_name == "round-trip"
    assert loaded.data[0]["name"] == "test process"
    assert loaded.data[0]["reference product"] == "test product"
    assert loaded.data[0]["location"] == "GLO"
    production, technosphere = loaded.data[0]["exchanges"]
    assert production["simapro category"] == "Materials/Test"
    assert technosphere["name"] == "market for product"
    assert technosphere["reference product"] == "product"
    assert technosphere["uncertainty type"] == 3
    assert technosphere["scale"] == pytest.approx(0.2)


def test_uvek_csv_round_trip_uses_uvek_mapping(tmp_path):
    uvek_profile = BackgroundProfile("bafu", "2025.0", "cut-off")
    data = [
        {
            "name": "Bentonite, at processing",
            "reference product": "Bentonite, at processing",
            "location": "DE",
            "unit": "kilogram",
            "exchanges": [
                {
                    "type": "production",
                    "name": "Bentonite, at processing",
                    "reference product": "Bentonite, at processing",
                    "location": "DE",
                    "unit": "kilogram",
                    "amount": 1.0,
                    "simapro category": "Materials/Test",
                }
            ],
        }
    ]
    source = SimaProInventory.from_data(data, background_profile=uvek_profile)
    path = source.write_csv(tmp_path / "uvek.csv", validate=False)

    assert "Bentonite, at processing/DE U" in path.read_text(encoding="latin-1")
    loaded = SimaProInventory.from_csv(path, background_profile=uvek_profile)
    assert loaded.background_profile == BackgroundProfile("uvek", "2025", "cutoff")
    assert loaded.data[0]["name"] == "Bentonite, at processing"
    assert loaded.data[0]["reference product"] == "Bentonite, at processing"
    assert loaded.data[0]["location"] == "DE"


def test_csv_reader_reports_system_model_mismatch(tmp_path):
    consequential = BackgroundProfile("ecoinvent", "3.9", "consequential")
    path = SimaProInventory.from_data(
        [minimal_activity()],
        background_profile=consequential,
    ).write_csv(tmp_path / "consequential.csv", validate=False)

    loaded = SimaProInventory.from_csv(path, background_profile=profile())
    report = loaded.validate(check_background_links=False)

    assert loaded.metadata["simapro detected system models"] == ["consequential"]
    assert "simapro_system_model_mismatch" in {issue.code for issue in report.issues}


def test_csv_round_trip_preserves_database_project_and_process_parameters(tmp_path):
    activity = minimal_activity(
        parameters=[
            {
                "name": "process_input",
                "amount": 4.0,
                "uncertainty type": 0,
                "comment": "process parameter",
            }
        ]
    )
    source = SimaProInventory.from_data(
        [activity],
        background_profile=profile(),
        database_parameters=[
            {
                "name": "db_input",
                "amount": 2.0,
                "uncertainty type": 0,
                "comment": "database input",
            },
            {
                "name": "db_calculated",
                "formula": "-db_input",
                "comment": "database formula",
            },
        ],
        project_parameters=[
            {
                "name": "project_input",
                "amount": 3.0,
                "uncertainty type": 0,
            }
        ],
    )

    path = source.write_csv(tmp_path / "parameters.csv", validate=False)
    contents = path.read_text(encoding="latin-1")
    loaded = SimaProInventory.from_csv(path, background_profile=profile())

    assert "Database Input parameters" in contents
    assert "Database Calculated parameters" in contents
    assert "Project Input parameters" in contents
    assert ";-db_input;" in contents
    assert ";'-db_input;" not in contents
    assert {parameter["name"] for parameter in loaded.database_parameters} == {
        "db_input",
        "db_calculated",
    }
    assert {parameter["name"] for parameter in loaded.project_parameters} == {"project_input"}
    assert loaded.data[0]["parameters"][0]["name"] == "process_input"


def test_normalize_import_data_converts_all_exchange_types_without_mutating():
    raw = [
        {
            "name": "Electricity {CH}| market for | Cut-off, U",
            "unit": "kilowatt hour",
            "filename": "source.csv",
            "simapro metadata": {
                "Comment": "dataset comment",
                "Category type": "Materials",
            },
            "exchanges": [
                {
                    "type": "production",
                    "name": "Electricity {CH}| market for | Cut-off, U",
                    "unit": "kilowatt hour",
                    "amount": 1.0,
                    "categories": ("Electricity",),
                },
                {
                    "type": "technosphere",
                    "name": "Heat| market | Cut-off, U",
                    "unit": "megajoule",
                    "amount": 2.0,
                    "input": (None, "heat"),
                },
                {
                    "type": "substitution",
                    "name": "Copper| production | Cut-off, U",
                    "unit": "kilogram",
                    "amount": 3.0,
                    "input": (None, "copper"),
                },
                {
                    "type": "biosphere",
                    "name": "Water, lake",
                    "categories": ("natural resource", "unspecified"),
                    "unit": "cubic meter",
                    "amount": 4.0,
                },
            ],
        }
    ]
    before = deepcopy(raw)

    normalized = normalize_simapro_import_data(
        raw,
        background_profile=profile(),
        database_name="converted-db",
        biosphere_flows=[("Water, lake", "natural resource", "in water")],
        biosphere_correspondence={},
        version_mapping={},
    )

    assert raw == before
    dataset = normalized[0]
    assert "filename" not in dataset
    assert dataset["name"] == "market for electricity"
    assert dataset["reference product"] == "electricity"
    assert dataset["location"] == "CH"
    assert dataset["comment"] == "dataset comment"
    assert "Comment" not in dataset["simapro metadata"]
    production, technosphere, substitution, biosphere = dataset["exchanges"]
    assert production["name"] == "market for electricity"
    assert production["reference product"] == "electricity"
    assert production["simapro category"] == "Materials/Electricity"
    assert technosphere["name"] == "market for heat"
    assert technosphere["reference product"] == "heat"
    assert "input" not in technosphere
    assert substitution["type"] == "technosphere"
    assert substitution["amount"] == -3.0
    assert biosphere["categories"] == ("natural resource", "in water")


def test_normalize_import_data_drops_final_waste_but_preserves_zero_and_empty_datasets():
    raw = [
        {
            "name": "Wastewater {GLO}| treatment of | Cut-off, U",
            "unit": "cubic meter",
            "simapro metadata": {"Category type": "Waste treatment"},
            "exchanges": [
                {
                    "type": "production",
                    "name": "Wastewater {GLO}| treatment of | Cut-off, U",
                    "unit": "cubic meter",
                    "amount": 1.0,
                    "categories": ("Wastewater",),
                },
                {
                    "type": "technosphere",
                    "name": "Non-hazardous waste disposed",
                    "categories": "Final waste flows",
                    "unit": "kilogram",
                    "amount": 1000.0,
                },
                {
                    "type": "technosphere",
                    "name": "Heat| market | Cut-off, U",
                    "unit": "megajoule",
                    "amount": 0.0,
                },
            ],
        },
        {"name": "empty", "exchanges": []},
    ]

    normalized = normalize_simapro_import_data(
        raw,
        background_profile=profile(),
        database_name="test",
        biosphere_flows=[],
        biosphere_correspondence={},
        version_mapping={},
    )

    assert len(normalized) == 2
    assert len(normalized[0]["exchanges"]) == 2
    assert normalized[0]["exchanges"][0]["type"] == "production"
    assert normalized[0]["exchanges"][1]["amount"] == 0.0
    assert normalized[1]["exchanges"] == []


def test_normalize_import_data_leaves_duplicate_detection_to_validation():
    raw = [
        {
            "name": name,
            "unit": "kilowatt hour",
            "simapro metadata": {"Category type": "Materials"},
            "exchanges": [
                {
                    "type": "production",
                    "name": name,
                    "unit": "kilowatt hour",
                    "amount": 1.0,
                    "categories": ("Electricity",),
                }
            ],
        }
        for name in (
            "Electricity {CH}| market for | Cut-off, U",
            "electricity {CH}| market for | Cut-off, U",
        )
    ]

    normalized = normalize_simapro_import_data(
        raw,
        background_profile=profile(),
        database_name="test",
        biosphere_flows=[],
        biosphere_correspondence={},
        version_mapping={},
    )
    report = SimaProInventory.from_data(
        normalized,
        background_profile=profile(),
    ).validate(check_background_links=False)

    assert [dataset["name"] for dataset in normalized] == [
        "market for electricity",
        "market for electricity",
    ]
    assert "duplicate_dataset_identity" in {issue.code for issue in report.issues}


def test_simapro_background_migration_is_explicit_and_bidirectional():
    exchange = {
        "name": "aluminium production, primary, ingot",
        "reference product": "aluminium, primary, ingot",
        "location": "RNA",
        "unit": "kilogram",
        "amount": 2.0,
        "type": "technosphere",
    }
    source = SimaProInventory.from_data(
        [minimal_activity(extra_exchanges=[exchange])],
        background_profile=profile("3.6"),
    )

    forward = source.migrate_background(profile("3.7"))
    backward = forward.migrate_background(
        profile("3.6"),
        policy=MigrationPolicy.permissive(),
    )

    assert isinstance(forward, SimaProInventory)
    assert source.data[0]["exchanges"][1]["location"] == "RNA"
    assert forward.data[0]["exchanges"][1]["location"] == "CA"
    assert backward.data[0]["exchanges"][1]["location"] == "RNA"
    assert "migration.technosphere_step_applied" in {
        change.code for change in forward.last_migration_report.changes
    }


def test_simapro_to_brightway_writes_importable_excel(tmp_path):
    source = SimaProInventory.from_data(
        [minimal_activity()],
        background_profile=profile(),
        database_name="converted-db",
    )

    output = source.to_brightway().write_excel(tmp_path / "converted", validate=False)
    importer = bw2io.ExcelImporter(output)

    assert importer.db_name == "converted-db"
    assert importer.data[0]["name"] == "test process"
    assert importer.data[0]["reference product"] == "test product"


def test_from_csv_validates_path_and_suffix(tmp_path):
    with pytest.raises(FileNotFoundError):
        SimaProInventory.from_csv(tmp_path / "missing.csv", background_profile=profile())

    wrong_suffix = tmp_path / "inventory.txt"
    wrong_suffix.write_text("not simapro", encoding="utf-8")
    with pytest.raises(ValueError, match=r"\.csv"):
        SimaProInventory.from_csv(wrong_suffix, background_profile=profile())
