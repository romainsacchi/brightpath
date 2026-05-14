import csv

import pytest

from brightpath import BrightwayConverter


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


def test_constructor_requires_exactly_one_inventory_source():
    with pytest.raises(ValueError, match="Provide either"):
        BrightwayConverter()

    with pytest.raises(ValueError, match="not both"):
        BrightwayConverter(filepath="inventory.xlsx", data=[])


def test_constructor_validates_direct_inventory_data():
    invalid = minimal_activity()
    invalid["exchanges"][0]["unit"] = "not a unit"

    with pytest.raises(ValueError, match="unknown exchange unit"):
        BrightwayConverter(data=[invalid])

    invalid = minimal_activity()
    del invalid["exchanges"][0]["simapro category"]

    with pytest.raises(ValueError, match="simapro category"):
        BrightwayConverter(data=[invalid])


def test_convert_to_simapro_rejects_unknown_database_and_format():
    converter = BrightwayConverter(data=[minimal_activity()])

    with pytest.raises(ValueError, match="Database"):
        converter.convert_to_simapro(database="unknown", format="data")

    with pytest.raises(ValueError, match="Format"):
        converter.convert_to_simapro(format="json")


def test_convert_to_simapro_includes_metadata_sections(tmp_path):
    metadata = tmp_path / "metadata.yaml"
    metadata.write_text(
        "\n".join(
            [
                "system description:",
                "  name: Test system",
                "  description: System description text",
                "literature reference:",
                "  name: Test reference",
                "  comment: Reference comment",
            ]
        ),
        encoding="utf-8",
    )
    converter = BrightwayConverter(data=[minimal_activity()], metadata=metadata)

    rows = converter.convert_to_simapro(format="data")
    cells = flatten_rows(rows)

    assert "Test system" in cells
    assert "Test reference" in cells
    assert "System description text" in cells
    assert "Reference comment" in cells


def test_convert_to_simapro_formats_technosphere_and_biosphere_rows():
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
    converter = BrightwayConverter(data=[activity])

    rows = converter.convert_to_simapro(format="data")
    cells = flatten_rows(rows)

    assert "Product {CH}| market for product | Cut-off, U" in cells
    assert "Electricity {CH}| market for electricity | Cut-off, U" in cells
    assert "Normal" in cells
    assert "4.000E-02" in cells
    assert "2.000E+03" in cells
    assert "kg" in cells


def test_waste_treatment_activity_uses_waste_treatment_section():
    converter = BrightwayConverter(
        data=[
            minimal_activity(
                name="treatment of municipal waste",
                type="waste treatment",
                extra_exchanges=[],
            )
        ]
    )

    rows = converter.convert_to_simapro(format="data")
    cells = flatten_rows(rows)

    assert "Waste treatment" in cells
    assert "Products" not in cells
    assert "1.000E+00" in cells


def test_convert_to_simapro_writes_csv_file(tmp_path):
    converter = BrightwayConverter(data=[minimal_activity(comment="@formula")], export_dir=tmp_path)

    message = converter.convert_to_simapro()
    [csv_path] = tmp_path.glob("simapro_ecoinvent_*.csv")

    assert str(csv_path) in message
    with open(csv_path, newline="", encoding="utf-8") as handle:
        cells = [cell for row in csv.reader(handle, delimiter=";") for cell in row]
    assert "'@formula " in cells


def test_convert_to_simapro_uses_explicit_filename_and_avoids_collisions(tmp_path):
    converter = BrightwayConverter(data=[minimal_activity()], export_dir=tmp_path)

    first = converter.convert_to_simapro(filename="export.csv")
    second = converter.convert_to_simapro(filename="export.csv")

    assert str(tmp_path / "export.csv") in first
    assert str(tmp_path / "export_1.csv") in second
    assert (tmp_path / "export.csv").exists()
    assert (tmp_path / "export_1.csv").exists()


def test_convert_to_simapro_records_unused_exchanges_without_printing():
    converter = BrightwayConverter(
        data=[
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
    )

    converter.convert_to_simapro(format="data")

    assert converter.unused_exchanges == [
        {
            "activity": "test process",
            "exchange": "Oxygen",
            "unit": "kilogram",
            "location": "GLO",
            "categories": ("air", "urban air close to ground"),
        }
    ]
