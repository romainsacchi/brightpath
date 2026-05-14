from types import SimpleNamespace

import pytest

from brightpath import simaproconverter
from brightpath.simaproconverter import (
    SimaproConverter,
    format_biosphere_exchange,
    format_technosphere_exchange,
    load_ecoinvent_activities,
)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        (
            "Electricity {CH}| market for | Cut-off, U",
            ("market for electricity", "electricity", "CH"),
        ),
        (
            "electricity {WECC, US only}| market for | Cut-off, U",
            ("market for electricity", "electricity", "US-WECC"),
        ),
        (
            "Aluminium, primary| production | Cut-off, U",
            ("aluminium production, primary", "aluminium, primary", "GLO"),
        ),
        (
            "waste plastic {GLO}| treatment of | Cut-off, U",
            ("treatment of waste plastic", "waste plastic", "GLO"),
        ),
        (
            "Copper {French Guiana}| production | Cut-off, U",
            ("copper production", "copper", "FG"),
        ),
    ],
)
def test_format_technosphere_exchange_parses_common_simapro_names(text, expected):
    assert format_technosphere_exchange(text) == expected


def test_format_technosphere_exchange_rejects_malformed_names():
    with pytest.raises(ValueError, match="empty"):
        format_technosphere_exchange("")

    with pytest.raises(ValueError, match="too many fields"):
        format_technosphere_exchange("a|b|c|d|e")

    with pytest.raises(ValueError, match="malformed.*braces"):
        format_technosphere_exchange("Electricity {CH| market for | Cut-off, U")

    with pytest.raises(ValueError, match="empty location"):
        format_technosphere_exchange("Electricity {}| market for | Cut-off, U")


def test_load_ecoinvent_activities_validates_version_and_loads_file(tmp_path, monkeypatch):
    export_dir = tmp_path / "export"
    export_dir.mkdir()
    (export_dir / "list_ei3.9_cutoff_activities.csv").write_text(
        "name,product,location\nmarket,electricity,CH\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(simaproconverter, "DATA_DIR", tmp_path)

    assert load_ecoinvent_activities("3.9") == [["market", "electricity", "CH"]]

    with pytest.raises(ValueError, match="Unsupported ecoinvent version"):
        load_ecoinvent_activities("../3.9")


def test_format_biosphere_exchange_normalizes_water_and_in_ground_flows():
    water = {
        "name": "Water, lake",
        "categories": ("natural resource", "unspecified"),
    }
    result = format_biosphere_exchange(
        water,
        "3.9",
        [("Water, lake", "natural resource", "in water")],
        {},
    )
    assert result["categories"] == ("natural resource", "in water")

    cadmium = {
        "name": "Cadmium, in ground",
        "categories": ("natural resource", "unspecified"),
    }
    result = format_biosphere_exchange(
        cadmium,
        "3.9",
        [("Cadmium", "natural resource", "in ground")],
        {},
    )
    assert result["name"] == "Cadmium"
    assert result["categories"] == ("natural resource", "in ground")


def test_format_biosphere_exchange_returns_copy_and_validates_categories():
    exchange = {
        "name": "Water, lake",
        "categories": ("natural resource", "unspecified"),
    }

    result = format_biosphere_exchange(
        exchange,
        "3.9",
        [("Water, lake", "natural resource", "in water")],
        {},
    )

    assert result is not exchange
    assert result["categories"] == ("natural resource", "in water")
    assert exchange["categories"] == ("natural resource", "unspecified")

    with pytest.raises(ValueError, match="missing categories"):
        format_biosphere_exchange({"name": "Flow"}, "3.9", [], {})

    with pytest.raises(ValueError, match="unsupported category"):
        format_biosphere_exchange({"name": "Flow", "categories": ("ocean",)}, "3.9", [], {})


def test_format_biosphere_exchange_applies_correspondence_mapping():
    exchange = {
        "name": "Old flow",
        "categories": ("air", "urban air close to ground"),
    }

    result = format_biosphere_exchange(
        exchange,
        "3.9",
        [("New flow", "air", "urban air close to ground")],
        {"air": {"Old flow": "New flow"}},
    )

    assert result["name"] == "New flow"


def make_converter(data, db_name="test_db"):
    converter = SimaproConverter.__new__(SimaproConverter)
    converter.i = SimpleNamespace(data=data, db_name=db_name)
    converter.db_name = db_name
    converter.ecoinvent_version = "3.9"
    converter.ei_biosphere_flows = [("Water, lake", "natural resource", "in water")]
    converter.biosphere_flows_correspondence = {}
    return converter


def test_check_database_name_fills_internal_inputs():
    data = [
        {
            "database": None,
            "exchanges": [
                {"type": "production", "input": (None, "activity")},
                {"type": "technosphere", "input": (None, "market")},
                {"type": "biosphere", "input": (None, "water")},
            ],
        }
    ]
    converter = make_converter(data, db_name="filled_db")

    converter.check_database_name()

    assert data[0]["database"] == "filled_db"
    assert data[0]["exchanges"][0]["input"] == ("filled_db", "activity")
    assert data[0]["exchanges"][1]["input"] == ("filled_db", "market")
    assert data[0]["exchanges"][2]["input"] == (None, "water")


def test_remove_empty_datasets_and_exchanges():
    data = [
        {"exchanges": []},
        {"exchanges": [{"type": "technosphere", "amount": 0.0}, {"type": "technosphere", "amount": 1.0}]},
    ]
    converter = make_converter(data)

    converter.remove_empty_datasets()
    converter.remove_empty_exchanges()

    assert converter.i.data == [{"exchanges": [{"type": "technosphere", "amount": 1.0}]}]


def test_check_inventories_raises_for_invalid_inventory():
    converter = make_converter(
        [
            {
                "name": "activity",
                "reference product": "product",
                "location": "GLO",
                "exchanges": [{"type": "technosphere", "amount": 1.0}],
            }
        ]
    )

    with pytest.raises(ValueError, match="exactly one production flow"):
        converter.check_inventories()


def test_convert_to_brightway_formats_dataset_and_exchanges():
    data = [
        {
            "name": "Electricity {CH}| market for | Cut-off, U",
            "simapro metadata": {"Comment": "dataset comment"},
            "exchanges": [
                {
                    "type": "production",
                    "name": "Electricity {CH}| market for | Cut-off, U",
                    "amount": 1.0,
                    "input": (None, "production"),
                },
                {
                    "type": "technosphere",
                    "name": "Heat| market | Cut-off, U",
                    "amount": 2.0,
                    "input": (None, "heat"),
                },
                {
                    "type": "substitution",
                    "name": "Copper| production | Cut-off, U",
                    "amount": 3.0,
                    "input": (None, "copper"),
                },
                {
                    "type": "biosphere",
                    "name": "Water, lake",
                    "categories": ("natural resource", "unspecified"),
                    "amount": 4.0,
                },
            ],
        }
    ]
    converter = make_converter(data, db_name="converted_db")

    converter.convert_to_brightway()

    dataset = converter.i.data[0]
    assert dataset["name"] == "market for electricity"
    assert dataset["reference product"] == "electricity"
    assert dataset["location"] == "CH"
    assert dataset["comment"] == "dataset comment"
    assert "Comment" not in dataset["simapro metadata"]

    production, technosphere, substitution, biosphere = dataset["exchanges"]
    assert production["name"] == "market for electricity"
    assert production["product"] == "electricity"
    assert production["input"] == ("converted_db", "production")
    assert technosphere["name"] == "market for heat"
    assert technosphere["product"] == "heat"
    assert substitution["type"] == "technosphere"
    assert substitution["amount"] == -3.0
    assert biosphere["categories"] == ("natural resource", "in water")


def test_convert_to_brightway_rejects_duplicate_parsed_datasets():
    data = [
        {
            "name": "Electricity {CH}| market for | Cut-off, U",
            "exchanges": [
                {
                    "type": "production",
                    "name": "Electricity {CH}| market for | Cut-off, U",
                    "amount": 1.0,
                }
            ],
        },
        {
            "name": "electricity {CH}| market for | Cut-off, U",
            "exchanges": [
                {
                    "type": "production",
                    "name": "electricity {CH}| market for | Cut-off, U",
                    "amount": 1.0,
                }
            ],
        },
    ]
    converter = make_converter(data)

    with pytest.raises(ValueError, match="Duplicate datasets"):
        converter.convert_to_brightway()
