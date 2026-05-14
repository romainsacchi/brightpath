import copy
import csv
from pathlib import Path

import pytest

from brightpath import BrightwayConverter
from brightpath.simaproconverter import format_technosphere_exchange, load_ecoinvent_activities
from brightpath.utils import check_simapro_inventory, ensure_unique_datasets


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


def test_brightway_converter_accepts_data_without_metadata_or_export_dir():
    converter = BrightwayConverter(data=[minimal_activity()])

    rows = converter.convert_to_simapro(format="data")

    assert rows


def test_brightway_conversion_does_not_mutate_input_data():
    data = [
        minimal_activity(
            extra_exchanges=[
                {
                    "type": "biosphere",
                    "name": "Water",
                    "categories": ("air", "urban air close to ground"),
                    "unit": "cubic meter",
                    "amount": 2.0,
                }
            ]
        )
    ]
    original = copy.deepcopy(data)

    BrightwayConverter(data=data).convert_to_simapro(format="data")

    assert data == original


def test_simapro_csv_export_escapes_formula_like_text(tmp_path):
    converter = BrightwayConverter(
        data=[minimal_activity(comment="=1+1")],
        export_dir=tmp_path,
    )

    converter.convert_to_simapro()
    [csv_path] = tmp_path.glob("simapro_ecoinvent_*.csv")

    with open(csv_path, newline="", encoding="utf-8") as handle:
        cells = [cell for row in csv.reader(handle, delimiter=";") for cell in row]

    assert any(cell.startswith("'=1+1") for cell in cells)


def test_check_simapro_inventory_does_not_write_sibling_by_default(tmp_path):
    source = tmp_path / "Inventory.CSV"
    source.write_text("Name;Unit\nTransport;min\n", encoding="latin-1")

    cleaned = Path(check_simapro_inventory(source))

    try:
        assert cleaned.exists()
        assert cleaned != source
        assert not (tmp_path / "inventory_edited.csv").exists()
        assert not (tmp_path / "Inventory_edited.CSV").exists()
        assert "minute" in cleaned.read_text(encoding="latin-1")
    finally:
        if cleaned.exists() and cleaned != source:
            cleaned.unlink()


def test_ensure_unique_datasets_uses_full_dataset_identity():
    data = [
        {"name": "market", "reference product": "electricity", "location": "CH"},
        {"name": "market", "reference product": "electricity", "location": "DE"},
    ]

    assert ensure_unique_datasets(data) == data

    with pytest.raises(ValueError, match="Duplicate datasets"):
        ensure_unique_datasets(data + [copy.deepcopy(data[0])])


def test_simapro_exchange_parser_rejects_empty_names():
    with pytest.raises(ValueError, match="empty"):
        format_technosphere_exchange("")


def test_ecoinvent_activity_loader_rejects_path_traversal():
    with pytest.raises(ValueError, match="Unsupported ecoinvent version"):
        load_ecoinvent_activities("../../../tmp/foo")
