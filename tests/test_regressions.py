import copy
import csv
from pathlib import Path

import pytest

from brightpath import BackgroundProfile, SimaProInventory
from brightpath.profiles import parse_simapro_technosphere_name
from brightpath.utils import check_simapro_inventory


def minimal_activity(extra_exchanges=None, **overrides):
    activity = {
        "name": "test process",
        "reference product": "test product",
        "location": "GLO",
        "unit": "kilogram",
        "comment": "Documented foreground dataset.",
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


def profile():
    return BackgroundProfile("ecoinvent", "3.9", "cutoff")


def test_simapro_inventory_accepts_direct_data_without_metadata():
    inventory = SimaProInventory.from_data([minimal_activity()], background_profile=profile())

    rows = inventory.render().rows

    assert rows


def test_simapro_rendering_does_not_mutate_input_data():
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

    SimaProInventory.from_data(data, background_profile=profile()).render()

    assert data == original


def test_simapro_csv_export_escapes_formula_like_text(tmp_path):
    inventory = SimaProInventory.from_data(
        [minimal_activity(comment="=1+1")],
        background_profile=profile(),
    )

    csv_path = inventory.write_csv(tmp_path / "inventory.csv", validate=False)

    with open(csv_path, newline="", encoding="latin-1") as handle:
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


def test_duplicate_validation_uses_full_dataset_identity():
    data = [
        {"name": "market", "reference product": "electricity", "location": "CH"},
        {"name": "market", "reference product": "electricity", "location": "DE"},
    ]

    unique = SimaProInventory.from_data(data, background_profile=profile())
    duplicate = SimaProInventory.from_data(
        data + [copy.deepcopy(data[0])],
        background_profile=profile(),
    )

    assert "duplicate_dataset_identity" not in {
        issue.code for issue in unique.validate(check_background_links=False).issues
    }
    assert "duplicate_dataset_identity" in {
        issue.code for issue in duplicate.validate(check_background_links=False).issues
    }


def test_simapro_exchange_parser_rejects_empty_names():
    with pytest.raises(ValueError, match="empty"):
        parse_simapro_technosphere_name("", profile=profile())


def test_simapro_profile_is_explicit_instead_of_filename_driven():
    with pytest.raises(ValueError, match="Unsupported background family"):
        parse_simapro_technosphere_name(
            "Electricity {CH}| market for | Cut-off, U",
            profile=BackgroundProfile("../../../tmp/foo", "3.9", "cutoff"),
        )
