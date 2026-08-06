from __future__ import annotations

import json
import zipfile

import pytest

pytest.importorskip("olca_schema")

from brightpath.analysis import SOURCE_FORMAT_OPENLCA_JSONLD, analyze_inventory, infer_source_format
from brightpath.core import BackgroundContext, BiosphereProfile, FormatProfile, InventoryContext, TechnosphereProfile
from brightpath.formats.openlca_jsonld import load_openlca_jsonld, write_openlca_jsonld
from brightpath.models import BackgroundProfile, InventoryDocument


def _context() -> InventoryContext:
    return InventoryContext(
        format=FormatProfile("openlca_jsonld"),
        background=BackgroundContext(
            technosphere=TechnosphereProfile("ecoinvent", "3.12", "cutoff"),
            biosphere=BiosphereProfile("ecoinvent", "3.12"),
        ),
    )


def _document() -> InventoryDocument:
    data = [
        {
            "name": "input process",
            "reference product": "intermediate",
            "location": "CH",
            "unit": "kilogram",
            "comment": "Foreground intermediate process.",
            "source": "Source A",
            "exchanges": [
                {
                    "type": "production",
                    "name": "input process",
                    "reference product": "intermediate",
                    "product": "intermediate",
                    "location": "CH",
                    "unit": "kilogram",
                    "amount": 1.0,
                    "simapro category": "Materials/Test",
                }
            ],
        },
        {
            "name": "output process",
            "reference product": "service",
            "location": "CH",
            "unit": "kilogram",
            "comment": "Foreground service process.",
            "exchanges": [
                {
                    "type": "production",
                    "name": "output process",
                    "reference product": "service",
                    "product": "service",
                    "location": "CH",
                    "unit": "kilogram",
                    "amount": 1.0,
                },
                {
                    "type": "technosphere",
                    "name": "input process",
                    "reference product": "intermediate",
                    "location": "CH",
                    "unit": "kilogram",
                    "amount": 2.0,
                },
                {
                    "type": "biosphere",
                    "name": "Carbon dioxide, fossil",
                    "categories": ("air", "urban air close to ground"),
                    "unit": "kilogram",
                    "amount": 0.5,
                },
            ],
            "parameters": [
                {
                    "name": "yield_factor",
                    "amount": 0.95,
                    "group": "calculation",
                    "comment": "Foreground process parameter.",
                }
            ],
        },
    ]
    return InventoryDocument(data=data, context=_context(), database_name="openlca-roundtrip")


def test_openlca_jsonld_round_trip_preserves_process_links_and_extensions(tmp_path):
    source = _document()

    archive = write_openlca_jsonld(source, tmp_path / "inventory.zip")
    loaded = load_openlca_jsonld(archive, context=source.context)

    assert loaded.context == source.context
    assert infer_source_format(archive) == SOURCE_FORMAT_OPENLCA_JSONLD
    assert loaded.data[0]["source"] == "Source A"
    assert loaded.data[0]["exchanges"][0]["simapro category"] == "Materials/Test"
    assert loaded.data[1]["parameters"][0]["group"] == "calculation"
    assert loaded.data[1]["exchanges"][1]["type"] == "technosphere"
    assert loaded.data[1]["exchanges"][1]["name"] == "input process"
    assert loaded.data[1]["exchanges"][1]["reference product"] == "intermediate"
    assert loaded.data[1]["exchanges"][1]["location"] == "CH"


def test_openlca_jsonld_reader_rejects_unsupported_root_entities(tmp_path):
    archive = write_openlca_jsonld(_document(), tmp_path / "inventory.zip")
    with zipfile.ZipFile(archive, mode="a", compression=zipfile.ZIP_DEFLATED) as handle:
        handle.writestr(
            "product_systems/test-product-system.json",
            json.dumps({"@type": "ProductSystem", "@id": "ps-1", "name": "Unsupported system"}),
        )

    with pytest.raises(ValueError, match="unsupported root entities"):
        load_openlca_jsonld(archive, context=_context())


def test_analyze_inventory_reports_openlca_jsonld_candidates(tmp_path):
    archive = write_openlca_jsonld(_document(), tmp_path / "inventory.zip")

    result = analyze_inventory(
        path=archive,
        source_profile=BackgroundProfile("ecoinvent", "3.12", "cutoff"),
    )

    assert result.detected_software == "openlca"
    assert result.detected_format == SOURCE_FORMAT_OPENLCA_JSONLD
    assert [candidate.name for candidate in result.candidates] == ["input process", "output process"]
