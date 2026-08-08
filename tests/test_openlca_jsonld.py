from __future__ import annotations

import json
import zipfile
from copy import deepcopy

import pytest

pytest.importorskip("olca_schema")

from brightpath.analysis import SOURCE_FORMAT_OPENLCA_JSONLD, analyze_inventory, infer_source_format
from brightpath.core import BackgroundContext, BiosphereProfile, FormatProfile, InventoryContext, TechnosphereProfile
from brightpath.exceptions import SerializationError
from brightpath.formats.openlca_categories import (
    build_openlca_process_category_catalog,
    resolve_openlca_process_category,
)
from brightpath.formats.openlca_jsonld import load_openlca_jsonld, write_openlca_jsonld
from brightpath.formats.openlca_references import load_openlca_reference_catalog
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


def _uvek_context() -> InventoryContext:
    return InventoryContext(
        format=FormatProfile("openlca_jsonld"),
        background=BackgroundContext(
            technosphere=TechnosphereProfile("uvek", "2025", "cutoff"),
            biosphere=BiosphereProfile("ecoinvent", "3.10"),
        ),
    )


def _uvek_document() -> InventoryDocument:
    return InventoryDocument(
        data=[
            {
                "name": "carbon dioxide capture",
                "reference product": "carbon dioxide, captured",
                "location": "RER",
                "unit": "kilogram",
                "exchanges": [
                    {
                        "type": "production",
                        "name": "carbon dioxide capture",
                        "reference product": "carbon dioxide, captured",
                        "location": "RER",
                        "unit": "kilogram",
                        "amount": 1.0,
                    },
                    {
                        "type": "technosphere",
                        "name": "Electricity, low voltage, at grid",
                        "reference product": "Electricity, low voltage, at grid",
                        "location": "RER",
                        "unit": "kilowatt hour",
                        "amount": 0.1,
                    },
                    {
                        "type": "biosphere",
                        "name": "Carbon dioxide, fossil",
                        "categories": ("air",),
                        "unit": "kilogram",
                        "amount": 0.0676,
                    },
                ],
            }
        ],
        context=_uvek_context(),
        database_name="uvek-linked-foreground",
    )


def test_openlca_jsonld_round_trip_preserves_process_links_and_extensions(tmp_path):
    source = _document()

    archive = write_openlca_jsonld(source, tmp_path / "inventory.zip")
    loaded = load_openlca_jsonld(archive, context=source.context)

    assert loaded.context == source.context
    assert infer_source_format(archive) == SOURCE_FORMAT_OPENLCA_JSONLD
    assert loaded.data[0]["source"] == "Source A"
    assert loaded.data[0]["openlca category"] == "material/Test"
    assert loaded.data[0]["exchanges"][0]["simapro category"] == "Materials/Test"
    assert loaded.data[1]["openlca category"] == "foreground/Uncategorized"
    assert loaded.data[1]["parameters"][0]["group"] == "calculation"
    assert loaded.data[1]["exchanges"][1]["type"] == "technosphere"
    assert loaded.data[1]["exchanges"][1]["name"] == "input process"
    assert loaded.data[1]["exchanges"][1]["reference product"] == "intermediate"
    assert loaded.data[1]["exchanges"][1]["location"] == "CH"


def test_uvek_openlca_export_references_existing_provider_and_characterized_flow(tmp_path):
    archive = write_openlca_jsonld(_uvek_document(), tmp_path / "inventory.zip")

    with zipfile.ZipFile(archive) as handle:
        process_name = next(name for name in handle.namelist() if name.startswith("processes/"))
        process = json.loads(handle.read(process_name))
        exchanges = {exchange["internalId"]: exchange for exchange in process["exchanges"]}
        external_flow_paths = {
            "flows/6e636642-6710-30fa-beae-bafdebd91217.json",
            "flows/349b29d1-3e58-4c66-98b9-9d1a076efd2e.json",
        }

        assert process["category"] == "material/chemicals/gases\\transformation"
        assert exchanges[2]["defaultProvider"]["@id"] == "0c5cc00d-0625-3fd0-bc34-5df18f4cfd77"
        assert exchanges[2]["flow"]["@id"] == "6e636642-6710-30fa-beae-bafdebd91217"
        assert exchanges[2]["flowProperty"]["@id"] == "f6811440-ee37-11de-8a39-0800200c9a66"
        assert exchanges[2]["unit"]["@id"] == "86ad2244-1f0e-4912-af53-7865283103e4"
        assert exchanges[3]["flow"]["@id"] == "349b29d1-3e58-4c66-98b9-9d1a076efd2e"
        assert exchanges[3]["flowProperty"]["@id"] == "93a60a56-a3c8-11da-a746-0800200b9a66"
        assert exchanges[3]["unit"]["@id"] == "20aadc24-a391-41cf-b340-3e4529f44bde"
        assert "defaultProvider" not in exchanges[3]
        assert external_flow_paths.isdisjoint(handle.namelist())


def test_openlca_export_preserves_explicit_process_category(tmp_path):
    source = _uvek_document()
    data = source.data
    data[0]["openlca category"] = "material/chemicals/gases\\transformation"
    categorized = InventoryDocument(
        data=data,
        context=source.context,
        database_name=source.database_name,
    )

    archive = write_openlca_jsonld(categorized, tmp_path / "inventory.zip")
    with zipfile.ZipFile(archive) as handle:
        process_name = next(name for name in handle.namelist() if name.startswith("processes/"))
        process = json.loads(handle.read(process_name))

    assert process["category"] == "material/chemicals/gases\\transformation"
    assert "openlca category" not in source.data[0]


def test_uvek_openlca_export_rejects_unresolved_background_instead_of_creating_lookalike(tmp_path):
    source = _uvek_document()
    data = source.data
    data[0]["exchanges"][1]["name"] = "Unknown UVEK supplier"
    data[0]["exchanges"][1]["reference product"] = "Unknown product"
    unresolved = InventoryDocument(
        data=data,
        context=source.context,
        database_name=source.database_name,
    )

    with pytest.raises(SerializationError, match="no exact technosphere reference"):
        write_openlca_jsonld(unresolved, tmp_path / "inventory.zip")


def test_packaged_uvek_openlca_reference_catalog_has_exact_reported_links():
    catalog = load_openlca_reference_catalog(_uvek_context())

    assert catalog is not None
    assert (
        catalog.technosphere[
            (
                "Electricity, low voltage, at grid",
                "Electricity, low voltage, at grid",
                "RER",
                "kilowatt hour",
            )
        ].process_id
        == "0c5cc00d-0625-3fd0-bc34-5df18f4cfd77"
    )
    assert catalog.biosphere[("Carbon dioxide, fossil", ("air",), "kilogram")].flow_id == (
        "349b29d1-3e58-4c66-98b9-9d1a076efd2e"
    )


def test_uvek_process_category_inference_uses_native_target_taxonomy_without_mutation():
    references = load_openlca_reference_catalog(_uvek_context())
    catalog = build_openlca_process_category_catalog(references)
    activity = _uvek_document().data[0]
    source = deepcopy(activity)

    inferred = resolve_openlca_process_category(activity, catalog=catalog)
    assert activity == source
    activity["exchanges"][0]["simapro category"] = "Materials/Chemicals/Gases/Transformation"
    translated = resolve_openlca_process_category(activity, catalog=catalog)
    activity["exchanges"][0].pop("simapro category")
    activity["name"] = "zxqv unknown foreground"
    activity["reference product"] = "zxqv unknown product"
    activity["exchanges"][0]["name"] = "zxqv unknown foreground"
    activity["exchanges"][0]["reference product"] = "zxqv unknown product"
    fallback = resolve_openlca_process_category(activity, catalog=catalog)

    assert inferred.category == "material/chemicals/gases\\transformation"
    assert inferred.method == "target_fuzzy_hierarchy"
    assert translated.category == "material/chemicals/gases\\transformation"
    assert translated.method == "simapro_category"
    assert fallback.category == "material/Others/unspecified"


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
