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


def _collision_document(compartments=("air", "water"), locations=("CH", "FR")):
    data = []
    for index, location in enumerate(locations):
        name = f"producer {index}"
        data.append(
            {
                "name": name,
                "reference product": "shared product",
                "location": location,
                "unit": "kilogram",
                "exchanges": [
                    {"type": "production", "name": name, "product": "shared product", "unit": "kilogram", "amount": 1},
                    *[
                        {
                            "type": "biosphere",
                            "name": "Zinc",
                            "unit": "kilogram",
                            "categories": (compartment,) if isinstance(compartment, str) else compartment,
                            "amount": 0.1,
                            "audit note": f"process {index} emission {number}",
                        }
                        for number, compartment in enumerate(compartments)
                    ],
                ],
            }
        )
    data.append(
        {
            "name": "consumer",
            "reference product": "service",
            "location": "GLO",
            "unit": "kilogram",
            "exchanges": [
                {"type": "production", "name": "consumer", "product": "service", "unit": "kilogram", "amount": 1},
                *[
                    {
                        "type": "technosphere",
                        "name": row["name"],
                        "product": row["reference product"],
                        "location": row["location"],
                        "unit": row["unit"],
                        "amount": 2,
                    }
                    for row in data
                ],
            ],
        }
    )
    return InventoryDocument(data=data, context=_context())


def _zip_entities(path, folder):
    with zipfile.ZipFile(path) as archive:
        return {
            entity["@id"]: entity
            for name in archive.namelist()
            if name.startswith(folder + "/")
            for entity in [json.loads(archive.read(name))]
        }


def test_elementary_compartments_and_shared_metadata_survive_round_trip(tmp_path):
    document = _collision_document(("air", "water", "soil", ("air", "urban air close to ground")))
    before = document.data
    path = write_openlca_jsonld(document, tmp_path / "compartments.zip")
    flows = _zip_entities(path, "flows")
    zinc = {key: value for key, value in flows.items() if value["name"] == "Zinc"}
    assert len(zinc) == 4
    assert {value["category"] for value in zinc.values()} == {
        "Emissions to air",
        "Emissions to water",
        "Emissions to soil",
        "Emissions to air/urban air close to ground",
    }
    assert all("location" not in flow for flow in zinc.values())
    for process in _zip_entities(path, "processes").values():
        if process["name"].startswith("producer"):
            assert [flows[exc["flow"]["@id"]]["category"] for exc in process["exchanges"][1:]] == [
                "Emissions to air",
                "Emissions to water",
                "Emissions to soil",
                "Emissions to air/urban air close to ground",
            ]
    loaded = load_openlca_jsonld(path, context=document.context)
    for row in loaded.data:
        if row["name"].startswith("producer"):
            index = row["name"].split()[-1]
            assert [exc["audit note"] for exc in row["exchanges"][1:]] == [
                f"process {index} emission {n}" for n in range(4)
            ]
    second = write_openlca_jsonld(loaded, tmp_path / "roundtrip.zip")
    assert set(_zip_entities(second, "flows")) == set(flows)
    assert document.data == before


@pytest.mark.parametrize("locations, expected_products", [(("CH", "FR"), 2), (("CH", "CH"), 1)])
def test_product_identity_and_provider_links(tmp_path, locations, expected_products):
    path = write_openlca_jsonld(_collision_document(locations=locations), tmp_path / "products.zip")
    flows, processes = _zip_entities(path, "flows"), _zip_entities(path, "processes")
    products = [flow for flow in flows.values() if flow["name"] == "shared product"]
    assert len(products) == expected_products
    consumer = next(p for p in processes.values() if p["name"] == "consumer")
    for index, exchange in enumerate(consumer["exchanges"][1:]):
        supplier = processes[exchange["defaultProvider"]["@id"]]
        assert supplier["name"] == f"producer {index}"
        assert exchange["flow"]["@id"] == supplier["exchanges"][0]["flow"]["@id"]
    assert len({p["location"]["@id"] for p in products}) == expected_products


def test_flow_ids_and_definitions_do_not_depend_on_order(tmp_path):
    document = _collision_document()
    first = write_openlca_jsonld(document, tmp_path / "first.zip")
    repeated = write_openlca_jsonld(document, tmp_path / "repeated.zip")
    reordered = InventoryDocument(data=list(reversed(document.data)), context=document.context)
    second = write_openlca_jsonld(reordered, tmp_path / "reordered.zip")
    assert _zip_entities(first, "flows") == _zip_entities(second, "flows") == _zip_entities(repeated, "flows")


def test_explicit_shared_flow_keeps_id_and_own_location(tmp_path):
    document = _collision_document()
    data = document.data
    explicit_id = "7993cc25-0c1b-413c-a8cd-929220916c5d"
    for index, row in enumerate(data[:2]):
        for exc in row["exchanges"][1:]:
            exc["categories"] = ("air",)
            exc["openlca flow"] = {
                "@id": explicit_id,
                "location": {"@type": "Location", "@id": "shared-region", "name": "Explicit region"},
            }
        # Complementary flow metadata must survive either registration order.
        row["exchanges"][1]["openlca flow"]["description" if index == 0 else "cas"] = (
            "description" if index == 0 else "7440-66-6"
        )
    path = write_openlca_jsonld(InventoryDocument(data=data, context=document.context), tmp_path / "explicit.zip")
    flow = _zip_entities(path, "flows")[explicit_id]
    assert flow["location"]["@id"] == "shared-region"
    assert flow["description"] == "description"
    assert flow["cas"] == "7440-66-6"


@pytest.mark.parametrize("conflict", ["category", "name", "unit", "location", "flowType", "description"])
def test_conflicting_explicit_flow_id_fails_without_overwriting_archive(tmp_path, conflict):
    document = _collision_document(compartments=("air",))
    data = document.data
    explicit_id = "7993cc25-0c1b-413c-a8cd-929220916c5d"
    for row in data[:2]:
        row["exchanges"][1]["openlca flow"] = {"@id": explicit_id}
    left, right = data[0]["exchanges"][1], data[1]["exchanges"][1]
    if conflict == "category":
        right["categories"] = ("water",)
    elif conflict in ("name", "unit"):
        right[conflict] = "Other substance" if conflict == "name" else "gram"
    elif conflict == "location":
        for index, exc in enumerate((left, right)):
            exc["openlca flow"]["location"] = {"@type": "Location", "@id": f"region-{index}"}
    elif conflict == "flowType":
        right["openlca flow"]["flowType"] = "PRODUCT_FLOW"
    else:
        left["openlca flow"]["description"] = "left"
        right["openlca flow"]["description"] = "right"
    path = tmp_path / "existing.zip"
    path.write_bytes(b"existing archive")
    with pytest.raises(SerializationError, match=explicit_id + ".*conflicting"):
        write_openlca_jsonld(InventoryDocument(data=data, context=document.context), path)
    assert path.read_bytes() == b"existing archive"


def test_legacy_exchange_metadata_is_still_read(tmp_path):
    path = write_openlca_jsonld(_collision_document(compartments=("air",), locations=("CH",)), tmp_path / "new.zip")
    legacy = tmp_path / "legacy.zip"
    with zipfile.ZipFile(path) as source, zipfile.ZipFile(legacy, "w") as target:
        for name in source.namelist():
            content = source.read(name)
            if name.startswith("flows/"):
                flow = json.loads(content)
                if flow["name"] == "Zinc":
                    properties = flow["otherProperties"]
                    values = properties.pop("brightpathExchangePropertiesByProcess")
                    properties["brightpathExchangeProperties"] = next(iter(values.values()))
                    content = json.dumps(flow).encode()
            target.writestr(name, content)
    loaded = load_openlca_jsonld(legacy, context=_context())
    producer = next(row for row in loaded.data if row["name"] == "producer 0")
    assert producer["exchanges"][1]["audit note"] == "process 0 emission 0"


def test_generated_ids_use_structured_fields_and_ignore_brightway_codes(tmp_path):
    document = _collision_document(compartments=("air",))
    data = document.data
    left, right = data[0]["exchanges"][1], data[1]["exchanges"][1]
    left.update(name="substance::variant", unit="kilogram", code="same-brightway-code")
    right.update(name="substance", unit="variant::kilogram", code="same-brightway-code")
    path = write_openlca_jsonld(InventoryDocument(data=data, context=document.context), tmp_path / "fields.zip")
    elementary = [f for f in _zip_entities(path, "flows").values() if f["flowType"] == "ELEMENTARY_FLOW"]
    assert len(elementary) == 2
    assert all(f["@id"] != "same-brightway-code" for f in elementary)


def test_category_normalization_deduplicates_equivalent_compartments(tmp_path):
    document = _collision_document(
        compartments=((" AIR ", " urban air close to ground "), ("air", "urban air close to ground"))
    )
    path = write_openlca_jsonld(document, tmp_path / "normalized.zip")
    elementary = [f for f in _zip_entities(path, "flows").values() if f["flowType"] == "ELEMENTARY_FLOW"]
    assert len(elementary) == 1
    assert elementary[0]["category"] == "Emissions to air/urban air close to ground"


def test_explicit_unregionalized_product_is_shared_across_supplier_locations(tmp_path):
    document = _collision_document()
    data = document.data
    explicit_id = "7993cc25-0c1b-413c-a8cd-929220916c5d"
    for row in data[:2]:
        row["exchanges"][0]["openlca flow"] = {"@id": explicit_id}
    path = write_openlca_jsonld(InventoryDocument(data=data, context=document.context), tmp_path / "shared.zip")
    assert "location" not in _zip_entities(path, "flows")[explicit_id]
    loaded = load_openlca_jsonld(path, context=document.context)
    again = write_openlca_jsonld(loaded, tmp_path / "again.zip")
    assert "location" not in _zip_entities(again, "flows")[explicit_id]
    consumer = next(row for row in loaded.data if row["name"] == "consumer")
    assert [(e["name"], e["location"]) for e in consumer["exchanges"][1:]] == [
        ("producer 0", "CH"),
        ("producer 1", "FR"),
    ]


@pytest.mark.parametrize(
    "amount, negative, expected_sign", [(2.0, None, 1), (-2.0, None, -1), (2.0, True, -1), (-2.0, False, 1)]
)
def test_lognormal_export_and_import_convert_geometric_parameters(tmp_path, amount, negative, expected_sign):
    import math

    document = _collision_document(compartments=("air",), locations=("CH",))
    data = document.data
    exchange = data[0]["exchanges"][1]
    exchange.update(amount=amount, **{"uncertainty type": 2, "loc": math.log(2), "scale": 0.2})
    if negative is not None:
        exchange["negative"] = negative
    source = InventoryDocument(data=data, context=document.context)
    before = source.data
    path = write_openlca_jsonld(source, tmp_path / "uncertainty.zip")
    producer = next(p for p in _zip_entities(path, "processes").values() if p["name"] == "producer 0")
    rendered = producer["exchanges"][1]
    assert rendered["amount"] == amount
    assert rendered["uncertainty"]["geomMean"] == pytest.approx(expected_sign * 2)
    assert rendered["uncertainty"]["geomSd"] == pytest.approx(math.exp(0.2))
    loaded = load_openlca_jsonld(path, context=source.context)
    row = next(d for d in loaded.data if d["name"] == "producer 0")
    result = row["exchanges"][1]
    assert result["loc"] == pytest.approx(math.log(2))
    assert result["scale"] == pytest.approx(0.2)
    assert result["negative"] == (expected_sign < 0)
    assert result["amount"] == amount
    assert source.data == before
    second = write_openlca_jsonld(loaded, tmp_path / "again.zip")
    p2 = next(p for p in _zip_entities(second, "processes").values() if p["name"] == "producer 0")
    assert p2["exchanges"][1]["uncertainty"] == rendered["uncertainty"]


@pytest.mark.parametrize(
    "loc, scale",
    [(None, 0.2), (0, None), (float("nan"), 0.2), (0, float("inf")), (0, -0.1), (1000, 0.2), (-1000, 0.2), (0, 1000)],
)
def test_invalid_lognormal_export_leaves_existing_archive_untouched(tmp_path, loc, scale):
    document = _collision_document(compartments=("air",), locations=("CH",))
    data = document.data
    data[0]["exchanges"][1].update({"uncertainty type": 2, "loc": loc, "scale": scale})
    path = tmp_path / "existing.zip"
    path.write_bytes(b"existing archive")
    with pytest.raises(SerializationError, match="Lognormal uncertainty"):
        write_openlca_jsonld(InventoryDocument(data=data, context=document.context), path)
    assert path.read_bytes() == b"existing archive"


@pytest.mark.parametrize(
    "geom_mean, geom_sd", [(0, 2), (2, 0.5), (None, 2), (2, None), (float("inf"), 2), (2, float("nan"))]
)
def test_invalid_imported_lognormal_is_rejected(geom_mean, geom_sd):
    import olca_schema as schema

    from brightpath.formats.openlca_jsonld import _legacy_uncertainty

    value = schema.Uncertainty(
        distribution_type=schema.UncertaintyType.LOG_NORMAL_DISTRIBUTION, geom_mean=geom_mean, geom_sd=geom_sd
    )
    with pytest.raises(SerializationError, match="Lognormal uncertainty"):
        _legacy_uncertainty(value)


@pytest.mark.parametrize("uncertainty_type", [6, 7, 99])
def test_unsupported_uncertainty_is_not_silently_dropped(tmp_path, uncertainty_type):
    document = _collision_document(compartments=("air",), locations=("CH",))
    data = document.data
    data[0]["exchanges"][1]["uncertainty type"] = uncertainty_type
    with pytest.raises(SerializationError, match="Unsupported openLCA uncertainty type"):
        write_openlca_jsonld(InventoryDocument(data=data, context=document.context), tmp_path / "unsupported.zip")


def test_lognormal_parameters_and_zero_spread_round_trip(tmp_path):
    import math

    document = _collision_document(compartments=("air",), locations=("CH",))
    data = document.data
    parameter = {
        "name": "factor",
        "amount": -2.0,
        "uncertainty type": 2,
        "loc": math.log(2),
        "scale": 0.0,
        "negative": True,
    }
    data[0]["parameters"] = [parameter]
    source = InventoryDocument(
        data=data,
        context=document.context,
        database_parameters=[parameter],
        project_parameters=[dict(parameter, name="project factor")],
    )
    path = write_openlca_jsonld(source, tmp_path / "parameters.zip")
    producer = next(p for p in _zip_entities(path, "processes").values() if p["name"] == "producer 0")
    parameters = [*producer["parameters"], *_zip_entities(path, "parameters").values()]
    assert len(parameters) == 3
    for value in parameters:
        assert value["uncertainty"]["geomMean"] == -2
        assert value["uncertainty"]["geomSd"] == 1
    loaded = load_openlca_jsonld(path, context=source.context)
    producer = next(p for p in loaded.data if p["name"] == "producer 0")
    for value in [*producer["parameters"], *loaded.database_parameters, *loaded.project_parameters]:
        assert value["loc"] == pytest.approx(math.log(2))
        assert value["scale"] == 0
        assert value["negative"] is True


@pytest.mark.parametrize(
    "kind, fields",
    [
        (0, {}),
        (1, {}),
        (3, {"loc": -2.0, "scale": 0.5}),
        (4, {"minimum": 1.0, "maximum": 3.0}),
        (5, {"minimum": 1.0, "maximum": 3.0, "loc": 2.0}),
    ],
)
def test_other_supported_uncertainty_types_keep_their_parameters(kind, fields):
    import olca_schema as schema

    from brightpath.formats.openlca_jsonld import _legacy_uncertainty, _schema_uncertainty

    result = _schema_uncertainty(schema, {"uncertainty type": kind, **fields})
    if kind in (0, 1):
        assert result is None
    else:
        assert _legacy_uncertainty(result) == {"uncertainty type": kind, **fields}
