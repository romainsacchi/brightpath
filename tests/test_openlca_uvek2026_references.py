"""Native v1.1 metadata contracts, without importing a background inventory."""

import hashlib
import json
from copy import deepcopy
from dataclasses import replace
from zipfile import ZipFile

import pytest

from brightpath import (
    DATA_DIR,
    BackgroundContext,
    BiosphereProfile,
    FormatProfile,
    InventoryContext,
    TechnosphereProfile,
)
from brightpath.formats.openlca_jsonld import load_openlca_jsonld, write_openlca_jsonld
from brightpath.formats.openlca_references import _load_catalog, load_openlca_reference_catalog
from brightpath.models import InventoryDocument
from brightpath.units import normalize_unit

DIRECTORY = DATA_DIR / "export/openlca_references"
FILENAME = "uvek__2026__cutoff__uvek__2026.json"
ROAD = ("Road", "Road", "CH", "meter-year")
MAINTENANCE = ("Operation, maintenance, road", "Operation, maintenance, road", "CH", "meter-year")
GAS = ("Natural gas, at production", "Natural gas, at production", "AE", "normal cubic meter")
NATIVE_SHA = "e63b7e7ef5cde04bfe1176a783a55b5f72e2b73f42c36632b6150af39d348c4c"


def context():
    return InventoryContext(
        FormatProfile("openlca_jsonld"),
        BackgroundContext(TechnosphereProfile("uvek", "2026", "cutoff"), BiosphereProfile("uvek", "2026")),
    )


@pytest.fixture(scope="module")
def payload():
    return json.loads((DIRECTORY / FILENAME).read_bytes())


@pytest.fixture(scope="module")
def catalog():
    result = load_openlca_reference_catalog(context())
    assert result is not None
    return result


def test_native_v11_resource_integrity_coverage_and_build_are_explicit(payload, catalog):
    raw = (DIRECTORY / FILENAME).read_bytes()
    manifest = json.loads((DIRECTORY / "RESOURCE_MANIFEST.json").read_bytes())
    resource = next(row for row in manifest["resources"] if row["file"] == FILENAME)
    assert resource["sha256"] == hashlib.sha256(raw).hexdigest()
    assert resource["size"] == len(raw)
    assert resource["profile"] == payload["profile"]
    assert len(catalog.technosphere) == resource["technosphere_references"] == 11_947
    assert len(catalog.biosphere) == resource["biosphere_references"] == 1_781
    assert payload["coverage"]["missing_biosphere_references"] == 898
    assert payload["coverage"]["canonical_biosphere_identities"] == 2_679
    assert resource["process_category_references"] == 11_947
    assert payload["native_source"]["supplied_revision"] == "v1.1"
    assert payload["native_source"]["archive_SHA256"] == NATIVE_SHA
    assert payload["canonical_source"]["revision"] == "v1"
    assert payload["canonical_source"]["sha256"] != NATIVE_SHA


@pytest.mark.parametrize(
    "background",
    [
        BackgroundContext(TechnosphereProfile("uvek", "2026", "cutoff"), BiosphereProfile("ecoinvent", "3.10")),
        BackgroundContext(TechnosphereProfile("uvek", "2026", "cutoff"), BiosphereProfile("uvek", "2025")),
        BackgroundContext(TechnosphereProfile("uvek", "2026.1", "cutoff"), BiosphereProfile("uvek", "2026")),
        BackgroundContext(TechnosphereProfile("uvek", "2026", "consequential"), BiosphereProfile("uvek", "2026")),
    ],
)
def test_catalog_does_not_leak_to_another_exact_paired_profile(background):
    assert load_openlca_reference_catalog(replace(context(), background=background)) is None


def test_existing_2025_pair_remains_separate():
    selected = replace(
        context(),
        background=BackgroundContext(
            TechnosphereProfile("uvek", "2025", "cutoff"), BiosphereProfile("ecoinvent", "3.10")
        ),
    )
    existing = load_openlca_reference_catalog(selected)
    assert existing is not None
    assert len(existing.technosphere) == 11_747


@pytest.mark.parametrize("identity", [ROAD, MAINTENANCE])
def test_meter_year_selects_actual_metre_times_year_not_kilometre_or_area(payload, catalog, identity):
    reference = catalog.technosphere[identity]
    assert reference.unit_id == "f7fe0af2-e764-4984-bb9f-2cbff6cd2f18"
    assert reference.unit_name == "m*a"
    quantity = next(q for q in payload["native_quantity_definitions"] if q["unit_id"] == reference.unit_id)
    assert quantity["property_name"] == "Length*Time"
    assert quantity["unit_group_id"] == "326eb58b-e5b3-4cea-b45a-2398c25109f8"
    assert quantity["selected_unit_factor"] == 0.001
    assert quantity["reference_unit_name"] == "km*a"
    assert quantity["reference_unit_factor"] == 1.0
    assert (*identity[:3], "square meter-year") not in catalog.technosphere
    assert (*identity[:3], "kilometer-year") not in catalog.technosphere


def test_gas_correspondence_is_an_exact_provider_key_not_a_global_unit_alias(catalog):
    reference = catalog.technosphere[GAS]
    assert reference.process_id == "529efc6c-39c2-3046-8d24-6ac3e83f7377"
    assert reference.flow_id == "d8ab565c-b21e-3722-a9a7-263ba3fbb43b"
    assert reference.unit_id == "1c3a9695-398d-4b1f-b07e-a8715b610f70"
    assert reference.unit_name == "m3"
    assert (*GAS[:3], "cubic meter") not in catalog.technosphere
    assert (GAS[0], GAS[1], "unknown location", GAS[3]) not in catalog.technosphere
    assert normalize_unit("Nm3") == "normal cubic meter"
    assert normalize_unit("m3") == "cubic meter"


@pytest.mark.parametrize(
    ("name", "native_id", "native_name"),
    [
        ("Carbon dioxide, biogenic", "eba59fd6-f37e-41dc-9ca3-c7ea22d602c7", "Carbon dioxide, non-fossil"),
        ("Carbon monoxide, biogenic", "2cb2333c-1599-46cf-8435-3dffce627524", "Carbon monoxide, non-fossil"),
    ],
)
def test_biogenic_origin_uses_recorded_non_fossil_native_id_without_scalar_projection(
    catalog, name, native_id, native_name
):
    reference = catalog.biosphere[(name, ("emissions to air", "unspecified"), "kilogram")]
    assert reference.flow_id == native_id
    assert reference.flow_name == native_name
    assert reference.unit_name == "kg"
    assert reference.amount_factor == 1.0
    assert "fossil" not in reference.flow_name.replace("non-fossil", "")
    assert reference.native_categories == ("Elementary flows", "Emission to air", "unspecified")
    assert (name, ("emissions to water", "unspecified"), "kilogram") not in catalog.biosphere
    assert (name, ("emissions to air", "unspecified"), "cubic meter") not in catalog.biosphere


def fixture_document(identity=ROAD):
    production = {
        "name": "Synthetic native reference control",
        "reference product": "synthetic output",
        "location": "GLO",
        "unit": "kilogram",
        "type": "production",
        "amount": 1.0,
    }
    exchange = dict(zip(("name", "reference product", "location", "unit"), identity, strict=True))
    exchange.update(
        type="technosphere", amount=1.0, formula="quantity", **{"uncertainty type": 3, "loc": 1.0, "scale": 0.1}
    )
    activity = {
        **{key: production[key] for key in ("name", "reference product", "location", "unit")},
        "comment": "Synthetic format control; no native inventory vector is included.",
        "parameters": [{"name": "quantity", "amount": 1.0}],
        "exchanges": [production, exchange],
    }
    return InventoryDocument(data=[activity], context=context(), database_name="synthetic-native-uvek2026")


@pytest.mark.parametrize("identity", [ROAD, MAINTENANCE, GAS])
def test_one_source_unit_is_one_selected_native_unit_with_parameters_and_input_unchanged(tmp_path, catalog, identity):
    doc = fixture_document(identity)
    before = deepcopy(doc.data)
    archive = write_openlca_jsonld(doc, tmp_path / "native-references.zip")
    reference = catalog.technosphere[identity]
    with ZipFile(archive) as package:
        processes = [json.loads(package.read(n)) for n in package.namelist() if n.startswith("processes/")]
        assert len(processes) == 1
        wire = next(row for row in processes[0]["exchanges"] if row.get("defaultProvider"))
        assert wire["amount"] == 1.0
        assert wire["unit"]["@id"] == reference.unit_id
        assert wire["defaultProvider"]["@id"] == reference.process_id
        assert f"processes/{reference.process_id}.json" not in package.namelist()
        assert f"flows/{reference.flow_id}.json" not in package.namelist()
    restored = load_openlca_jsonld(archive, context=context()).data[0]["exchanges"][1]
    for field in (
        "name",
        "reference product",
        "location",
        "unit",
        "amount",
        "formula",
        "uncertainty type",
        "loc",
        "scale",
    ):
        assert restored[field] == before[0]["exchanges"][1][field]
    assert doc.data == before


def test_unknown_quantity_is_refused_without_native_stub_or_name_guess(tmp_path):
    from brightpath.exceptions import SerializationError

    doc = fixture_document((*GAS[:3], "cubic meter"))
    with pytest.raises(SerializationError, match="no exact technosphere reference"):
        write_openlca_jsonld(doc, tmp_path / "unsupported.zip")
    assert not (tmp_path / "unsupported.zip").exists()


def test_missing_biosphere_is_left_unresolved_instead_of_merging_fresh_water(catalog):
    canonical = json.loads((DATA_DIR / "export/reference_catalogs/uvek__2026__cutoff.json").read_bytes())
    missing = [
        r for r in canonical["biosphere"] if (r["name"], tuple(r["categories"]), r["unit"]) not in catalog.biosphere
    ]
    assert len(missing) == 898
    assert any(r["categories"] == ["emissions to water", "river"] for r in missing)
    assert all(
        reference.native_categories[-1] != "fresh water" or identity[1][-1] == "fresh water"
        for identity, reference in catalog.biosphere.items()
    )


def test_native_reference_kinds_are_retained_as_metadata_not_inferred_from_provider_names(payload):
    assert sum(row["native_flow_type"] == "WASTE_FLOW" for row in payload["technosphere"]) == 1_112
    assert sum(row["native_flow_type"] == "PRODUCT_FLOW" for row in payload["technosphere"]) == 10_835
    assert all(isinstance(row["native_reference_is_input"], bool) for row in payload["technosphere"])


def test_resource_has_no_native_inventory_values_or_local_paths(payload):
    forbidden = {
        "amount",
        "exchanges",
        "uncertainty type",
        "loc",
        "scale",
        "formula",
        "description",
        "characterizationFactors",
    }

    def inspect(value):
        if isinstance(value, dict):
            assert not forbidden.intersection(value)
            for child in value.values():
                inspect(child)
        elif isinstance(value, list):
            for child in value:
                inspect(child)
        elif isinstance(value, str):
            assert not any(path in value for path in ("/Users/", "/private/", "/work/"))

    inspect(payload)


def test_mutated_catalog_cannot_load_against_original_manifest(tmp_path):
    (tmp_path / "RESOURCE_MANIFEST.json").write_bytes((DIRECTORY / "RESOURCE_MANIFEST.json").read_bytes())
    changed = (DIRECTORY / FILENAME).read_bytes().replace(b'"unit_name": "m*a"', b'"unit_name": "m2*a"', 1)
    path = tmp_path / FILENAME
    path.write_bytes(changed)
    with pytest.raises(ValueError, match="integrity manifest"):
        _load_catalog(path)
