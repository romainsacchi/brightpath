"""Synthetic-only writer/inverse tests for the one reviewed native Mass projection."""

import json
import math
from copy import deepcopy
from dataclasses import fields
from zipfile import ZipFile

import pytest

from brightpath import BackgroundProfile, BiosphereProfile, FormatProfile, InventoryContext
from brightpath.core import BackgroundContext
from brightpath.formats.ilcd import load_ilcd, write_ilcd
from brightpath.formats.ilcd_background import automatic_references
from brightpath.formats.ilcd_package import NS, parse_xml, q
from brightpath.formats.ilcd_references import ILCDReferenceCatalog
from brightpath.formats.openlca_jsonld import load_openlca_jsonld, render_openlca_jsonld_package, write_openlca_jsonld
from brightpath.formats.openlca_native_uncertainty import COMPATIBILITY, apply, prepare
from brightpath.formats.openlca_quantity_projection import (
    IDENTITY,
    NATIVE_UUID,
    PENDING_FACTOR,
    PROFILE,
    PROPERTY_UUID,
    SOURCE_UUID,
    UNIT_UUID,
    catalog_projection_factor,
    reviewed_mapping,
    scale_detached,
)
from brightpath.formats.openlca_references import load_openlca_reference_catalog
from brightpath.models import InventoryDocument


def context(format_id="openlca_jsonld", *, version="2025", bio="3.10"):
    return InventoryContext(
        FormatProfile(format_id),
        BackgroundContext(
            BackgroundProfile("uvek", version, "cutoff").to_technosphere_profile(), BiosphereProfile("ecoinvent", bio)
        ),
    )


def document(kind=0, *, sign=1, formula=False, format_id="openlca_jsonld"):
    identity = {
        "name": "Synthetic fossilwater fixture",
        "reference product": "fixture",
        "location": "CH",
        "unit": "kilogram",
    }
    flow = {
        "name": "Water",
        "categories": IDENTITY[1],
        "unit": "cubic meter",
        "type": "biosphere",
        "amount": sign * 2.0,
        "comment": "Synthetic foreground quantity, not native coefficients.",
    }
    if kind:
        flow["uncertainty type"] = kind
    if kind == 2:
        flow.update(loc=math.log(4), scale=math.log(1.5), negative=sign < 0)
    elif kind == 3:
        flow.update(loc=sign * 4.0, scale=0.5)
    elif kind in (4, 5):
        flow.update(minimum=1.0 if sign > 0 else -5.0, maximum=5.0 if sign > 0 else -1.0)
        if kind == 5:
            flow["loc"] = sign * 4.0
    if formula:
        flow["formula"] = "dose"
    return InventoryDocument(
        data=[
            {
                **identity,
                "code": "11111111-1111-4111-8111-111111111111",
                "comment": "Synthetic format-only projection.",
                "parameters": [{"name": "dose", "amount": sign * 2.0}],
                "exchanges": [{**identity, "type": "production", "amount": 1.0}, flow],
            }
        ],
        context=context(format_id),
    )


def assert_flow(actual, expected):
    for key in ("name", "unit", "categories", "formula", "comment", "uncertainty type", "negative"):
        if key in expected:
            if key == "categories":
                assert tuple(actual[key]) == tuple(expected[key])
            else:
                assert actual.get(key) == expected[key], key
    for key in ("amount", "loc", "scale", "minimum", "maximum"):
        if key in expected:
            assert actual[key] == pytest.approx(expected[key], rel=1e-12, abs=1e-14), key
    assert PENDING_FACTOR not in actual


def write_graph(source, path):
    native, specifications = prepare(source, compatibility=COMPATIBILITY)
    rendered = apply(render_openlca_jsonld_package(native), specifications)
    from olca_schema.zipio import ZipWriter

    with ZipWriter(path) as writer:
        for field in fields(rendered):
            for entity in getattr(rendered, field.name).values():
                writer.write(entity)
    return path


def raw_exchange(path):
    with ZipFile(path) as archive:
        process = next(json.loads(archive.read(n)) for n in archive.namelist() if n.startswith("processes/"))
        exchange = next(e for e in process["exchanges"] if e.get("flow", {}).get("@id") == NATIVE_UUID)
        assert f"flows/{NATIVE_UUID}.json" not in archive.namelist()
    return process, exchange


def test_pinned_native_reference_is_mass_kg_and_explicit1000_not_a_volume_property():
    ref = load_openlca_reference_catalog(context()).biosphere[IDENTITY]
    assert (
        ref.flow_id,
        ref.flow_property_id,
        ref.flow_property_name,
        ref.unit_id,
        ref.unit_name,
        ref.amount_factor,
    ) == (NATIVE_UUID, PROPERTY_UUID, "Mass", UNIT_UUID, "kg", 1000.0)
    native = reviewed_mapping()["mappings"][0]["native"]
    assert native["reference_unit_factor"] == 1.0
    assert "Volume" not in json.dumps(native)


@pytest.mark.parametrize(
    "mutation",
    [
        "profile",
        "source_uuid",
        "name",
        "categories",
        "source_unit",
        "native_uuid",
        "property",
        "unit",
        "factor",
        "projection_id",
    ],
)
def test_projection_rejects_inexact_scope_even_with_a_valid_resource(mutation):
    row = {
        "name": "Water",
        "categories": list(IDENTITY[1]),
        "unit": "cubic meter",
        "source_flow_id": SOURCE_UUID,
        "flow_id": NATIVE_UUID,
        "flow_name": "Water",
        "flow_property_id": PROPERTY_UUID,
        "flow_property_name": "Mass",
        "unit_id": UNIT_UUID,
        "unit_name": "kg",
        "amount_factor": 1000.0,
        "native_categories": ["emissions to water", "fossilwater"],
        "quantity_projection_id": reviewed_mapping()["mappings"][0]["id"],
    }
    profile = deepcopy(PROFILE)
    changes = {
        "source_uuid": ("source_flow_id", NATIVE_UUID),
        "name": ("name", "Water, unspecified natural origin"),
        "categories": ("categories", ["natural resource", "fossil well"]),
        "source_unit": ("unit", "kilogram"),
        "native_uuid": ("flow_id", SOURCE_UUID),
        "property": ("flow_property_name", "Volume"),
        "unit": ("unit_id", SOURCE_UUID),
        "factor": ("amount_factor", 1.0),
        "projection_id": ("quantity_projection_id", "other"),
    }
    if mutation == "profile":
        profile["technosphere"]["version"] = "2026"
    else:
        row[changes[mutation][0]] = changes[mutation][1]
    before = deepcopy((row, profile))
    with pytest.raises(ValueError):
        catalog_projection_factor(row, profile)
    assert (row, profile) == before


@pytest.mark.parametrize("kind", [0, 2, 3, 4, 5])
@pytest.mark.parametrize("sign", [-1, 1])
@pytest.mark.parametrize("formula", [False, True])
def test_common_scaling_inverse_preserves_signed_nominal_independent_marginal_formula_and_source(kind, sign, formula):
    source = document(kind, sign=sign, formula=formula).data[0]["exchanges"][1]
    before = deepcopy(source)
    native = scale_detached(source, 1000.0)
    assert native["amount"] == source["amount"] * 1000
    if kind == 2:
        assert native["loc"] == source["loc"] + math.log(1000)
        assert native["scale"] == source["scale"]
    if kind == 3:
        assert native["scale"] == source["scale"] * 1000
    assert_flow(scale_detached(native, 1000.0, inverse=True), source)
    assert source == before


@pytest.mark.parametrize("kind", [0, 3, 4, 5])
@pytest.mark.parametrize("sign", [-1, 1])
@pytest.mark.parametrize("formula", [False, True])
def test_actual_jsonld_writer_and_inverse_use_native_mass_and_keep_source(tmp_path, kind, sign, formula):
    source = document(kind, sign=sign, formula=formula)
    before = source.data
    path = write_openlca_jsonld(source, tmp_path / "water.zip")
    _, wire = raw_exchange(path)
    assert wire["amount"] == sign * 2000
    assert wire["flowProperty"]["@id"] == PROPERTY_UUID
    assert wire["unit"]["@id"] == UNIT_UUID
    assert wire["flow"]["refUnit"] == "kg"
    restored = load_openlca_jsonld(path, context=context())
    assert_flow(restored.data[0]["exchanges"][1], before[0]["exchanges"][1])
    assert source.data == before


@pytest.mark.parametrize("sign", [-1, 1])
@pytest.mark.parametrize("formula", [False, True])
def test_native_graph_projection_shifts_loc_without_recentering_or_changing_sigma(tmp_path, sign, formula):
    source = document(2, sign=sign, formula=formula)
    before = source.data
    path = write_graph(source, tmp_path / "native.zip")
    process, wire = raw_exchange(path)
    latent = next(p for p in process["parameters"] if "_latent_" in p["name"])
    assert wire["amount"] == sign * 2000
    assert latent["value"] == pytest.approx(math.log(4000))
    assert latent["uncertainty"]["mean"] == pytest.approx(math.log(4000))
    assert latent["uncertainty"]["sd"] == math.log(1.5)
    restored = load_openlca_jsonld(path, context=context())
    assert_flow(restored.data[0]["exchanges"][1], before[0]["exchanges"][1])
    assert [p["name"] for p in restored.data[0]["parameters"]] == ["dose"]
    assert source.data == before


@pytest.mark.parametrize("kind", [0, 2, 3, 4, 5])
@pytest.mark.parametrize("sign", [-1, 1])
@pytest.mark.parametrize("formula", [False, True])
def test_actual_ilcd_native_mass_factor_and_strict_inverse(tmp_path, kind, sign, formula):
    source = document(kind, sign=sign, formula=formula, format_id="ilcd")
    before = source.data
    references = automatic_references(source, ILCDReferenceCatalog(), dialect="openlca")
    row = references.lookup(before[0]["exchanges"][1], "biosphere")
    assert row["flow_id"] == NATIVE_UUID and row["amount_factor"] == 1000 and row["reference_unit"] == "kilogram"
    path = write_ilcd(
        source,
        tmp_path / "water-ilcd.zip",
        metadata_profile={
            "schema_version": 1,
            "defaults": {
                "dataset_version": "01.00.000",
                "process_type": "Unit process, single operation",
                "reference_year": 2020,
                "valid_until": 2030,
            },
        },
        dialect="openlca",
    )
    with ZipFile(path) as archive:
        flow = parse_xml(archive.read(f"flows/{NATIVE_UUID}.xml"))
        properties = flow.findall("f:flowProperties/f:flowProperty", NS)
        assert len(properties) == 1
        assert properties[0].find("f:referenceToFlowPropertyDataSet", NS).get("refObjectId") == PROPERTY_UUID
        assert float(properties[0].findtext("f:meanValue", namespaces=NS)) == 1
        group = next(
            parse_xml(archive.read(n))
            for n in archive.namelist()
            if n.startswith("unitgroups/") and UNIT_UUID.encode() in archive.read(n)
        )
        assert any(
            u.get(q("olca", "unitId")) == UNIT_UUID and u.findtext("u:name", namespaces=NS) == "kg"
            for u in group.findall("u:units/u:unit", NS)
        )
    restored = load_ilcd(path, context=context("ilcd"))
    assert_flow(restored.data[0]["exchanges"][1], before[0]["exchanges"][1])
    assert source.data == before


def test_other_profiles_and_other_water_compartments_do_not_get_a_factor():
    assert load_openlca_reference_catalog(context(version="2026")) is None
    assert load_openlca_reference_catalog(context(bio="3.9")) is None
    catalog = load_openlca_reference_catalog(context())
    assert all(
        getattr(ref, "amount_factor", 1) == 1 for identity, ref in catalog.biosphere.items() if identity != IDENTITY
    )


@pytest.mark.parametrize("mutation", ["unit", "property", "flow", "metadata_factor"])
def test_actual_native_wire_or_metadata_tampering_cannot_change_quantity_binding(tmp_path, mutation):
    source = document(2, formula=True)
    path = write_graph(source, tmp_path / "native.zip")
    with ZipFile(path) as archive:
        payload = {n: archive.read(n) for n in archive.namelist()}
    name = next(n for n in payload if n.startswith("processes/"))
    process = json.loads(payload[name])
    wire = next(e for e in process["exchanges"] if e["flow"]["@id"] == NATIVE_UUID)
    if mutation == "unit":
        wire["unit"]["@id"] = SOURCE_UUID
    elif mutation == "property":
        wire["flowProperty"]["@id"] = SOURCE_UUID
    elif mutation == "flow":
        wire["flow"]["@id"] = SOURCE_UUID
    else:
        # Factor is derived from the trusted catalog, never foreground extras.
        process.setdefault("otherProperties", {}).setdefault("brightpathExternalExchangeProperties", {})[
            str(wire["internalId"])
        ] = {PENDING_FACTOR: 1.0}
    payload[name] = json.dumps(process).encode()
    with ZipFile(tmp_path / "tampered.zip", "w") as archive:
        for name, content in payload.items():
            archive.writestr(name, content)
    with pytest.raises(ValueError):
        load_openlca_jsonld(tmp_path / "tampered.zip", context=context())


def test_literal_projected_native_graph_requires_canonical_decode_for_document_readback(tmp_path):
    path = write_graph(document(2), tmp_path / "native.zip")
    with pytest.raises(ValueError, match="require native uncertainty graph decoding"):
        load_openlca_jsonld(path, context=context(), decode_native_uncertainty=False)


@pytest.mark.parametrize("format_id", ["openlca_jsonld", "ilcd"])
def test_second_roundtrip_has_no_double_scaling_or_stale_native_helpers(tmp_path, format_id):
    source = document(2, formula=True, format_id=format_id)
    before = source.data
    if format_id == "openlca_jsonld":
        path = write_graph(source, tmp_path / "first.zip")
        restored = load_openlca_jsonld(path, context=context())
        second = write_graph(restored, tmp_path / "second.zip")
        actual = load_openlca_jsonld(second, context=context())
    else:
        metadata = {
            "schema_version": 1,
            "defaults": {
                "dataset_version": "01.00.000",
                "process_type": "Unit process, single operation",
                "reference_year": 2020,
                "valid_until": 2030,
            },
        }
        path = write_ilcd(source, tmp_path / "first.zip", metadata_profile=metadata, dialect="openlca")
        restored = load_ilcd(path, context=context("ilcd"))
        second = write_ilcd(restored, tmp_path / "second.zip", metadata_profile=metadata, dialect="openlca")
        actual = load_ilcd(second, context=context("ilcd"))
    assert_flow(actual.data[0]["exchanges"][1], before[0]["exchanges"][1])
    assert source.data == before


def test_generator_materializes_the_exact_reviewed_mapping_without_mutating_raw_metadata(monkeypatch):
    import importlib.util
    from pathlib import Path

    spec = importlib.util.spec_from_file_location(
        "quantity_projection_generator",
        Path(__file__).resolve().parents[1] / "scripts/generate_openlca_reference_catalog.py",
    )
    generator = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(generator)
    bundle = deepcopy(reviewed_mapping())
    mapping = bundle["mappings"][0]

    class Flow(dict):
        @property
        def key(self):
            return ("bio", self["code"])

    source = mapping["source"]
    flow = Flow(
        code=source["flow_id"], name=source["name"], categories=tuple(source["categories"]), unit=source["unit"]
    )
    raw = {"processes": [], "native_source": bundle["native_source"], "flows": [mapping["native"]]}
    before = deepcopy((flow, raw, bundle))
    monkeypatch.setattr(generator.bd, "Database", lambda name: [] if name == "tech" else [flow])
    result = generator.generate_catalog(
        raw_references=raw,
        technosphere_database="tech",
        biosphere_database="bio",
        technosphere_profile=PROFILE["technosphere"],
        biosphere_profile=PROFILE["biosphere"],
        source="Synthetic fixture of verified native metadata",
        biosphere_correspondences=bundle,
    )
    row = result["biosphere"][0]
    assert catalog_projection_factor(row, PROFILE) == 1000
    assert row["source_flow_id"] == SOURCE_UUID
    assert (flow, raw, bundle) == before


def test_bound_aliases_and_changed_formula_are_not_silently_overwritten():
    flow = document(5, formula=True).data[0]["exchanges"][1]
    flow["min"] = flow.pop("minimum")
    flow["max"] = flow.pop("maximum")
    before = deepcopy(flow)
    native = scale_detached(flow, 1000.0)
    assert native["min"] == 1000 and native["max"] == 5000
    assert_flow(scale_detached(native, 1000.0, inverse=True), flow)
    assert flow == before
    native["formula"] = "(dose + 1) * (1000.0)"
    assert scale_detached(native, 1000.0, inverse=True)["formula"] == "dose + 1"


def test_unsupported_distribution_and_conflicting_bounds_fail_without_source_mutation():
    for flow in [
        {"amount": 2, "uncertainty type": 6},
        {"amount": 2, "uncertainty type": 4, "min": 1, "minimum": 2, "maximum": 5},
    ]:
        before = deepcopy(flow)
        with pytest.raises(ValueError):
            scale_detached(flow, 1000.0)
        assert flow == before


def test_native_quantity_resource_bytes_are_manifest_guarded(tmp_path, monkeypatch):
    from brightpath import DATA_DIR
    from brightpath.formats import openlca_quantity_projection as projection

    directory = tmp_path / "export/openlca_references"
    directory.mkdir(parents=True)
    for name in ["native_quantity_projections.json", "RESOURCE_MANIFEST.json"]:
        (directory / name).write_bytes((DATA_DIR / "export/openlca_references" / name).read_bytes())
    monkeypatch.setattr(projection, "DATA_DIR", tmp_path)
    projection.reviewed_mapping.cache_clear()
    try:
        assert projection.reviewed_mapping()["mappings"][0]["amount_factor"] == 1000
        file = directory / "native_quantity_projections.json"
        file.write_bytes(file.read_bytes() + b"\n")
        projection.reviewed_mapping.cache_clear()
        with pytest.raises(ValueError, match="integrity"):
            projection.reviewed_mapping()
    finally:
        projection.reviewed_mapping.cache_clear()


def test_changed_native_log_location_is_inverted_not_recovered_from_source_metadata(tmp_path):
    path = write_graph(document(2), tmp_path / "native.zip")
    with ZipFile(path) as archive:
        payload = {n: archive.read(n) for n in archive.namelist()}
    name = next(n for n in payload if n.startswith("processes/"))
    process = json.loads(payload[name])
    latent = next(p for p in process["parameters"] if "_latent_" in p["name"])
    latent["value"] = latent["uncertainty"]["mean"] = math.log(5000)
    payload[name] = json.dumps(process).encode()
    with ZipFile(tmp_path / "changed.zip", "w") as archive:
        for name, content in payload.items():
            archive.writestr(name, content)
    result = load_openlca_jsonld(tmp_path / "changed.zip", context=context()).data[0]["exchanges"][1]
    assert result["amount"] == 2.0
    assert result["loc"] == pytest.approx(math.log(5))
    assert result["scale"] == math.log(1.5)
