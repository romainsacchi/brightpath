"""Five audited native references: strict generator joins and actual JSON-LD refs."""

import importlib.util
import json
from copy import deepcopy
from pathlib import Path
from zipfile import ZipFile

import pytest

from brightpath import (
    BackgroundContext,
    BiosphereProfile,
    FormatProfile,
    InventoryContext,
    TechnosphereProfile,
)
from brightpath.exceptions import SerializationError
from brightpath.formats.openlca_jsonld import load_openlca_jsonld, write_openlca_jsonld
from brightpath.formats.openlca_references import load_openlca_reference_catalog
from brightpath.models import InventoryDocument

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "native_correspondence_generator", ROOT / "scripts/generate_openlca_reference_catalog.py"
)
GENERATOR = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GENERATOR)
BUNDLE = json.loads((ROOT / "brightpath/data/export/openlca_references/biosphere_correspondences.json").read_text())
PROFILE = BUNDLE["profile"]


def test_openlca_manifest_bytes_invalidate_migration_fingerprint_without_touching_actual_resources(
    tmp_path, monkeypatch
):
    from brightpath.background import uvek_export

    actual_data = uvek_export.DATA_DIR
    manifest_names = (
        "migrations/RESOURCE_MANIFEST.json",
        "export/reference_catalogs/RESOURCE_MANIFEST.json",
        "export/openlca_references/RESOURCE_MANIFEST.json",
    )
    protected = {actual_data / name: (actual_data / name).read_bytes() for name in manifest_names}
    protected[uvek_export.SCHEMA_PATH] = uvek_export.SCHEMA_PATH.read_bytes()
    copied_data = tmp_path / "fingerprint-data"
    for name in manifest_names:
        target = copied_data / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(protected[actual_data / name])
    copied_schema = tmp_path / "migration-schema.json"
    copied_schema.write_bytes(protected[uvek_export.SCHEMA_PATH])
    monkeypatch.setattr(uvek_export, "DATA_DIR", copied_data)
    monkeypatch.setattr(uvek_export, "SCHEMA_PATH", copied_schema)

    original_fingerprint = uvek_export.migration_resource_fingerprint()
    openlca_manifest = copied_data / manifest_names[-1]
    original_bytes = openlca_manifest.read_bytes()
    openlca_manifest.write_bytes(original_bytes + b"\n")
    assert json.loads(openlca_manifest.read_bytes()) == json.loads(original_bytes)
    assert uvek_export.migration_resource_fingerprint() != original_fingerprint

    openlca_manifest.write_bytes(original_bytes)
    assert uvek_export.migration_resource_fingerprint() == original_fingerprint
    assert all(path.read_bytes() == before for path, before in protected.items())


@pytest.mark.parametrize(
    "name,categories,native_uuid",
    [
        ("Argon", ("air",), "fe0acd60-3ddc-11dd-a350-0050c2490048"),
        ("Nitric acid", ("air", "urban air close to ground"), "37d014cb-8c2d-4c98-ab5c-2d4550822c2f"),
        ("Oxygen", ("air",), "08a91e70-3ddc-11dd-9745-0050c2490048"),
        ("Oxygen", ("natural resource", "in air"), "c132f668-6330-45fe-b230-d8de62e64ff1"),
        ("Sulfuric acid", ("water",), "63e82565-8549-11e0-9d78-0800200c9a66"),
    ],
)
def test_catalog_matches_independently_recorded_native_uuid_and_mass_references(name, categories, native_uuid):
    reference = load_openlca_reference_catalog(context()).biosphere[(name, categories, "kilogram")]
    assert reference.flow_id == native_uuid
    assert reference.flow_name == name
    assert reference.flow_property_id == "93a60a56-a3c8-11da-a746-0800200b9a66"
    assert reference.flow_property_name == "Mass"
    assert reference.unit_id == "20aadc24-a391-41cf-b340-3e4529f44bde"
    assert reference.unit_name == "kg"


class FakeFlow(dict):
    @property
    def key(self):
        return ("biosphere", self["code"])


def source_flow(mapping):
    source = mapping["source"]
    return FakeFlow(
        code=source["flow_id"], name=source["name"], categories=tuple(source["categories"]), unit=source["unit"]
    )


def raw_native():
    return {
        "processes": [],
        "native_source": deepcopy(BUNDLE["native_source"]),
        "flows": [deepcopy(mapping["native"]) for mapping in BUNDLE["mappings"]],
    }


def generate(monkeypatch, raw, flows=None, bundle=None, **kwargs):
    if flows is None:
        flows = [source_flow(mapping) for mapping in BUNDLE["mappings"]]
    monkeypatch.setattr(GENERATOR.bd, "Database", lambda name: [] if name == "uvek" else flows)
    return GENERATOR.generate_catalog(
        raw_references=raw,
        technosphere_database="uvek",
        biosphere_database="biosphere",
        technosphere_profile=PROFILE["technosphere"],
        biosphere_profile=PROFILE["biosphere"],
        source="synthetic metadata fixture using recorded native references",
        biosphere_correspondences=deepcopy(BUNDLE) if bundle is None else bundle,
        **kwargs,
    )


def test_generator_uses_five_recorded_native_uuid_and_unit_references_without_mutation(monkeypatch):
    raw = raw_native()
    flows = [source_flow(mapping) for mapping in BUNDLE["mappings"]]
    before = deepcopy((raw, flows, BUNDLE))
    result = generate(monkeypatch, raw, flows)
    assert result["coverage"]["biosphere_references"] == 5
    assert result["coverage"]["missing_biosphere_references"] == 0
    for mapping in BUNDLE["mappings"]:
        row = next(
            row
            for row in result["biosphere"]
            if row["name"] == mapping["source"]["name"] and row["categories"] == mapping["source"]["categories"]
        )
        assert row["flow_id"] == mapping["native"]["flow_ref_id"] != mapping["source"]["flow_id"]
        assert row["flow_property_id"] == mapping["native"]["flow_property_ref_id"]
        assert row["unit_id"] == mapping["native"]["unit_ref_id"]
        assert row["unit_name"] == "kg" and row["unit"] == "kilogram"
    assert (raw, flows, BUNDLE) == before


@pytest.mark.parametrize(
    "field,value",
    [
        ("flow_ref_id", "00000000-0000-4000-8000-000000000001"),
        ("unit_ref_id", "00000000-0000-4000-8000-000000000002"),
        ("flow_property_ref_id", "00000000-0000-4000-8000-000000000003"),
        ("unit", "g"),
        ("flow_property_name", "Volume"),
        ("unit_group_ref_id", "00000000-0000-4000-8000-000000000004"),
        ("reference_unit_factor", 1000.0),
        ("flow_type", "PRODUCT_FLOW"),
        ("category_path", ["Elementary flows", "Emission to water", "unspecified"]),
    ],
)
def test_generator_rejects_changed_native_metadata_instead_of_inferring_a_link(monkeypatch, field, value):
    raw = raw_native()
    raw["flows"][0][field] = value
    before = deepcopy(raw)
    with pytest.raises(ValueError, match="correspondence|recorded native metadata"):
        generate(monkeypatch, raw)
    assert raw == before


def test_generator_rejects_ambiguous_native_uuid_rows(monkeypatch):
    raw = raw_native()
    raw["flows"].append(deepcopy(raw["flows"][0]))
    with pytest.raises(ValueError, match="exactly one native flow"):
        generate(monkeypatch, raw)


def test_generator_rejects_wrong_native_build(monkeypatch):
    raw = raw_native()
    raw["native_source"]["archive_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="source build"):
        generate(monkeypatch, raw)


def test_generator_rejects_unexpected_original_uuid_in_pinned_native_build(monkeypatch):
    raw = raw_native()
    unexpected = deepcopy(raw["flows"][0])
    unexpected["flow_ref_id"] = BUNDLE["mappings"][0]["source"]["flow_id"]
    raw["flows"].append(unexpected)
    with pytest.raises(ValueError, match="unexpectedly also exists"):
        generate(monkeypatch, raw)


def test_default_correspondence_loader_rejects_integrity_mismatch(tmp_path, monkeypatch):
    path = tmp_path / "biosphere_correspondences.json"
    path.write_text(json.dumps(BUNDLE))
    (tmp_path / "RESOURCE_MANIFEST.json").write_text(
        json.dumps({"biosphere_correspondences": {"file": path.name, "size": path.stat().st_size, "sha256": "0" * 64}})
    )
    monkeypatch.setattr(GENERATOR, "_CORRESPONDENCE_PATH", path)
    with pytest.raises(ValueError, match="integrity manifest"):
        GENERATOR._correspondence_index(PROFILE, None)


@pytest.mark.parametrize("field,value", [("name", "Different chemical"), ("categories", ("water",)), ("unit", "gram")])
def test_generator_rejects_source_identity_change_under_same_uuid(monkeypatch, field, value):
    flows = [source_flow(mapping) for mapping in BUNDLE["mappings"]]
    flows[0][field] = value
    with pytest.raises(ValueError, match="Source biosphere identity"):
        generate(monkeypatch, raw_native(), flows)


def test_generator_rejects_other_profile_or_duplicate_correspondences(monkeypatch):
    bundle = deepcopy(BUNDLE)
    bundle["profile"]["technosphere"]["version"] = "2026"
    with pytest.raises(ValueError, match="exact profile"):
        generate(monkeypatch, raw_native(), bundle=bundle)
    bundle = deepcopy(BUNDLE)
    bundle["mappings"].append(deepcopy(bundle["mappings"][0]))
    with pytest.raises(ValueError, match="Ambiguous"):
        generate(monkeypatch, raw_native(), bundle=bundle)


def test_generator_does_not_match_unlisted_flow_by_name_or_convert_water(monkeypatch):
    unknown = source_flow(BUNDLE["mappings"][0])
    unknown["code"] = "00000000-0000-4000-8000-000000000099"
    water = FakeFlow(
        code="2256a142-8242-4b4f-b9aa-a167803989ca",
        name="Water",
        categories=("water", "fossil well"),
        unit="cubic meter",
    )
    result = generate(monkeypatch, raw_native(), [unknown, water], allow_missing_biosphere=True)
    assert result["biosphere"] == []
    assert result["coverage"]["missing_biosphere_references"] == 2


def context():
    return InventoryContext(
        FormatProfile("openlca_jsonld"),
        BackgroundContext(TechnosphereProfile("uvek", "2025", "cutoff"), BiosphereProfile("ecoinvent", "3.10")),
    )


def document():
    name = "Synthetic exact-reference test"
    exchanges = [
        {
            "type": "production",
            "name": name,
            "reference product": name,
            "location": "CH",
            "unit": "unit",
            "amount": 1.0,
        },
        {
            "type": "technosphere",
            "name": "Electricity, low voltage, at grid",
            "reference product": "Electricity, low voltage, at grid",
            "location": "RER",
            "unit": "kilowatt hour",
            "amount": 0.5,
        },
    ]
    for index, mapping in enumerate(BUNDLE["mappings"]):
        source = mapping["source"]
        exchanges.append(
            {
                "type": "biosphere",
                "name": source["name"],
                "categories": tuple(source["categories"]),
                "unit": source["unit"],
                "amount": (index + 1) * 0.125,
            }
        )
    return InventoryDocument(
        data=[{"name": name, "reference product": name, "location": "CH", "unit": "unit", "exchanges": exchanges}],
        context=context(),
    )


def test_actual_jsonld_uses_recorded_native_refs_keeps_provider_and_roundtrips_without_mutation(tmp_path):
    source = document()
    before = deepcopy(source.data)
    archive = write_openlca_jsonld(source, tmp_path / "native-refs.zip")
    with ZipFile(archive) as z:
        process = json.loads(z.read(next(name for name in z.namelist() if name.startswith("processes/"))))
        by_name = {(e["flow"]["name"], e["flow"]["@id"]): e for e in process["exchanges"]}
        provider = next(e for e in process["exchanges"] if e.get("defaultProvider"))
        assert provider["defaultProvider"]["@id"] == "0c5cc00d-0625-3fd0-bc34-5df18f4cfd77"
        assert provider["flow"]["@id"] == "6e636642-6710-30fa-beae-bafdebd91217"
        for mapping in BUNDLE["mappings"]:
            native = mapping["native"]
            exchange = by_name[(native["flow_name"], native["flow_ref_id"])]
            assert exchange["flowProperty"]["@id"] == native["flow_property_ref_id"]
            assert exchange["unit"]["@id"] == native["unit_ref_id"]
            assert "defaultProvider" not in exchange
            assert "flows/" + native["flow_ref_id"] + ".json" not in z.namelist()
    loaded = load_openlca_jsonld(archive, context=context())
    assert [(e["name"], e.get("categories"), e["unit"], e["amount"]) for e in loaded.data[0]["exchanges"]] == [
        (e["name"], e.get("categories"), e["unit"], e["amount"]) for e in before[0]["exchanges"]
    ]
    assert source.data == before


@pytest.mark.parametrize("variant", ["resource", "unit"])
def test_runtime_reference_catalog_requires_exact_reviewed_water_context(tmp_path, variant):
    catalog = load_openlca_reference_catalog(context())
    assert catalog.biosphere[("Water", ("water", "fossil well"), "cubic meter")].amount_factor == 1000
    source = document()
    data = source.data
    data[0]["exchanges"].append(
        {
            "type": "biosphere",
            "name": "Water",
            "categories": ("water", "fossil well"),
            "unit": "cubic meter",
            "amount": 1.0,
        }
    )
    if variant == "resource":
        data[0]["exchanges"][-1]["categories"] = ("natural resource", "fossil well")
    else:
        data[0]["exchanges"][-1]["unit"] = "kilogram"
    with pytest.raises(SerializationError, match="no exact biosphere reference"):
        write_openlca_jsonld(InventoryDocument(data=data, context=context()), tmp_path / "unlinked.zip")
