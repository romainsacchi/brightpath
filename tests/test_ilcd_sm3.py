"""Verified ecoinvent Sm3 native fallback; never an ordinary-volume proxy."""

import hashlib
import json
import math
import shutil
import zipfile
from types import SimpleNamespace

import pytest
from test_ilcd import FLOW, PROCESS, context, dataset, document, profile

from brightpath import DATA_DIR
from brightpath.background import InMemoryCatalogProvider
from brightpath.background.catalogs import TechnosphereCatalog
from brightpath.core import BackgroundContext, BiosphereProfile, InventoryContext, TechnosphereProfile
from brightpath.formats import ilcd_background
from brightpath.formats.ilcd import load_ilcd, validate_ilcd_package, write_ilcd
from brightpath.formats.ilcd_background import openlca_key, quantity_definition
from brightpath.formats.ilcd_package import NS, ILCDError, parse_xml, q

UNIT = "225da0bd-ba54-48bc-9a0e-5fa27d722185"
GROUP = "c2dba802-b9a4-345b-b4b3-847754301054"
PROPERTY = "c57e6ed2-5c28-3348-aaee-75d637b80c77"
FIELDS = ("name", "reference product", "location", "unit")
VERIFIED = [
    ("3.9.1", "cutoff"),
    ("3.10.1", "cutoff"),
    ("3.10.1", "consequential"),
    ("3.11", "cutoff"),
    ("3.12", "cutoff"),
]


def target(version="3.12", model="cutoff", family="ecoinvent"):
    return InventoryContext(
        context().format,
        BackgroundContext(TechnosphereProfile(family, version, model), BiosphereProfile("ecoinvent", version)),
    )


def foreground_source(version="3.12", model="cutoff", uncertainty=None):
    record = dataset("sm3", "Standard gas service", "Standard gas", 100)
    record["unit"] = record["exchanges"][0]["unit"] = "standard cubic meter"
    record["parameters"] = [{"name": "dose", "amount": 100}]
    record["exchanges"][0].update(uncertainty or {"uncertainty type": 3, "loc": 100, "scale": 0.125}, formula="dose")
    return document(record).replace(context=target(version, model))


def supplier_source(version="3.12", model="cutoff", *, external=True, kind=3):
    supplier = dataset("supplier", "Synthetic standard gas supplier", "Synthetic standard gas", 100)
    supplier["unit"] = supplier["exchanges"][0]["unit"] = "standard cubic meter"
    consumer = dataset("consumer", "Gas consumer", "Consumed product", 1)
    consumer["parameters"] = [{"name": "dose", "amount": 2}]
    exchange = {
        **{key: supplier[key] for key in FIELDS},
        "type": "technosphere",
        "amount": -2 if kind == 2 else 2,
        "formula": "-dose" if kind == 2 else "dose",
        "uncertainty type": kind,
        "loc": math.log(3) if kind == 2 else 3,
        "scale": math.log(1.5) if kind == 2 else 0.125,
    }
    if kind == 2:
        exchange["negative"] = True
    consumer["exchanges"].append(exchange)
    ctx = target(version, model)
    key = tuple(supplier[field] for field in FIELDS)
    provider = InMemoryCatalogProvider(
        technosphere=[
            TechnosphereCatalog(ctx.background.technosphere, frozenset([key]), references={key: (PROCESS, FLOW)})
        ]
    )
    source = document(consumer) if external else document(supplier, consumer)
    return source.replace(context=ctx), provider


def assert_metadata(path):
    with zipfile.ZipFile(path) as archive:
        group = parse_xml(archive.read(f"unitgroups/{GROUP}.xml"))
        prop = parse_xml(archive.read(f"flowproperties/{PROPERTY}.xml"))
        units = group.findall("u:units/u:unit", NS)
        assert len(units) == 1
        assert units[0].get(q("olca", "unitId")) == UNIT
        assert units[0].findtext("u:name", namespaces=NS) == "Sm3"
        assert float(units[0].findtext("u:meanValue", namespaces=NS)) == 1
        assert (
            prop.find("fp:flowPropertiesInformation/fp:quantitativeReference/fp:referenceToReferenceUnitGroup", NS).get(
                "refObjectId"
            )
            == GROUP
        )
        for root, prefix in [(group, "u"), (prop, "fp")]:
            comment = root.findtext(
                f"{prefix}:"
                + ("unitGroupInformation" if prefix == "u" else "flowPropertiesInformation")
                + f"/{prefix}:dataSetInformation/c:generalComment",
                namespaces=NS,
            )
            assert "15 degrees C" in comment and "101.325 kPa" in comment


@pytest.mark.parametrize("version,model", VERIFIED)
def test_sm3_foreground_exact_verified_profile_and_no_coefficient_conversion(tmp_path, version, model):
    source = foreground_source(version, model)
    before = source.data
    path = write_ilcd(source, tmp_path / "sm3.zip", metadata_profile=profile(), dialect="openlca")
    assert not validate_ilcd_package(path)
    assert_metadata(path)
    result = load_ilcd(path, context=target(version, model)).data[0]
    assert result["unit"] == result["exchanges"][0]["unit"] == "standard cubic meter"
    for key in ("amount", "loc", "scale", "formula"):
        assert result["exchanges"][0][key] == before[0]["exchanges"][0][key]
    assert source.data == before
    again = write_ilcd(load_ilcd(path, context=target(version, model)), tmp_path / "again.zip")
    assert load_ilcd(again, context=target(version, model)).data[0]["exchanges"][0]["amount"] == 100


@pytest.mark.parametrize("external", [False, True])
@pytest.mark.parametrize("kind", [2, 3])
@pytest.mark.parametrize("version", ["3.11", "3.12"])
def test_sm3_provider_reuses_existing_background_UUIDs_and_retains_science(tmp_path, external, kind, version):
    source, provider = supplier_source(version=version, external=external, kind=kind)
    before = source.data
    path = write_ilcd(
        source, tmp_path / "supplier.zip", metadata_profile=profile(), dialect="openlca", catalog_provider=provider
    )
    assert not validate_ilcd_package(path)
    assert_metadata(path)
    rows = {record["name"]: record for record in load_ilcd(path, context=target(version)).data}
    result = rows["Gas consumer"]["exchanges"][1]
    original = next(row for row in before if row["name"] == "Gas consumer")["exchanges"][1]
    for key in (*FIELDS, "amount", "uncertainty type", "scale", "formula"):
        assert result[key] == original[key]
    assert result["loc"] == pytest.approx(original["loc"], abs=1e-14)
    assert result.get("negative", False) == original.get("negative", False)
    if external:
        assert result["ilcd"]["flow_id"] == FLOW
        assert result["ilcd"]["process_id"] == openlca_key(PROCESS, FLOW)
        with zipfile.ZipFile(path) as archive:
            assert sum(name.startswith("processes/") for name in archive.namelist()) == 1
    assert source.data == before


@pytest.mark.parametrize(
    "version,model",
    [
        ("3.7.1", "cutoff"),
        ("3.9", "cutoff"),
        ("3.10", "cutoff"),
        ("3.11", "consequential"),
        ("3.12", "consequential"),
        ("3.13", "cutoff"),
    ],
)
def test_sm3_unverified_native_profiles_are_not_claimed(tmp_path, version, model):
    source = foreground_source(version, model)
    with pytest.raises(ILCDError) as error:
        write_ilcd(source, tmp_path / "unsupported.zip", metadata_profile=profile(), dialect="openlca")
    assert error.value.code == "ilcd.unit_mapping_required"
    assert not (tmp_path / "unsupported.zip").exists()


@pytest.mark.parametrize("version", ["3.11", "3.12"])
def test_sm3_requires_verified_profile_and_exact_quantity_pair(version):
    with pytest.raises(ILCDError):
        quantity_definition("standard cubic meter")
    with pytest.raises(ILCDError):
        quantity_definition("standard cubic meter", profile=TechnosphereProfile("uvek", "2025", "cutoff"))
    volume = quantity_definition("cubic meter")
    assert volume["property_id"] != PROPERTY and volume["group_id"] != GROUP and volume["unit_id"] != UNIT
    ordinary_ref = SimpleNamespace(flow_property_id=volume["property_id"], unit_id=volume["unit_id"])
    with pytest.raises(ILCDError):
        quantity_definition("standard cubic meter", ordinary_ref, profile=target(version).background.technosphere)
    exact_ref = SimpleNamespace(flow_property_id=PROPERTY, unit_id=UNIT)
    assert quantity_definition("Sm3", exact_ref, profile=target(version).background.technosphere)["factor"] == 1
    with pytest.raises(ILCDError):
        quantity_definition("cubic meter", exact_ref, profile=target(version).background.technosphere)
    for unit in ("normal cubic meter", "Nm3", "unverified gas unit"):
        with pytest.raises(ILCDError):
            quantity_definition(unit, profile=target(version).background.technosphere)


def test_sm3_3_11_records_exact_native_vintage_witness_without_changing_quantity():
    metadata = ilcd_background._sm3_metadata()
    entries = [row for row in metadata["profiles"] if row["version"] == "3.11"]
    assert entries == [
        {
            "family": "ecoinvent",
            "version": "3.11",
            "system_model": "cutoff",
            "unit_master_sha256": "e5a424fc15ad1f509743f166f9b4cb156f2d759896d11b9e4342e96189d4dc9b",
        }
    ]
    witness = metadata["provenance"]["ecoinvent_3_11_unit_witness"]
    assert witness["archive_filename"] == "ecoinvent 3.11_cutoff_ecoSpold02.7z"
    assert witness["member"] == "MasterData/Units.xml"
    assert witness["units_xml_sha256"] == entries[0]["unit_master_sha256"]
    assert witness["units_xml_bytes"] == 22080
    assert witness["unit_uuid"] == UNIT and witness["unit_name"] == "Sm3"
    assert witness["unit_comment"] == ("standard cubic metre = cubic metre of gas at 15°C; 101.325 kPa (ISO 13443)")
    quantity = quantity_definition("Sm3", profile=target("3.11").background.technosphere)
    assert quantity == quantity_definition("Sm3", profile=target("3.12").background.technosphere)
    assert quantity["group_id"] == GROUP and quantity["property_id"] == PROPERTY
    assert quantity["unit_id"] == UNIT and quantity["factor"] == 1


def test_sm3_integrity_manifest_and_independent_copy(tmp_path, monkeypatch):
    directory = DATA_DIR / "export" / "ilcd_quantities"
    manifest = json.loads((directory / "SM3_RESOURCE_MANIFEST.json").read_text())
    raw = (directory / manifest["file"]).read_bytes()
    assert len(raw) == manifest["size"] and hashlib.sha256(raw).hexdigest() == manifest["sha256"]
    original = quantity_definition("standard cubic meter", profile=target().background.technosphere)
    original["units"][0]["factor"] = 999
    assert (
        quantity_definition("standard cubic meter", profile=target().background.technosphere)["units"][0]["factor"] == 1
    )
    fake = tmp_path / "export" / "ilcd_quantities"
    shutil.copytree(directory, fake)
    (fake / manifest["file"]).write_bytes(raw + b" ")
    with monkeypatch.context() as patch:
        patch.setattr(ilcd_background, "DATA_DIR", tmp_path)
        ilcd_background._sm3_metadata.cache_clear()
        try:
            with pytest.raises(ILCDError) as error:
                quantity_definition("standard cubic meter", profile=target().background.technosphere)
            assert error.value.code == "ilcd.quantity_catalog_invalid"
        finally:
            ilcd_background._sm3_metadata.cache_clear()
