"""Native person-transport quantity bindings without numerical conversion."""

import math
import zipfile
from copy import deepcopy

import pytest
from test_ilcd import FLOW, PROCESS, context, dataset, document, profile

from brightpath.background import InMemoryCatalogProvider
from brightpath.background.catalogs import TechnosphereCatalog
from brightpath.formats.ilcd import load_ilcd, validate_ilcd_package, write_ilcd
from brightpath.formats.ilcd_background import openlca_key
from brightpath.formats.ilcd_package import NS, ILCDError, parse_xml, q
from brightpath.units import normalize_unit

PROPERTY_ID = "8766e10f-9f41-4db0-8173-ad0d002a5b98"
GROUP_ID = "11d161f0-37e3-4d49-bf7a-ff4f31a9e5c7"
UNIT_ID = "fe8da65d-f0ea-4496-b13e-1955aaa412d7"
IDENTITY = ("name", "reference product", "location", "unit")
SCIENCE = ("amount", "uncertainty type", "loc", "scale", "minimum", "maximum", "formula")


def assert_quantity(path):
    with zipfile.ZipFile(path) as archive:
        assert f"flowproperties/{PROPERTY_ID}.xml" in archive.namelist()
        prop = parse_xml(archive.read(f"flowproperties/{PROPERTY_ID}.xml"))
        group_ref = prop.find(
            "fp:flowPropertiesInformation/fp:quantitativeReference/fp:referenceToReferenceUnitGroup", NS
        )
        assert group_ref.get("refObjectId") == GROUP_ID
        group = parse_xml(archive.read(f"unitgroups/{GROUP_ID}.xml"))
        index = group.findtext(
            "u:unitGroupInformation/u:quantitativeReference/u:referenceToReferenceUnit", namespaces=NS
        )
        units = group.findall("u:units/u:unit", NS)
        (reference,) = [node for node in units if node.get("dataSetInternalID") == index]
        assert reference.get(q("olca", "unitId")) == UNIT_ID
        assert reference.findtext("u:name", namespaces=NS) == "p*km"
        assert float(reference.findtext("u:meanValue", namespaces=NS)) == 1
        (miles,) = [node for node in units if node.findtext("u:name", namespaces=NS) == "p*mi"]
        assert float(miles.findtext("u:meanValue", namespaces=NS)) == 1.609344


@pytest.mark.parametrize("dialect", ["core", "openlca"])
@pytest.mark.parametrize(
    "uncertainty",
    [
        {"uncertainty type": 2, "loc": math.log(100), "scale": math.log(1.5)},
        {"uncertainty type": 3, "loc": 100, "scale": 2},
        {"uncertainty type": 4, "minimum": 80, "maximum": 120},
    ],
)
def test_person_transport_production_roundtrip_preserves_science_and_identity(tmp_path, dialect, uncertainty):
    record = dataset("passenger-transport", "Passenger transport", "Passenger service", 100)
    record["unit"] = record["exchanges"][0]["unit"] = "person kilometer"
    record["parameters"] = [{"name": "dose", "amount": 100}]
    record["exchanges"][0].update(uncertainty, formula="dose")
    original = deepcopy(record)
    source = document(record)
    before = source.data
    path = write_ilcd(source, tmp_path / "person.zip", metadata_profile=profile(), dialect=dialect)
    assert validate_ilcd_package(path) == ()
    assert_quantity(path)
    restored = load_ilcd(path, context=context())
    result = restored.data[0]
    (production,) = result["exchanges"]
    for field in IDENTITY:
        assert result[field] == production[field] == original[field]
    for field in SCIENCE:
        assert production.get(field) == original["exchanges"][0].get(field)
    again = write_ilcd(restored, tmp_path / "again.zip")
    repeated = load_ilcd(again, context=context()).data[0]
    assert repeated["code"] == result["code"]
    assert repeated["exchanges"][0]["amount"] == 100
    assert source.data == before and record == original
    assert normalize_unit("p*km") == "p*km"  # No global/other-format alias.


@pytest.mark.parametrize("external", [False, True])
def test_person_transport_supplier_keeps_formula_normal_uncertainty_and_native_provider(tmp_path, external):
    producer = dataset("passenger-provider", "Passenger provider", "Passenger service", 100)
    producer["unit"] = producer["exchanges"][0]["unit"] = "person kilometer"
    consumer = dataset("consumer", "Consumer", "Consumer product", 1)
    consumer["parameters"] = [{"name": "dose", "amount": 2}]
    exchange = {
        **{field: producer[field] for field in IDENTITY},
        "type": "technosphere",
        "amount": 2,
        "formula": "dose",
        "uncertainty type": 3,
        "loc": 2,
        "scale": 0.125,
    }
    consumer["exchanges"].append(exchange)
    provider = InMemoryCatalogProvider(
        technosphere=[
            TechnosphereCatalog(
                context().background.technosphere,
                frozenset([tuple(producer[field] for field in IDENTITY)]),
                references={tuple(producer[field] for field in IDENTITY): (PROCESS, FLOW)},
            )
        ]
    )
    source = document(consumer) if external else document(producer, consumer)
    before = source.data
    path = write_ilcd(
        source, tmp_path / "supplier.zip", metadata_profile=profile(), dialect="openlca", catalog_provider=provider
    )
    assert validate_ilcd_package(path) == ()
    assert_quantity(path)
    records = {row["name"]: row for row in load_ilcd(path, context=context()).data}
    actual = next(row for row in records["Consumer"]["exchanges"] if row["type"] == "technosphere")
    for field in (*IDENTITY, *SCIENCE):
        assert actual.get(field) == exchange.get(field)
    if external:
        assert actual["ilcd"]["flow_id"] == FLOW
        assert actual["ilcd"]["process_id"] == openlca_key(PROCESS, FLOW)
        with zipfile.ZipFile(path) as archive:
            assert sum(name.startswith("processes/") for name in archive.namelist()) == 1
    else:
        assert actual["ilcd"]["process_id"] == records["Passenger provider"]["code"]
    assert source.data == before


@pytest.mark.parametrize("unit", ["normal cubic meter", "vehicle kilometer", "unknown unit"])
def test_unrelated_foreground_units_remain_unsupported(tmp_path, unit):
    record = dataset()
    record["unit"] = record["exchanges"][0]["unit"] = unit
    source = document(record)
    before = source.data
    with pytest.raises(ILCDError, match="No explicit ILCD quantity definition"):
        write_ilcd(source, tmp_path / "unsupported.zip", metadata_profile=profile(), dialect="openlca")
    assert not (tmp_path / "unsupported.zip").exists()
    assert source.data == before
