"""UVEK resource vocabulary is preserved while native wire direction is Input."""

import json
from copy import deepcopy
from zipfile import ZipFile

import pytest

from brightpath import BackgroundContext, BiosphereProfile, FormatProfile, InventoryContext, TechnosphereProfile
from brightpath.formats.ilcd import load_ilcd, write_ilcd
from brightpath.formats.ilcd_package import NS, parse_xml
from brightpath.formats.openlca_jsonld import load_openlca_jsonld, write_openlca_jsonld
from brightpath.formats.openlca_references import load_openlca_reference_catalog
from brightpath.models import InventoryDocument

RESOURCES = [
    ("Occupation, agriculture", ("resources", "land"), "square meter-year", "5dbd8205-fa09-3895-90d3-5e1d7406c179"),
    ("Air", ("resources", "in air"), "kilogram", "fe0acd60-3ddc-11dd-aaa4-0050c2490048"),
]
PROFILE = {
    "schema_version": 1,
    "defaults": {
        "dataset_version": "01.00.000",
        "reference_year": 2026,
        "valid_until": 2027,
        "process_type": "Unit process, single operation",
    },
}


def document(resource, amount, format_id):
    name, categories, unit, _flow = resource
    context = InventoryContext(
        FormatProfile(format_id),
        BackgroundContext(TechnosphereProfile("uvek", "2026", "cutoff"), BiosphereProfile("uvek", "2026")),
    )
    activity = {
        "name": "Synthetic native resource consumer",
        "reference product": "synthetic output",
        "location": "GLO",
        "unit": "kilogram",
    }
    return InventoryDocument(
        data=[
            {
                **activity,
                "code": "synthetic-native-resource-consumer",
                "comment": "Synthetic foreground; exact native resource metadata, no background inventory.",
                "parameters": [{"name": "dose", "amount": amount}],
                "exchanges": [
                    {**activity, "type": "production", "amount": 1.0},
                    {
                        "name": name,
                        "categories": categories,
                        "unit": unit,
                        "type": "biosphere",
                        "amount": amount,
                        "formula": "dose",
                        "uncertainty type": 3,
                        "loc": amount,
                        "scale": 0.1,
                    },
                ],
            }
        ],
        context=context,
        database_name="synthetic-native-resource",
    )


@pytest.mark.parametrize("resource", RESOURCES, ids=["land", "in-air"])
@pytest.mark.parametrize("amount", [-2.0, 2.0])
@pytest.mark.parametrize("format_id", ["openlca_jsonld", "ilcd"])
def test_native_resource_alias_uses_input_without_changing_any_signed_coordinate(tmp_path, resource, amount, format_id):
    doc = document(resource, amount, format_id)
    before = deepcopy(doc.data)
    reference = load_openlca_reference_catalog(doc.context).biosphere[resource[:3]]
    assert reference.flow_id == resource[3]
    assert reference.native_categories == ("Elementary flows", "Resource", resource[1][1])
    if format_id == "openlca_jsonld":
        path = write_openlca_jsonld(doc, tmp_path / "resource.zip")
        with ZipFile(path) as archive:
            processes = [json.loads(archive.read(n)) for n in archive.namelist() if n.startswith("processes/")]
            wire = next(row for row in processes[0]["exchanges"] if row["flow"]["@id"] == resource[3])
            assert wire["isInput"] is True
            assert wire["flow"]["flowType"] == "ELEMENTARY_FLOW"
            assert wire["unit"]["@id"] == reference.unit_id
            assert wire["amount"] == amount
            assert wire["amountFormula"] == "dose"
        restored = load_openlca_jsonld(path, context=doc.context)
    else:
        path = write_ilcd(doc, tmp_path / "resource.zip", dialect="openlca", metadata_profile=PROFILE)
        with ZipFile(path) as archive:
            root = parse_xml(archive.read(next(n for n in archive.namelist() if n.startswith("processes/"))))
            wire = root.findall("p:exchanges/p:exchange", NS)[1]
            assert wire.findtext("p:exchangeDirection", namespaces=NS) == "Input"
            assert wire.find("p:referenceToFlowDataSet", NS).get("refObjectId") == resource[3]
            assert float(wire.findtext("p:resultingAmount", namespaces=NS)) == amount
        restored = load_ilcd(path, context=doc.context)
    actual = restored.data[0]["exchanges"][1]
    for field in ("name", "categories", "unit", "type", "amount", "formula", "uncertainty type", "loc", "scale"):
        if field == "categories":
            assert tuple(actual[field]) == tuple(before[0]["exchanges"][1][field])
        else:
            assert actual[field] == before[0]["exchanges"][1][field]
    assert doc.data == before
