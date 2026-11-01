"""Exact native provider direction is independent of the authored scalar sign."""

import json
import math
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from zipfile import ZipFile

import pytest

from brightpath import BackgroundContext, BiosphereProfile, FormatProfile, InventoryContext, TechnosphereProfile
from brightpath.formats.ilcd import load_ilcd, write_ilcd
from brightpath.formats.ilcd_package import NS, ILCDError, parse_xml, xml_bytes
from brightpath.formats.openlca_jsonld import load_openlca_jsonld, write_openlca_jsonld
from brightpath.formats.openlca_references import load_openlca_reference_catalog
from brightpath.models import InventoryDocument

FIELDS = ("name", "reference product", "location", "unit")
WASTE = (
    "Disposal, solvents mixture, 16.5% water, to hazardous waste incineration",
    "Disposal, solvents mixture, 16.5% water, to hazardous waste incineration",
    "CH",
    "kilogram",
)
PROFILE = {
    "schema_version": 1,
    "defaults": {
        "dataset_version": "01.00.000",
        "reference_year": 2026,
        "valid_until": 2027,
        "process_type": "Unit process, single operation",
    },
}


def context(version="2026", format_id="openlca_jsonld"):
    return InventoryContext(
        FormatProfile(format_id),
        BackgroundContext(
            TechnosphereProfile("uvek", version, "cutoff"),
            BiosphereProfile("uvek", "2026") if version == "2026" else BiosphereProfile("ecoinvent", "3.10"),
        ),
    )


def document(amount, uncertainty=0, version="2026", format_id="openlca_jsonld"):
    activity = dict(zip(FIELDS, ("Synthetic waste consumer", "consumer output", "GLO", "kilogram"), strict=True))
    row = dict(zip(FIELDS, WASTE, strict=True))
    row.update(type="technosphere", amount=amount, formula="dose")
    if uncertainty == 3:
        row.update({"uncertainty type": 3, "loc": amount, "scale": 0.1})
    elif uncertainty == 2:
        # exp(sigma)=1.25 is exact in ILCD's three-decimal native field.
        row.update(
            {"uncertainty type": 2, "loc": math.log(abs(amount)), "scale": math.log(1.25), "negative": amount < 0}
        )
    return InventoryDocument(
        data=[
            {
                **activity,
                "code": "synthetic-native-waste-consumer",
                "comment": "Synthetic consumer of a verified native reference; no background recipe is included.",
                "parameters": [{"name": "dose", "amount": amount}],
                "exchanges": [{**activity, "type": "production", "amount": 1.0}, row],
            }
        ],
        context=context(version, format_id),
        database_name="synthetic-native-direction",
    )


def json_entities(path):
    with ZipFile(path) as archive:
        return {n: json.loads(archive.read(n)) for n in archive.namelist() if n.endswith(".json")}


def rewrite_json(path, entities):
    with ZipFile(path, "w") as archive:
        for name, value in entities.items():
            archive.writestr(name, json.dumps(value))


def assert_preserved(before, after):
    for key in (*FIELDS, "amount", "formula", "uncertainty type", "loc", "scale", "negative"):
        if key in before:
            if key in {"loc", "scale"}:
                assert after[key] == pytest.approx(before[key], rel=1e-12, abs=1e-12)
            else:
                assert after[key] == before[key]


@pytest.mark.parametrize("amount", [-2.0, 2.0])
@pytest.mark.parametrize("uncertainty", [0, 2, 3])
def test_native2026_jsonld_waste_is_foreground_output_with_original_signed_scalar(tmp_path, amount, uncertainty):
    doc = document(amount, uncertainty)
    before = deepcopy(doc.data)
    path = write_openlca_jsonld(doc, tmp_path / "native-waste.zip")
    entities = json_entities(path)
    process = next(v for k, v in entities.items() if k.startswith("processes/"))
    wire = next(r for r in process["exchanges"] if r.get("defaultProvider"))
    assert wire["flow"]["flowType"] == "WASTE_FLOW"
    assert wire["flow"]["@id"] == "4487861f-24e6-378a-aad9-16973cbb14f2"
    assert wire["defaultProvider"]["@id"] == "5603e594-cc40-4c59-8aaf-625bf939228e"
    assert wire["isInput"] is False
    assert wire["amount"] == amount
    assert wire["amountFormula"] == "dose"
    restored = load_openlca_jsonld(path, context=doc.context).data[0]["exchanges"][1]
    assert_preserved(before[0]["exchanges"][1], restored)
    assert doc.data == before


@pytest.mark.parametrize("mutation", ["flow_type", "direction", "provider"])
def test_native2026_jsonld_rejects_changed_type_direction_or_provider(tmp_path, mutation):
    doc = document(-2.0, 3)
    path = write_openlca_jsonld(doc, tmp_path / "changed.zip")
    entities = json_entities(path)
    process = next(v for k, v in entities.items() if k.startswith("processes/"))
    wire = next(r for r in process["exchanges"] if r.get("defaultProvider"))
    if mutation == "flow_type":
        wire["flow"]["flowType"] = "PRODUCT_FLOW"
    elif mutation == "direction":
        wire["isInput"] = True
    else:
        wire["defaultProvider"]["@id"] = "00000000-0000-4000-8000-000000000001"
    rewrite_json(path, entities)
    with pytest.raises(ValueError, match="flow type|direction|Unknown or ambiguous"):
        load_openlca_jsonld(path, context=doc.context)


@pytest.mark.parametrize("amount", [-2.0, 2.0])
def test_native2025_verified_waste_uses_its_own_native_flow_uuid_and_input_reference(tmp_path, amount):
    doc = document(amount, 3, version="2025")
    reference = load_openlca_reference_catalog(doc.context).technosphere[WASTE]
    assert reference.native_flow_type == "WASTE_FLOW"
    assert reference.native_reference_is_input is True
    assert reference.flow_id == "aef7e2a0-7858-4d9a-aa56-42b862959d6a"
    assert reference.flow_id != load_openlca_reference_catalog(context()).technosphere[WASTE].flow_id
    path = write_openlca_jsonld(doc, tmp_path / "legacy2025.zip")
    process = next(v for k, v in json_entities(path).items() if k.startswith("processes/"))
    wire = next(r for r in process["exchanges"] if r.get("defaultProvider"))
    assert wire["flow"]["flowType"] == "WASTE_FLOW"
    assert wire["isInput"] is False
    assert wire["amount"] == amount
    assert_preserved(
        doc.data[0]["exchanges"][1], load_openlca_jsonld(path, context=doc.context).data[0]["exchanges"][1]
    )


def test_unverified_synthetic_catalog_retains_legacy_default_without_name_inference(tmp_path, monkeypatch):
    from brightpath.formats import openlca_jsonld
    from brightpath.formats.openlca_references import OpenLCAReferenceCatalog, OpenLCATechnosphereReference

    key = ("Synthetic unverified disposal provider", "synthetic service", "GLO", "kilogram")
    reference = OpenLCATechnosphereReference(
        process_id="00000000-0000-4000-8000-000000000001",
        process_name=key[0],
        flow_id="00000000-0000-4000-8000-000000000002",
        flow_name=key[1],
        flow_property_id="93a60a56-a3c8-11da-a746-0800200b9a66",
        flow_property_name="Mass",
        unit_id="20aadc24-a391-41cf-b340-3e4529f44bde",
        unit_name="kg",
        location_id="00000000-0000-4000-8000-000000000003",
        location=key[2],
        category="synthetic",
    )
    assert reference.native_reference_is_input is None
    assert reference.native_flow_type == "PRODUCT_FLOW"
    synthetic = OpenLCAReferenceCatalog({key: reference}, {}, "Synthetic fallback control, not native metadata")
    monkeypatch.setattr(openlca_jsonld, "load_openlca_reference_catalog", lambda selected: synthetic)
    source = document(2.0, 3)
    data = source.data
    data[0]["exchanges"][1].update(dict(zip(FIELDS, key, strict=True)))
    doc = InventoryDocument(data=data, context=source.context)
    path = write_openlca_jsonld(doc, tmp_path / "synthetic-fallback.zip")
    process = next(v for k, v in json_entities(path).items() if k.startswith("processes/"))
    wire = next(r for r in process["exchanges"] if r.get("defaultProvider"))
    assert wire["flow"]["flowType"] == "PRODUCT_FLOW"
    assert wire["isInput"] is True
    assert wire["amount"] == 2.0
    assert_preserved(data[0]["exchanges"][1], load_openlca_jsonld(path, context=doc.context).data[0]["exchanges"][1])


@pytest.mark.parametrize(
    "changes",
    [{"native_reference_is_input": False}, {"native_reference_is_input": 1}, {"native_flow_type": "ELEMENTARY_FLOW"}],
)
def test_inconsistent_or_coerced_native_contract_is_rejected(changes):
    reference = load_openlca_reference_catalog(context()).technosphere[WASTE]
    assert reference.native_flow_type == "WASTE_FLOW"
    assert reference.native_reference_is_input is True
    with pytest.raises(ValueError, match="product output or waste input"):
        replace(reference, **changes)


@pytest.mark.parametrize("amount", [-2.0, 2.0])
@pytest.mark.parametrize("uncertainty", [0, 2, 3])
def test_native2026_ilcd_waste_preserves_formula_and_both_signed_marginals(tmp_path, amount, uncertainty):
    doc = document(amount, uncertainty, format_id="ilcd")
    before = deepcopy(doc.data)
    path = write_ilcd(doc, tmp_path / "native-waste-ilcd.zip", dialect="openlca", metadata_profile=PROFILE)
    with ZipFile(path) as archive:
        process = parse_xml(archive.read(next(n for n in archive.namelist() if n.startswith("processes/"))))
        wire = process.findall("p:exchanges/p:exchange", NS)[1]
        assert wire.findtext("p:exchangeDirection", namespaces=NS) == "Output"
        assert float(wire.findtext("p:resultingAmount", namespaces=NS)) == amount
        flow_id = wire.find("p:referenceToFlowDataSet", NS).get("refObjectId")
        flow = parse_xml(archive.read(f"flows/{flow_id}.xml"))
        assert flow_id == "4487861f-24e6-378a-aad9-16973cbb14f2"
        assert flow.findtext("f:modellingAndValidation/f:LCIMethod/f:typeOfDataSet", namespaces=NS) == "Waste flow"
    restored = load_ilcd(path, context=doc.context).data[0]["exchanges"][1]
    assert_preserved(before[0]["exchanges"][1], restored)
    assert doc.data == before


@pytest.mark.parametrize("mutation", ["flow_type", "direction", "provider"])
def test_native2026_ilcd_rejects_changed_type_direction_or_provider(tmp_path, mutation):
    doc = document(-2.0, format_id="ilcd")
    path = write_ilcd(doc, tmp_path / "changed-ilcd.zip", dialect="openlca", metadata_profile=PROFILE)
    with ZipFile(path) as archive:
        entries = {n: archive.read(n) for n in archive.namelist()}
    process_path = next(n for n in entries if n.startswith("processes/"))
    process = parse_xml(entries[process_path])
    wire = process.findall("p:exchanges/p:exchange", NS)[1]
    if mutation == "direction":
        wire.find("p:exchangeDirection", NS).text = "Input"
    elif mutation == "provider":
        wire.set(f"{{{NS['olca']}}}defaultProvider", "00000000-0000-4000-8000-000000000001")
    else:
        flow_path = "flows/4487861f-24e6-378a-aad9-16973cbb14f2.xml"
        flow = parse_xml(entries[flow_path])
        flow.find("f:modellingAndValidation/f:LCIMethod/f:typeOfDataSet", NS).text = "Product flow"
        entries[flow_path] = xml_bytes(flow)
    entries[process_path] = xml_bytes(process)
    with ZipFile(path, "w") as archive:
        for name, data in entries.items():
            archive.writestr(name, data)
    with pytest.raises(ILCDError):
        load_ilcd(path, context=doc.context)


@pytest.mark.parametrize("amount", [-2.0, 2.0])
def test_ecoinvent312_local_negative_reference_keeps_legacy_waste_coordinates(tmp_path, amount):
    # This is an authored local negative-reference model, not a claim about an
    # uninspected native ecoinvent background provider's flow kind.
    consumer = document(amount, 3, format_id="ilcd").data[0]
    treatment = dict(zip(FIELDS, WASTE, strict=True))
    treatment.update(
        code="synthetic-local-negative-reference-treatment",
        comment="Synthetic negative-reference treatment control.",
        exchanges=[{**dict(zip(FIELDS, WASTE, strict=True)), "type": "production", "amount": -1.0}],
    )
    selected = InventoryContext(
        FormatProfile("ilcd"),
        BackgroundContext(TechnosphereProfile("ecoinvent", "3.12", "cutoff"), BiosphereProfile("ecoinvent", "3.12")),
    )
    doc = InventoryDocument(data=[treatment, consumer], context=selected, database_name="synthetic-local-waste")
    before = deepcopy(doc.data)
    path = write_ilcd(doc, tmp_path / "local-waste.zip", dialect="openlca", metadata_profile=PROFILE)
    with ZipFile(path) as archive:
        roots = [parse_xml(archive.read(n)) for n in archive.namelist() if n.startswith("processes/")]
        consumer_root = next(r for r in roots if len(r.findall("p:exchanges/p:exchange", NS)) == 2)
        wire = consumer_root.findall("p:exchanges/p:exchange", NS)[1]
        assert wire.findtext("p:exchangeDirection", namespaces=NS) == "Output"
        assert float(wire.findtext("p:resultingAmount", namespaces=NS)) == -amount
    restored = load_ilcd(path, context=selected)
    actual = next(r for r in restored.data if r["name"] == consumer["name"])["exchanges"][1]
    assert_preserved(before[1]["exchanges"][1], actual)
    assert doc.data == before


@pytest.mark.parametrize("extra_kind", ["PRODUCT_FLOW", "WASTE_FLOW"])
def test_unverified_additional_output_is_not_exempted_from_single_output_guard(extra_kind):
    from brightpath.formats.openlca_jsonld import _select_quantitative_reference

    flows = {
        name: SimpleNamespace(flow_type=SimpleNamespace(name=kind))
        for name, kind in (("product", "PRODUCT_FLOW"), ("extra", extra_kind))
    }
    process = SimpleNamespace(
        name="Synthetic unverified extra output",
        exchanges=[
            SimpleNamespace(flow=SimpleNamespace(id=key), is_input=False, is_avoided_product=False) for key in flows
        ],
    )
    with pytest.raises(ValueError, match="multiple non-elementary output"):
        _select_quantitative_reference(process, flows)


@pytest.mark.parametrize("amount", [-2.0, 2.0])
def test_native_waste_does_not_waive_ilcd_strict_dispersion_guard(tmp_path, amount):
    doc = document(amount, 2, format_id="ilcd")
    data = doc.data
    data[0]["exchanges"][1]["scale"] = 0.1
    doc = InventoryDocument(data=data, context=doc.context, database_name=doc.database_name)
    before = deepcopy(doc.data)
    with pytest.raises(ILCDError, match="cannot represent.*dispersion exactly"):
        write_ilcd(doc, tmp_path / "unrepresentable.zip", dialect="openlca", metadata_profile=PROFILE)
    assert doc.data == before
