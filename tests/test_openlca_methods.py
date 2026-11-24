"""Synthetic method packages exercise matching without shipping ecoinvent data."""

import csv
import json
import uuid
import zipfile
from copy import deepcopy

import pytest

from brightpath.core import BackgroundContext, BiosphereProfile, FormatProfile, InventoryContext, TechnosphereProfile
from brightpath.exceptions import SerializationError
from brightpath.formats.openlca_jsonld import (
    load_openlca_jsonld,
    write_ecoinvent_openlca_jsonld,
    write_openlca_jsonld,
)
from brightpath.formats.openlca_methods import OpenLCAMethodMapping
from brightpath.models import InventoryDocument

_CADMIUM_ECOINVENT_ID = "2712fe5f-0d3c-4c8e-8bc4-26c604413849"
_CADMIUM_NEXUS_ID = "c087ef23-4b5b-40f6-932a-7939b72386c4"


def uid(label):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, "brightpath-test:" + label))


@pytest.fixture
def package(tmp_path):
    root = tmp_path / "methods"
    records = {
        "flows": {
            "@id": uid("flow"),
            "name": "Test emission",
            "flowType": "ELEMENTARY_FLOW",
            "category": "Elementary flows/Emission to air/low population density",
            "flowProperties": [
                {"flowProperty": {"@id": uid("property")}, "isRefFlowProperty": True, "conversionFactor": 1}
            ],
        },
        "flow_properties": {"@id": uid("property"), "name": "Mass", "unitGroup": {"@id": uid("group")}},
        "unit_groups": {
            "@id": uid("group"),
            "name": "Units of mass",
            "units": [{"@id": uid("unit"), "name": "kg", "isRefUnit": True, "conversionFactor": 1}],
        },
    }
    for folder, value in records.items():
        (root / folder).mkdir(parents=True)
        (root / folder / (value["@id"] + ".json")).write_text(json.dumps(value))
    source = tmp_path / "source.csv"
    with source.open("w", newline="") as stream:
        csv.writer(stream).writerows(
            [
                ["Test emission", "air", "non-urban air or from high stacks", "kilogram", uid("flow")],
                ["Uncharacterized", "water", "unspecified", "kilogram", uid("missing")],
            ]
        )
    return root, source


def document(version="3.12", model="cutoff"):
    context = InventoryContext(
        format=FormatProfile("openlca_jsonld"),
        background=BackgroundContext(
            technosphere=TechnosphereProfile("ecoinvent", "3.12", model),
            biosphere=BiosphereProfile("ecoinvent", version),
        ),
    )
    return InventoryDocument(
        context=context,
        data=[
            {
                "name": "Test process",
                "reference product": "Test product",
                "unit": "kilogram",
                "location": "GLO",
                "exchanges": [
                    {
                        "name": "Test process",
                        "product": "Test product",
                        "type": "production",
                        "unit": "kilogram",
                        "amount": 1,
                    },
                    {
                        "name": "Test emission",
                        "categories": ("air", "non-urban air or from high stacks"),
                        "unit": "kilogram",
                        "amount": 2,
                        "type": "biosphere",
                    },
                    {
                        "name": "Uncharacterized",
                        "categories": ("water", "unspecified"),
                        "unit": "kilogram",
                        "amount": 3,
                        "type": "biosphere",
                    },
                ],
            }
        ],
    )


@pytest.fixture
def legacy_nexus_flow(package):
    root, source = package
    flow_dir = root / "flows"
    flow = json.loads((flow_dir / (uid("flow") + ".json")).read_text())
    flow.update(
        {
            "@id": _CADMIUM_NEXUS_ID,
            "name": "Cadmium II",
            "category": "Elementary flows/Emission to water/unspecified",
        }
    )
    (flow_dir / (_CADMIUM_NEXUS_ID + ".json")).write_text(json.dumps(flow))
    with source.open("a", newline="") as stream:
        csv.writer(stream).writerow(["Cadmium II", "water", "unspecified", "kilogram", _CADMIUM_ECOINVENT_ID])
    return {
        "type": "biosphere",
        "name": "Cadmium II",
        "categories": ("water", "unspecified"),
        "unit": "kilogram",
        "input": ("biosphere", _CADMIUM_ECOINVENT_ID),
        "amount": 0.01,
    }


def read_zip(path):
    with zipfile.ZipFile(path) as archive:
        return {name: json.loads(archive.read(name)) for name in archive.namelist() if name.endswith(".json")}


@pytest.mark.parametrize("model", ["cutoff", "consequential"])
def test_exact_ids_missing_flows_and_nonmutation(package, tmp_path, model):
    mapping = OpenLCAMethodMapping(*package)
    source = document(model=model)
    before = deepcopy(source.data)
    path = write_openlca_jsonld(source, tmp_path / "inventory.zip", method_mapping=mapping)
    entities = read_zip(path)
    process = next(v for k, v in entities.items() if k.startswith("processes/"))
    emission = process["exchanges"][1]
    assert emission["flow"]["@id"] == uid("flow")
    assert emission["flowProperty"]["@id"] == uid("property")
    assert emission["unit"]["@id"] == uid("unit")
    assert emission["amount"] == 2
    assert f"flows/{uid('flow')}.json" not in entities  # supplied by original method package
    assert f"flows/{uid('missing')}.json" in entities
    assert process["exchanges"][2]["flow"]["@id"] == uid("missing")
    assert process["exchanges"][2]["amount"] == 3
    report = json.loads(path.with_suffix(".biosphere-coverage.json").read_text())
    assert report["inventory_mapped_flows"] == 1
    assert report["inventory_unmapped_flows"][0]["reason"] == "absent_from_method_package"
    assert source.data == before
    repeated = write_openlca_jsonld(source, tmp_path / "repeat.zip", method_mapping=mapping)
    assert read_zip(repeated) == entities


def test_zip_input(package, tmp_path):
    root, csv_path = package
    archive = tmp_path / "methods.zip"
    with zipfile.ZipFile(archive, "w") as output:
        for path in root.rglob("*.json"):
            output.write(path, path.relative_to(root).as_posix())
    assert OpenLCAMethodMapping(archive, csv_path).audit(document()) == {
        **OpenLCAMethodMapping(root, csv_path).audit(document()),
        "source": str(archive),
    }


def test_remapped_uuid_uses_package_flow_and_reports_both_ids(package, tmp_path):
    root, source = package
    source_id, package_id = uid("ecoinvent source"), uid("flow")
    source.write_text(source.read_text().replace(package_id, source_id))
    mapping = OpenLCAMethodMapping(
        root,
        source,
        biosphere_version="3.11",
        flow_uuid_map={
            source_id: package_id,
            uid("missing"): uid("absent target"),
            uid("absent source"): uid("other target"),
        },
    )
    doc = document(version="3.11")
    before = doc.data
    emission = before[0]["exchanges"][1]
    emission["input"] = ("biosphere", source_id)
    emission["openlca flow"] = {"@id": package_id}
    doc = InventoryDocument(data=before, context=doc.context)

    path = write_openlca_jsonld(doc, tmp_path / "remapped.zip", method_mapping=mapping)
    entries = read_zip(path)
    process = next(value for name, value in entries.items() if name.startswith("processes/"))
    exchange = process["exchanges"][1]
    assert exchange["flow"]["@id"] == package_id
    assert exchange["flowProperty"]["@id"] == uid("property")
    assert exchange["unit"]["@id"] == uid("unit")
    assert exchange["amount"] == 2
    assert f"flows/{package_id}.json" not in entries
    assert f"flows/{source_id}.json" not in entries
    report = json.loads(path.with_suffix(".biosphere-coverage.json").read_text())
    assert report["mapped_source_flows"] == report["remapped_source_flows"] == report["inventory_mapped_flows"] == 1
    assert report["inventory_uuid_remaps"] == {source_id: package_id}
    assert report["source_flows_absent_from_package"] == 1
    assert doc.data == before

    contradictory = dict(emission, name="Different emission")
    with pytest.raises(SerializationError, match="conflicts with UUID"):
        mapping.resolve(contradictory)


@pytest.mark.parametrize("change", ["name", "category", "unit"])
def test_remapped_uuid_requires_compatible_metadata(package, change):
    root, source = package
    path = next((root / ("unit_groups" if change == "unit" else "flows")).glob("*.json"))
    value = json.loads(path.read_text())
    if change == "name":
        value["name"] = "Different emission"
    elif change == "category":
        value["category"] = "Elementary flows/Emission to soil/agricultural"
    else:
        value["units"][0]["name"] = "m3"
    path.write_text(json.dumps(value))
    source_id, package_id = uid("ecoinvent source"), uid("flow")
    source.write_text(source.read_text().replace(package_id, source_id))
    with pytest.raises(ValueError, match="incompatible"):
        OpenLCAMethodMapping(root, source, flow_uuid_map={source_id: package_id})


def test_remapped_uuid_cannot_override_direct_match_or_reuse_target(package):
    flow_id = uid("flow")
    with pytest.raises(ValueError, match="conflicts with a direct UUID match"):
        OpenLCAMethodMapping(*package, flow_uuid_map={uid("missing"): flow_id})
    with pytest.raises(ValueError, match="must be one-to-one"):
        OpenLCAMethodMapping(*package, flow_uuid_map={uid("first"): flow_id, uid("second"): flow_id})


@pytest.mark.parametrize("version", ["3.9", "3.9.1", "3.10", "3.11"])
def test_bundled_nexus_remaps_link_legacy_exchanges(package, legacy_nexus_flow, tmp_path, version):
    mapping = OpenLCAMethodMapping(*package, biosphere_version=version)
    doc = document(version=version)
    data = doc.data
    data[0]["exchanges"].append(legacy_nexus_flow)
    doc = InventoryDocument(data=data, context=doc.context)

    path = write_openlca_jsonld(doc, tmp_path / "legacy-nexus.zip", method_mapping=mapping)
    entries = read_zip(path)
    process = next(value for name, value in entries.items() if name.startswith("processes/"))
    assert process["exchanges"][-1]["flow"]["@id"] == _CADMIUM_NEXUS_ID
    assert f"flows/{_CADMIUM_NEXUS_ID}.json" not in entries
    assert mapping.audit(doc)["inventory_uuid_remaps"] == {_CADMIUM_ECOINVENT_ID: _CADMIUM_NEXUS_ID}


@pytest.mark.parametrize("version", ["3.10.1", "3.12", "3.13"])
def test_unverified_or_modern_release_does_not_use_legacy_remaps(package, legacy_nexus_flow, version):
    mapping = OpenLCAMethodMapping(*package, biosphere_version=version)
    assert mapping.resolve(legacy_nexus_flow) == (_CADMIUM_ECOINVENT_ID, None)


def test_explicit_empty_map_disables_bundled_nexus_remaps(package, legacy_nexus_flow):
    mapping = OpenLCAMethodMapping(*package, biosphere_version="3.11", flow_uuid_map={})
    assert mapping.resolve(legacy_nexus_flow) == (_CADMIUM_ECOINVENT_ID, None)


@pytest.mark.parametrize(
    "version,remaps",
    [("3.8", 41), ("3.9", 232), ("3.9.1", 232), ("3.10", 373), ("3.11", 373), ("3.12", 0), ("3.13", 0)],
)
def test_ecoinvent_export_loads_bundled_method_references(tmp_path, version, remaps):
    fossil_co2 = "349b29d1-3e58-4c66-98b9-9d1a076efd2e"
    doc = document(version=version)
    data = doc.data
    data[0]["exchanges"][1].update(name="Carbon dioxide, fossil", categories=("air", "unspecified"))
    doc = InventoryDocument(data=data, context=doc.context)

    path = write_ecoinvent_openlca_jsonld(doc, tmp_path / "ecoinvent.zip")
    entries = read_zip(path)
    process = next(value for name, value in entries.items() if name.startswith("processes/"))
    assert process["exchanges"][1]["flow"]["@id"] == fossil_co2
    assert f"flows/{fossil_co2}.json" not in entries
    coverage = json.loads(path.with_suffix(".biosphere-coverage.json").read_text())
    assert coverage["biosphere"]["version"] == version
    assert coverage["source"].endswith(".zip")
    assert len(coverage["method_package_sha256"]) == 64
    assert coverage["remapped_source_flows"] == remaps
    assert coverage["inventory_mapped_flows"] == 1


def test_direct_match_takes_precedence_over_bundled_nexus_remap(package, legacy_nexus_flow):
    flow_dir = package[0] / "flows"
    flow = json.loads((flow_dir / (_CADMIUM_NEXUS_ID + ".json")).read_text())
    flow["@id"] = _CADMIUM_ECOINVENT_ID
    (flow_dir / (_CADMIUM_ECOINVENT_ID + ".json")).write_text(json.dumps(flow))

    mapping = OpenLCAMethodMapping(*package, biosphere_version="3.11")
    flow_id, reference = mapping.resolve(legacy_nexus_flow)
    assert flow_id == reference.flow_id == _CADMIUM_ECOINVENT_ID


def test_wrong_context_fails_before_writing(package, tmp_path):
    path = tmp_path / "invalid.zip"
    with pytest.raises(SerializationError, match="3.12 biosphere"):
        write_openlca_jsonld(document(version="3.11"), path, method_mapping=OpenLCAMethodMapping(*package))
    assert not path.exists()


@pytest.mark.parametrize("change", ["unit", "name", "category", "quantity", "duplicate"])
def test_rejects_incompatible_package(package, change):
    root, source = package
    folder = "unit_groups" if change == "unit" else "flows"
    path = next((root / folder).glob("*.json"))
    value = json.loads(path.read_text())
    if change == "unit":
        value["units"][0]["name"] = "m3"
    elif change == "name":
        value["name"] = "Other flow"
    elif change == "category":
        value["category"] = "Elementary flows/Emission to water/unspecified"
    elif change == "quantity":
        value["flowProperties"][0]["flowProperty"]["@id"] = uid("nonexistent")
    else:
        (path.parent / "duplicate.json").write_text(json.dumps(value))
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError):
        OpenLCAMethodMapping(root, source)


def test_uuid_cannot_override_conflicting_identity(package):
    mapping = OpenLCAMethodMapping(*package)
    exchange = document().data[0]["exchanges"][1]
    exchange["input"] = ("biosphere", uid("missing"))
    with pytest.raises(SerializationError, match="conflicts with UUID"):
        mapping.resolve(exchange)


def test_unknown_flow_is_preserved_and_reported(package, tmp_path):
    source = document()
    data = source.data
    data[0]["exchanges"][1]["name"] = "New flow"
    source = InventoryDocument(data=data, context=source.context)
    mapping = OpenLCAMethodMapping(*package)
    result = mapping.audit(source)
    assert result["inventory_unmapped_flows"][0]["reason"] == "unknown_source_flow"
    path = write_openlca_jsonld(source, tmp_path / "unknown.zip", method_mapping=mapping)
    process = next(v for k, v in read_zip(path).items() if k.startswith("processes/"))
    assert len(process["exchanges"]) == 3


def test_standard_volume_alias_is_uuid_scoped(package):
    root, source = package
    path = next((root / "unit_groups").glob("*.json"))
    value = json.loads(path.read_text())
    value["units"][0]["name"] = "m3"
    path.write_text(json.dumps(value))
    source.write_text(source.read_text().replace("kilogram", "standard cubic meter"))
    with pytest.raises(ValueError, match="incompatible units"):
        OpenLCAMethodMapping(root, source)
    path = next((root / "flows").glob("*.json"))
    code = "7c337428-fb1b-45c7-bbb2-2ee4d29e17ba"
    value = json.loads(path.read_text())
    value["@id"] = code
    path.write_text(json.dumps(value))
    source.write_text(source.read_text().replace(uid("flow"), code))
    mapping = OpenLCAMethodMapping(root, source)
    exchange = document().data[0]["exchanges"][1]
    exchange["unit"] = "standard cubic meter"
    assert mapping.resolve(exchange)[1].unit_name == "m3"


def test_cubic_meter_year_matches_nexus_reference_unit(package):
    root, source = package
    path = next((root / "unit_groups").glob("*.json"))
    value = json.loads(path.read_text())
    value["units"][0]["name"] = "m3*a"
    path.write_text(json.dumps(value))
    source.write_text(source.read_text().replace("kilogram", "cubic meter-year"))

    mapping = OpenLCAMethodMapping(root, source, biosphere_version="3.13")
    exchange = document().data[0]["exchanges"][1]
    exchange["unit"] = "cubic meter-year"
    assert mapping.resolve(exchange)[1].unit_name == "m3*a"


def test_ei313_economic_flow_uses_the_native_eur2005_exchange_unit(package, tmp_path):
    root, source = package
    code = "acb9b748-3464-4c0d-9055-639c420e4dc3"
    flow_path = next((root / "flows").glob("*.json"))
    flow = json.loads(flow_path.read_text())
    flow.update({"@id": code, "name": "Net operating surplus", "category": "Elementary flows/Economic/unspecified"})
    flow_path.write_text(json.dumps(flow))
    property_path = next((root / "flow_properties").glob("*.json"))
    quantity = json.loads(property_path.read_text())
    quantity["name"] = "Market value"
    property_path.write_text(json.dumps(quantity))
    group_path = next((root / "unit_groups").glob("*.json"))
    group = json.loads(group_path.read_text())
    group["units"] = [
        {"@id": uid("euro2024"), "name": "EUR 2024", "isRefUnit": True, "conversionFactor": 1},
        {"@id": uid("euro2005"), "name": "EUR 2005", "conversionFactor": 1.49},
    ]
    group_path.write_text(json.dumps(group))
    source.write_text(f"Net operating surplus,economic,primary production factor,EUR2005,{code}\n")

    mapping = OpenLCAMethodMapping(root, source, biosphere_version="3.13")
    doc = document(version="3.13")
    data = doc.data
    data[0]["exchanges"][1].update(
        name="Net operating surplus", categories=("economic", "primary production factor"), unit="EUR2005"
    )
    path = write_openlca_jsonld(
        InventoryDocument(data=data, context=doc.context), tmp_path / "economic.zip", method_mapping=mapping
    )
    entries = read_zip(path)
    process = next(value for name, value in entries.items() if name.startswith("processes/"))
    exchange = process["exchanges"][1]
    assert exchange["flow"]["@id"] == code
    assert exchange["flow"]["refUnit"] == "EUR 2024"
    assert exchange["unit"]["@id"] == uid("euro2005")
    assert exchange["flowProperty"]["@id"] == uid("property")
    assert exchange["amount"] == 2

    with pytest.raises(ValueError, match="incompatible name or compartment"):
        OpenLCAMethodMapping(root, source, biosphere_version="3.12")


@pytest.mark.parametrize(
    "version", ["3.5", "3.6", "3.7", "3.7.1", "3.8", "3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12", "3.13"]
)
@pytest.mark.parametrize("legacy", [False, True])
def test_versioned_packages_and_legacy_schema(package, tmp_path, version, legacy):
    root, source = package
    if legacy:
        for folder, current, old in [
            ("flows", "isRefFlowProperty", "referenceFlowProperty"),
            ("unit_groups", "isRefUnit", "referenceUnit"),
        ]:
            path = next((root / folder).glob("*.json"))
            value = json.loads(path.read_text())
            items = value["flowProperties"] if folder == "flows" else value["units"]
            for item in items:
                item[old] = item.pop(current)
            if folder == "flows":
                value["category"] = {
                    "name": "low population density",
                    "categoryPath": ["Elementary flows", "Emission to air"],
                }
            path.write_text(json.dumps(value))
        source.write_text(source.read_text().replace(",", ";"))
    mapping = OpenLCAMethodMapping(root, source, biosphere_version=version)
    doc = document(version=version)
    assert mapping.audit(doc)["biosphere"]["version"] == version
    path = write_openlca_jsonld(doc, tmp_path / "versioned.zip", method_mapping=mapping)
    process = next(v for k, v in read_zip(path).items() if k.startswith("processes/"))
    assert process["exchanges"][1]["flow"]["@id"] == uid("flow")
    assert process["exchanges"][1]["unit"]["@id"] == uid("unit")
    other = "3.11" if version != "3.11" else "3.12"
    with pytest.raises(SerializationError, match="biosphere context"):
        mapping.check_context(document(version=other).context)


def test_patch_version_is_not_collapsed(package):
    mapping = OpenLCAMethodMapping(*package, biosphere_version="3.9.1")
    with pytest.raises(SerializationError, match="3.9.1 biosphere"):
        mapping.check_context(document(version="3.9").context)


def test_zolca_backup_explains_required_conversion(package):
    with pytest.raises(ValueError, match="Export it as JSON-LD"):
        OpenLCAMethodMapping(package[0].with_suffix(".zolca"), package[1], biosphere_version="3.5")


@pytest.mark.parametrize("version", ["", "3", "3.4", "3.14", "3.9.x", 3.12, None])
def test_invalid_biosphere_version(package, version):
    with pytest.raises(ValueError, match="exact ecoinvent biosphere version"):
        OpenLCAMethodMapping(*package, biosphere_version=version)


def test_conflicting_reference_flags_rejected(package):
    path = next((package[0] / "flows").glob("*.json"))
    value = json.loads(path.read_text())
    value["flowProperties"][0]["referenceFlowProperty"] = False
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="Conflicting"):
        OpenLCAMethodMapping(*package)


def test_gas_unit_alias_rows_share_one_source_uuid(package):
    root, source = package
    code = "7c337428-fb1b-45c7-bbb2-2ee4d29e17ba"
    path = next((root / "flows").glob("*.json"))
    value = json.loads(path.read_text())
    value["@id"] = code
    path.write_text(json.dumps(value))
    path = next((root / "unit_groups").glob("*.json"))
    value = json.loads(path.read_text())
    value["units"][0]["name"] = "m3"
    path.write_text(json.dumps(value))
    with source.open("w", newline="") as stream:
        csv.writer(stream).writerows(
            [
                ["Test emission", "air", "non-urban air or from high stacks", unit, code]
                for unit in ("standard cubic meter", "Sm3")
            ]
        )
    mapping = OpenLCAMethodMapping(root, source, biosphere_version="3.9.1")
    for unit in ("standard cubic meter", "Sm3"):
        exchange = document().data[0]["exchanges"][1]
        exchange.update(unit=unit, input=("biosphere", code))
        assert mapping.resolve(exchange)[1].unit_name == "m3"


def test_conflicting_source_uuid_rows_still_fail(package):
    with package[1].open("a") as stream:
        csv.writer(stream).writerow(["Other emission", "water", "unspecified", "kilogram", uid("flow")])
    with pytest.raises(ValueError, match="Conflicting biosphere"):
        OpenLCAMethodMapping(*package)


@pytest.mark.parametrize("change", ["name", "category", "unit"])
def test_preserve_conflicts_avoids_package_uuid_and_reports(package, tmp_path, change):
    root, source = package
    folder = "unit_groups" if change == "unit" else "flows"
    file = next((root / folder).glob("*.json"))
    value = json.loads(file.read_text())
    if change == "name":
        value["name"] = "Different substance"
    elif change == "category":
        value["category"] = "Elementary flows/Emission to soil/agricultural"
    else:
        value["units"][0]["name"] = "m3"
    file.write_text(json.dumps(value))
    mapping = OpenLCAMethodMapping(root, source, biosphere_version="3.8", conflict_policy="preserve")
    doc = document(version="3.8")
    before = deepcopy(doc.data)
    path = write_openlca_jsonld(doc, tmp_path / "conflict.zip", method_mapping=mapping)
    entities = read_zip(path)
    process = next(v for k, v in entities.items() if k.startswith("processes/"))
    exc = process["exchanges"][1]
    assert exc["flow"]["@id"] != uid("flow")
    assert f"flows/{uid('flow')}.json" not in entities
    flow = entities[f"flows/{exc['flow']['@id']}.json"]
    assert flow["name"] == "Test emission"
    assert exc["amount"] == 2
    report = mapping.audit(doc)
    assert report["source_flows_absent_from_package"] == 1
    assert report["source_flows_conflicting_with_package"] == 1
    conflict = next(f for f in report["inventory_unmapped_flows"] if f["reason"] == "conflicting_method_package_flow")
    assert conflict["conflict"]["source_uuid"] == uid("flow")
    assert conflict["conflict"]["export_uuid"] == exc["flow"]["@id"]
    assert conflict["uuid"] == exc["flow"]["@id"]
    assert doc.data == before
    loaded = load_openlca_jsonld(path, context=doc.context)
    reexported = write_openlca_jsonld(loaded, tmp_path / "roundtrip.zip", method_mapping=mapping)
    again_process = next(v for k, v in read_zip(reexported).items() if k.startswith("processes/"))
    assert again_process["exchanges"][1]["flow"]["@id"] == exc["flow"]["@id"]
    again = OpenLCAMethodMapping(root, source, biosphere_version="3.8", conflict_policy="preserve")
    assert again.resolve(doc.data[0]["exchanges"][1]) == mapping.resolve(doc.data[0]["exchanges"][1])
    contradictory = deepcopy(doc.data[0]["exchanges"][1])
    contradictory["input"] = ("biosphere", uid("missing"))
    with pytest.raises(SerializationError, match="conflicts with UUID"):
        mapping.resolve(contradictory)


def test_unknown_conflict_policy_rejected(package):
    with pytest.raises(ValueError, match="conflict_policy"):
        OpenLCAMethodMapping(*package, conflict_policy="ignore")
