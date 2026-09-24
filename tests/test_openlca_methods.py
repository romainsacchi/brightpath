"""Synthetic method packages exercise matching without shipping ecoinvent data."""

import csv
import json
import uuid
import zipfile
from copy import deepcopy

import pytest

from brightpath.core import BackgroundContext, BiosphereProfile, FormatProfile, InventoryContext, TechnosphereProfile
from brightpath.exceptions import SerializationError
from brightpath.formats.openlca_jsonld import write_openlca_jsonld
from brightpath.formats.openlca_methods import OpenLCAMethodMapping
from brightpath.models import InventoryDocument


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
