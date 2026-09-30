import json
from copy import deepcopy
from dataclasses import asdict, replace
from io import BytesIO
from types import SimpleNamespace
from zipfile import ZipFile

import pytest
from preservation_helpers import (
    Profile,
    assert_preserved,
    background_context,
    export_snapshots,
    import_artifact,
    synthetic_snapshot,
)

UVEK = Profile("uvek", "2025")
SUPPLIER = {
    "name": "Electricity, low voltage, at grid",
    "reference product": "Electricity, low voltage, at grid",
    "location": "CH",
    "unit": "kilowatt hour",
}
UNKNOWN_ID = "00000000-0000-0000-0000-000000000001"


def context(profile=UVEK):
    from brightpath import FormatProfile, InventoryContext

    return InventoryContext(FormatProfile("openlca_jsonld"), background_context(profile))


@pytest.fixture
def snapshot():
    return synthetic_snapshot(SUPPLIER)


def export_snapshot(snapshot, software="openlca"):
    return export_snapshots([snapshot], UVEK, software)


def raw_package(artifact):
    with ZipFile(BytesIO(artifact.content)) as archive:
        return {name: json.loads(archive.read(name)) for name in archive.namelist() if name.endswith(".json")}


def artifact_from_raw(entities):
    buffer = BytesIO()
    with ZipFile(buffer, "w") as archive:
        for name, value in entities.items():
            archive.writestr(name, json.dumps(value))
    return SimpleNamespace(filename="inventory.zip", content=buffer.getvalue())


def process(entities):
    return next(value for name, value in entities.items() if name.startswith("processes/"))


def tech_exchange(entities):
    return next(exchange for exchange in process(entities)["exchanges"] if exchange.get("isInput"))


def bio_exchange(entities):
    return next(
        exchange for exchange in process(entities)["exchanges"] if exchange["flow"].get("flowType") == "ELEMENTARY_FLOW"
    )


@pytest.mark.parametrize("software", ["brightway", "openlca", "simapro"])
def test_linked_uvek_openlca_upload_and_export_preserve_inventory(snapshot, software):
    before = deepcopy(snapshot)
    exported = export_snapshot(snapshot)
    entities = raw_package(exported)
    background = tech_exchange(entities)
    assert f"flows/{background['flow']['@id']}.json" not in entities
    assert f"processes/{background['defaultProvider']['@id']}.json" not in entities
    restored = import_artifact(exported, UVEK)
    assert len(restored) == 1
    assert_preserved(snapshot, restored[0])
    for exchange in restored[0]["exchanges"][1:]:
        assert "openlca flow property" not in exchange
        assert "openlca unit group" not in exchange
    reexported = export_snapshot(restored[0], software)
    assert_preserved(snapshot, import_artifact(reexported, UVEK)[0])
    if software == "openlca":
        returned = raw_package(reexported)
        reference = tech_exchange(returned)
        for field in ("flow", "defaultProvider", "flowProperty", "unit", "location"):
            assert reference[field] == background[field]
        assert f"flows/{background['flow']['@id']}.json" not in returned
    assert snapshot == before


@pytest.mark.parametrize("entry_point", ["package", "document", "profile"])
def test_direct_loaders_resolve_with_the_declared_context(snapshot, tmp_path, entry_point):
    from brightpath.formats.openlca_jsonld import load_openlca_jsonld, load_openlca_jsonld_package
    from brightpath.models import BackgroundProfile

    path = tmp_path / "source.zip"
    path.write_bytes(export_snapshot(snapshot).content)
    if entry_point == "package":
        loaded = load_openlca_jsonld_package(path, context=context())
    elif entry_point == "document":
        loaded = load_openlca_jsonld(path, context=context())
    else:
        loaded = load_openlca_jsonld(path, background_profile=BackgroundProfile(**asdict(UVEK)))
    actual = loaded.data[0]
    assert actual["exchanges"][1]["name"] == SUPPLIER["name"]
    assert actual["exchanges"][1]["unit"] == "kilowatt hour"
    assert actual["exchanges"][2]["categories"] == ("air",)


@pytest.mark.parametrize("variant", ["none", "ecoinvent", "version", "model", "biosphere", "format"])
def test_missing_or_inexact_context_does_not_resolve_external_references(snapshot, tmp_path, variant):
    from brightpath import BiosphereProfile, FormatProfile
    from brightpath.formats.openlca_jsonld import load_openlca_jsonld_package

    selected = context()
    if variant == "none":
        selected = None
    elif variant == "ecoinvent":
        selected = context(Profile("ecoinvent", "3.10"))
    elif variant == "version":
        selected = context(Profile("uvek", "2024"))
    elif variant == "model":
        selected = context(Profile("uvek", "2025", "consequential"))
    elif variant == "biosphere":
        selected = replace(
            selected, background=replace(selected.background, biosphere=BiosphereProfile("ecoinvent", "3.9"))
        )
    else:
        selected = replace(selected, format=FormatProfile("brightway_excel"))
    path = tmp_path / "source.zip"
    path.write_bytes(export_snapshot(snapshot).content)
    with pytest.raises(ValueError, match="missing flow|Explicit context format"):
        load_openlca_jsonld_package(path, context=selected)


@pytest.mark.parametrize("field", ["flow", "defaultProvider", "flowProperty", "unit", "location"])
@pytest.mark.parametrize("mutation", ["identifier", "name", "type"])
def test_conflicting_external_references_are_rejected(snapshot, field, mutation):
    entities = raw_package(export_snapshot(snapshot))
    reference = tech_exchange(entities)[field]
    reference[{"identifier": "@id", "name": "name", "type": "@type"}[mutation]] = {
        "identifier": UNKNOWN_ID,
        "name": "Untrusted label",
        "type": "WrongType",
    }[mutation]
    artifact = artifact_from_raw(entities)
    before = artifact.content
    with pytest.raises(ValueError, match="Import rejected:.*[Ee]xternal openLCA"):
        import_artifact(artifact, UVEK)
    assert artifact.content == before


@pytest.mark.parametrize("field", ["defaultProvider", "flowProperty", "unit"])
def test_missing_external_identifiers_are_not_guessed_from_names(snapshot, field):
    entities = raw_package(export_snapshot(snapshot))
    tech_exchange(entities)[field].pop("@id")
    with pytest.raises(ValueError, match="[Ee]xternal openLCA"):
        import_artifact(artifact_from_raw(entities), UVEK)


@pytest.mark.parametrize(
    "mutation", ["type", "unit", "location", "tech_direction", "bio_direction", "bio_provider", "reference"]
)
def test_external_reference_semantics_are_checked(snapshot, mutation):
    entities = raw_package(export_snapshot(snapshot))
    exchange = tech_exchange(entities)
    if mutation == "type":
        exchange["flow"]["flowType"] = "ELEMENTARY_FLOW"
    elif mutation == "unit":
        exchange["flow"]["refUnit"] = "kg"
    elif mutation == "location":
        exchange["defaultProvider"]["location"] = "GLO"
    elif mutation == "tech_direction":
        exchange["isInput"] = False
    elif mutation == "bio_direction":
        bio_exchange(entities)["isInput"] = True
    elif mutation == "bio_provider":
        bio_exchange(entities)["defaultProvider"] = {"@type": "Process", "name": "Untrusted provider"}
    else:
        exchange["isQuantitativeReference"] = True
    with pytest.raises(ValueError, match="[Ee]xternal openLCA"):
        import_artifact(artifact_from_raw(entities), UVEK)


@pytest.mark.parametrize("field", ["name", "unit", "amount", "formula", "categories"])
def test_extension_metadata_cannot_override_resolved_values(snapshot, field):
    entities = raw_package(export_snapshot(snapshot))
    tech_exchange(entities)["otherProperties"] = {"brightpath": {field: "override"}}
    with pytest.raises(ValueError, match="cannot override identity or exchange values"):
        import_artifact(artifact_from_raw(entities), UVEK)


def embed_background_flow(entities):
    exchange = tech_exchange(entities)
    flow = {
        "@type": "Flow",
        "@id": exchange["flow"]["@id"],
        "name": exchange["flow"]["name"],
        "flowType": "PRODUCT_FLOW",
        "flowProperties": [
            {
                "flowProperty": deepcopy(exchange["flowProperty"]),
                "conversionFactor": 1.0,
                "isRefFlowProperty": True,
            }
        ],
    }
    entities[f"flows/{flow['@id']}.json"] = flow
    return flow


def test_partial_embedded_definitions_preserve_catalog_identity(snapshot):
    entities = raw_package(export_snapshot(snapshot))
    embed_background_flow(entities)
    assert_preserved(snapshot, import_artifact(artifact_from_raw(entities), UVEK)[0])


@pytest.mark.parametrize("mutation", ["name", "type", "quantity", "factor", "duplicate_reference"])
def test_embedded_flow_definitions_cannot_conflict_with_catalog(snapshot, mutation):
    entities = raw_package(export_snapshot(snapshot))
    flow = embed_background_flow(entities)
    if mutation == "name":
        flow["name"] = "Another product"
    elif mutation == "type":
        flow["flowType"] = "ELEMENTARY_FLOW"
    elif mutation == "quantity":
        flow["flowProperties"][0]["flowProperty"]["@id"] = UNKNOWN_ID
    elif mutation == "duplicate_reference":
        flow["flowProperties"].append(
            {
                "flowProperty": {"@id": UNKNOWN_ID},
                "conversionFactor": 1.0,
                "isRefFlowProperty": True,
            }
        )
    else:
        flow["flowProperties"][0]["conversionFactor"] = 1000
    with pytest.raises(ValueError, match="[Ee]xternal openLCA"):
        import_artifact(artifact_from_raw(entities), UVEK)


def test_duplicate_embedded_uuid_is_rejected(snapshot):
    entities = raw_package(export_snapshot(snapshot))
    flow = embed_background_flow(entities)
    entities[f"flows/{UNKNOWN_ID}.json"] = deepcopy(flow)
    with pytest.raises(ValueError, match="duplicate openLCA identifiers"):
        import_artifact(artifact_from_raw(entities), UVEK)


@pytest.mark.parametrize("axis", ["technosphere", "biosphere"])
def test_ambiguous_catalog_identifiers_are_not_resolved(snapshot, tmp_path, monkeypatch, axis):
    from brightpath.formats import openlca_jsonld
    from brightpath.formats.openlca_references import load_openlca_reference_catalog

    artifact = export_snapshot(snapshot)
    path = tmp_path / "source.zip"
    path.write_bytes(artifact.content)
    catalog = load_openlca_reference_catalog(context())
    references = dict(getattr(catalog, axis))
    if axis == "technosphere":
        identity = tuple(SUPPLIER[key] for key in ("name", "reference product", "location", "unit"))
    else:
        identity = ("Carbon dioxide, fossil", ("air",), "kilogram")
    references[("Conflicting identity", *identity[1:])] = references[identity]
    ambiguous = replace(catalog, **{axis: references})
    monkeypatch.setattr(openlca_jsonld, "load_openlca_reference_catalog", lambda selected: ambiguous)
    with pytest.raises(ValueError, match="ambiguous external openLCA"):
        openlca_jsonld.load_openlca_jsonld_package(path, context=context())


def test_native_resource_input_keeps_categories_and_direction(snapshot):
    from brightpath.formats.openlca_references import load_openlca_reference_catalog

    catalog = load_openlca_reference_catalog(context())
    identity = next(key for key in catalog.biosphere if key[1][:1] == ("natural resource",))
    snapshot["exchanges"][2].update(name=identity[0], categories=identity[1], unit=identity[2])
    assert_preserved(snapshot, import_artifact(export_snapshot(snapshot), UVEK)[0])


def test_analysis_rejects_conflicting_declared_profiles(snapshot, tmp_path):
    from brightpath.analysis import analyze_inventory
    from brightpath.models import BackgroundProfile

    path = tmp_path / "source.zip"
    path.write_bytes(export_snapshot(snapshot).content)
    result = analyze_inventory(
        path=path, source_context=context(), source_profile=BackgroundProfile("ecoinvent", "3.10", "cutoff")
    )
    assert result.has_errors
    assert not result.candidates
    assert any("source_profile conflicts" in issue.message for issue in result.file_issues)


def test_analyzer_honors_explicit_biosphere_override(snapshot, tmp_path):
    from brightpath import BiosphereProfile
    from brightpath.analysis import analyze_inventory

    selected = context()
    selected = replace(
        selected, background=replace(selected.background, biosphere=BiosphereProfile("ecoinvent", "3.9"))
    )
    path = tmp_path / "source.zip"
    path.write_bytes(export_snapshot(snapshot).content)
    result = analyze_inventory(path=path, source_context=selected)
    assert not result.candidates
    assert any("missing flow" in issue.message for issue in result.file_issues)


def test_known_provider_cannot_be_paired_with_another_embedded_flow(snapshot):
    entities = raw_package(export_snapshot(snapshot))
    flow = embed_background_flow(entities)
    del entities[f"flows/{flow['@id']}.json"]
    flow["@id"] = UNKNOWN_ID
    tech_exchange(entities)["flow"]["@id"] = UNKNOWN_ID
    entities[f"flows/{UNKNOWN_ID}.json"] = flow
    with pytest.raises(ValueError, match="Unknown or ambiguous external openLCA"):
        import_artifact(artifact_from_raw(entities), UVEK)


@pytest.mark.parametrize("mutation", [None, "quantity_name", "unit_name", "unit_id"])
def test_embedded_quantities_must_agree_with_known_references(snapshot, mutation):
    entities = raw_package(export_snapshot(snapshot))
    exchange = tech_exchange(entities)
    quantity = {
        **exchange["flowProperty"],
        "unitGroup": {"@type": "UnitGroup", "@id": UNKNOWN_ID},
    }
    group = {
        "@type": "UnitGroup",
        "@id": UNKNOWN_ID,
        "name": "Synthetic energy units",
        "units": [{**exchange["unit"], "conversionFactor": 3.6, "isRefUnit": False}],
    }
    if mutation == "quantity_name":
        quantity["name"] = "Wrong quantity"
    elif mutation == "unit_name":
        group["units"][0]["name"] = "kg"
    elif mutation == "unit_id":
        group["units"][0]["@id"] = UNKNOWN_ID
    entities[f"flow_properties/{quantity['@id']}.json"] = quantity
    entities[f"unit_groups/{UNKNOWN_ID}.json"] = group
    artifact = artifact_from_raw(entities)
    if mutation is None:
        assert_preserved(snapshot, import_artifact(artifact, UVEK)[0])
    else:
        with pytest.raises(ValueError, match="[Ee]xternal openLCA"):
            import_artifact(artifact, UVEK)


def test_embedded_biosphere_compartments_cannot_override_catalog(snapshot):
    entities = raw_package(export_snapshot(snapshot))
    exchange = bio_exchange(entities)
    flow = {
        **exchange["flow"],
        "category": "water",
        "flowProperties": [
            {
                "flowProperty": exchange["flowProperty"],
                "conversionFactor": 1.0,
                "isRefFlowProperty": True,
            }
        ],
    }
    entities[f"flows/{flow['@id']}.json"] = flow
    with pytest.raises(ValueError, match="conflicting biosphere categories"):
        import_artifact(artifact_from_raw(entities), UVEK)


def test_catalog_loading_errors_fail_closed(snapshot, tmp_path, monkeypatch):
    from brightpath.formats import openlca_jsonld

    path = tmp_path / "source.zip"
    path.write_bytes(export_snapshot(snapshot).content)

    def corrupt_catalog(selected):
        raise ValueError("Reference catalog does not match its integrity manifest")

    monkeypatch.setattr(openlca_jsonld, "load_openlca_reference_catalog", corrupt_catalog)
    with pytest.raises(ValueError, match="integrity manifest"):
        openlca_jsonld.load_openlca_jsonld_package(path, context=context())


@pytest.mark.parametrize("with_provider", [False, True])
def test_self_contained_local_provider_is_not_replaced_by_catalog(snapshot, with_provider, tmp_path):
    entities = raw_package(export_snapshot(snapshot))
    flow = embed_background_flow(entities)
    exchange = tech_exchange(entities)
    provider_id = exchange["defaultProvider"]["@id"]
    entities[f"processes/{provider_id}.json"] = {
        "@type": "Process",
        "@id": provider_id,
        "name": "Synthetic local provider",
        "processType": "UNIT_PROCESS",
        "location": exchange["location"],
        "exchanges": [
            {
                "internalId": 1,
                "flow": exchange["flow"],
                "flowProperty": exchange["flowProperty"],
                "unit": exchange["unit"],
                "isInput": False,
                "isQuantitativeReference": True,
                "amount": 1.0,
            }
        ],
    }
    if with_provider:
        exchange["defaultProvider"]["name"] = "Synthetic local provider"
    else:
        exchange.pop("defaultProvider")
    from brightpath.formats.openlca_jsonld import load_openlca_jsonld_package

    path = tmp_path / "source.zip"
    path.write_bytes(artifact_from_raw(entities).content)
    data = load_openlca_jsonld_package(path, context=context()).data
    local = next(item for item in data if item["name"] == "Synthetic local provider")
    focal = next(item for item in data if item["name"] == snapshot["name"])
    assert focal["exchanges"][1]["name"] == local["name"]
    assert focal["exchanges"][1]["reference product"] == flow["name"]
    assert focal["exchanges"][1]["amount"] == snapshot["exchanges"][1]["amount"]
