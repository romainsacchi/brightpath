"""Native external IDs are distinct from self-contained generic JSON-LD IDs."""

import json
from copy import deepcopy
from zipfile import ZipFile

import pytest

from brightpath.background.catalogs import BiosphereCatalog, InMemoryCatalogProvider, TechnosphereCatalog
from brightpath.core.context import (
    BackgroundContext,
    BiosphereProfile,
    FormatProfile,
    InventoryContext,
    TechnosphereProfile,
)
from brightpath.formats import openlca_ecospold2_references as native
from brightpath.formats.ilcd_background import openlca_key, quantity_definition
from brightpath.formats.openlca_jsonld import load_openlca_jsonld, write_openlca_jsonld
from brightpath.models import InventoryDocument

ACTIVITY = "00000000-0000-4000-8000-000000000001"
PRODUCT = "00000000-0000-4000-8000-000000000002"
BIO = "00000000-0000-4000-8000-000000000003"
OTHER = "00000000-0000-4000-8000-000000000004"
TECH = ("Native electricity supplier", "electricity", "RER", "kilowatt hour")
AIR = ("Synthetic air emission", ("air", "urban air close to ground"), "kilogram")
FIELDS = ("name", "reference product", "location", "unit")


def context(version="3.12", model="cutoff"):
    return InventoryContext(
        FormatProfile("openlca_jsonld"),
        BackgroundContext(TechnosphereProfile("ecoinvent", version, model), BiosphereProfile("ecoinvent", version)),
    )


def record(name, product):
    identity = dict(zip(FIELDS, (name, product, "CH", "kilogram"), strict=True))
    return {**identity, "exchanges": [{**identity, "type": "production", "amount": 1.0}]}


def document(ctx=None):
    local = record("Local foreground", "local product")
    consumer = record("Consumer", "service")
    consumer["exchanges"] += [
        {**dict(zip(FIELDS, TECH, strict=True)), "type": "technosphere", "amount": 2.5, "formula": "dose"},
        {"name": AIR[0], "categories": AIR[1], "unit": AIR[2], "type": "biosphere", "amount": -0.2},
        {**{k: local[k] for k in FIELDS}, "type": "technosphere", "amount": 3.0},
    ]
    consumer["parameters"] = [{"name": "dose", "amount": 2.5}]
    return InventoryDocument(data=[local, consumer], context=ctx or context(), database_name="synthetic")


def provider(ctx=None, extra_tech=None, extra_bio=None):
    ctx = ctx or context()
    tech = {TECH: (ACTIVITY, PRODUCT), **(extra_tech or {})}
    bio = {AIR: BIO, **(extra_bio or {})}
    return InMemoryCatalogProvider(
        technosphere=[TechnosphereCatalog(ctx.background.technosphere, frozenset(tech), references=tech)],
        biosphere=[BiosphereCatalog(ctx.background.biosphere, frozenset(bio), references=bio)],
    )


def wire_ref(kind, identifier, name, **values):
    return {"@type": kind, "@id": identifier, "name": name, **values}


def read_raw(path):
    with ZipFile(path) as z:
        return {name: json.loads(z.read(name)) for name in z.namelist() if name.endswith(".json")}


def write_raw(path, entities):
    with ZipFile(path, "w") as z:
        for name, value in entities.items():
            z.writestr(name, json.dumps(value))


def rows(entities):
    result = {}
    for name, value in entities.items():
        if name == "olca-schema.json":
            continue
        result.setdefault(name.split("/", 1)[0], []).append(value)
    return result


def externalize(entities, references):
    for filename, process in entities.copy().items():
        if not filename.startswith("processes/"):
            continue
        for exchange in process["exchanges"]:
            label = exchange["flow"].get("name")
            if label not in (TECH[1], AIR[0]):
                continue
            ref = references.technosphere[TECH] if label == TECH[1] else references.biosphere[AIR]
            entities.pop(f"flows/{exchange['flow']['@id']}.json")
            exchange["flow"] = wire_ref(
                "Flow",
                ref.flow_id,
                ref.flow_name,
                flowType="PRODUCT_FLOW" if label == TECH[1] else "ELEMENTARY_FLOW",
            )
            exchange["unit"] = wire_ref("Unit", ref.unit_id, ref.unit_name)
            exchange["flowProperty"] = wire_ref("FlowProperty", ref.flow_property_id, ref.flow_property_name)
            exchange.pop("location", None)
            if label == TECH[1]:
                exchange["defaultProvider"] = wire_ref(
                    "Process", ref.process_id, ref.process_name, location=ref.location
                )
    return entities


@pytest.fixture
def linked(tmp_path):
    doc = document()
    refs = native.references_for_document(doc, catalog_provider=provider())
    path = write_openlca_jsonld(doc, tmp_path / "generic.zip")
    return doc, externalize(read_raw(path), refs)


@pytest.mark.parametrize("version,model", [("3.9.1", "cutoff"), ("3.12", "cutoff"), ("3.10.1", "consequential")])
def test_used_native_ids_and_selected_kwh_unit_without_amount_scaling(version, model):
    ctx = context(version, model)
    source = document(ctx)
    before = source.data
    catalog = native.references_for_document(source, catalog_provider=provider(ctx))
    assert set(catalog.technosphere) == {TECH}
    assert set(catalog.biosphere) == {AIR}
    ref = catalog.technosphere[TECH]
    q = quantity_definition("kilowatt hour", profile=ctx.background.technosphere)
    assert ref.process_id == openlca_key(ACTIVITY, PRODUCT)
    assert ref.flow_id == PRODUCT
    assert ref.unit_id == q["unit_id"]
    assert ref.unit_name == "kWh" != q["reference_unit"]
    assert q["factor"] == 3.6
    assert ref.location_id == ref.category == ""
    assert ref.location == TECH[2]
    assert catalog.biosphere[AIR].flow_id == BIO
    assert source.data == before


@pytest.mark.parametrize("wrong_unit", [False, True])
def test_3_11_sm3_native_provider_references_and_actual_zip_readback(tmp_path, monkeypatch, wrong_unit):
    ctx = context("3.11")
    gas = ("Synthetic standard gas supplier", "Synthetic standard gas", "RER", "standard cubic meter")
    consumer = record("Gas consumer", "Synthetic service")
    consumer["parameters"] = [{"name": "dose", "amount": 0.125}]
    consumer["exchanges"].append(
        {
            **dict(zip(FIELDS, gas, strict=True)),
            "type": "technosphere",
            "amount": 0.125,
            "uncertainty type": 3,
            "loc": 0.125,
            "scale": 0.01,
            "formula": "dose",
        }
    )
    source = InventoryDocument(data=[consumer], context=ctx, database_name="synthetic")
    before = source.data
    catalogs = InMemoryCatalogProvider(
        technosphere=[
            TechnosphereCatalog(ctx.background.technosphere, frozenset([gas]), references={gas: (ACTIVITY, PRODUCT)})
        ]
    )
    refs = native.references_for_document(source, catalog_provider=catalogs)
    ref = refs.technosphere[gas]
    assert ref.process_id == openlca_key(ACTIVITY, PRODUCT) and ref.flow_id == PRODUCT
    assert ref.unit_id == "225da0bd-ba54-48bc-9a0e-5fa27d722185" and ref.unit_name == "Sm3"
    assert ref.flow_property_id == "c57e6ed2-5c28-3348-aaee-75d637b80c77"
    quantity = quantity_definition(gas[3], profile=ctx.background.technosphere)
    assert quantity["group_id"] == "c2dba802-b9a4-345b-b4b3-847754301054" and quantity["factor"] == 1

    # As in the other native readback fixtures, replace only generic external
    # references with the exact native wire IDs; this is not a native import claim.
    path = write_openlca_jsonld(source, tmp_path / "sm3.zip")
    entities = read_raw(path)
    process = rows(entities)["processes"][0]
    exchange = process["exchanges"][1]
    entities.pop(f"flows/{exchange['flow']['@id']}.json")
    exchange["flow"] = wire_ref("Flow", ref.flow_id, ref.flow_name, flowType="PRODUCT_FLOW")
    exchange["defaultProvider"] = wire_ref("Process", ref.process_id, ref.process_name, location=ref.location)
    exchange["flowProperty"] = wire_ref("FlowProperty", ref.flow_property_id, ref.flow_property_name)
    exchange["unit"] = wire_ref("Unit", ref.unit_id, ref.unit_name)
    exchange.pop("location", None)
    if wrong_unit:
        exchange["unit"]["@id"] = quantity_definition("cubic meter")["unit_id"]
    write_raw(path, entities)
    monkeypatch.setattr(native, "catalog_provider_from_environment", lambda: catalogs)
    if wrong_unit:
        with pytest.raises(ValueError, match="native|External openLCA"):
            load_openlca_jsonld(path, context=ctx)
    else:
        restored = load_openlca_jsonld(path, context=ctx).data[0]["exchanges"][1]
        assert tuple(restored[key] for key in FIELDS) == gas
        for key in ("amount", "loc", "scale", "formula", "uncertainty type"):
            assert restored[key] == before[0]["exchanges"][1][key]
    assert source.data == before


def test_local_and_explicit_missing_foregrounds_do_not_trigger_catalog_lookup(monkeypatch):
    doc = document()
    data = doc.data
    data[1]["exchanges"] = [x for x in data[1]["exchanges"] if x["type"] != "biosphere"]
    doc = doc.replace(data=data)
    monkeypatch.setattr(native, "catalog_provider_from_environment", lambda: pytest.fail("catalog not needed"))
    refs = native.references_for_document(doc, missing_foreground=[TECH])
    assert not refs.technosphere and not refs.biosphere


@pytest.mark.parametrize("kind", ["technosphere", "biosphere"])
def test_unknown_used_external_identity_fails_closed(kind):
    doc = document()
    data = doc.data
    next(x for x in data[1]["exchanges"] if x["type"] == kind)["name"] = "Not in exact catalog"
    with pytest.raises(ValueError, match="No exact native"):
        native.references_for_document(doc.replace(data=data), catalog_provider=provider())


def test_unknown_native_unit_is_not_approximated():
    key = (*TECH[:3], "unknown quantum")
    doc = document()
    data = doc.data
    data[1]["exchanges"][1]["unit"] = key[3]
    with pytest.raises(ValueError, match="native openLCA unit.*ecoinvent 3.12 cutoff") as error:
        native.references_for_document(
            doc.replace(data=data), catalog_provider=provider(extra_tech={key: (ACTIVITY, OTHER)})
        )
    assert "ILCD" not in str(error.value)


def test_profile_conflict_is_rejected():
    from types import SimpleNamespace

    wrong = provider(context("3.9.1")).load_technosphere(context("3.9.1").background.technosphere)
    bad = SimpleNamespace(load_technosphere=lambda profile: wrong)
    with pytest.raises(ValueError, match="profile conflicts"):
        native.references_for_document(document(), catalog_provider=bad)


@pytest.mark.parametrize("family,bio_version", [("uvek", "3.12"), ("ecoinvent", "3.11")])
def test_other_or_unpaired_context_keeps_existing_dispatch(family, bio_version):
    ctx = InventoryContext(
        FormatProfile("openlca_jsonld"),
        BackgroundContext(TechnosphereProfile(family, "3.12", "cutoff"), BiosphereProfile("ecoinvent", bio_version)),
    )
    assert native.references_for_document(document(ctx)) is None
    assert native.references_for_package({}, ctx) is None
    assert native.references_for_package({}, None) is None


def test_generic_self_contained_writer_and_reader_defaults_stay_unchanged(tmp_path, monkeypatch):
    doc = document()
    path = write_openlca_jsonld(doc, tmp_path / "legacy.zip")
    entities = read_raw(path)
    monkeypatch.setattr(
        native, "catalog_provider_from_environment", lambda: pytest.fail("legacy package needs no catalog")
    )
    assert native.references_for_package(rows(entities), doc.context) is None
    restored = load_openlca_jsonld(path, context=doc.context)
    assert len(restored.data) == 2
    assert restored.data[1]["exchanges"][1]["amount"] == 2.5
    assert all(
        e["flow"]["@id"] in {f["@id"] for f in rows(entities)["flows"]}
        for p in rows(entities)["processes"]
        for e in p["exchanges"]
    )


def test_native_readback_hook_preserves_local_supplier_and_external_amount_formula(linked, tmp_path, monkeypatch):
    doc, entities = linked
    before = deepcopy(entities)
    monkeypatch.setattr(native, "catalog_provider_from_environment", provider)
    path = tmp_path / "native.zip"
    write_raw(path, entities)
    result = load_openlca_jsonld(path, context=doc.context).data
    external = result[1]["exchanges"][1]
    assert tuple(external[k] for k in FIELDS) == TECH
    assert external["amount"] == 2.5 and external["formula"] == "dose"
    assert result[1]["exchanges"][2]["amount"] == -0.2
    assert result[1]["exchanges"][3]["name"] == "Local foreground"
    assert entities == before


@pytest.mark.parametrize("field", ["flow", "defaultProvider", "unit", "flowProperty"])
@pytest.mark.parametrize("mutation", ["id", "missing", "name", "type"])
def test_native_readback_rejects_mutated_reference_contracts(linked, tmp_path, monkeypatch, field, mutation):
    doc, entities = linked
    exchange = next(p for p in rows(entities)["processes"] if p["name"] == "Consumer")["exchanges"][1]
    ref = exchange[field]
    if mutation == "missing":
        ref.pop("@id")
    else:
        ref[{"id": "@id", "name": "name", "type": "@type"}[mutation]] = {
            "id": OTHER,
            "name": "Different",
            "type": "Wrong",
        }[mutation]
    monkeypatch.setattr(native, "catalog_provider_from_environment", provider)
    path = tmp_path / "invalid.zip"
    write_raw(path, entities)
    with pytest.raises(ValueError, match="External openLCA|Unknown or ambiguous external"):
        load_openlca_jsonld(path, context=doc.context)


def test_reverse_catalog_filters_products_before_keygen(linked, monkeypatch):
    doc, entities = linked
    irrelevant = ("Unused", "unused product", "GLO", "kilogram")
    catalog_provider = provider(extra_tech={irrelevant: (OTHER, OTHER)})
    actual = native._process_id
    calls = []

    def keygen(*parts):
        calls.append(parts)
        return actual(*parts)

    monkeypatch.setattr(native, "_process_id", keygen)
    refs = native.references_for_package(rows(entities), doc.context, catalog_provider=catalog_provider)
    assert list(refs.technosphere) == [TECH]
    assert calls == [(ACTIVITY, PRODUCT)]


def test_native_identifier_ambiguity_is_not_resolved_by_matching_names(linked):
    doc, entities = linked
    alias = ("Different supplier label", TECH[1], TECH[2], TECH[3])
    with pytest.raises(ValueError, match="ambiguous native"):
        native.references_for_package(
            rows(entities), doc.context, catalog_provider=provider(extra_tech={alias: (ACTIVITY, PRODUCT)})
        )


def test_empty_or_unavailable_context_stays_generic_for_package_reader(linked):
    doc, entities = linked
    assert (
        native.references_for_package(rows(entities), doc.context, catalog_provider=InMemoryCatalogProvider()) is None
    )


def test_biosphere_missing_ids_and_wrong_units_are_rejected(linked):
    doc, entities = linked
    exchange = next(p for p in rows(entities)["processes"] if p["name"] == "Consumer")["exchanges"][2]
    exchange["unit"]["@id"] = OTHER
    with pytest.raises(ValueError, match="unknown/ambiguous native"):
        native.references_for_package(rows(entities), doc.context, catalog_provider=provider())


@pytest.mark.parametrize("unit", ["Becquerel", "meter-year", "square meter-year", "hectare", "year", "kilowatt hour"])
def test_native_selected_unit_spelling_comes_from_verified_metadata(unit):
    key = (*TECH[:3], unit)
    source = document()
    data = source.data
    data[1]["exchanges"][1]["unit"] = unit
    source = source.replace(data=data)
    refs = native.references_for_document(source, catalog_provider=provider(extra_tech={key: (ACTIVITY, OTHER)}))
    q = quantity_definition(unit, profile=source.context.background.technosphere)
    selected = next(row for row in q["units"] if row["id"] == q["unit_id"])
    assert refs.technosphere[key].unit_name == selected["name"]
    assert refs.technosphere[key].unit_id == selected["id"]
    assert source.data[1]["exchanges"][1]["amount"] == 2.5


@pytest.mark.parametrize("mutation", ["direction", "location", "reference_unit_hint"])
def test_external_exchange_semantics_are_checked_after_native_id_resolution(linked, tmp_path, monkeypatch, mutation):
    doc, entities = linked
    exchange = next(p for p in rows(entities)["processes"] if p["name"] == "Consumer")["exchanges"][1]
    if mutation == "direction":
        exchange["isInput"] = False
    elif mutation == "location":
        exchange["defaultProvider"]["location"] = "US"
    else:
        exchange["flow"]["refUnit"] = "MJ"
    monkeypatch.setattr(native, "catalog_provider_from_environment", provider)
    path = tmp_path / "invalid-semantics.zip"
    write_raw(path, entities)
    with pytest.raises(ValueError, match="External openLCA"):
        load_openlca_jsonld(path, context=doc.context)


def test_biosphere_identifier_ambiguity_is_not_name_guessed(linked):
    doc, entities = linked
    other = ("Another emission label", AIR[1], AIR[2])
    with pytest.raises(ValueError, match="ambiguous native"):
        native.references_for_package(rows(entities), doc.context, catalog_provider=provider(extra_bio={other: BIO}))


def test_observed_native_heat_pump_provider_and_product_id_witness():
    identity = (
        "heat pump production, heat and power co-generation unit, 160kW electrical",
        "heat pump, heat and power co-generation unit, 160kW electrical",
        "RER",
        "unit",
    )
    activity = "331ed391-2bfa-5d0a-ae96-346f536da7f5"
    product = "013d2a59-5d91-4d06-acfc-4ea125843c19"
    doc = document()
    data = doc.data
    data[1]["exchanges"][1].update(dict(zip(FIELDS, identity, strict=True)))
    catalog = provider(extra_tech={identity: (activity, product)})
    refs = native.references_for_document(doc.replace(data=data), catalog_provider=catalog)
    assert refs.technosphere[identity].process_id == "04e3483c-8138-351a-96b8-557713409a8f"
    assert refs.technosphere[identity].flow_id == product
    assert data[1]["exchanges"][1]["amount"] == 2.5
