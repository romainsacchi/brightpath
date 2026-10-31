"""Native sodium labels are UUID/context bindings, never chemistry substitutions."""

import csv
import math
from copy import deepcopy

import pytest

from brightpath.background.catalogs import BiosphereCatalog, InMemoryCatalogProvider, PackageCatalogProvider
from brightpath.core import BiosphereProfile
from brightpath.exceptions import SimaProSerializationError
from brightpath.formats.simapro_csv import load_simapro_csv, render_simapro_rows, write_simapro_csv
from brightpath.formats.simapro_sodium_identity import SodiumIdentityGuard, remember_sodium_wire_labels
from brightpath.models import BackgroundProfile, InventoryDocument, InventoryFormat

BINDINGS = [
    (("water",), "", "5ac961f0-8e53-42ff-b13f-1a7fb9133868"),
    (("water", "ground-"), "groundwater", "ee9fdd7c-5e29-4bdb-852a-3258f06ec2ab"),
    (("water", "ground-, long-term"), "groundwater, long-term", "fb83d61e-6fe3-4da7-8eb2-b0e2dc85a76d"),
    (("water", "ocean"), "ocean", "9310f497-3205-4717-aaa8-1a94c321f7d8"),
    (("water", "surface water"), "river", "1fc409bc-b8e7-48b2-92d5-2ced4aa7bae2"),
]


def source_document(version, categories, *, family="ecoinvent", name=None, **fields):
    provider = PackageCatalogProvider()
    bio = BiosphereProfile("ecoinvent", "3.10" if family == "uvek" else version)
    catalog = provider.load_biosphere(bio)
    flow_id = next(uid for cats, _, uid in BINDINGS if cats == categories)
    selected = [
        identity
        for identity, uid in catalog.references.items()
        if uid == flow_id and identity[1:] == (categories, "kilogram")
    ]
    assert len(selected) == 1
    activity = dict(
        name="Native sodium fixture",
        comment="Synthetic native identity fixture.",
        **{"reference product": "test output"},
        location="GLO",
        unit="kilogram",
    )
    exchange = dict(
        type="biosphere",
        name=name or selected[0][0],
        categories=categories,
        unit="kilogram",
        amount=2.5,
        **{"uncertainty type": 2, "loc": math.log(2.5), "scale": 0.2},
        comment="No source-identity restoration",
    )
    exchange.update(fields)
    doc = InventoryDocument(
        data=[
            {
                **activity,
                "exchanges": [
                    {**activity, "type": "production", "amount": 1, "simapro category": "Materials/Test"},
                    exchange,
                ],
            }
        ],
        background_profile=BackgroundProfile(family, version, "cutoff"),
        inventory_format=InventoryFormat.SIMAPRO_CSV,
    )
    return doc, provider, catalog


@pytest.mark.parametrize("version", ["3.6", "3.7", "3.8", "3.9.1", "3.10.1", "3.11", "3.12"])
@pytest.mark.parametrize("categories,subcompartment,flow_id", BINDINGS)
def test_actual_native_rows_and_same_selected_uuid_roundtrip(tmp_path, version, categories, subcompartment, flow_id):
    doc, provider, catalog = source_document(version, categories)
    before = deepcopy(doc.data)
    path, result = write_simapro_csv(doc, tmp_path / "native.csv")
    assert not result.has_errors
    with path.open(encoding="latin-1", newline="") as stream:
        rows = list(csv.reader(stream, delimiter=";"))
    literal = [row for row in rows if len(row) > 2 and row[:3] == ["Sodium", subcompartment, "kg"]]
    assert len(literal) == 1
    assert not any(row and row[0] in {"Sodium I", "Sodium, ion"} for row in rows)
    loaded = load_simapro_csv(path, background_profile=doc.background_profile, catalog_provider=provider)
    actual = next(e for e in loaded.data[0]["exchanges"] if e["type"] == "biosphere")
    identity = actual["name"], tuple(actual["categories"]), actual["unit"]
    assert identity == (before[0]["exchanges"][1]["name"], categories, "kilogram")
    assert catalog.references[identity] == flow_id
    for key in ("amount", "loc", "scale"):
        assert actual[key] == pytest.approx(before[0]["exchanges"][1][key], rel=1e-12)
    assert actual["uncertainty type"] == 2
    assert doc.data == before


def test_ei_biosphere_rule_is_independent_of_UVEK_technosphere():
    doc, provider, catalog = source_document("2025", ("water",), family="uvek")
    guard = SodiumIdentityGuard(doc.context.background.biosphere, {}, provider)
    exchange = doc.data[0]["exchanges"][1]
    assert guard.wire_name(exchange, "Sodium I") == "Sodium"
    raw = {**exchange, "name": "Sodium, ion"}
    assert guard.restore_wire_identity(raw, "Sodium")
    assert catalog.references[(raw["name"], tuple(raw["categories"]), raw["unit"])] == BINDINGS[0][2]


@pytest.mark.parametrize(
    "categories,generic_uuid",
    [
        (("water",), "723f56f9-c68e-5692-a969-0fb2a70bce20"),
        (("water", "ground-"), "73a1b292-e31d-4100-a413-304795a3656f"),
    ],
)
def test_distinct_generic_root_and_groundwater_refused_without_source_change(tmp_path, categories, generic_uuid):
    doc, provider, catalog = source_document("3.9.1", categories, name="Sodium")
    before = deepcopy(doc.data)
    assert catalog.references[("Sodium", categories, "kilogram")] == generic_uuid
    result = render_simapro_rows(doc)
    assert not result.rows
    issue = next(i for i in result.issues if i.code == "simapro_biosphere_identity_ambiguous")
    assert generic_uuid in issue.message
    with pytest.raises(SimaProSerializationError, match="cannot preserve the distinction"):
        write_simapro_csv(doc, tmp_path / "generic.csv")
    assert not (tmp_path / "generic.csv").exists()
    assert doc.data == before


def guard_for(references, *, version="3.9.1"):
    profile = BiosphereProfile("ecoinvent", version)
    catalog = BiosphereCatalog(profile, references.keys(), references=references)
    return SodiumIdentityGuard(profile, {}, InMemoryCatalogProvider(biosphere=[catalog]))


def test_reader_prefers_proven_ionic_uuid_over_other_valid_generic_name():
    generic = ("Sodium", ("water",), "kilogram")
    ionic = ("Sodium I", ("water",), "kilogram")
    guard = guard_for({generic: "723f56f9-c68e-5692-a969-0fb2a70bce20", ionic: BINDINGS[0][2]})
    exchange = {
        "type": "biosphere",
        "name": "Sodium",
        "categories": ("water",),
        "unit": "kilogram",
        "amount": -2.5,
        "formula": "alpha",
        "uncertainty type": 3,
        "loc": -2.5,
        "scale": 0.25,
        "comment": "Generic Sodium source claim must be ignored",
    }
    before = deepcopy(exchange)
    assert guard.restore_wire_identity(exchange, "Sodium")
    assert exchange == {**before, "name": "Sodium I"}
    assert guard.wire_name(exchange, "Sodium I") == "Sodium"


@pytest.mark.parametrize(
    "refs",
    [
        {("Sodium I", ("water",), "kilogram"): "00000000-0000-4000-8000-000000000001"},
        {},
        {("Sodium I", ("water",), "kilogram"): BINDINGS[0][2], ("Sodium, ion", ("water",), "kilogram"): BINDINGS[0][2]},
        {("Sodium grade X", ("water",), "kilogram"): BINDINGS[0][2]},
    ],
)
def test_missing_wrong_ambiguous_custom_catalogue_identity_never_guessed(refs):
    guard = guard_for(refs)
    exchange = {"type": "biosphere", "name": "Sodium", "categories": ("water",), "unit": "kilogram", "amount": 2.5}
    before = deepcopy(exchange)
    with pytest.raises(ValueError, match="unique verified ionic Sodium"):
        guard.restore_wire_identity(exchange, "Sodium")
    assert exchange == before


@pytest.mark.parametrize(
    "fields", [{"categories": ("water", "unknown")}, {"unit": "gram"}, {"categories": ("water", "river, custom grade")}]
)
def test_unproven_water_context_or_unit_is_refused_not_aliased(fields):
    guard = guard_for({("Sodium I", ("water",), "kilogram"): BINDINGS[0][2]})
    exchange = {
        "type": "biosphere",
        "name": "Sodium",
        "categories": ("water",),
        "unit": "kilogram",
        "amount": 2.5,
        **fields,
    }
    before = deepcopy(exchange)
    with pytest.raises(ValueError, match="compartment/unit"):
        guard.restore_wire_identity(exchange, "Sodium")
    assert exchange == before


def test_unrelated_contexts_and_native_UVEK_are_untouched():
    guard = guard_for({})
    exchange = {"type": "biosphere", "name": "Sodium", "categories": ("air",), "unit": "kilogram", "amount": 2.5}
    assert not guard.restore_wire_identity(exchange, "Sodium")
    guard = SodiumIdentityGuard(BiosphereProfile("uvek", "2026"), {})
    exchange["categories"] = ("water",)
    assert not guard.restore_wire_identity(exchange, "Sodium")
    assert guard.wire_name(exchange, "Original native name") == "Original native name"


def test_native_label_snapshot_cannot_come_from_comment():
    raw = [
        {
            "exchanges": [
                {"type": "biosphere", "name": "Sodium", "comment": "Sodium I"},
                {"type": "biosphere", "name": "Unknown grade", "comment": "Sodium"},
            ]
        }
    ]
    remember_sodium_wire_labels(raw)
    assert raw[0]["exchanges"][0]["simapro name"] == "Sodium"
    assert "simapro name" not in raw[0]["exchanges"][1]


@pytest.mark.parametrize("categories,subcompartment,flow_id", BINDINGS)
def test_every_different_generic_UUID_is_refused_even_outside_root(categories, subcompartment, flow_id):
    generic_uuid = "00000000-0000-4000-8000-000000000001"
    generic = ("Sodium", categories, "kilogram")
    ionic = ("Sodium I", categories, "kilogram")
    guard = guard_for({generic: generic_uuid, ionic: flow_id})
    exchange = {"type": "biosphere", "name": "Sodium", "categories": categories, "unit": "kilogram", "amount": -2.5}
    before = deepcopy(exchange)
    issue = guard.issue(exchange, "exchange")
    assert issue.code == "simapro_biosphere_identity_ambiguous"
    assert generic_uuid in issue.message and flow_id in issue.message
    assert exchange == before


def test_signed_formula_actual_roundtrip(tmp_path):
    doc, provider, catalog = source_document(
        "3.9.1",
        ("water", "surface water"),
        amount=-2.5,
        **{"uncertainty type": 0, "formula": "alpha"},
    )
    data = doc.data
    data[0]["parameters"] = [{"name": "alpha", "amount": -2.5}]
    doc = InventoryDocument(
        data=data, background_profile=doc.background_profile, inventory_format=InventoryFormat.SIMAPRO_CSV
    )
    before = deepcopy(doc.data)
    path, _ = write_simapro_csv(doc, tmp_path / "formula.csv")
    loaded = load_simapro_csv(path, background_profile=doc.background_profile, catalog_provider=provider)
    actual = next(e for e in loaded.data[0]["exchanges"] if e["type"] == "biosphere")
    assert actual["name"] == "Sodium I"
    assert catalog.references[(actual["name"], tuple(actual["categories"]), actual["unit"])] == BINDINGS[-1][2]
    assert actual["formula"] == "alpha"
    assert actual["amount"] == -2.5
    assert actual.get("uncertainty type", 0) in {0, 1}
    assert doc.data == before


def test_writer_unknown_ionic_UUID_is_structured_refusal_not_native_alias():
    guard = guard_for({("Sodium I", ("water",), "kilogram"): "00000000-0000-4000-8000-000000000001"})
    exchange = {"type": "biosphere", "name": "Sodium I", "categories": ("water",), "unit": "kilogram", "amount": 2.5}
    before = deepcopy(exchange)
    assert guard.wire_name(exchange, "Sodium I") == "Sodium I"
    issue = guard.issue(exchange, "exchange")
    assert issue.code == "simapro_biosphere_unresolved"
    assert exchange == before


def test_signed_lognormal_actual_roundtrip(tmp_path):
    doc, provider, catalog = source_document("3.9.1", ("water",), amount=-2.5, negative=True)
    before = deepcopy(doc.data)
    path, _ = write_simapro_csv(doc, tmp_path / "negative.csv")
    loaded = load_simapro_csv(path, background_profile=doc.background_profile, catalog_provider=provider)
    actual = next(e for e in loaded.data[0]["exchanges"] if e["type"] == "biosphere")
    assert catalog.references[(actual["name"], tuple(actual["categories"]), actual["unit"])] == BINDINGS[0][2]
    assert actual["amount"] == -2.5 and actual["negative"] is True
    assert actual["loc"] == pytest.approx(math.log(2.5), rel=1e-12)
    assert actual["scale"] == pytest.approx(0.2, rel=1e-12)
    assert doc.data == before


def test_injected_provider_cannot_supply_a_different_profile():
    selected = BiosphereProfile("ecoinvent", "3.9.1")
    other = BiosphereCatalog(
        BiosphereProfile("ecoinvent", "3.12"),
        {("Sodium I", ("water",), "kilogram")},
        references={("Sodium I", ("water",), "kilogram"): BINDINGS[0][2]},
    )

    class WrongProfileProvider:
        def load_biosphere(self, profile):
            assert profile == selected
            return other

    guard = SodiumIdentityGuard(selected, {}, WrongProfileProvider())
    exchange = {"type": "biosphere", "name": "Sodium I", "categories": ("water",), "unit": "kilogram", "amount": 2.5}
    before = deepcopy(exchange)
    assert guard.issue(exchange, "exchange").code == "simapro_biosphere_unresolved"
    with pytest.raises(ValueError, match="exact declared"):
        guard.restore_wire_identity(exchange, "Sodium")
    assert exchange == before


def raw_sodium_dataset():
    name = "test output {GLO}| Native sodium fixture | Cut-off, U"
    return [
        {
            "name": name,
            "unit": "kilogram",
            "exchanges": [
                {"name": name, "unit": "kilogram", "type": "production", "amount": 1},
                {"name": "Sodium", "unit": "kilogram", "type": "biosphere", "categories": ("water",), "amount": 2.5},
            ],
        }
    ]


def test_custom_flowset_without_provider_cannot_use_environment_catalog(monkeypatch):
    from brightpath.formats import simapro_sodium_identity
    from brightpath.formats.simapro_csv import normalize_simapro_import_data

    def forbidden():
        raise AssertionError("Caller custom flowset must not silently select a packaged catalogue")

    monkeypatch.setattr(simapro_sodium_identity, "catalog_provider_from_environment", forbidden)
    raw = raw_sodium_dataset()
    before = deepcopy(raw)
    with pytest.raises(ValueError, match="explicit UUID-bearing"):
        normalize_simapro_import_data(
            raw,
            background_profile=BackgroundProfile("ecoinvent", "3.9.1", "cutoff"),
            database_name="fixture",
            biosphere_flows={("Sodium I", "water", "unspecified")},
            biosphere_correspondence={},
            version_mapping={},
        )
    assert raw == before


@pytest.mark.parametrize("allow_B", [False, True])
def test_provider_binding_also_must_belong_to_explicit_flowset(allow_B):
    from brightpath.formats.simapro_csv import normalize_simapro_import_data

    guard = guard_for(
        {
            ("Sodium", ("water",), "kilogram"): "723f56f9-c68e-5692-a969-0fb2a70bce20",
            ("Sodium I", ("water",), "kilogram"): BINDINGS[0][2],
        }
    )
    raw = raw_sodium_dataset()
    before = deepcopy(raw)
    kwargs = dict(
        background_profile=BackgroundProfile("ecoinvent", "3.9.1", "cutoff"),
        database_name="fixture",
        catalog_provider=guard.provider,
        biosphere_flows={("Sodium I" if allow_B else "Sodium", "water", "unspecified")},
        biosphere_correspondence={},
        version_mapping={},
    )
    if allow_B:
        result = normalize_simapro_import_data(raw, **kwargs)
        assert result[0]["exchanges"][1]["name"] == "Sodium I"
        assert result[0]["exchanges"][1]["amount"] == 2.5
    else:
        with pytest.raises(ValueError, match="excluded by the supplied"):
            normalize_simapro_import_data(raw, **kwargs)
    assert raw == before


def test_public_facade_default_provider_preserves_native_B(tmp_path):
    from brightpath import SimaProInventory

    doc, _, catalog = source_document("3.9.1", ("water",))
    path, _ = write_simapro_csv(doc, tmp_path / "public.csv")
    loaded = SimaProInventory.from_csv(path, background_profile=doc.background_profile)
    exchange = next(e for e in loaded.data[0]["exchanges"] if e["type"] == "biosphere")
    assert catalog.references[(exchange["name"], tuple(exchange["categories"]), exchange["unit"])] == BINDINGS[0][2]
    assert exchange["amount"] == 2.5


def test_other_manual_flowset_import_does_not_require_a_catalogue(monkeypatch):
    from brightpath.formats import simapro_sodium_identity
    from brightpath.formats.simapro_csv import normalize_simapro_import_data

    def forbidden():
        raise AssertionError("Unrelated manual import must not load a catalogue")

    monkeypatch.setattr(simapro_sodium_identity, "catalog_provider_from_environment", forbidden)
    raw = raw_sodium_dataset()
    raw[0]["exchanges"][1]["name"] = "Arsenic"
    before = deepcopy(raw)
    result = normalize_simapro_import_data(
        raw,
        background_profile=BackgroundProfile("ecoinvent", "3.9.1", "cutoff"),
        database_name="fixture",
        biosphere_flows={("Arsenic", "water", "unspecified")},
        biosphere_correspondence={},
        version_mapping={},
    )
    assert result[0]["exchanges"][1]["name"] == "Arsenic"
    assert result[0]["exchanges"][1]["amount"] == 2.5
    assert raw == before
