"""Portable water-ion codec controls: metadata and synthetic records only."""

import csv
import inspect
import json
import math
from copy import deepcopy
from pathlib import Path

import pytest

from brightpath import BackgroundProfile, BiosphereProfile, InventoryFormat
from brightpath.background.catalogs import BiosphereCatalog, InMemoryCatalogProvider, TechnosphereCatalog
from brightpath.formats.simapro_csv import load_simapro_csv, write_simapro_csv
from brightpath.formats.simapro_water_ions import _UUIDS, _WIRE, WaterIonIdentityGuard
from brightpath.models import InventoryDocument, default_biosphere_profile

FIXTURE = Path(__file__).parent / "fixtures"
FACTS = json.loads((FIXTURE / "simapro_water_ion_evidence.json").read_text())
EVIDENCE = [
    {
        **row,
        "name": row["family"],
        "unit": FACTS["canonical_unit"],
        "wire_name": FACTS["families"][row["family"]]["wire"],
        "source_definition": FACTS["families"][row["family"]]["source_definition"],
    }
    for row in FACTS["identities"]
]
CATALOGS = {
    version: [
        {
            "name": FACTS["catalog_name_overrides"]
            .get(version, {})
            .get(str(index), FACTS["families"][EVIDENCE[index]["family"]]["catalog_names"][version]),
            "family": EVIDENCE[index]["family"],
            "categories": EVIDENCE[index]["categories"],
            "flow_id": EVIDENCE[index]["flow_id"],
            "unit": FACTS["canonical_unit"],
        }
        for index in order
    ]
    for version, order in FACTS["catalog_order"].items()
}
PROFILES = [("ecoinvent", v, "cutoff") for v in ["3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12"]]
PROFILES += [("uvek", "2025", "cutoff"), ("ecoinvent", "3.12", "consequential")]
PROFILES += [("ecoinvent", v, "cutoff") for v in ["3.6", "3.7", "3.8"]]


def provider_for(profile, rows=None):
    bio = default_biosphere_profile(profile)
    rows = rows if rows is not None else CATALOGS[bio.version]
    refs = {(r["name"], tuple(r["categories"]), r["unit"]): r["flow_id"] for r in rows}
    return InMemoryCatalogProvider(
        technosphere=[TechnosphereCatalog(profile.to_technosphere_profile(), frozenset())],
        biosphere=[BiosphereCatalog(bio, frozenset(refs), references=refs)],
    )


@pytest.mark.parametrize("family,version,model", PROFILES)
def test_actual_csv_all_qualified_families_and_witnessed_contexts(tmp_path, monkeypatch, family, version, model):
    profile = BackgroundProfile(family, version, model)
    bio = default_biosphere_profile(profile)
    provider = provider_for(profile)
    monkeypatch.setattr("brightpath.formats.simapro_water_ions.catalog_provider_from_environment", lambda: provider)
    process = {
        "name": "Synthetic water-ion carrier",
        "reference product": "synthetic output",
        "location": "GLO",
        "unit": "kilogram",
        "comment": "Synthetic quantities; no native inventory vectors.",
    }
    exchanges = []
    for i, row in enumerate(CATALOGS[bio.version]):
        exchanges.append(
            {
                "name": row["name"],
                "categories": tuple(row["categories"]),
                "unit": row["unit"],
                "type": "biosphere",
                "amount": (-1 if i % 2 else 1) * 0.125,
                "uncertainty type": 2,
                "loc": math.log(0.125),
                "scale": 0.2,
                "negative": bool(i % 2),
            }
        )
    doc = InventoryDocument(
        data=[
            {
                **process,
                "exchanges": [
                    {**process, "type": "production", "amount": 1, "simapro category": "Materials/Test"},
                    *exchanges,
                ],
            }
        ],
        background_profile=profile,
        inventory_format=InventoryFormat.SIMAPRO_CSV,
    )
    before = deepcopy(doc.data)
    kwargs = (
        {"catalog_provider": provider} if "catalog_provider" in inspect.signature(write_simapro_csv).parameters else {}
    )
    path, result = write_simapro_csv(doc, tmp_path / "inventory.csv", **kwargs)
    assert not result.has_errors
    with path.open(encoding="latin-1", newline="") as stream:
        rows = list(csv.reader(stream, delimiter=";"))
    observed = []
    section = None
    for row in rows:
        if len(row) == 1 and row[0]:
            section = row[0]
        if section == "Emissions to water" and len(row) >= 3:
            observed.append(tuple(row[:3]))
    assert len(observed) == len(CATALOGS[bio.version])
    modern = bio.version not in {"3.6", "3.7", "3.8"}
    contexts = {
        ("water",): "",
        ("water", "ground-"): "groundwater",
        ("water", "ground-, long-term"): "groundwater, long-term",
        ("water", "ocean"): "ocean",
        ("water", "surface water"): "river",
    }
    expected = [
        (
            (
                FACTS["families"][row["family"]]["wire"]
                if modern
                else FACTS["legacy"]["wire_names"][bio.version][row["family"]]
            ),
            contexts[tuple(row["categories"])],
            FACTS["wire_unit"],
        )
        for row in CATALOGS[bio.version]
    ]
    assert sorted(observed) == sorted(expected)
    restored = load_simapro_csv(path, background_profile=profile, biosphere_profile=bio, catalog_provider=provider)
    actual = [e for e in restored.data[0]["exchanges"] if e["type"] == "biosphere"]
    assert len(actual) == len(CATALOGS[bio.version])
    if not modern:
        # Pinned expected outputs from unmodified source preimages. No old
        # production module, native DB or local research file is loaded in CI.
        variant = "shared" if "catalog_provider" in inspect.signature(write_simapro_csv).parameters else "clean5220"
        group = FACTS["legacy"]["groups"][FACTS["legacy"]["profiles"][variant][bio.version]]
        expected_records = [{**group["common"], **row} for row in group["rows"]]
        assert json.loads(json.dumps(actual)) == expected_records
    for original, returned in zip(exchanges, actual, strict=True):
        identity = (returned["name"], tuple(returned["categories"]), returned["unit"])
        wanted = (original["name"], original["categories"], original["unit"])
        if modern:
            assert identity == wanted
            assert provider.load_biosphere(bio).references[identity] == provider.load_biosphere(bio).references[wanted]
        assert returned["amount"] == original["amount"]
        assert returned["uncertainty type"] == 2
        assert returned["loc"] == pytest.approx(original["loc"])
        assert returned["scale"] == pytest.approx(original["scale"])
    assert doc.data == before


@pytest.mark.parametrize("row", EVIDENCE, ids=lambda r: r["name"] + "-" + r["wire_context"])
def test_primary_exact_UUID_context_evidence_and_guard(row):
    profile = BackgroundProfile("ecoinvent", "3.12", "cutoff")
    provider = provider_for(profile)
    guard = WaterIonIdentityGuard(default_biosphere_profile(profile), provider)
    source = {
        "type": "biosphere",
        "name": row["name"],
        "categories": tuple(row["categories"]),
        "unit": "kilogram",
        "amount": -0.125,
        "formula": "-dose",
        "uncertainty type": 2,
        "loc": math.log(0.2),
        "scale": 0.3,
        "negative": True,
        "input": ("original", row["flow_id"]),
    }
    before = deepcopy(source)
    assert guard.wire_name(source, "fallback") == row["wire_name"]
    assert source == before
    wire = {**source, "name": "ignored_parser_alias"}
    allowed = {(row["name"], "water", row["categories"][1] if len(row["categories"]) > 1 else "unspecified")}
    assert guard.restore_wire_identity(wire, row["wire_name"], allowed_flows=allowed)
    assert wire == source
    assert _UUIDS[tuple(row["categories"])][row["name"]] == row["flow_id"]
    if row["name"] not in {"Caesium I", "Molybdenum VI", "Strontium II", "Chromium III", "Chromium VI"}:
        assert row["native95_literal"]
    elif row["name"] in FACTS["qualified_chromium"]:
        assert FACTS["qualified_chromium"][row["name"]]["definition_same_CAS_formula_kg_water"]
    else:
        assert row["source_definition"]["name"] == row["name"]
        assert FACTS["qualified_modern"][row["name"]]["stock_context"] == "Waterborne emissions"
        assert FACTS["qualified_modern"][row["name"]]["exact_original_canonical_UUID_retained"]
    assert row["public94_exact_factor1"]


@pytest.mark.parametrize("name", list(_WIRE))
@pytest.mark.parametrize(
    "problem",
    ["wrong_uuid", "missing_identity", "duplicate_uuid", "native_literal_collision", "excluded_flowset", "no_provider"],
)
def test_fail_closed_and_nonmutation(name, problem):
    profile = BackgroundProfile("ecoinvent", "3.12", "cutoff")
    rows = deepcopy(CATALOGS["3.12"])
    target = next(r for r in rows if r["name"] == name and r["categories"] == ["water"])
    if problem == "wrong_uuid":
        target["flow_id"] = "00000000-0000-0000-0000-000000000000"
    if problem == "missing_identity":
        rows.remove(target)
    if problem == "duplicate_uuid":
        rows.append({**target, "name": "different canonical identity"})
    if problem == "native_literal_collision":
        rows.append({**target, "name": _WIRE[name], "flow_id": "00000000-0000-0000-0000-000000000001"})
    guard = WaterIonIdentityGuard(
        default_biosphere_profile(profile), None if problem == "no_provider" else provider_for(profile, rows)
    )
    value = {
        "type": "biosphere",
        "name": "parsed alias",
        "categories": ("water",),
        "unit": "kilogram",
        "amount": 0.125,
        "formula": "dose",
        "loc": math.log(0.2),
        "scale": 0.3,
    }
    before = deepcopy(value)
    with pytest.raises(ValueError):
        guard.restore_wire_identity(value, _WIRE[name], allowed_flows=set() if problem == "excluded_flowset" else None)
    assert value == before


def test_wrong_exact_catalog_profile_rejected():
    profile = BackgroundProfile("ecoinvent", "3.12", "cutoff")
    wrong = provider_for(BackgroundProfile("ecoinvent", "3.10", "cutoff")).load_biosphere(
        BiosphereProfile("ecoinvent", "3.10")
    )

    class WrongProvider:
        def load_biosphere(self, profile):
            return wrong

    guard = WaterIonIdentityGuard(default_biosphere_profile(profile), WrongProvider())
    with pytest.raises(ValueError, match="exact declared"):
        guard.wire_name(
            {"name": "Calcium II", "type": "biosphere", "categories": ("water",), "unit": "kilogram"}, "fallback"
        )


@pytest.mark.parametrize("version", ["3.6", "3.7", "3.8"])
def test_old_profiles_helper_is_noop(version):
    guard = WaterIonIdentityGuard(BiosphereProfile("ecoinvent", version))
    for canonical, wire in _WIRE.items():
        value = {"type": "biosphere", "name": canonical, "categories": ("water",), "unit": "kilogram"}
        before = deepcopy(value)
        assert guard.wire_name(value, "legacy") == "legacy"
        assert not guard.restore_wire_identity(value, wire)
        assert value == before


@pytest.mark.parametrize("name", ["Sodium", "Sodium I", "Sodium, ion", "Manganese", "Chromium", "Calcium"])
def test_other_identities_not_relabelled(name):
    guard = WaterIonIdentityGuard(BiosphereProfile("ecoinvent", "3.12"))
    value = {"type": "biosphere", "name": name, "categories": ("water",), "unit": "kilogram"}
    assert guard.wire_name(value, "unchanged") == "unchanged"
    assert value["name"] == name


@pytest.mark.parametrize("categories", [("air",), ("soil", "agricultural"), ("natural resource", "in ground")])
def test_other_compartments_unchanged(categories):
    guard = WaterIonIdentityGuard(BiosphereProfile("ecoinvent", "3.12"))
    value = {"type": "biosphere", "name": "Cadmium II", "categories": categories, "unit": "kilogram"}
    before = deepcopy(value)
    assert guard.wire_name(value, "unchanged") == "unchanged"
    assert not guard.restore_wire_identity(value, "Cadmium (II)")
    assert value == before


def test_native_uvek_profile_unchanged():
    guard = WaterIonIdentityGuard(BiosphereProfile("uvek", "2026"))
    value = {"type": "biosphere", "name": "Cadmium II", "categories": ("water",), "unit": "kilogram"}
    assert guard.wire_name(value, "unchanged") == "unchanged"
    assert not guard.restore_wire_identity(value, "Cadmium (II)")


def test_calcium_stock_discrepancy_explicit():
    proof = FACTS["calcium"]
    assert proof["separate_generic_water_Calcium_UUIDs"] == 0
    assert proof["native_EI3101_CAS"] != proof["public_stock_CAS"]


@pytest.mark.parametrize("name", list(_WIRE))
@pytest.mark.parametrize("problem", ["unit", "context"])
def test_recognized_water_label_invalid_context_or_unit_rejected(name, problem):
    guard = WaterIonIdentityGuard(BiosphereProfile("ecoinvent", "3.12"))
    value = {
        "type": "biosphere",
        "name": name,
        "categories": ("water",),
        "unit": "kilogram",
        "amount": 0.125,
        "formula": "dose",
        "uncertainty type": 2,
        "loc": math.log(0.2),
        "scale": 0.3,
    }
    if problem == "unit":
        value["unit"] = "cubic meter"
    else:
        value["categories"] = ("water", "unverified context")
    before = deepcopy(value)
    with pytest.raises(ValueError, match="compartment/unit"):
        guard.wire_name(value, "fallback")
    with pytest.raises(ValueError, match="compartment/unit"):
        guard.restore_wire_identity(value, _WIRE[name])
    assert value == before


def test_actual_deterministic_formula_roundtrip(tmp_path, monkeypatch):
    profile = BackgroundProfile("ecoinvent", "3.9.1", "cutoff")
    bio = default_biosphere_profile(profile)
    provider = provider_for(profile)
    monkeypatch.setattr("brightpath.formats.simapro_water_ions.catalog_provider_from_environment", lambda: provider)
    process = {
        "name": "Synthetic formula carrier",
        "reference product": "synthetic output",
        "location": "GLO",
        "unit": "kilogram",
        "comment": "Synthetic formula; no native vector.",
    }
    flow = {
        "name": "Calcium II",
        "categories": ("water",),
        "unit": "kilogram",
        "type": "biosphere",
        "amount": 0.125,
        "formula": "dose",
        "uncertainty type": 0,
    }
    doc = InventoryDocument(
        data=[
            {
                **process,
                "parameters": [{"name": "dose", "amount": 0.125}],
                "exchanges": [
                    {**process, "type": "production", "amount": 1, "simapro category": "Materials/Test"},
                    flow,
                ],
            }
        ],
        background_profile=profile,
        inventory_format=InventoryFormat.SIMAPRO_CSV,
    )
    before = deepcopy(doc.data)
    kwargs = (
        {"catalog_provider": provider} if "catalog_provider" in inspect.signature(write_simapro_csv).parameters else {}
    )
    path, result = write_simapro_csv(doc, tmp_path / "formula.csv", **kwargs)
    assert not result.has_errors
    restored = load_simapro_csv(path, background_profile=profile, biosphere_profile=bio, catalog_provider=provider)
    returned = next(e for e in restored.data[0]["exchanges"] if e["type"] == "biosphere")
    assert returned["name"] == "Calcium II"
    assert returned["formula"] == "dose"
    assert returned["amount"] == 0.125
    assert returned.get("uncertainty type", 0) in (None, 0, 1)
    assert doc.data == before
