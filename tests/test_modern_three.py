"""Accepted specific modern correspondences; no legacy stock-ID equivalence."""

import csv
import inspect
import math
from copy import deepcopy

import pytest
from test_simapro_water_ions import CATALOGS, EVIDENCE, FACTS, provider_for

from brightpath import BackgroundProfile, InventoryFormat
from brightpath.background.catalogs import BiosphereCatalog, InMemoryCatalogProvider, TechnosphereCatalog
from brightpath.formats.simapro_csv import load_simapro_csv, write_simapro_csv
from brightpath.formats.simapro_sodium_identity import SodiumIdentityGuard
from brightpath.formats.simapro_water_ions import _WIRE, WaterIonIdentityGuard
from brightpath.models import InventoryDocument, default_biosphere_profile
from brightpath.utils import get_simapro_biosphere

ADDED = {"Caesium I": "Cesium (I)", "Molybdenum VI": "Molybdenum (VI)", "Strontium II": "Strontium (II)"}
CSV_FAMILIES = {**ADDED, "Chromium III": "Chromium (III)", "Chromium VI": "Chromium (VI)"}
VERSIONS = ["3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12"]


@pytest.mark.parametrize("family", ADDED)
def test_qualified_chemical_definition_and_source_CAS_discrepancy_preserved(family):
    q = FACTS["qualified_modern"][family]
    assert q["stock_class"] == "Emission" and q["stock_context"] == "Waterborne emissions" and q["stock_unit"] == "kg"
    assert q["amount_factor"] == 1
    assert not q["new_stoichiometric_or_compound_mass_interpretation"]
    assert q["source_CAS_discrepancy_retained"]
    assert not q["source_CAS_corrected_or_declared_authoritatively_stale"]
    assert not q["legacy_and_modern_stock_UUID_equivalence_claimed"]
    assert not q["native95_stock_binding_or_LCIA_equivalence_claimed"]
    expected_charge = {"Caesium I": 1, "Molybdenum VI": 6, "Strontium II": 2}[family]
    assert q["independent_chemical_basis"]["net_charge"] == expected_charge
    assert _WIRE[family] == ADDED[family]
    source = next(r for r in EVIDENCE if r["name"] == family)
    assert (
        source["source_definition"]["formula"]
        == {"Caesium I": "Cs+", "Molybdenum VI": "Mo+6", "Strontium II": "Sr+2"}[family]
    )
    assert source["source_definition"]["CAS"].lstrip("0") != q["modern_stock_CAS"].lstrip("0")
    profile = BackgroundProfile("ecoinvent", "3.12", "cutoff")
    guard = WaterIonIdentityGuard(default_biosphere_profile(profile), provider_for(profile))
    flow = {
        "name": family,
        "categories": tuple(source["categories"]),
        "unit": "kilogram",
        "type": "biosphere",
        "amount": -0.125,
        "formula": "-dose",
        "uncertainty type": 2,
        "loc": math.log(0.2),
        "scale": 0.3,
        "CAS": source["source_definition"]["CAS"],
    }
    before = deepcopy(flow)
    assert guard.wire_name(flow, "fallback") == ADDED[family]
    assert flow == before
    wire = {**flow, "name": "parser alias"}
    assert guard.restore_wire_identity(wire, ADDED[family])
    assert wire == flow


@pytest.mark.parametrize("family", CSV_FAMILIES)
@pytest.mark.parametrize("version", VERSIONS)
def test_actual_CSV_new_alias_all_contexts_formula_and_uncertainty_separate(tmp_path, monkeypatch, family, version):
    profile = BackgroundProfile("ecoinvent", version, "cutoff")
    bio = default_biosphere_profile(profile)
    provider = provider_for(profile)
    monkeypatch.setattr("brightpath.formats.simapro_water_ions.catalog_provider_from_environment", lambda: provider)
    ids = {r["flow_id"] for r in EVIDENCE if r["name"] == family}
    rows = [r for r in CATALOGS[version] if r["flow_id"] in ids]
    assert len(rows) == 5
    exchanges = []
    for i, row in enumerate(rows, 1):
        common = {"name": row["name"], "categories": tuple(row["categories"]), "unit": "kilogram", "type": "biosphere"}
        exchanges.append({**common, "amount": 0.125 * i, "formula": f"dose*{i}", "uncertainty type": 0})
        exchanges.append(
            {**common, "amount": -0.125, "uncertainty type": 2, "loc": math.log(0.125), "scale": 0.3, "negative": True}
        )
    process = {
        "name": "Synthetic qualified modern ion carrier",
        "reference product": "synthetic output",
        "unit": "kilogram",
        "location": "GLO",
        "comment": "Synthetic quantities; no native inventory vectors.",
    }
    document = InventoryDocument(
        data=[
            {
                **process,
                "parameters": [{"name": "dose", "amount": 0.125}],
                "exchanges": [
                    {**process, "type": "production", "amount": 1, "simapro category": "Materials/Test"},
                    *exchanges,
                ],
            }
        ],
        background_profile=profile,
        inventory_format=InventoryFormat.SIMAPRO_CSV,
    )
    before = deepcopy(document.data)
    kwargs = (
        {"catalog_provider": provider} if "catalog_provider" in inspect.signature(write_simapro_csv).parameters else {}
    )
    path, result = write_simapro_csv(document, tmp_path / "qualified.csv", **kwargs)
    assert not result.has_errors
    with path.open(encoding="latin-1", newline="") as stream:
        wire = [r for r in csv.reader(stream, delimiter=";") if r and r[0] == CSV_FAMILIES[family]]
    assert len(wire) == 10 and all(r[2] == "kg" for r in wire)
    restored = load_simapro_csv(path, background_profile=profile, biosphere_profile=bio, catalog_provider=provider)
    actual = [e for e in restored.data[0]["exchanges"] if e["type"] == "biosphere"]
    assert len(actual) == 10
    refs = provider.load_biosphere(bio).references
    for source, returned in zip(exchanges, actual, strict=True):
        identity = (returned["name"], tuple(returned["categories"]), returned["unit"])
        expected = (source["name"], source["categories"], source["unit"])
        assert identity == expected and refs[identity] == refs[expected]
        assert returned["amount"] == source["amount"]
        if source.get("formula"):
            assert returned["formula"] == source["formula"]
            assert returned.get("uncertainty type", 0) in (None, 0, 1)
        else:
            assert returned["uncertainty type"] == 2
            assert returned["loc"] == pytest.approx(source["loc"])
            assert returned["scale"] == pytest.approx(source["scale"])
    assert document.data == before


@pytest.mark.parametrize("family", ["Caesium I", "Strontium II"])
def test_wrong_name_for_earlier_exact_profile_rejected(family):
    profile = BackgroundProfile("ecoinvent", "3.9.1", "cutoff")
    guard = WaterIonIdentityGuard(default_biosphere_profile(profile), provider_for(profile))
    value = {"name": family, "categories": ("water",), "unit": "kilogram", "type": "biosphere", "amount": 1}
    before = deepcopy(value)
    with pytest.raises(ValueError, match="source name"):
        guard.wire_name(value, "fallback")
    assert value == before


@pytest.mark.parametrize("name", ["Caesium chloride", "Molybdenum trioxide", "Strontium carbonate"])
def test_compounds_are_not_reinterpreted_or_rescaled(name):
    guard = WaterIonIdentityGuard(default_biosphere_profile(BackgroundProfile("ecoinvent", "3.12", "cutoff")))
    value = {
        "name": name,
        "categories": ("water",),
        "unit": "kilogram",
        "type": "biosphere",
        "amount": 2,
        "formula": "dose",
    }
    before = deepcopy(value)
    assert guard.wire_name(value, "untouched") == "untouched"
    assert value == before


def test_native_sodium_generic_A_still_refused():
    profile = BackgroundProfile("ecoinvent", "3.8", "cutoff")
    bio = default_biosphere_profile(profile)
    rows = FACTS["sodium38"]
    refs = {(r["name"], tuple(r["categories"]), r["unit"]): r["flow_id"] for r in rows}
    provider = InMemoryCatalogProvider(
        technosphere=[TechnosphereCatalog(profile.to_technosphere_profile(), frozenset())],
        biosphere=[BiosphereCatalog(bio, frozenset(refs), references=refs)],
    )
    source = {"name": "Sodium", "categories": ("water",), "unit": "kilogram", "type": "biosphere", "amount": 1}
    issue = SodiumIdentityGuard(bio, get_simapro_biosphere(), provider).issue(source, "exchange")
    assert issue is not None and issue.code == "simapro_biosphere_identity_ambiguous"
    assert WaterIonIdentityGuard(bio, provider).wire_name(source, "Sodium") == "Sodium"


@pytest.mark.parametrize("family", ["Chromium III", "Chromium VI"])
def test_chromium_exact_definition_legacy_valid_and_stock_ids_distinct(family):
    q = FACTS["qualified_chromium"][family]
    old, modern = q["legacy_source_QName"], q["modern_source_QName"]
    assert old["name"] == family  # Positive legacy9.4 evidence, not invalidated.
    assert modern["name"] == family.replace(" III", " (III)").replace(" VI", " (VI)")
    assert old["stock_UUID"].lower() != modern["stock_UUID"].lower()
    assert not q["same_stock_UUID"]
    assert q["definition_same_CAS_formula_kg_water"]
    assert old["CAS"].lstrip("0") == modern["CAS"].lstrip("0")
    assert old["formula"] == modern["formula"]
    assert old["unit"] == modern["unit"] == "kg"
    assert old["context"] == modern["context"] == "Waterborne emissions"
    source = FACTS["families"][family]["source_definition"]
    assert source["CAS"].lstrip("0") == modern["CAS"].lstrip("0")
    assert source["formula"] == {"Chromium III": "Cr+3", "Chromium VI": "Cr+6"}[family]


@pytest.mark.parametrize("family,other", [("Chromium III", "Chromium VI"), ("Chromium VI", "Chromium III")])
def test_chromium_other_valence_UUID_rejected_before_mutation(family, other):
    profile = BackgroundProfile("ecoinvent", "3.12", "cutoff")
    rows = deepcopy(CATALOGS["3.12"])
    source = next(r for r in rows if r["name"] == family and r["categories"] == ["water"])
    other_row = next(r for r in rows if r["name"] == other and r["categories"] == ["water"])
    assert source["flow_id"] != other_row["flow_id"]
    source["flow_id"] = other_row["flow_id"]
    guard = WaterIonIdentityGuard(default_biosphere_profile(profile), provider_for(profile, rows))
    exchange = {"name": family, "type": "biosphere", "categories": ("water",), "unit": "kilogram", "amount": -0.125}
    before = deepcopy(exchange)
    with pytest.raises(ValueError):
        guard.wire_name(exchange, family)
    assert exchange == before


def test_chromium_native95_witness_limits_are_not_silently_expanded():
    rows = [r for r in EVIDENCE if r["name"] in {"Chromium III", "Chromium VI"}]
    assert len(rows) == 10 and sum(r["native95_literal"] for r in rows) == 8
    assert {(r["name"], tuple(r["categories"])) for r in rows if not r["native95_literal"]} == {
        ("Chromium III", ("water", "ground-, long-term")),
        ("Chromium VI", ("water", "ocean")),
    }
