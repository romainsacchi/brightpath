"""Exact UVEK inputs must be represented or refused, never silently dropped.

Native wire evidence is documented in the accompanying review receipt; these
synthetic regressions exercise software behavior, not a native LCI/LCIA claim.
"""

import csv
import math
import re
from copy import deepcopy

import pytest

from brightpath.background.catalogs import PackageCatalogProvider
from brightpath.core.context import BiosphereProfile
from brightpath.formats.simapro_csv import load_simapro_csv, render_simapro_rows
from brightpath.formats.simapro_native_uncertainty import prepare, render
from brightpath.models import BackgroundProfile, InventoryDocument, InventoryFormat

CONTRACTS = [
    ("2025", "Occupation, traffic area, road network", ("natural resource", "land"), "square meter-year"),
    ("2025", "Oxygen", ("air",), "kilogram"),
    ("2025", "Oxygen", ("natural resource", "in air"), "kilogram"),
    ("2025", "Radon-220", ("air", "non-urban air or from high stacks"), "kilo Becquerel"),
    ("2025", "Radon-222", ("air", "non-urban air or from high stacks"), "kilo Becquerel"),
    ("2025", "Radon-222", ("air", "urban air close to ground"), "kilo Becquerel"),
    ("2025", "Radon-222", ("air",), "kilo Becquerel"),
    ("2026", "Occupation, traffic area, road network", ("resources", "land"), "square meter-year"),
    ("2026", "Oxygen", ("emissions to air", "unspecified"), "kilogram"),
]
TECH_NAMES = [
    "market for activated carbon, granular",
    "market for iodine",
    "market for molybdenum trioxide",
    "market for fly ash and scrubber sludge",
    "market for barium oxide",
    "market for strontium carbonate",
    "market for polysulfone",
]


def document(version="2025", exchanges=(), suppliers=()):
    head = {
        "name": "Synthetic blacklist consumer",
        "reference product": "synthetic output",
        "location": "GLO",
        "unit": "kilogram",
    }
    activity = {
        **head,
        "exchanges": [
            {**head, "type": "production", "amount": 1.0, "simapro category": "Materials/Other"},
            *deepcopy(exchanges),
        ],
    }
    activity["comment"] = "Synthetic input-preservation regression fixture."
    supplier_data = deepcopy(suppliers)
    for supplier in supplier_data:
        supplier.setdefault("comment", "Synthetic exact foreground supplier.")
    return InventoryDocument(
        data=[activity, *supplier_data],
        background_profile=BackgroundProfile("uvek", version, "cutoff"),
        biosphere_profile=(
            BiosphereProfile("ecoinvent", "3.10") if version == "2025" else BiosphereProfile("uvek", "2026")
        ),
        inventory_format=InventoryFormat.BRIGHTWAY_EXCEL,
    )


@pytest.mark.parametrize("version,name,categories,unit", CONTRACTS)
def test_actual_native_graph_file_keeps_one_consumer_and_exact_source_fields(tmp_path, version, name, categories, unit):
    exchange = {
        "name": name,
        "type": "biosphere",
        "categories": categories,
        "unit": unit,
        "amount": 7.0,
        "uncertainty type": 2,
        "loc": math.log(11),
        "scale": 0.2,
    }
    source = document(version, [exchange])
    before = deepcopy(source.data)
    prepared, specs = prepare(source)
    assert len(specs) == 1
    rows = render(prepared, specs)
    occurrences = [
        c
        for row in rows
        for c in row
        if isinstance(c, str) and re.search(r"\biff\s*\(", c, re.I) and re.search(r"\b" + specs[0].latent + r"\b", c)
    ]
    assert len(occurrences) == 1
    latent = next(row for row in rows if row and row[0] == specs[0].latent)
    assert (
        latent[2] == "Normal"
        and float(latent[1]) == exchange["loc"]
        and float(latent[3]) == 2 * exchange["scale"]
        and latent[7] == ""
    )
    path = tmp_path / "native-input.csv"
    with path.open("w", encoding="latin-1", newline="") as stream:
        csv.writer(stream, delimiter=";").writerows(rows)
    loaded = load_simapro_csv(
        path,
        background_profile=source.background_profile,
        biosphere_profile=source.biosphere_profile,
        catalog_provider=PackageCatalogProvider(),
    )
    found = [e for a in loaded.data for e in a["exchanges"] if e["type"] == "biosphere"]
    assert len(found) == 1
    actual = found[0]
    assert (actual["name"], tuple(actual["categories"]), actual["unit"]) == (name, categories, unit)
    assert actual["amount"] == exchange["amount"] and actual["uncertainty type"] == 2
    assert actual["loc"] == pytest.approx(exchange["loc"], rel=1e-12)
    assert actual["scale"] == pytest.approx(exchange["scale"], rel=1e-12)
    assert source.data == before


def test_centered_3625_and_independent_2870_2_road_rows_both_survive():
    rows = [
        {
            "name": "Occupation, traffic area, road network",
            "type": "biosphere",
            "categories": ("natural resource", "land"),
            "unit": "square meter-year",
            "amount": amount,
            "uncertainty type": 2,
            "loc": mu,
            "scale": 0.20442967058809,
        }
        for amount, mu in [(3625.0, math.log(3625.0)), (2870.2, 7.962136992736832)]
    ]
    source = document(exchanges=rows)
    before = deepcopy(source.data)
    prepared, specs = prepare(source)
    assert len(specs) == 1
    wire = render(prepared, specs)
    emitted = [r for r in wire if len(r) == 9 and r[0] == rows[0]["name"]]
    assert len(emitted) == 2
    assert any(r[3] == "3625" and r[4] == "Lognormal" for r in emitted)
    assert any(specs[0].latent in str(r[3]) for r in emitted)
    assert source.data == before


@pytest.mark.parametrize("name", TECH_NAMES)
@pytest.mark.parametrize("negative", [False, True])
def test_blacklisted_name_cannot_drop_exact_included_material_or_waste_supplier(name, negative):
    head = {"name": name, "reference product": "synthetic foreground commodity", "location": "GLO", "unit": "kilogram"}
    supplier = {
        **head,
        "exchanges": [
            {
                **head,
                "type": "production",
                "amount": -1.0 if negative else 1.0,
                "simapro category": "Waste treatment/Other" if negative else "Materials/Other",
            }
        ],
    }
    source = document(
        exchanges=[{**head, "type": "technosphere", "amount": -0.125 if negative else 0.125}], suppliers=[supplier]
    )
    before = deepcopy(source.data)
    result = render_simapro_rows(source)
    assert not result.has_errors, result.issues
    assert not any(i.code == "simapro_exchange_unused" for i in result.issues)
    assert any(len(r) == 8 and abs(float(r[2])) == 0.125 for r in result.rows if len(r) == 8 and r[1] == "kg")
    assert source.data == before


@pytest.mark.parametrize("change", ["reference product", "location", "unit", "absent"])
def test_external_or_different_foreground_contract_is_refused_not_chosen_by_name(change):
    head = {
        "name": "market for polysulfone",
        "reference product": "foreground polymer",
        "location": "GLO",
        "unit": "kilogram",
    }
    supplier = {
        **head,
        "exchanges": [{**head, "type": "production", "amount": 1.0, "simapro category": "Materials/Other"}],
    }
    exchange = {**head, "type": "technosphere", "amount": 0.125}
    if change != "absent":
        exchange[change] = {"reference product": "another polymer", "location": "RER", "unit": "cubic meter"}[change]
    source = document(exchanges=[exchange], suppliers=[] if change == "absent" else [supplier])
    before = deepcopy(source.data)
    result = render_simapro_rows(source)
    assert result.has_errors and result.rows == []
    if change == "reference product":
        # The existing complete-contract guard runs before row generation.
        assert any("conflicts with a foreground SimaPro product name and unit" in i.message for i in result.issues)
    else:
        assert any("not omitted" in i.message for i in result.issues)
    assert source.data == before


@pytest.mark.parametrize(
    "name,unit,categories",
    [
        ("Carbon-14", "kilo Becquerel", ("air", "non-urban air or from high stacks")),
        ("Gangue, in ground", "kilogram", ("natural resource", "in ground")),
        ("Oxygen", "kilogram", ("water", "surface water")),
    ],
)
def test_unreviewed_active_blacklist_inputs_refuse_before_partial_package(name, unit, categories):
    source = document(
        exchanges=[{"name": name, "type": "biosphere", "categories": categories, "unit": unit, "amount": 1.0}]
    )
    result = render_simapro_rows(source)
    assert result.has_errors and result.rows == []
    assert any("not omitted" in issue.message for issue in result.issues)


def test_source_formula_on_retained_oxygen_is_not_replaced():
    source = document(
        exchanges=[
            {
                "name": "Oxygen",
                "type": "biosphere",
                "categories": ("air",),
                "unit": "kilogram",
                "amount": 0.125,
                "formula": "1/8",
                "uncertainty type": 0,
            }
        ]
    )
    before = deepcopy(source.data)
    result = render_simapro_rows(source)
    assert not result.has_errors
    oxygen = [r for r in result.rows if len(r) == 9 and r[0] == "Oxygen"]
    assert len(oxygen) == 1 and oxygen[0][3] == "1/8"
    assert source.data == before


@pytest.mark.parametrize(
    "metadata", [{"uncertainty type": 3, "loc": 0.0, "scale": 1.0}, {"uncertainty type": 0, "formula": "1-1"}]
)
def test_active_zero_known_oxygen_is_emitted(metadata):
    source = document(
        exchanges=[
            {
                "name": "Oxygen",
                "type": "biosphere",
                "categories": ("air",),
                "unit": "kilogram",
                "amount": 0.0,
                **metadata,
            }
        ]
    )
    before = deepcopy(source.data)
    result = render_simapro_rows(source)
    assert not result.has_errors
    assert len([r for r in result.rows if len(r) == 9 and r[0] == "Oxygen"]) == 1
    assert source.data == before


def test_inactive_unverified_zero_remains_no_demand():
    source = document(
        exchanges=[
            {
                "name": "Carbon-14",
                "type": "biosphere",
                "categories": ("air", "non-urban air or from high stacks"),
                "unit": "kilo Becquerel",
                "amount": 0.0,
                "uncertainty type": 0,
            }
        ]
    )
    result = render_simapro_rows(source)
    assert not result.has_errors
    assert not any(len(row) == 9 and row[0] == "Carbon-14" for row in result.rows)
