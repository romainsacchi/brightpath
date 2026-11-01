"""Arithmetic in native value cells is a formula even without parameter names."""

import csv
import math
from copy import deepcopy

import pytest

from brightpath import BackgroundProfile, BiosphereProfile, InventoryFormat
from brightpath.background import BiosphereCatalog, InMemoryCatalogProvider
from brightpath.formats.simapro_csv import (
    _ExpressionPreservingSimaProExtractor,
    load_simapro_csv,
    write_simapro_csv,
)
from brightpath.models import InventoryDocument

PROFILE = BackgroundProfile("ecoinvent", "3.12", "cutoff")


@pytest.mark.parametrize("expression,amount", [("7.6*3.6", 27.36), ("1/2", 0.5), ("(2+3)*4", 20), ("-2*3", -6)])
def test_native_biosphere_constant_expression_keeps_formula_and_value(expression, amount):
    line = ["Heat, waste", "", "MJ", expression, "Undefined", "0", "0", "0", "provenance"]
    before = deepcopy(line)
    result = _ExpressionPreservingSimaProExtractor.parse_biosphere_flow(line, "air", {})
    assert result["formula"] == expression
    assert result["amount"] == pytest.approx(amount)
    assert result["comment"] == "provenance"
    assert line == before


@pytest.mark.parametrize("value", ["27.36", "-2", "+3", "1e-6", " 2.5 ", "25%", "2,5"])
def test_numeric_and_percentage_cells_are_not_invented_formulas(value):
    result = _ExpressionPreservingSimaProExtractor.parse_input_line(
        ["input", "kg", value, "Undefined", "0", "0", "0", ""], "Materials/fuels", {}
    )
    assert "formula" not in result


def test_constant_expression_retains_native_distribution_not_an_independent_hidden_centre():
    result = _ExpressionPreservingSimaProExtractor.parse_input_line(
        ["input", "kg", "1/2", "Lognormal", "1.44", "0", "0", ""], "Materials/fuels", {}
    )
    assert result["formula"] == "1/2"
    assert result["amount"] == 0.5
    assert result["uncertainty type"] == 2
    assert result["loc"] == math.log(0.5)
    assert result["scale"] == pytest.approx(math.log(1.2))


@pytest.mark.parametrize("section", ["Materials/fuels", "Electricity/heat", "Waste to treatment", "Avoided products"])
def test_input_sections_preserve_constant_expression(section):
    result = _ExpressionPreservingSimaProExtractor.parse_input_line(
        ["input", "kg", "2*3", "Undefined", "0", "0", "0", ""], section, {}
    )
    assert result["formula"] == "2*3" and result["amount"] == 6


@pytest.mark.parametrize(
    "method,line",
    [
        ("parse_reference_product", ["output", "kg", "2*3", "100%", "", "Test", ""]),
        ("parse_waste_treatment", ["output", "kg", "2*3", "", "Test", ""]),
        (
            "parse_final_waste_flow",
            ["Waste mass, total, placed in landfill", "", "kg", "2*3", "Undefined", "0", "0", "0"],
        ),
    ],
)
def test_outputs_and_final_waste_preserve_native_expression(method, line):
    result = getattr(_ExpressionPreservingSimaProExtractor, method)(line, {})
    assert result["formula"] == "2*3" and result["amount"] == 6


def test_swapped_value_and_unit_fields_keep_the_expression():
    result = _ExpressionPreservingSimaProExtractor.parse_biosphere_flow(
        ["Heat, waste", "", "7.6*3.6", "MJ", "Undefined", "0", "0", "0", ""], "air", {}
    )
    assert result["formula"] == "7.6*3.6" and result["unit"] == "MJ"


def test_native_csv_constant_expression_roundtrip_without_hidden_comment_metadata(tmp_path):
    identity = {
        "name": "constant expression process",
        "reference product": "product",
        "location": "GLO",
        "unit": "kilogram",
    }
    flow = {
        "name": "Heat, waste",
        "categories": ("air",),
        "unit": "megajoule",
        "type": "biosphere",
        "amount": 27.36,
        "formula": "7.6*3.6",
    }
    source = InventoryDocument(
        data=[
            {
                **identity,
                "comment": "Native arithmetic value-cell fixture; no encoded metadata.",
                "exchanges": [
                    {**identity, "type": "production", "amount": 1, "simapro category": "material/Test"},
                    flow,
                ],
            }
        ],
        background_profile=PROFILE,
        inventory_format=InventoryFormat.SIMAPRO_CSV,
    )
    before = source.data
    path, _ = write_simapro_csv(source, tmp_path / "constant.csv")
    rows = list(csv.reader(path.open(encoding="latin-1", newline=""), delimiter=";"))
    (wire,) = [row for row in rows if row and row[0] == "Heat, waste"]
    assert wire[3] == "7.6*3.6" and not wire[-1]
    assert b"BrightPath parameter" not in path.read_bytes() and b"BrightPath activity" not in path.read_bytes()
    provider = InMemoryCatalogProvider(
        biosphere=[
            BiosphereCatalog(BiosphereProfile("ecoinvent", "3.12"), frozenset({("Heat, waste", ("air",), "megajoule")}))
        ]
    )
    loaded = load_simapro_csv(path, background_profile=PROFILE, catalog_provider=provider)
    actual = next(e for e in loaded.data[0]["exchanges"] if e["type"] == "biosphere")
    assert actual["formula"] == flow["formula"] and actual["amount"] == flow["amount"]
    assert source.data == before
