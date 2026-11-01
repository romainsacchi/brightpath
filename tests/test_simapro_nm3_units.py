"""Same-spelling normal-volume CSV support; no proprietary Desktop compatibility claim."""

import csv
import math
from copy import deepcopy
from pathlib import Path

import pytest

from brightpath import BackgroundProfile, SimaProInventory, utils
from brightpath.core.context import (
    BackgroundContext,
    BiosphereProfile,
    FormatProfile,
    InventoryContext,
    TechnosphereProfile,
)
from brightpath.models import InventoryDocument

DATA = Path(utils.DATA_DIR)
SCIENCE = ("amount", "loc", "scale", "shape", "minimum", "maximum", "formula")


def overlay(tmp_path, contents):
    root = tmp_path / "overlay-data"
    export = root / "export"
    export.mkdir(parents=True)
    for source in (DATA / "export").iterdir():
        if source.name != "simapro_units.yaml":
            (export / source.name).symlink_to(source, target_is_directory=source.is_dir())
    (export / "simapro_units.yaml").write_text(contents)
    return root


@pytest.fixture
def resource(tmp_path, monkeypatch):
    contents = (DATA / "export/simapro_units.yaml").read_text()
    assert contents.count("normal cubic meter: Nm3") == 1
    old = "\n".join(line for line in contents.splitlines() if line != "normal cubic meter: Nm3") + "\n"
    monkeypatch.setattr(utils, "DATA_DIR", overlay(tmp_path / "candidate", contents))
    return contents, old


def document(exchange, *, unit="normal cubic meter", parameters=None):
    def activity(name, amount_unit):
        return {
            "name": name,
            "reference product": name,
            "location": "AE",
            "unit": amount_unit,
            "comment": "Synthetic unit-spelling fixture, not native background inventory.",
            "code": name,
            "exchanges": [
                {
                    "name": name,
                    "reference product": name,
                    "location": "AE",
                    "unit": amount_unit,
                    "type": "production",
                    "amount": 1,
                    "simapro category": "Materials/Test",
                }
            ],
        }

    consumer = activity("Private Nm3 consumer", "kilogram")
    provider = activity("Private Nm3 provider", unit)
    consumer["exchanges"].append(
        {
            "name": provider["name"],
            "reference product": provider["name"],
            "location": "AE",
            "unit": unit,
            "type": "technosphere",
            **exchange,
        }
    )
    if parameters:
        consumer["parameters"] = deepcopy(parameters)
    context = InventoryContext(
        FormatProfile("simapro_csv"),
        BackgroundContext(TechnosphereProfile("uvek", "2026", "cutoff"), BiosphereProfile("uvek", "2026")),
    )
    return InventoryDocument(data=[consumer, provider], context=context, database_name="private-nm3-test")


def write_read(doc, destination):
    before = deepcopy(doc.data)
    source = SimaProInventory(doc)
    context = source.context
    filename = source.write_csv(destination)
    with filename.open(encoding="latin-1", newline="") as stream:
        rows = list(csv.reader(stream, delimiter=";"))
    loaded = SimaProInventory.from_csv(filename, context=context, database_name="private-nm3-test")
    assert doc.data == before and source.data == before
    assert source.context == loaded.context == context
    assert source.background_profile == loaded.background_profile == BackgroundProfile("uvek", "2026", "cutoff")
    assert source.biosphere_profile == loaded.biosphere_profile == BiosphereProfile("uvek", "2026")
    assert ["Quantities"] not in rows and ["Units"] not in rows
    links = [e for a in loaded.data for e in a["exchanges"] if e["type"] == "technosphere"]
    assert len(links) == 1
    return rows, links[0], loaded


@pytest.mark.parametrize(
    "exchange",
    [
        {"amount": 2.0},
        {"amount": 2.0, "uncertainty type": 2, "loc": math.log(2), "scale": 0.3, "negative": False},
        {"amount": -2.0, "uncertainty type": 2, "loc": math.log(2), "scale": 0.3, "negative": True},
        {"amount": 2.0, "uncertainty type": 3, "loc": 2.0, "scale": 0.3},
        {"amount": -2.0, "uncertainty type": 3, "loc": -2.0, "scale": 0.3},
        {"amount": 2.0, "uncertainty type": 4, "minimum": 1.0, "maximum": 3.0},
        {"amount": 2.0, "uncertainty type": 5, "loc": 2.0, "minimum": 1.0, "maximum": 3.0},
        {"amount": 0.0, "uncertainty type": 5, "loc": 0.0, "minimum": 0.0, "maximum": 0.3},
    ],
)
def test_literal_nm3_and_actual_reader_preserve_science(resource, tmp_path, exchange):
    rows, after, _ = write_read(document(exchange), tmp_path / "nm3.csv")
    native = [r for r in rows if len(r) == 8 and "Private Nm3 provider" in r[0]]
    assert len(native) == 1 and native[0][1] == "Nm3"
    assert after["unit"] == "normal cubic meter"
    for field in SCIENCE:
        if field in exchange:
            if field == "formula":
                assert after.get(field) == exchange[field]
            else:
                assert after.get(field) == pytest.approx(exchange[field], rel=1e-10, abs=0)
    if exchange.get("uncertainty type") in {2, 3, 4, 5}:
        assert after["uncertainty type"] == exchange["uncertainty type"]
    else:
        assert int(after.get("uncertainty type") or 0) in {0, 1}
    if exchange.get("uncertainty type") == 2:
        assert bool(after.get("negative", after["amount"] < 0)) == exchange["negative"]


def test_formula_is_not_scaled_or_wrapped(resource, tmp_path):
    doc = document({"amount": 2.0, "formula": "dose"}, parameters=[{"name": "dose", "amount": 2.0}])
    rows, after, _ = write_read(doc, tmp_path / "formula.csv")
    native = next(r for r in rows if len(r) == 8 and "Private Nm3 provider" in r[0])
    assert native[1:3] == ["Nm3", "dose"]
    assert after["unit"] == "normal cubic meter" and after["amount"] == 2 and after["formula"] == "dose"


def scientific_payload(rows):
    result = []
    section = None
    for row in rows:
        if not row:
            section = None
        elif len(row) == 1:
            section = row[0]
        elif section in {
            "Products",
            "Materials/fuels",
            "Electricity/heat",
            "Input parameters",
            "Calculated parameters",
        }:
            result.append((section, row[:-1]))
    return result


@pytest.mark.parametrize("unit, wire", [("cubic meter", "m3"), ("standard cubic meter", "m3")])
def test_existing_m3_and_sm3_behavior_is_unchanged(resource, tmp_path, monkeypatch, unit, wire):
    # This control compares real predecessor behavior. The existing ambiguous
    # Sm3 foreground product spelling is refused by the native identity guard.
    # Adding Nm3 must neither suppress that refusal nor change its wire bytes.
    doc = document({"amount": 2.0, "uncertainty type": 3, "loc": 2.0, "scale": 0.3}, unit=unit)

    def run_control(destination):
        before = deepcopy(doc.data)
        source = SimaProInventory(doc)
        filename = source.write_csv(destination)
        with filename.open(encoding="latin-1", newline="") as stream:
            rows = list(csv.reader(stream, delimiter=";"))
        assert next(r for r in rows if len(r) == 8 and "Private Nm3 provider" in r[0])[1] == wire
        if unit == "standard cubic meter":
            with pytest.raises(ValueError, match="activity identity metadata conflicts") as caught:
                SimaProInventory.from_csv(filename, context=source.context, database_name="private-nm3-test")
            outcome = (type(caught.value).__name__, str(caught.value))
        else:
            loaded = SimaProInventory.from_csv(filename, context=source.context, database_name="private-nm3-test")
            exchange = next(e for a in loaded.data for e in a["exchanges"] if e["type"] == "technosphere")
            outcome = {k: exchange.get(k) for k in (*SCIENCE, "unit", "uncertainty type")}
        assert doc.data == before
        return scientific_payload(rows), outcome

    candidate = run_control(tmp_path / "candidate-control.csv")
    with monkeypatch.context() as patch:
        patch.setattr(utils, "DATA_DIR", overlay(tmp_path / "predecessor", resource[1]))
        predecessor = run_control(tmp_path / "predecessor-control.csv")
    assert candidate == predecessor


def test_other_unknown_units_still_refuse(resource):
    result = SimaProInventory(document({"amount": 1.0}, unit="unsupported unit - do not invent")).render()
    assert result.has_errors and not result.rows
