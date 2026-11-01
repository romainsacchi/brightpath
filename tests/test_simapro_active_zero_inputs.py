"""Zero nominal inputs retain active distributions and formulas in native CSV fields."""

import csv
from copy import deepcopy

import pytest
from test_simapro_inventory import minimal_activity, profile
from test_simapro_waste_export import link, process, section_rows

from brightpath import BackgroundProfile, SimaProInventory

HEAT = {
    "name": "heat production, natural gas, at industrial furnace >100kW",
    "reference product": "heat, district or industrial, natural gas",
    "location": "RoW",
    "unit": "megajoule",
    "type": "technosphere",
    "amount": 0,
}
CARBON = {
    "name": "Carbon dioxide, fossil",
    "categories": ("air", "urban air close to ground"),
    "unit": "kilogram",
    "type": "biosphere",
    "amount": 0,
}
TRIANGLE = {"uncertainty type": 5, "loc": 0, "minimum": 0, "maximum": 0.3}


def roundtrip(tmp_path, exchanges, *, parameters=None, validate=True):
    data = [minimal_activity(extra_exchanges=deepcopy(exchanges))]
    if parameters:
        data[0]["parameters"] = deepcopy(parameters)
    before = deepcopy(data)
    source = SimaProInventory.from_data(data, background_profile=profile("3.12"))
    original = source.data
    path = source.write_csv(tmp_path / "zero-inputs.csv", validate=validate)
    with path.open(encoding="latin-1", newline="") as stream:
        rows = list(csv.reader(stream, delimiter=";"))
    loaded = SimaProInventory.from_csv(path, background_profile=source.background_profile).data
    assert data == before and source.data == original == before
    return rows, loaded[0]


def test_native_zero_heat_triangle_is_written_and_read_with_all_original_fields(tmp_path):
    # Actual shape from lithium brine manganese removal: zero mode, nonzero support.
    rows, activity = roundtrip(tmp_path, [{**HEAT, **TRIANGLE}])
    (wire,) = section_rows(rows, "Electricity/heat")
    assert wire[1:7] == ["MJ", "0", "Triangle", "0", "0", "0.3"]
    (exchange,) = [e for e in activity["exchanges"] if e["type"] == "technosphere"]
    for key, value in {**HEAT, **TRIANGLE}.items():
        assert exchange[key] == value
    assert not exchange.get("formula")


@pytest.mark.parametrize(
    "distribution,wire_fields",
    [
        (TRIANGLE, ["Triangle", "0", "0", "0.3"]),
        ({"uncertainty type": 3, "loc": 0, "scale": 0.2}, ["Normal", "0.4", "0", "0"]),
        ({"uncertainty type": 4, "minimum": -0.2, "maximum": 0.3}, ["Uniform", "0", "-0.2", "0.3"]),
    ],
)
def test_zero_biosphere_distribution_is_native_not_restored_from_comment(tmp_path, distribution, wire_fields):
    rows, activity = roundtrip(tmp_path, [{**CARBON, **distribution}])
    (wire,) = section_rows(rows, "Emissions to air")
    assert wire[2:4] == ["kg", "0"] and wire[4:8] == wire_fields
    assert wire[8] == ""
    (exchange,) = [e for e in activity["exchanges"] if e["type"] == "biosphere"]
    assert tuple(exchange["categories"]) == CARBON["categories"]
    for key, value in {**CARBON, **distribution}.items():
        if key != "categories":
            assert exchange[key] == value


@pytest.mark.parametrize("base,section,offset", [(HEAT, "Electricity/heat", 2), (CARBON, "Emissions to air", 3)])
@pytest.mark.parametrize("kind", [0, 1])
def test_zero_formula_is_not_an_inactive_placeholder(tmp_path, base, section, offset, kind):
    original = {**base, "formula": "dose", "uncertainty type": kind}
    rows, activity = roundtrip(tmp_path, [original], parameters=[{"name": "dose", "amount": 0}])
    (wire,) = section_rows(rows, section)
    assert wire[offset] == "dose"  # Literal native formula, no numeric comment stash.
    (exchange,) = [e for e in activity["exchanges"] if e["type"] == base["type"]]
    assert exchange["amount"] == 0 and exchange["formula"] == "dose"
    assert exchange["name"] == base["name"] and exchange["unit"] == base["unit"]


@pytest.mark.parametrize("base,section", [(HEAT, "Electricity/heat"), (CARBON, "Emissions to air")])
@pytest.mark.parametrize("kind", [None, 0, 1])
def test_inactive_formula_free_zero_placeholders_still_omitted(tmp_path, base, section, kind):
    original = dict(base)
    if kind is not None:
        original.update({"uncertainty type": kind, "loc": 4, "scale": 9, "minimum": -2, "maximum": 3})
    rows, activity = roundtrip(tmp_path, [original])
    assert section_rows(rows, section) == []
    assert not any(e["type"] == base["type"] for e in activity["exchanges"])


def test_zero_waste_input_keeps_existing_sign_and_bound_reflection(tmp_path):
    consumer = process("consumer")
    waste = process("disposal service", "waste treatment/Test", -2)
    consumer["exchanges"].append(link(waste, 0, **TRIANGLE))
    data = [consumer, waste]
    before = deepcopy(data)
    source = SimaProInventory.from_data(data, background_profile=profile("3.12"))
    path = source.write_csv(tmp_path / "zero-waste.csv", validate=False)
    with path.open(encoding="latin-1", newline="") as stream:
        rows = list(csv.reader(stream, delimiter=";"))
    (wire,) = section_rows(rows, "Waste to treatment")
    assert wire[1:7] == ["kg", "0", "Triangle", "0", "-0.3", "0"]
    loaded = SimaProInventory.from_csv(path, background_profile=source.background_profile).data
    activity = next(a for a in loaded if a["name"] == "consumer")
    (exchange,) = [e for e in activity["exchanges"] if e["type"] == "technosphere"]
    assert exchange["amount"] == exchange["loc"] == exchange["minimum"] == 0
    assert exchange["maximum"] == 0.3 and exchange["uncertainty type"] == 5
    assert data == before and source.data == before


def test_zero_final_waste_distribution_uses_final_waste_selector(tmp_path):
    indicator = {
        **CARBON,
        **TRIANGLE,
        "name": "Waste mass, total, placed in landfill",
        "categories": ("inventory indicator", "waste"),
    }
    rows, activity = roundtrip(tmp_path, [indicator], validate=False)
    (wire,) = section_rows(rows, "Final waste flows")
    assert wire[:8] == [indicator["name"], "", "kg", "0", "Triangle", "0", "0", "0.3"]
    (exchange,) = [e for e in activity["exchanges"] if e["type"] == "biosphere"]
    assert exchange["name"] == indicator["name"]
    assert tuple(exchange["categories"]) == indicator["categories"]
    for key, value in TRIANGLE.items():
        assert exchange[key] == value
    assert exchange["amount"] == 0


def test_zero_water_normal_keeps_physical_unit_and_spread_conversion(tmp_path):
    water = {**CARBON, "name": "Water", "unit": "cubic meter", "uncertainty type": 3, "loc": 0, "scale": 0.0001}
    rows, activity = roundtrip(tmp_path, [water])
    (wire,) = section_rows(rows, "Emissions to air")
    assert wire[2:6] == ["kg", "0", "Normal", "0.2"]
    (exchange,) = [e for e in activity["exchanges"] if e["type"] == "biosphere"]
    assert exchange["unit"] == "cubic meter" and exchange["amount"] == exchange["loc"] == 0
    assert exchange["scale"] == 0.0001


def test_zero_active_input_still_refuses_unknown_unit():
    source = SimaProInventory.from_data(
        [minimal_activity(extra_exchanges=[{**HEAT, **TRIANGLE, "unit": "unsupported synthetic unit"}])],
        background_profile=profile("3.12"),
    )
    result = source.render()
    assert result.has_errors and not result.rows


def test_zero_native_uvek_input_still_refuses_unsupported_compartment():
    source = SimaProInventory.from_data(
        [minimal_activity(extra_exchanges=[{**CARBON, **TRIANGLE, "categories": ("unsupported compartment",)}])],
        background_profile=BackgroundProfile("uvek", "2026", "cutoff"),
    )
    result = source.render()
    assert result.has_errors and not result.rows
    assert any(issue.code == "simapro_native_compartment_unsupported" for issue in result.issues)
