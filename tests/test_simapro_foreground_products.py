"""Keep allocated foreground products distinct in actual SimaPro CSV rows."""

import csv
import math
from copy import deepcopy
from numbers import Real

import pytest
from preservation_helpers import identity

from brightpath import BackgroundProfile, SimaProInventory
from brightpath.formats.simapro_csv import _uvek_foreground_link_name
from brightpath.profiles import format_simapro_technosphere_name

PROFILE = BackgroundProfile("uvek", "2025", "cutoff")
FIELDS = ("name", "reference product", "location", "unit")


def triad(sign=1):
    data = []
    for i, (product, unit) in enumerate(
        (
            ("municipal solid waste", "kilogram"),
            ("electricity, medium voltage", "kilowatt hour"),
            ("heat, district or industrial, other than natural gas", "megajoule"),
        )
    ):
        fields = dict(zip(FIELDS, ("Synthetic allocated incineration", product, "RER", unit), strict=True))
        data.append(
            {
                **fields,
                "database": "private",
                "code": "product-" + str(i),
                "type": "process",
                "comment": "Private synthetic identity proof",
                "parameters": [{"name": "dose", "amount": 0.25}],
                "exchanges": [
                    {
                        **fields,
                        "type": "production",
                        "amount": sign if i == 0 else 1,
                        "simapro category": (
                            "waste treatment/Synthetic" if i == 0 and sign < 0 else "material/Synthetic"
                        ),
                    }
                ],
            }
        )
    for activity in data:
        for i, supplier in enumerate(data):
            edge = {**{k: supplier[k] for k in FIELDS}, "type": "technosphere", "amount": 0.25}
            if i == 1:
                edge["formula"] = "dose"
            elif i == 0:
                edge.update({"uncertainty type": 2, "loc": math.log(0.25), "scale": 0.2})
            else:
                edge.update({"uncertainty type": 3, "loc": 0.25, "scale": 0.03})
            activity["exchanges"].append(edge)
    return data


def render(data, profile=PROFILE):
    inv = SimaProInventory.from_data(data, background_profile=profile)
    return inv, inv.render()


def assert_preserved(expected, actual):
    assert identity(actual) == identity(expected)
    assert actual["parameters"] == expected["parameters"]
    remaining = list(actual["exchanges"])
    assert len(remaining) == len(expected["exchanges"])
    for edge in expected["exchanges"]:
        candidates = [row for row in remaining if row["type"] == edge["type"] and identity(row) == identity(edge)]
        assert len(candidates) == 1
        found = candidates[0]
        remaining.remove(found)
        assert found.get("formula", "") == edge.get("formula", "")
        for key in ("amount", "uncertainty type", "loc", "scale", "minimum", "maximum", "shape"):
            if edge.get(key) is not None:
                value = found.get(key)
                assert isinstance(value, Real) and not isinstance(value, bool), (key, value)
                assert math.isfinite(value) and math.isclose(value, edge[key], rel_tol=1e-12, abs_tol=1e-12), (
                    key,
                    value,
                    edge[key],
                )


@pytest.mark.parametrize("sign", [1, -1])
@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("version", ["2025", "2026"])
def test_actual_three_product_write_readback_and_all_links(tmp_path, sign, reverse, version):
    data = triad(sign)
    if reverse:
        data.reverse()
    original = deepcopy(data)
    profile = BackgroundProfile("uvek", version, "cutoff")
    inv, result = render(data, profile)
    assert not result.has_errors, result.issues
    path = inv.write_csv(tmp_path / "three-products.csv")
    loaded = SimaProInventory.from_csv(path, background_profile=profile).to_brightway().data
    mapped = {identity(row): row for row in loaded}
    assert len(mapped) == 3
    for row in data:
        assert_preserved(row, mapped[identity(row)])
    assert data == original and inv.data == original
    with path.open(encoding="latin-1", newline="") as f:
        rows = list(csv.reader(f, delimiter=";"))
    labels = {_uvek_foreground_link_name({k: row[k] for k in FIELDS}) for row in data}
    assert len(labels) == 3
    inputs = []
    outputs = []
    active = None
    for row in rows:
        if row == ["Products"] or row == ["Waste treatment"]:
            active = "output"
        elif row == ["Materials/fuels"] or row == ["Electricity/heat"] or row == ["Waste to treatment"]:
            active = "input"
        elif not row:
            active = None
        elif active == "input":
            inputs.append(row)
        elif active == "output":
            outputs.append(row)
    assert len(outputs) == 3 and len(inputs) == 9
    native = {r[0]: r[1] for r in outputs}
    assert set(native) == labels and set(native.values()) == {"kg", "kWh", "MJ"}
    assert all(native[row[0]] == row[1] for row in inputs)


def test_external_background_label_is_literal_and_unchanged():
    data = triad()
    background = {
        "name": "Electricity, low voltage, at grid",
        "reference product": "Electricity, low voltage, at grid",
        "location": "CH",
        "unit": "kilowatt hour",
        "type": "technosphere",
        "amount": 2,
    }
    data[0]["exchanges"].append(background)
    before = deepcopy(data)
    _, result = render(data)
    assert not result.has_errors, result.issues
    literal = format_simapro_technosphere_name(
        **dict(
            name=background["name"],
            reference_product=background["reference product"],
            location="CH",
            unit="kilowatt hour",
            profile=PROFILE,
        )
    )
    assert any(row[:3] == [literal, "kWh", "2"] for row in result.rows)
    assert data == before


@pytest.mark.parametrize(
    "mutation", ["duplicate", "same_product_new_unit", "case_collision", "unicode_collision", "background_conflict"]
)
def test_real_ambiguities_remain_refused(mutation):
    data = triad()
    if mutation == "duplicate":
        duplicate = deepcopy(data[0])
        duplicate["code"] = "other-code"
        data.append(duplicate)
    elif mutation == "same_product_new_unit":
        data[1]["reference product"] = data[0]["reference product"]
        data[1]["exchanges"][0]["reference product"] = data[0]["reference product"]
    elif mutation in {"case_collision", "unicode_collision"}:
        data[0]["name"] = "Process A" if mutation == "case_collision" else "Process α"
        data[1]["name"] = "process a" if mutation == "case_collision" else "Process β"
        for row in data:
            row["exchanges"] = [row["exchanges"][0]]
            row["exchanges"][0]["name"] = row["name"]
    else:
        data[0]["exchanges"].append(
            {
                **{k: data[0][k] for k in FIELDS},
                "reference product": "different background product",
                "type": "technosphere",
                "amount": 1,
            }
        )
    original = deepcopy(data)
    _, result = render(data)
    assert result.has_errors and not result.rows
    assert any(
        "Ambiguous foreground" in issue.message or "supplier conflicts" in issue.message for issue in result.issues
    )
    assert data == original


def test_generated_alias_cannot_collide_with_external_supplier():
    data = triad()
    label = _uvek_foreground_link_name({k: data[0][k] for k in FIELDS})
    data[1]["exchanges"].append(
        {
            "name": label.rsplit("/", 1)[0],
            "reference product": "external product",
            "location": "RER",
            "unit": "kilogram",
            "type": "technosphere",
            "amount": 1,
        }
    )
    _, result = render(data)
    assert result.has_errors and any("alias collides" in issue.message for issue in result.issues)


def test_normal_volume_uses_literal_unit_without_conversion():
    data = triad()
    data[0]["exchanges"].append(
        {
            "name": "Natural gas, at production",
            "reference product": "Natural gas, at production",
            "location": "AE",
            "unit": "normal cubic meter",
            "type": "technosphere",
            "amount": 1,
        }
    )
    original = deepcopy(data)
    _, result = render(data, BackgroundProfile("uvek", "2026", "cutoff"))
    assert not result.has_errors, result.issues
    gas = [row for row in result.rows if len(row) == 8 and row[1] == "Nm3"]
    assert len(gas) == 1 and float(gas[0][2]) == 1
    assert data == original


def test_ecoinvent_does_not_gain_uvek_foreground_aliases():
    _, result = render(triad(), BackgroundProfile("ecoinvent", "3.12", "cutoff"))
    assert not result.has_errors, result.issues
    assert not any("[BrightPath fg " in str(cell) for row in result.rows for cell in row)
