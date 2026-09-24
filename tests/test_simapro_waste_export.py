"""Check waste orientation with linked matrices and an independent CSV parser."""

import math
from copy import deepcopy

import numpy as np
import pytest
from bw2io.extractors.simapro_csv import SimaProCSVExtractor

from brightpath import BackgroundProfile, SimaProInventory


def process(name, category="material/Test", production=1):
    return {
        "name": name,
        "reference product": name + " product",
        "unit": "kilogram",
        "location": "GLO",
        "type": "process",
        "comment": "Synthetic waste test",
        "exchanges": [
            {
                "name": name,
                "reference product": name + " product",
                "location": "GLO",
                "unit": "kilogram",
                "type": "production",
                "amount": production,
                "simapro category": category,
            }
        ],
    }


def link(supplier, amount, **kwargs):
    return {
        "name": supplier["name"],
        "reference product": supplier["reference product"],
        "location": supplier["location"],
        "unit": supplier["unit"],
        "type": "technosphere",
        "amount": amount,
        **kwargs,
    }


def render(data, model="cutoff"):
    source = SimaProInventory.from_data(data, background_profile=BackgroundProfile("ecoinvent", "3.12", model))
    result = source.render()
    assert not result.has_errors, result.issues
    return source, result.rows


def section_rows(rows, section):
    active = False
    result = []
    for row in rows:
        if row == [section]:
            active = True
        elif not row:
            active = False
        elif active:
            result.append(row)
    return result


@pytest.mark.parametrize("model", ["cutoff", "consequential"])
@pytest.mark.parametrize("amount", [-0.5, 0.5])
@pytest.mark.parametrize("kind", [0, 2, 3, 4, 5])
def test_waste_signs_and_distributions_follow_category_not_name(tmp_path, model, amount, kind):
    # Deliberately neutral name and generic Brightway process type.
    waste = process("disposal service", "waste treatment/Test", -2)
    consumer = process("consumer")
    lower, upper = (amount - 0.1, amount + 0.1)
    consumer["exchanges"].append(
        link(
            waste,
            amount,
            **{
                "uncertainty type": kind,
                "scale": 0.2,
                "loc": math.log(abs(amount)) if kind == 2 else amount,
                "minimum": lower,
                "maximum": upper,
            },
        )
    )
    original = deepcopy([consumer, waste])
    source, rows = render([consumer, waste], model)
    row = section_rows(rows, "Waste to treatment")[0]
    parsed = SimaProCSVExtractor.create_distribution(*row[2:7])
    assert parsed["amount"] == -amount
    if kind in (2, 3):
        assert parsed["scale"] == pytest.approx(0.2)
    if kind == 2:
        assert parsed["loc"] == pytest.approx(math.log(abs(amount)))
        assert parsed["negative"] == (-amount < 0)
    if kind in (4, 5):
        assert parsed["minimum"] == pytest.approx(-upper)
        assert parsed["maximum"] == pytest.approx(-lower)
    if kind in (3, 5):
        assert parsed["loc"] == -amount
    assert section_rows(rows, "Waste treatment")[0][2] == "2"
    assert source.data == original
    assert [consumer, waste] == original
    path = source.write_csv(tmp_path / "uncertain-waste.csv", validate=False)
    restored = SimaProInventory.from_csv(path, background_profile=source.background_profile)
    restored_consumer = next(ds for ds in restored.data if ds["name"] == "consumer")
    restored_link = next(exc for exc in restored_consumer["exchanges"] if exc["type"] == "technosphere")
    assert restored_link["amount"] == amount
    if kind in (2, 3):
        assert restored_link["scale"] == pytest.approx(0.2)
    if kind == 2:
        assert restored_link["negative"] == (amount < 0)
        assert restored_link["loc"] == pytest.approx(math.log(abs(amount)))
    if kind in (4, 5):
        assert restored_link["minimum"] == pytest.approx(lower)
        assert restored_link["maximum"] == pytest.approx(upper)
    if kind in (3, 5):
        assert restored_link["loc"] == amount


def matrix(data):
    names = sorted(ds["name"] for ds in data)
    index = {name: i for i, name in enumerate(names)}
    result = np.zeros((len(data), len(data)))
    for ds in data:
        for exc in ds["exchanges"]:
            if exc["type"] in {"production", "technosphere"}:
                result[index[exc["name"]], index[ds["name"]]] += exc["amount"] * (
                    1 if exc["type"] == "production" else -1
                )
    return names, result


@pytest.mark.parametrize("model", ["cutoff", "consequential"])
def test_linked_inventory_round_trip_preserves_matrix_and_supply(tmp_path, model):
    consumer = process("consumer")
    waste = process("disposal service", "waste treatment/Test", -2)
    # A treatment-like name must not override the included supplier's material category.
    material = process("treatment of ordinary material", production=3)
    consumer["exchanges"] += [link(waste, -0.6), link(waste, 0.2)]
    waste["exchanges"] += [link(material, 3), link(material, -0.5)]
    data = [consumer, waste, material]
    source, rows = render(data, model)
    ordinary = section_rows(rows, "Materials/fuels")
    assert [float(row[2]) for row in ordinary] == [3, -0.5]
    path = source.write_csv(tmp_path / "linked.csv", validate=False)
    loaded = SimaProInventory.from_csv(path, background_profile=source.background_profile)
    names, original = matrix(data)
    loaded_names, restored = matrix(loaded.data)
    assert loaded_names == names
    np.testing.assert_allclose(restored, original, atol=0, rtol=1e-14)
    demand = np.array([1 if name == "consumer" else 0 for name in names])
    expected = np.linalg.solve(original, demand)
    np.testing.assert_allclose(np.linalg.solve(restored, demand), expected)
    # Independently check the physical service demand and credited material demand.
    assert expected[names.index("disposal service")] == pytest.approx(0.2)
    assert expected[names.index("treatment of ordinary material")] == pytest.approx(1 / 6)


def test_conflicting_included_supplier_categories_fail():
    supplier = process("supplier")
    conflict = deepcopy(supplier)
    conflict["exchanges"][0]["simapro category"] = "waste treatment/Test"
    source = SimaProInventory.from_data(
        [supplier, conflict], background_profile=BackgroundProfile("ecoinvent", "3.12", "cutoff")
    )
    result = source.render()
    assert result.has_errors
    assert any("Conflicting SimaPro supplier categories" in issue.message for issue in result.issues)


def test_external_supplier_explicit_category_overrides_name_heuristic():
    consumer = process("consumer")
    external = process("external service")
    consumer["exchanges"].append(link(external, -0.5, **{"simapro category": "waste treatment/Test"}))
    _, rows = render([consumer])
    assert section_rows(rows, "Waste to treatment")[0][2] == "0.5"


@pytest.mark.parametrize("fields", [{"uncertainty type": 6}, {"formula": "waste_parameter"}])
def test_unsupported_waste_reflection_fails_explicitly(fields):
    waste = process("disposal service", "waste treatment/Test", -1)
    consumer = process("consumer")
    consumer["exchanges"].append(link(waste, -0.5, **fields))
    source = SimaProInventory.from_data(
        [consumer, waste], background_profile=BackgroundProfile("ecoinvent", "3.12", "cutoff")
    )
    assert source.render().has_errors


@pytest.mark.parametrize(
    "profile",
    [
        BackgroundProfile("ecoinvent", "3.12", "cutoff"),
        BackgroundProfile("ecoinvent", "3.12", "consequential"),
        BackgroundProfile("uvek", "2025", "cutoff"),
    ],
)
@pytest.mark.parametrize("amount", [-2.0, -1.0, 1.0, 2.0])
def test_waste_reference_is_positive_and_supplier_link_is_sign_flipped(profile, amount):
    waste = process("disposal service", "waste treatment/Test", amount)
    consumer = process("consumer")
    material = process("ordinary material")
    consumer["exchanges"].append(
        link(
            waste,
            amount,
            **{
                "uncertainty type": 5,
                "loc": amount,
                "minimum": amount - 0.5,
                "maximum": amount + 0.5,
            },
        )
    )
    waste["exchanges"].append(link(material, -0.25))
    source = SimaProInventory.from_data([consumer, waste, material], background_profile=profile)
    original = source.data
    result = source.render()
    assert not result.has_errors, result.issues
    assert float(section_rows(result.rows, "Waste treatment")[0][2]) == abs(amount)
    row = section_rows(result.rows, "Waste to treatment")[0]
    parsed = SimaProCSVExtractor.create_distribution(*row[2:7])
    assert parsed["amount"] == -amount
    assert parsed["minimum"] == -amount - 0.5
    assert parsed["maximum"] == -amount + 0.5
    assert float(section_rows(result.rows, "Materials/fuels")[0][2]) == -0.25
    assert source.data == original
