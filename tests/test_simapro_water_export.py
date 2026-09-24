"""Quantity and distribution preservation at the SimaPro water boundary."""

import math
from copy import deepcopy

import pytest
from bw2io.extractors.simapro_csv import SimaProCSVExtractor

from brightpath import BackgroundProfile, SimaProInventory


def inventory(exchange):
    return SimaProInventory.from_data(
        [
            {
                "name": "water test",
                "reference product": "test product",
                "location": "GLO",
                "unit": "kilogram",
                "comment": "Synthetic water conversion test",
                "exchanges": [
                    {
                        "name": "water test",
                        "reference product": "test product",
                        "location": "GLO",
                        "type": "production",
                        "unit": "kilogram",
                        "amount": 1,
                        "simapro category": "material/Test",
                    },
                    exchange,
                ],
            }
        ],
        background_profile=BackgroundProfile("ecoinvent", "3.12", "cutoff"),
    )


def water(**kwargs):
    return {
        "name": "Water",
        "categories": ("air", "unspecified"),
        "type": "biosphere",
        "unit": "cubic meter",
        "amount": 0.002,
        **kwargs,
    }


def water_row(result):
    assert not result.has_errors, result.issues
    return next(row for row in result.rows if len(row) == 9 and row[0] == "Water")


@pytest.mark.parametrize(
    "kind,parameters",
    [
        (0, {}),
        (1, {}),
        (2, {"loc": math.log(0.002), "scale": 0.2}),
        (3, {"loc": 0.002, "scale": 0.0001}),
        (4, {"minimum": 0.001, "maximum": 0.003}),
        (5, {"loc": 0.002, "minimum": 0.001, "maximum": 0.003}),
    ],
)
@pytest.mark.parametrize("category", ["air", "water", "soil"])
def test_water_distribution_survives_unit_conversion(kind, parameters, category):
    exchange = water(categories=(category, "unspecified"), **{"uncertainty type": kind}, **parameters)
    original = deepcopy(exchange)
    source = inventory(exchange)
    before = source.data
    row = water_row(source.render())
    assert row[1:3] == ["", "kg"]
    parsed = SimaProCSVExtractor.create_distribution(*row[3:8])
    assert parsed["amount"] == pytest.approx(2)
    if kind == 2:
        assert parsed["loc"] == pytest.approx(math.log(2))
        assert parsed["scale"] == pytest.approx(0.2)
    elif kind == 3:
        assert parsed["loc"] == pytest.approx(2)
        assert parsed["scale"] == pytest.approx(0.1)
    elif kind in (4, 5):
        assert parsed["minimum"] == pytest.approx(1)
        assert parsed["maximum"] == pytest.approx(3)
    assert source.data == before
    assert exchange == original
    assert water_row(source.render()) == row


@pytest.mark.parametrize("kind", [2, 3, 4, 5])
def test_negative_water_keeps_signed_distribution(kind):
    row = water_row(
        inventory(
            water(
                amount=-0.002,
                **{
                    "uncertainty type": kind,
                    "loc": math.log(0.002) if kind == 2 else -0.002,
                    "scale": 0.2 if kind == 2 else 0.0001,
                    "minimum": -0.003,
                    "maximum": -0.001,
                },
            )
        ).render()
    )
    parsed = SimaProCSVExtractor.create_distribution(*row[3:8])
    assert parsed["amount"] == pytest.approx(-2)
    assert parsed["negative"]
    if kind in (4, 5):
        assert (parsed["minimum"], parsed["maximum"]) == pytest.approx((-3, -1))


def test_water_already_in_kilograms_is_not_converted_again():
    row = water_row(
        inventory(
            water(
                unit="kilogram",
                amount=2,
                **{
                    "uncertainty type": 3,
                    "scale": 0.1,
                },
            )
        ).render()
    )
    parsed = SimaProCSVExtractor.create_distribution(*row[3:8])
    assert parsed["amount"] == 2
    assert parsed["scale"] == pytest.approx(0.1)


def test_resource_water_is_not_scaled():
    row = water_row(inventory(water(categories=("natural resource", "in water"))).render())
    assert row[2:4] == ["m3", "0.002"]


def test_short_bound_aliases_are_scaled():
    row = water_row(inventory(water(**{"uncertainty type": 4, "min": 0.001, "max": 0.003})).render())
    assert row[6:8] == ["1", "3"]


@pytest.mark.parametrize(
    "fields,detail",
    [
        ({"unit": "megajoule"}, "Cannot convert Water"),
        ({"uncertainty type": 6}, "Unsupported Water emission uncertainty"),
        ({"formula": "water_parameter"}, "formulas cannot be preserved"),
        ({"categories": ("air", "unknown compartment")}, "No SimaPro subcompartment mapping"),
    ],
)
def test_unsupported_water_conversion_is_reported(fields, detail):
    result = inventory(water(**fields)).render()
    assert result.has_errors
    assert not result.rows
    assert any(detail in issue.message for issue in result.issues)
