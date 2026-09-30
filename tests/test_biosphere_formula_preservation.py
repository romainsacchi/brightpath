from copy import deepcopy

import pytest


@pytest.mark.parametrize("expression", ["dose", "shared * efficiency", "", None])
def test_biosphere_target_never_treats_chemical_metadata_as_an_amount_formula(
    expression,
):
    from brightpath.migrations.engine import _apply_biosphere_target

    exchange = {
        "type": "biosphere",
        "name": "Old elementary flow",
        "categories": ["air"],
        "unit": "kilogram",
        "amount": 12.0,
        "uncertainty type": 3,
        "loc": 12.0,
        "scale": 0.2,
        "input": ("old-biosphere", "old-flow"),
    }
    if expression is not None:
        exchange["formula"] = expression
    before = deepcopy(exchange)
    target = {
        "name": "Carbon dioxide, fossil",
        "unit": "kg",
        "uuid": "new-flow",
        "formula": "CO2",
    }
    before_target = deepcopy(target)
    _apply_biosphere_target(exchange, target)
    for key in ("formula", "amount", "uncertainty type", "loc", "scale"):
        assert exchange.get(key) == before.get(key)
    assert ("formula" in exchange) == ("formula" in before)
    assert exchange["name"] == "Carbon dioxide, fossil"
    assert exchange["unit"] == "kilogram"
    assert exchange["uuid"] == "new-flow"
    assert "input" not in exchange
    assert target == before_target
