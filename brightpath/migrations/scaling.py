"""Consistent exchange quantity, uncertainty and formula scaling."""

from __future__ import annotations

import math


def _finite_factor(value):
    if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value) or value == 0:
        raise ValueError("Conversion factor must be finite and nonzero.")
    return float(value)


def scale_exchange(exchange, factor):
    """Scale supported uncertainties and expressions together with their amount."""
    factor = _finite_factor(factor)
    if factor == 1:
        return
    uncertainty = exchange.get("uncertainty type", 0)
    if uncertainty not in (0, 1, 2, 3, 4):
        raise ValueError(f"Cannot safely scale uncertainty type {uncertainty}.")
    amount = float(exchange["amount"])
    if not math.isfinite(amount):
        raise ValueError("Cannot scale a non-finite amount.")
    exchange["amount"] = amount * factor
    if "loc" in exchange:
        exchange["loc"] = (
            float(exchange["loc"]) + math.log(abs(factor)) if uncertainty == 2 else float(exchange["loc"]) * factor
        )
    if uncertainty == 2:
        exchange["negative"] = bool(exchange.get("negative", amount < 0)) != (factor < 0)
    elif "negative" in exchange:
        exchange["negative"] = exchange["amount"] < 0
    if uncertainty == 3 and "scale" in exchange:
        exchange["scale"] = float(exchange["scale"]) * abs(factor)
    bounds = {field: float(exchange[field]) * factor for field in ("minimum", "maximum") if field in exchange}
    for field in ("minimum", "maximum"):
        exchange.pop(field, None)
    for field, value in bounds.items():
        target = {"minimum": "maximum", "maximum": "minimum"}[field] if factor < 0 else field
        exchange[target] = value
    if exchange.get("formula"):
        exchange["formula"] = f"({exchange['formula']}) * ({factor!r})"
