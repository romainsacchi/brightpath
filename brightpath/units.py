"""Small unit-normalization helper used by migration code.

This intentionally mirrors the canonical aliases used by ``bw2io`` without
requiring the full Brightway I/O stack for in-memory conversions.
"""

UNIT_ALIASES = {
    "a": "year",
    "bq": "Becquerel",
    "g": "gram",
    "gj": "gigajoule",
    "h": "hour",
    "ha": "hectare",
    "hr": "hour",
    "item(s)": "unit",
    "kbq": "kilo Becquerel",
    "kg": "kilogram",
    "kgkm": "kilogram kilometer",
    "kg*day": "kilogram day",
    "km": "kilometer",
    "kj": "kilojoule",
    "kwh": "kilowatt hour",
    "l": "litre",
    "lu": "livestock unit",
    "m": "meter",
    "m*year": "meter-year",
    "m2": "square meter",
    "m2*year": "square meter-year",
    "m2a": "square meter-year",
    "m2*a": "square meter-year",
    "m2y": "square meter-year",
    "m3": "cubic meter",
    "m3*year": "cubic meter-year",
    "m3a": "cubic meter-year",
    "m3y": "cubic meter-year",
    "ma": "meter-year",
    "metric ton*km": "ton kilometer",
    "mj": "megajoule",
    "my": "meter-year",
    "nm3": "normal cubic meter",
    "sm3": "standard cubic meter",
    "p": "unit",
    "personkm": "person kilometer",
    "person*km": "person kilometer",
    "pkm": "person kilometer",
    "tonnes": "ton",
    "t": "ton",
    "tkm": "ton kilometer",
    "t*km": "ton kilometer",
    "vkm": "vehicle kilometer",
    "kg sw": "kilogram separative work unit",
    "km*year": "kilometer-year",
    "wh": "watt hour",
}


def normalize_unit(value):
    """Return Brightway's canonical unit alias while preserving unknown values."""

    return UNIT_ALIASES.get(value.lower() if isinstance(value, str) else value, value)
