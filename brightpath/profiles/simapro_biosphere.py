"""Version-independent SimaPro representations of ecoinvent flow families.

Evidence is documented in docs/simapro-reference-evidence.rst. These are format
rules, not migrations or claims about target LCIA characterization.
"""

from collections.abc import Mapping

from brightpath.units import normalize_unit

FINAL_WASTE_NAMES = frozenset({"Waste mass, total, placed in landfill", "Organic carbon, placed in landfill"})
TURBINE_WATER = "Water, turbine use, unspecified natural origin"
SALT_WATER = "Water, salt, ocean"
_ARABLE_TRANSFORMATIONS = frozenset(
    {"Transformation, to annual crop", "Transformation, to arable land, unspecified use"}
)
# Observed as distinct native Resources/land rows in the licensed SimaPro 9.5
# reference; see docs/simapro-reference-evidence.rst. Legacy aliases collapse
# mining into industrial land and annual into permanent crops.
_NATIVE_LAND_FLOW_UNITS = {
    "Occupation, mineral extraction site": "square meter-year",
    "Transformation, to mineral extraction site": "square meter",
    "Transformation, from annual crop": "square meter",
    "Occupation, annual crop, irrigated, intensive": "square meter-year",
    "Occupation, annual crop, non-irrigated, intensive": "square meter-year",
    **{name: "square meter" for name in _ARABLE_TRANSFORMATIONS},
}
# Native SimaPro stores these radionuclides in kBq, not kg or Bq.
_RADIONUCLIDES = frozenset(
    {
        "Radon-222",
        "Radon-220",
        "Carbon-14",
        "Xenon-133",
        "Xenon-135",
        "Noble gases, radioactive, unspecified",
        "Hydrogen-3, Tritium",
    }
)
REVIEWED_ECOINVENT_FLOW_UNITS = {
    **_NATIVE_LAND_FLOW_UNITS,
    "Oxygen": "kilogram",
    "Occupation, traffic area, road network": "square meter-year",
    TURBINE_WATER: "cubic meter",
    SALT_WATER: "cubic meter",
    "Volume occupied, reservoir": "cubic meter-year",
    "Energy, gross calorific value, in biomass, primary forest": "megajoule",
    **{name: "kilo Becquerel" for name in _RADIONUCLIDES},
}


def resolve_ecoinvent_flow_name(exchange: Mapping, mapped_name: str) -> str:
    """Retain distinct water and land identities when legacy labels collapse them."""
    if (
        exchange.get("name") in _NATIVE_LAND_FLOW_UNITS
        and tuple(exchange.get("categories") or ()) == ("natural resource", "land")
        and normalize_unit(exchange.get("unit", "")) == _NATIVE_LAND_FLOW_UNITS[exchange["name"]]
    ):
        # Full native labels retain the land class where a legacy alias would
        # collapse distinct catalog identities. This is a format rule, not an
        # assumption that different land classes have equal impacts.
        return exchange["name"]
    if (
        exchange.get("name") == SALT_WATER
        and (exchange.get("categories") or (None,))[0] == "natural resource"
        and normalize_unit(exchange.get("unit", "")) == "cubic meter"
    ):
        return SALT_WATER
    return mapped_name


def validate_ecoinvent_flow(exchange: Mapping) -> None:
    """Reject incompatible units instead of silently losing or rescaling a flow."""
    name = exchange.get("name", "")
    base_name = TURBINE_WATER if str(name).startswith(TURBINE_WATER + ", ") else name
    expected = REVIEWED_ECOINVENT_FLOW_UNITS.get(base_name)
    if expected and normalize_unit(exchange.get("unit", "")) != expected:
        raise ValueError(f"SimaPro flow {name!r} requires {expected!r}; got {exchange.get('unit')!r}.")
    if (exchange.get("categories") or (None,))[0] == "inventory indicator":
        native = exchange.get("simapro section") == "Final waste flows"
        if name not in FINAL_WASTE_NAMES and not native:
            raise ValueError(
                f"No reviewed SimaPro representation for inventory indicator {name!r}. "
                "Supply a reviewed mapping; the exchange cannot be silently omitted."
            )
        if name in FINAL_WASTE_NAMES and normalize_unit(exchange.get("unit", "")) != "kilogram":
            raise ValueError(f"SimaPro final-waste indicator {name!r} requires kilograms.")


def restore_final_waste_flow(exchange: dict) -> None:
    """Preserve a native final-waste row as a canonical inventory indicator."""
    categories = exchange.get("categories") or ()
    subcompartment = categories[1] if isinstance(categories, (tuple, list)) and len(categories) > 1 else ""
    exchange.update(
        type="biosphere",
        categories=("inventory indicator", "waste"),
        **{"simapro section": "Final waste flows", "simapro subcompartment": subcompartment},
    )
    exchange.pop("input", None)
