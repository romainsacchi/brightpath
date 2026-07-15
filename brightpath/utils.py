import csv
import json
import logging
import re
import tempfile
from dataclasses import dataclass
from numbers import Real
from pathlib import Path
from typing import Any
from typing import Optional as TypingOptional

import numpy as np
import yaml

from . import DATA_DIR

logger = logging.getLogger(__name__)

ALLOWED_EXCHANGE_TYPES = {"production", "technosphere", "biosphere"}
ALLOWED_BIOSPHERE_CATEGORIES = {"natural resource", "air", "water", "soil"}
DATASET_UNIT_ALIASES = {
    "kg": "kilogram",
    "kwh": "kilowatt hour",
    "kw": "kilowatt",
    "mj": "megajoule",
    "km": "kilometer",
    "m": "meter",
    "m2": "square meter",
    "m3": "cubic meter",
    "h": "hour",
    "pkm": "person-kilometer",
    "vkm": "vehicle-kilometer",
    "tkm": "ton kilometer",
    "l": "liter",
    "ha": "hectare",
    "person kilometer": "person-kilometer",
    "vehicle kilometer": "vehicle-kilometer",
    "square meter year": "square meter-year",
    "meter year": "meter-year",
}
TRANSFORMATION_FROM_PATTERN = re.compile(r"\btransformation\b[\s,]+from\b", re.IGNORECASE)
TRANSFORMATION_TO_PATTERN = re.compile(r"\btransformation\b[\s,]+to\b", re.IGNORECASE)
COMBUSTION_ACTIVITY_HINTS = (
    "boiler",
    "burn",
    "cogeneration",
    "co-generation",
    "combustion",
    "engine",
    "furnace",
    "heat production",
    "heater",
    "incineration",
    "kiln",
    "power plant",
    "steam production",
    "thermal energy",
    "turbine",
)


@dataclass(frozen=True)
class FossilFuelHeuristic:
    label: str
    patterns: tuple[str, ...]
    factors_by_unit: dict[str, float]


FOSSIL_FUEL_HEURISTICS = (
    FossilFuelHeuristic(
        label="natural gas",
        patterns=("natural gas",),
        factors_by_unit={
            "cubic meter": 1.96,
            "kilogram": 2.75,
            "kilowatt hour": 0.202,
            "megajoule": 0.0561,
        },
    ),
    FossilFuelHeuristic(
        label="diesel",
        patterns=("diesel",),
        factors_by_unit={
            "kilogram": 3.15,
            "liter": 2.68,
            "megajoule": 0.074,
        },
    ),
    FossilFuelHeuristic(
        label="gasoline",
        patterns=("gasoline", "petrol"),
        factors_by_unit={
            "kilogram": 3.09,
            "liter": 2.31,
            "megajoule": 0.069,
        },
    ),
    FossilFuelHeuristic(
        label="light fuel oil",
        patterns=("light fuel oil",),
        factors_by_unit={
            "kilogram": 3.17,
            "megajoule": 0.074,
        },
    ),
    FossilFuelHeuristic(
        label="heavy fuel oil",
        patterns=("heavy fuel oil",),
        factors_by_unit={
            "kilogram": 3.11,
            "megajoule": 0.077,
        },
    ),
    FossilFuelHeuristic(
        label="hard coal",
        patterns=("hard coal",),
        factors_by_unit={
            "kilogram": 2.42,
            "megajoule": 0.094,
        },
    ),
    FossilFuelHeuristic(
        label="liquefied petroleum gas",
        patterns=("liquefied petroleum gas", "lpg"),
        factors_by_unit={
            "kilogram": 3.00,
            "megajoule": 0.064,
        },
    ),
)


def _load_yaml_file(filepath: Path, description: str):
    if not filepath.is_file():
        raise FileNotFoundError(f"{description} could not be found at {filepath}.")

    try:
        with open(filepath, "r", encoding="utf-8") as stream:
            data = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        raise ValueError(f"Could not parse {description} at {filepath}.") from exc

    if data is None:
        raise ValueError(f"{description} at {filepath} is empty.")

    return data


def escape_spreadsheet_formula(value):
    """
    Escape text values that spreadsheet programs can interpret as formulas.
    Numeric negative values are left untouched so SimaPro amounts stay numeric.
    """
    if not isinstance(value, str):
        return value

    stripped = value.lstrip()
    if not stripped:
        return value

    first = stripped[0]
    if first in ("=", "+", "@"):
        return f"'{value}"

    if first == "-":
        try:
            float(stripped)
        except ValueError:
            return f"'{value}"

    return value


def get_simapro_biosphere() -> dict[str, str]:
    # Load the matching dictionary between ecoinvent and Simapro biosphere flows
    # for each ecoinvent biosphere flow name, it gives the corresponding Simapro name

    filename = "simapro-biosphere.json"
    filepath = DATA_DIR / "export" / filename
    if not filepath.is_file():
        raise FileNotFoundError(
            "The dictionary of biosphere flow match " "between ecoinvent and Simapro could not be found."
        )
    with open(filepath, encoding="utf-8") as json_file:
        data = json.load(json_file)
    dict_bio = {}
    for d in data:
        dict_bio[d[2]] = d[1]

    return dict_bio


def get_simapro_subcompartments() -> dict[str, str]:
    # Load the matching dictionary between ecoinvent and Simapro subcompartments
    # contained in simapro_subcompartments.yaml

    filename = "simapro_subcompartments.yaml"
    filepath = DATA_DIR / "export" / filename
    return _load_yaml_file(
        filepath,
        "The dictionary of subcompartments match between ecoinvent and Simapro",
    )


def get_simapro_ecoinvent_blacklist():
    # Load the list of Simapro biosphere flows that
    # should be excluded from the export

    filename = "simapro_blacklist.yaml"
    filepath = DATA_DIR / "export" / filename
    return _load_yaml_file(filepath, "The SimaPro ecoinvent blacklist")


simapro_ecoinvent_blacklist = get_simapro_ecoinvent_blacklist()


def get_simapro_uvek_blacklist():
    # Load the list of Simapro uvek flows that
    # should be excluded from the export

    filename = "uvek_blacklist.yaml"
    filepath = DATA_DIR / "export" / filename
    return _load_yaml_file(filepath, "The SimaPro UVEK blacklist")


simapro_uvek_blacklist = get_simapro_uvek_blacklist()


def get_simapro_fields_list() -> list[str]:
    """
    Load the list of Simapro fields that
    should be included in the export.
    :return: list of Simapro fields
    """
    # Load the list of Simapro fields that
    # should be included in the export

    filename = "simapro_fields.yaml"
    filepath = DATA_DIR / "export" / filename

    return _load_yaml_file(filepath, "The SimaPro fields list")


def get_simapro_units():
    """
    Load the list of Simapro fields that
    should be included in the export.
    :return: list of Simapro fields
    """
    # Load the list of Simapro fields that
    # should be included in the export

    filename = "simapro_units.yaml"
    filepath = DATA_DIR / "export" / filename

    return _load_yaml_file(filepath, "The SimaPro units list")


def get_biosphere_units() -> set[str]:
    """
    Load the units used by the bundled biosphere flow reference table.
    """

    filepath = DATA_DIR / "export" / "flows_biosphere_39.csv"
    if not filepath.is_file():
        raise FileNotFoundError(f"Biosphere flow reference table could not be found at {filepath}.")

    units: set[str] = set()
    with open(filepath, encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle, delimiter=";")
        for row in reader:
            if len(row) < 4:
                continue
            unit = (row[3] or "").strip()
            if unit:
                units.add(unit)
    return units


def get_simapro_headers():
    """
    Load the list of Simapro fields that
    should be included in the export.
    :return: list of Simapro fields
    """
    # Load the list of Simapro fields that
    # should be included in the export

    filename = "simapro_headers.yaml"
    filepath = DATA_DIR / "export" / filename

    return _load_yaml_file(filepath, "The SimaPro headers list")


def get_simapro_ecoinvent_exceptions():
    """
    Load the YAML file "simapro_ei_exceptions.yaml"
    and return it as a dictionary.
    :return:
    """

    filename = "simapro_ei_exceptions.yaml"
    filepath = DATA_DIR / "export" / filename

    return _load_yaml_file(filepath, "The SimaPro ecoinvent exceptions")


ecoinvent_exceptions = get_simapro_ecoinvent_exceptions()


def get_waste_exchange_names():
    """
    Load the list of names that
    indicate that the input is a
    waste treatment.
    :return: list of name
    """

    filename = "waste_exchange_names.yaml"
    filepath = DATA_DIR / "export" / filename

    return _load_yaml_file(filepath, "The waste exchange names list")


def _context(activity_index: int, exchange_index: int = None) -> str:
    if exchange_index is None:
        return f"activity[{activity_index}]"
    return f"activity[{activity_index}].exchanges[{exchange_index}]"


def _is_number(value) -> bool:
    return isinstance(value, Real) and not isinstance(value, bool)


def _has_text(value) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _normalize_dataset_unit(value: str) -> str:
    normalized = (value or "").strip()
    if not normalized:
        return ""
    return DATASET_UNIT_ALIASES.get(normalized.lower(), normalized.lower())


def _iter_activity_exchanges(activity: dict, *, exchange_type: str = None):
    for exchange in activity.get("exchanges", []):
        if not isinstance(exchange, dict):
            continue
        if exchange_type is not None and exchange.get("type") != exchange_type:
            continue
        yield exchange


def _exchange_categories(exchange: dict) -> tuple[str, ...]:
    categories = exchange.get("categories")
    if isinstance(categories, (list, tuple)):
        return tuple(str(item).strip().lower() for item in categories if str(item).strip())
    if isinstance(categories, str):
        if "::" in categories:
            return tuple(item.strip().lower() for item in categories.split("::") if item.strip())
        normalized = categories.strip().lower()
        return (normalized,) if normalized else ()
    return ()


def _exchange_label(exchange: dict) -> str:
    parts = [str(exchange.get("name") or "").strip()]
    reference_product = str(exchange.get("reference product") or "").strip()
    location = str(exchange.get("location") or "").strip()
    unit = str(exchange.get("unit") or "").strip()
    if reference_product:
        parts.append(reference_product)
    if location:
        parts.append(location)
    if unit:
        parts.append(unit)
    return " | ".join(part for part in parts if part)


def _format_examples(values: list[str], *, limit: int = 3) -> str:
    if not values:
        return ""
    shown = values[:limit]
    suffix = f"; +{len(values) - limit} more" if len(values) > limit else ""
    return "; ".join(shown) + suffix


def _numeric_amount(value: Any) -> TypingOptional[float]:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        normalized = str(value).strip()
        if not normalized:
            return None
        return float(normalized)
    except (TypeError, ValueError):
        return None


def _match_fossil_fuel_heuristic(exchange: dict) -> TypingOptional[FossilFuelHeuristic]:
    reference_product = str(exchange.get("reference product") or "").strip().lower()
    name = str(exchange.get("name") or "").strip().lower()
    for heuristic in FOSSIL_FUEL_HEURISTICS:
        if any(pattern in reference_product for pattern in heuristic.patterns):
            return heuristic
        if any(
            name.startswith(candidate)
            for pattern in heuristic.patterns
            for candidate in (
                pattern,
                f"market for {pattern}",
                f"market group for {pattern}",
                f"supply of {pattern}",
                f"{pattern} production",
            )
        ):
            return heuristic
    return None


def _water_balance_warning(activity_ctx: str, activity: dict) -> TypingOptional[str]:
    water_intakes: list[str] = []
    water_releases: list[str] = []
    for exchange in _iter_activity_exchanges(activity, exchange_type="biosphere"):
        name_lower = str(exchange.get("name") or "").strip().lower()
        categories = _exchange_categories(exchange)
        if "water" not in name_lower and not any("water" in category for category in categories):
            continue
        if categories[:1] == ("natural resource",):
            water_intakes.append(_exchange_label(exchange))
        elif categories[:1] == ("water",):
            water_releases.append(_exchange_label(exchange))

    if water_intakes and not water_releases:
        return (
            f"{activity_ctx}: water resource intake flows were detected, but no water release flows "
            f"were found in this dataset. Detected intakes: {_format_examples(water_intakes)}. "
            "Suggested fix: Check whether a water release, evaporation flow, or retained water flow "
            "is missing, or document why the water balance is intentionally one-sided."
        )
    return None


def _transformation_pair_warning(activity_ctx: str, activity: dict) -> TypingOptional[str]:
    transformation_from: list[str] = []
    transformation_to: list[str] = []
    for exchange in _iter_activity_exchanges(activity, exchange_type="biosphere"):
        name = str(exchange.get("name") or "").strip()
        if not name:
            continue
        if TRANSFORMATION_FROM_PATTERN.search(name):
            transformation_from.append(_exchange_label(exchange))
        if TRANSFORMATION_TO_PATTERN.search(name):
            transformation_to.append(_exchange_label(exchange))

    if transformation_from and not transformation_to:
        return (
            f"{activity_ctx}: land-use exchanges look incomplete: 'transformation from' was found "
            f"without any matching 'transformation to' exchange. Detected flows: "
            f"{_format_examples(transformation_from)}. Suggested fix: Check whether the "
            "corresponding 'transformation to' flow is missing, or document why this dataset "
            "intentionally records only one side of the land transformation."
        )
    if transformation_to and not transformation_from:
        return (
            f"{activity_ctx}: land-use exchanges look incomplete: 'transformation to' was found "
            f"without any matching 'transformation from' exchange. Detected flows: "
            f"{_format_examples(transformation_to)}. Suggested fix: Check whether the "
            "corresponding 'transformation from' flow is missing, or document why this dataset "
            "intentionally records only one side of the land transformation."
        )
    return None


def _fuel_co2_warning(activity_ctx: str, activity: dict) -> TypingOptional[str]:
    expected_fossil_co2 = 0.0
    detected_fuels: list[str] = []
    for exchange in _iter_activity_exchanges(activity, exchange_type="technosphere"):
        heuristic = _match_fossil_fuel_heuristic(exchange)
        if heuristic is None:
            continue
        amount = _numeric_amount(exchange.get("amount"))
        unit = _normalize_dataset_unit(str(exchange.get("unit") or ""))
        factor = heuristic.factors_by_unit.get(unit)
        if amount is None or amount <= 0 or factor is None:
            continue
        expected_fossil_co2 += amount * factor
        detected_fuels.append(f"{heuristic.label} via {_exchange_label(exchange)} ({amount:g} {unit})")

    if expected_fossil_co2 < 0.2:
        return None

    actual_fossil_co2 = 0.0
    for exchange in _iter_activity_exchanges(activity, exchange_type="biosphere"):
        name_lower = str(exchange.get("name") or "").strip().lower()
        if name_lower != "carbon dioxide, fossil":
            continue
        amount = _numeric_amount(exchange.get("amount"))
        if amount is not None:
            actual_fossil_co2 += amount

    activity_name = str(activity.get("name") or "").strip().lower()
    looks_combustion_like = any(hint in activity_name for hint in COMBUSTION_ACTIVITY_HINTS)
    if not looks_combustion_like and actual_fossil_co2 <= 0:
        return None

    gap = abs(actual_fossil_co2 - expected_fossil_co2)
    relative_gap = gap / max(expected_fossil_co2, 1e-9)
    if actual_fossil_co2 <= 0.05 or (gap > 0.25 and relative_gap > 0.35):
        return (
            f"{activity_ctx}: approximate combustion check: detected fossil fuel inputs suggest "
            f"about {expected_fossil_co2:.2f} kg of Carbon dioxide, fossil, but this dataset "
            f"reports {actual_fossil_co2:.2f} kg. Fuel inputs used in this heuristic: "
            f"{_format_examples(detected_fuels)}. Suggested fix: Check whether direct fossil CO2 "
            "emissions are missing, whether the fuel is used as feedstock rather than combusted, "
            "or whether the fuel quantity or unit conversion is incorrect."
        )
    return None


def inspect_brightway_inventory(
    data: list,
    *,
    require_simapro_category: bool = True,
    validate_units: bool = True,
) -> tuple[list[str], list[str]]:
    """
    Inspect Brightway-style inventories and return contextual error and warning messages.

    When ``require_simapro_category`` is ``True``, missing ``simapro category`` values on
    production exchanges are treated as blocking conversion errors. Set ``validate_units=False``
    for software-neutral validation where foreground units are checked through link compatibility
    instead of the legacy SimaPro-oriented unit whitelist.
    """
    errors: list[str] = []
    warnings: list[str] = []

    if not isinstance(data, list):
        raise ValueError("Inventory data must be a list of activity dictionaries.")

    known_units = {_normalize_dataset_unit(unit) for unit in get_simapro_units()}
    known_biosphere_units = {_normalize_dataset_unit(unit) for unit in get_biosphere_units()}
    required_activity_keys = (
        "name",
        "reference product",
        "location",
        "unit",
        "comment",
        "exchanges",
    )
    required_tech_keys = ("name", "reference product", "location", "unit", "amount")
    required_bio_keys = ("name", "categories", "unit", "amount")

    for activity_index, activity in enumerate(data):
        activity_ctx = _context(activity_index)
        if not isinstance(activity, dict):
            errors.append(f"{activity_ctx}: activity must be a dictionary.")
            continue

        for key in required_activity_keys:
            if key not in activity:
                errors.append(f"{activity_ctx}: missing required activity field `{key}`.")

        for key in ("name", "reference product", "location", "unit", "comment"):
            if key in activity and not _has_text(activity[key]):
                errors.append(f"{activity_ctx}: activity field `{key}` must be a non-empty string.")

        if (
            "unit" in activity
            and _has_text(activity["unit"])
            and validate_units
            and _normalize_dataset_unit(activity["unit"]) not in known_units
        ):
            errors.append(f"{activity_ctx}: unknown activity unit `{activity['unit']}`.")

        exchanges = activity.get("exchanges")
        if not isinstance(exchanges, list):
            errors.append(f"{activity_ctx}: `exchanges` must be a list.")
            continue

        production_count = 0
        production_with_category = False

        for exchange_index, exchange in enumerate(exchanges):
            exchange_ctx = _context(activity_index, exchange_index)
            if not isinstance(exchange, dict):
                errors.append(f"{exchange_ctx}: exchange must be a dictionary.")
                continue

            exchange_type = exchange.get("type")
            if exchange_type not in ALLOWED_EXCHANGE_TYPES:
                errors.append(
                    f"{exchange_ctx}: unsupported exchange type `{exchange_type}`; "
                    f"expected one of {sorted(ALLOWED_EXCHANGE_TYPES)}."
                )
                continue

            required_keys = required_bio_keys if exchange_type == "biosphere" else required_tech_keys
            for key in required_keys:
                if key not in exchange:
                    errors.append(f"{exchange_ctx}: missing required exchange field `{key}`.")

            if "amount" in exchange and not _is_number(exchange["amount"]):
                errors.append(f"{exchange_ctx}: `amount` must be a number.")

            if exchange_type in ("production", "technosphere"):
                if (
                    "unit" in exchange
                    and _has_text(exchange["unit"])
                    and validate_units
                    and _normalize_dataset_unit(exchange["unit"]) not in known_units
                ):
                    errors.append(f"{exchange_ctx}: unknown exchange unit `{exchange['unit']}`.")
                for key in ("name", "reference product", "location", "unit"):
                    if key in exchange and not _has_text(exchange[key]):
                        errors.append(f"{exchange_ctx}: exchange field `{key}` must be a non-empty string.")

            if exchange_type == "biosphere":
                if (
                    "unit" in exchange
                    and _has_text(exchange["unit"])
                    and validate_units
                    and _normalize_dataset_unit(exchange["unit"]) not in known_biosphere_units
                ):
                    errors.append(f"{exchange_ctx}: unknown exchange unit `{exchange['unit']}`.")
                if "name" in exchange and not _has_text(exchange["name"]):
                    errors.append(f"{exchange_ctx}: exchange field `name` must be a non-empty string.")

                categories = exchange.get("categories")
                if not categories:
                    errors.append(f"{exchange_ctx}: biosphere exchange is missing categories.")
                elif not isinstance(categories, (tuple, list)):
                    errors.append(f"{exchange_ctx}: biosphere categories must be a tuple or list.")
                elif categories[0] not in ALLOWED_BIOSPHERE_CATEGORIES:
                    errors.append(
                        f"{exchange_ctx}: unsupported biosphere category `{categories[0]}`; "
                        f"expected one of {sorted(ALLOWED_BIOSPHERE_CATEGORIES)}."
                    )

            if exchange_type == "production":
                production_count += 1
                if _has_text(exchange.get("simapro category")):
                    production_with_category = True

        if production_count != 1:
            errors.append(f"{activity_ctx}: expected exactly one production exchange, found {production_count}.")
        elif require_simapro_category and not production_with_category:
            message = f"{activity_ctx}: production exchange must define a non-empty `simapro category`."
            errors.append(message)

        for warning in (
            _water_balance_warning(activity_ctx, activity),
            _transformation_pair_warning(activity_ctx, activity),
            _fuel_co2_warning(activity_ctx, activity),
        ):
            if warning is not None:
                warnings.append(warning)

    return errors, warnings


def is_activity_waste_treatment(activity: dict, database: str) -> bool:
    """
    Detect whether the given activity is a
    process or a waste treatment.
    :param activity:
    :return: True or False
    """

    if "type" in activity:
        if activity["type"] == "process":
            return False
        if activity["type"] == "waste treatment":
            return True

    if is_a_waste_treatment(activity["name"], database) is True:
        return True

    return False


def is_a_waste_treatment(name: str, database: str) -> bool:
    """
    Detect if name contains typical to waste treatment.
    :param name: exchange name
    :param database: database to link to
    :return: bool.
    """
    WASTE_TERMS = get_waste_exchange_names()
    NOT_WASTE_TERMS = [
        # "plant",
        "incineration plant"
    ]

    if any(term.lower() in name.lower() for term in WASTE_TERMS) is True:
        if any(term.lower() in name.lower() for term in NOT_WASTE_TERMS) is False:
            if database == "ecoinvent":
                if not any(term.lower() in name.lower() for term in ecoinvent_exceptions["waste"]):
                    return True
                else:
                    return False
            return True
    return False


def find_production_exchange(activity: dict) -> dict:
    """
    Find the production exchange of the given activity.
    :param activity:
    :return: production exchange
    """
    for exc in activity["exchanges"]:
        if exc["type"] == "production":
            return exc
    raise ValueError(f"The activity {activity['name']} does " f"not have a production exchange.")


def get_technosphere_exchanges(activity: dict) -> list:
    """
    Get the technosphere exchanges of the given activity.
    :param activity:
    :return: technosphere exchanges
    """
    return [exc for exc in activity["exchanges"] if exc["type"] == "technosphere" and exc["amount"] != 0]


def get_biosphere_exchanges(activity: dict, category: str = None) -> list:
    """
    Get the technosphere exchanges of the given activity.
    :param activity: activity
    :param category: biosphere category
    :return: biosphere exchanges
    """
    exchanges = []
    for exc in activity["exchanges"]:
        if exc["type"] != "biosphere" or exc["amount"] == 0:
            continue

        categories = exc.get("categories")
        if not categories:
            raise ValueError(f"Biosphere exchange {exc.get('name')} is missing categories.")

        if categories[0] == category:
            exchanges.append(exc)

    return exchanges


def is_blacklisted(exchange: dict, database: str) -> bool:
    """
    Check whether a name is blacklisted or not
    :param name: name
    :param database: database to link to.
    :return: bool
    """

    if exchange["name"] in simapro_ecoinvent_blacklist:
        return True

    if database == "uvek":
        if exchange["name"] in simapro_uvek_blacklist:
            return True

    return False


def convert_sd_to_sd2(value: float, uncertainty_type: str) -> float:
    """
    Convert standard deviation of underlying lognormal distirbution
    to standard deviation squared.
    :param value:
    :return: squared standard deviation
    """

    if uncertainty_type == "Lognormal":
        return np.exp(value) ** 2

    if uncertainty_type == "Normal":
        # normal distribution
        return value**2

    if uncertainty_type in ["not defined", "Unspecified", "Undefined", "Triangle", "Uniform"]:
        # normal distribution
        return 0

    logger.warning(
        "No SimaPro uncertainty scale conversion is implemented for %s; using 0.",
        uncertainty_type,
    )
    return 0


def round_floats_in_string(s):
    # Pattern to detect float numbers in a string
    pattern = re.compile(r"[-+]?\d*\.\d+")

    # Function to apply to each match
    def round_match(match):
        return str(round(float(match.group()), 2))

    # Apply function to each match
    return pattern.sub(round_match, s)


def get_subcategory(category: str) -> str:
    """
    Extract Simapro subcategory from string
    :param category:
    :return:
    """

    if len(category.split("/")) > 1:
        subcategory = category.split("/")[1:]
        # replace "/" with backslash
        subcategory = "\\".join(subcategory)
    else:
        subcategory = ""

    return subcategory


def flag_exchanges(activity: dict) -> dict:
    """
    We flag exchanges to keep track of whether they have been
    processed or not.
    :param activity: activity
    :return: activity with flagged exchanged
    """

    for exc in activity["exchanges"]:
        exc["used"] = False

    return activity


def collect_unused_exchanges(inventories: list) -> list[dict]:
    """
    Collect exchanges that have not been marked as used during conversion.
    :param inventories:
    :return: list of unused exchange summaries
    """

    unused_exchanges = []
    for activity in inventories:
        for exc in activity["exchanges"]:
            if exc.get("used", False) is False and exc["amount"] != 0:
                unused_exchanges.append(
                    {
                        "activity": activity.get("name"),
                        "exchange": exc.get("name"),
                        "unit": exc.get("unit"),
                        "location": exc.get("location", "GLO"),
                        "categories": exc.get("categories"),
                    }
                )

    return unused_exchanges


def check_simapro_inventory(file, output_path=None):
    # read CSV file
    new_file_data = []
    changed = False
    source = Path(file)
    with open(source, "r", encoding="latin-1") as f:
        data = csv.reader(f, delimiter=";")
        for row in data:
            updated_row = search_for_forbidden_units(row)
            changed = changed or updated_row != row
            row = updated_row
            new_file_data.append(row)

    if not changed and output_path is None:
        return source

    if output_path is None:
        suffix = source.suffix or ".csv"
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="latin-1",
            newline="",
            suffix=suffix,
            prefix=f"{source.stem}_edited_",
            delete=False,
        ) as temp:
            output = Path(temp.name)
            writer = csv.writer(temp, delimiter=";")
            for row in new_file_data:
                writer.writerow(row)
    else:
        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        with open(output, mode="w", encoding="latin-1", newline="") as e:
            writer = csv.writer(e, delimiter=";")
            for row in new_file_data:
                writer.writerow(row)

    logger.info("Cleaned SimaPro inventory file saved as: %s.", output)
    return output


def search_for_forbidden_units(row: list) -> list:
    """
    Search for forbidden units.
    Returns the csv row.

    :param row: list of values
    :return: list of values

    """
    FORBIDDEN_UNITS = {
        "min": "minute",
    }

    updated = list(row)
    for v, val in enumerate(updated):
        if val in FORBIDDEN_UNITS:
            logger.warning(
                "Unit %s replaced by %s.",
                val,
                FORBIDDEN_UNITS[val],
            )
            updated[v] = FORBIDDEN_UNITS[val]

    return updated


def load_biosphere_correspondence():
    filename = "correspondence_biosphere_flows.yaml"
    filepath = DATA_DIR / "export" / filename
    return _load_yaml_file(
        filepath,
        "The dictionary of biosphere flow correspondence between ecoinvent and Simapro",
    )


def load_simapro_brightway_biosphere_mapping(version: str):
    normalized_version = version.replace(".", "")
    filename = f"simapro_to_brightway_biosphere_ei{normalized_version}.yaml"
    filepath = DATA_DIR / "export" / filename
    if not filepath.is_file():
        return {}

    return _load_yaml_file(
        filepath,
        "The version-specific SimaPro to Brightway biosphere flow mapping",
    )


def load_ei_biosphere_flows():
    filename = "flows_biosphere_39.csv"
    filepath = DATA_DIR / "export" / filename
    if not filepath.is_file():
        raise FileNotFoundError(
            "The dictionary of subcompartments match " "between ecoinvent and Simapro could not be found."
        )

    with open(filepath, encoding="utf-8") as f:
        data = [[val.strip() for val in r.split(";")] for r in f.readlines()]

    return list(set([(r[0], r[1], r[2]) for r in data]))


def lower_cap_first_letter(s):
    # Check if the string starts with an acronym (all uppercase letters followed by a space, end of string, dash, or comma)
    if re.match(r"^[A-Z]+(\s|$|-|,)", s):
        return s  # Keep acronyms unchanged
    return s[0].lower() + s[1:] if s else s  # Lowercase first letter otherwise
