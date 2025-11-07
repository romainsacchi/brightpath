import csv
import json
import logging
import re
from pathlib import Path
from typing import Dict, Tuple

import bw2io
import numpy as np
import yaml
from bw2io.importers.excel import ExcelImporter
from prettytable import PrettyTable
from voluptuous import Optional, Required, Schema, Url

from . import DATA_DIR

logging.basicConfig(
    level=logging.DEBUG,
    filename="brightpath.log",  # Log file to save the entries
    filemode="a",  # Append to the log file if it exists, 'w' to overwrite
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def get_simapro_biosphere() -> Dict[str, str]:
    """Load the correspondence between ecoinvent and SimaPro biosphere flows.

    :return: Mapping from an ecoinvent biosphere flow name to its SimaPro
        equivalent name.
    :rtype: dict[str, str]
    :raises FileNotFoundError: If the mapping file is missing from
        ``brightpath/data/export``.
    :raises json.JSONDecodeError: If the mapping file cannot be parsed.
    """

    filename = "simapro-biosphere.json"
    filepath = DATA_DIR / "export" / filename
    if not filepath.is_file():
        raise FileNotFoundError(
            "The dictionary of biosphere flow match "
            "between ecoinvent and Simapro could not be found."
        )
    with open(filepath, encoding="utf-8") as json_file:
        data = json.load(json_file)
    dict_bio = {}
    for d in data:
        dict_bio[d[2]] = d[1]

    return dict_bio


def get_simapro_subcompartments() -> Dict[str, str]:
    """Load the mapping of biosphere sub-compartments.

    :return: Mapping from ecoinvent sub-compartment names to their SimaPro
        equivalents.
    :rtype: dict[str, str]
    :raises FileNotFoundError: If the YAML file with the mapping is missing.
    :raises yaml.YAMLError: If the YAML file cannot be parsed.
    """

    filename = "simapro_subcompartments.yaml"
    filepath = DATA_DIR / "export" / filename
    if not filepath.is_file():
        raise FileNotFoundError(
            "The dictionary of subcompartments match "
            "between ecoinvent and Simapro could not be found."
        )

    # read YAML file
    with open(filepath, "r") as stream:
        try:
            data = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

    return data


def get_simapro_technosphere() -> Dict[Tuple[str, str], str]:
    """Load the correspondence between ecoinvent and SimaPro product flows.

    :return: Mapping where the key is the pair ``(name, location)`` of an
        ecoinvent technosphere exchange and the value is the SimaPro exchange
        name.
    :rtype: dict[tuple[str, str], str]
    :raises FileNotFoundError: If the CSV mapping file is missing.
    """

    filename = "simapro-technosphere-3.5.csv"
    filepath = DATA_DIR / "export" / filename
    with open(filepath, encoding="utf-8") as f:
        csv_list = [[val.strip() for val in r.split(";")] for r in f.readlines()]
    (_, _, *header), *data = csv_list

    dict_tech = {}
    for row in data:
        name, location, simapro_name = row
        simapro_name = simapro_name.split("|")[:2]
        dict_tech[(name, location)] = "|".join(simapro_name)

    return dict_tech


def get_simapro_ecoinvent_blacklist():
    """Load the list of exchanges to exclude when exporting to SimaPro.

    :return: Dictionary describing exchanges that must be skipped for the
        ecoinvent export.
    :rtype: dict
    :raises FileNotFoundError: If the blacklist file is missing.
    :raises yaml.YAMLError: If the blacklist file cannot be parsed.
    """

    filename = "simapro_blacklist.yaml"
    filepath = DATA_DIR / "export" / filename
    # read YAML file
    with open(filepath, "r") as stream:
        try:
            data = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    return data


simapro_ecoinvent_blacklist = get_simapro_ecoinvent_blacklist()


def get_simapro_uvek_blacklist():
    """Load the blacklist of SimaPro UVEK exchanges.

    :return: Dictionary describing exchanges that must be skipped when
        targeting the UVEK database.
    :rtype: dict
    :raises FileNotFoundError: If the blacklist YAML file cannot be found.
    :raises yaml.YAMLError: If the blacklist file cannot be parsed.
    """

    filename = "uvek_blacklist.yaml"
    filepath = DATA_DIR / "export" / filename
    # read YAML file
    with open(filepath, "r") as stream:
        try:
            data = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    return data


simapro_uvek_blacklist = get_simapro_uvek_blacklist()


def get_ecoinvent_to_uvek_mapping():
    """Load the mapping between ecoinvent flows and UVEK identifiers.

    :return: Dictionary keyed by a tuple consisting of the ecoinvent flow
        name, location and additional qualifiers, pointing to the UVEK
        identifier.
    :rtype: dict[tuple[str, str, str, str], str]
    :raises FileNotFoundError: If the CSV mapping file cannot be found.
    """
    filename = "ecoinvent_to_uvek_mapping.csv"
    filepath = DATA_DIR / "export" / filename
    with open(filepath, "r") as file:
        reader = csv.reader(file)
        next(reader)
        dictionary = {tuple(row[:4]): row[-1] for row in reader}

    return dictionary


def get_ecoinvent_transport_distances():
    """Load default transport distances for ecoinvent flows.

    :return: Mapping from exchange name to a dictionary containing transport
        distances per mode and region.
    :rtype: dict[str, dict[str, str]]
    :raises FileNotFoundError: If the CSV with transport distances is missing.
    """
    filename = "ei_transport.csv"
    filepath = DATA_DIR / "export" / filename
    with open(filepath, "r") as file:
        reader = csv.reader(file, delimiter=";")
        next(reader)
        dictionary = {
            row[0]: {
                "train - RER": row[3],
                "lorry - RER": row[4],
                "barge - RER": row[5],
                "train - CH": row[6],
                "lorry - CH": row[7],
                "barge - CH": row[8],
            }
            for row in reader
        }

    return dictionary


ecoinvent_uvek_mapping = get_ecoinvent_to_uvek_mapping()
ecoinvent_transport_distances = get_ecoinvent_transport_distances()


def get_simapro_fields_list() -> list[str]:
    """Return the ordered list of SimaPro section names.

    :return: Sequence of field names that describes the structure of a
        SimaPro CSV export.
    :rtype: list[str]
    :raises FileNotFoundError: If the YAML definition file is missing.
    :raises yaml.YAMLError: If the YAML file cannot be parsed.
    """

    filename = "simapro_fields.yaml"
    filepath = DATA_DIR / "export" / filename

    # read YAML file
    with open(filepath, "r") as stream:
        try:
            data = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    return data


def get_simapro_units():
    """Load the mapping of Brightway units to SimaPro units.

    :return: Dictionary mapping source units to their SimaPro counterparts.
    :rtype: dict[str, str]
    :raises FileNotFoundError: If the YAML definition file is missing.
    :raises yaml.YAMLError: If the YAML file cannot be parsed.
    """

    filename = "simapro_units.yaml"
    filepath = DATA_DIR / "export" / filename

    # read YAML file
    with open(filepath, "r") as stream:
        try:
            data = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    return data


def get_simapro_headers():
    """Load the SimaPro header rows that precede each export.

    :return: List of header strings used when generating SimaPro CSV files.
    :rtype: list[str]
    :raises FileNotFoundError: If the YAML definition file is missing.
    :raises yaml.YAMLError: If the YAML file cannot be parsed.
    """

    filename = "simapro_headers.yaml"
    filepath = DATA_DIR / "export" / filename

    # read YAML file
    with open(filepath, "r") as stream:
        try:
            data = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

    return data


def get_simapro_ecoinvent_exceptions():
    """Load the list of special-case ecoinvent flows.

    :return: Dictionary describing exchanges that require bespoke handling
        during the conversion.
    :rtype: dict
    :raises FileNotFoundError: If the YAML exception file is missing.
    :raises yaml.YAMLError: If the YAML file cannot be parsed.
    """

    filename = "simapro_ei_exceptions.yaml"
    filepath = DATA_DIR / "export" / filename

    # read YAML file
    with open(filepath, "r") as stream:
        try:
            data = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

    return data


ecoinvent_exceptions = get_simapro_ecoinvent_exceptions()


def get_waste_exchange_names():
    """Return the keywords that identify waste-treatment exchanges.

    :return: List of strings that indicate an exchange represents waste
        treatment.
    :rtype: list[str]
    :raises FileNotFoundError: If the YAML file cannot be found.
    :raises yaml.YAMLError: If the YAML file cannot be parsed.
    """

    filename = "waste_exchange_names.yaml"
    filepath = DATA_DIR / "export" / filename

    # read YAML file
    with open(filepath, "r") as stream:
        try:
            data = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    return data


def check_inventories(data: list) -> None:
    """Validate that inventories contain the required information.

    The function verifies that each exchange includes the mandatory keys
    expected by the conversion logic. A :class:`ValueError` is raised when
    missing data is detected and the offending exchanges are displayed.

    :param data: Brightway activities that should be checked.
    :type data: list[dict]
    :raises ValueError: If an exchange misses mandatory attributes.
    """

    MANDATORY_TECH_EXC_KEYS = ["name", "reference product", "location", "unit"]

    MANDATORY_BIO_EXC_KEYS = ["name", "categories", "unit"]

    faulty_exchanges = []

    for activity in data:
        for exchange in activity["exchanges"]:
            if exchange["type"] in ["production", "technosphere"]:
                if not all(key in exchange.keys() for key in MANDATORY_TECH_EXC_KEYS):
                    faulty_exchanges.append(
                        [
                            exchange.get("name"),
                            exchange.get("reference unit"),
                            exchange.get("location"),
                            "--",
                            exchange.get("unit"),
                        ]
                    )
            else:
                if not all(key in exchange.keys() for key in MANDATORY_BIO_EXC_KEYS):
                    faulty_exchanges.append(
                        [
                            exchange.get("name"),
                            "--",
                            "--",
                            exchange.get("categories"),
                            exchange.get("unit"),
                        ]
                    )

    # print prettytable with faulty exchanges
    if len(faulty_exchanges) > 0:
        table = PrettyTable()
        table.field_names = [
            "Name",
            "Reference product",
            "Location",
            "Categories",
            "Unit",
        ]
        for exc in faulty_exchanges:
            table.add_row(exc)

        print(table)
        raise ValueError(
            "Some exchanges do not have mandatory exchange "
            "fields (marked 'None' in table above)."
        )


def import_bw_inventories(filepath: str) -> list[dict]:
    """Load Brightway inventories from an Excel workbook.

    The importer relies on :mod:`bw2io` to read Brightway exports and ensures
    that the required migrations are available.

    :param filepath: Path to the Excel inventory spreadsheet.
    :type filepath: str
    :return: List of activities in Brightway format.
    :rtype: list[dict]
    :raises FileNotFoundError: If ``filepath`` does not exist.
    :raises ValueError: If the provided file does not have the ``.xlsx`` suffix.
    """
    # using bw2io, we load the inventories contained
    # in the spreadsheet file
    # and return a list of dictionaries

    # if filepath is a string, convert to Path object
    if isinstance(filepath, str):
        filepath = Path(filepath)

    # check that filepath is a file
    if not filepath.is_file():
        raise FileNotFoundError("The file could not be found.")
    # check that suffix is .xlsx
    if filepath.suffix != ".xlsx":
        raise ValueError("The file must be a .xlsx spreadsheet.")

    # import the inventories
    importer = ExcelImporter(filepath)
    # check if all necessary migration files are present
    if "biosphere-2-3-categories" not in bw2io.migrations:
        bw2io.create_core_migrations()
    importer.apply_strategies()

    check_inventories(importer.data)

    return importer.data


def check_metadata(metadata: dict) -> dict:
    """Validate metadata against the expected schema.

    :param metadata: Raw metadata read from the YAML file.
    :type metadata: dict
    :return: Sanitised metadata that matches the schema.
    :rtype: dict
    :raises voluptuous.error.MultipleInvalid: If validation fails.
    """
    # metadata dictionary should conform to the following schema:
    # Define the validation schema
    system_description_schema = Schema(
        {
            Required("name"): str,
            Optional("category"): str,
            Optional("description"): str,
            Optional("cut-off rules"): str,
            Optional("energy model"): str,
            Optional("transport model"): str,
            Optional("allocation rules"): str,
        }
    )

    literature_reference_schema = Schema(
        {
            Required("name"): str,
            Optional("documentation link"): Url(),
            Optional("comment"): str,
            Optional("category"): str,
            Optional("description"): str,
        }
    )

    main_schema = Schema(
        {
            Required("system description"): system_description_schema,
            Required("literature reference"): literature_reference_schema,
        }
    )

    # Validate against schema
    validated_data = main_schema(metadata)

    return validated_data


def load_inventory_metadata(filepath: str) -> dict:
    """Load and validate inventory metadata from disk.

    :param filepath: Path to the YAML document containing the metadata.
    :type filepath: str
    :return: Validated metadata dictionary.
    :rtype: dict
    :raises FileNotFoundError: If the metadata file does not exist.
    :raises ValueError: If the file does not have a ``.yaml`` extension.
    :raises yaml.YAMLError: If the metadata file cannot be parsed.
    :raises voluptuous.error.MultipleInvalid: If the metadata structure is
        invalid.
    """
    # if filepath is a string, convert to Path object
    if isinstance(filepath, str):
        filepath = Path(filepath)

    # check that filepath is a file
    if not filepath.is_file():
        raise FileNotFoundError("The file could not be found.")
    # check that suffix is .yaml
    if filepath.suffix != ".yaml":
        raise ValueError("The file must be a .yaml file.")

    # read YAML file
    with open(filepath, "r") as stream:
        try:
            data = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

    # check that metadata is valid
    data = check_metadata(data)

    return data


def is_activity_waste_treatment(activity: dict, database: str) -> bool:
    """Determine whether an activity represents waste treatment.

    :param activity: Brightway activity dictionary to inspect.
    :type activity: dict
    :param database: Name of the target database used for heuristics.
    :type database: str
    :return: ``True`` if the activity is a waste treatment process.
    :rtype: bool
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
    """Check whether an exchange name matches waste-treatment keywords.

    :param name: Exchange name to analyse.
    :type name: str
    :param database: Target database used to refine the decision.
    :type database: str
    :return: ``True`` if the exchange is considered waste treatment.
    :rtype: bool
    """
    WASTE_TERMS = get_waste_exchange_names()
    NOT_WASTE_TERMS = [
        # "plant",
        "incineration plant"
    ]

    if any(term.lower() in name.lower() for term in WASTE_TERMS) is True:
        if any(term.lower() in name.lower() for term in NOT_WASTE_TERMS) is False:
            if database == "ecoinvent":
                if not any(
                    term.lower() in name.lower()
                    for term in ecoinvent_exceptions["waste"]
                ):
                    return True
                else:
                    return False
            return True
    return False


def find_production_exchange(activity: dict) -> dict:
    """Retrieve the production exchange from an activity.

    :param activity: Activity whose production exchange should be returned.
    :type activity: dict
    :return: The production exchange of the activity.
    :rtype: dict
    :raises ValueError: If the activity does not contain a production exchange.
    """
    for exc in activity["exchanges"]:
        if exc["type"] == "production":
            return exc
    raise ValueError(
        f"The activity {activity['name']} does " f"not have a production exchange."
    )


def get_technosphere_exchanges(activity: dict) -> list:
    """Return the technosphere exchanges from an activity.

    :param activity: Activity for which technosphere exchanges should be
        collected.
    :type activity: dict
    :return: Technosphere exchanges with non-zero amounts.
    :rtype: list[dict]
    """
    return [
        exc
        for exc in activity["exchanges"]
        if exc["type"] == "technosphere" and exc["amount"] != 0
    ]


def get_biosphere_exchanges(activity: dict, category: str = None) -> list:
    """Return biosphere exchanges optionally filtered by category.

    :param activity: Activity for which biosphere exchanges should be
        collected.
    :type activity: dict
    :param category: Biosphere compartment to filter for, e.g. ``"air"``.
    :type category: str | None
    :return: Biosphere exchanges that match the optional category.
    :rtype: list[dict]
    """
    return [
        exc
        for exc in activity["exchanges"]
        if exc["type"] == "biosphere"
        and exc.get("categories")[0] == category
        and exc["amount"] != 0
    ]


def format_exchange_name(
    name: str, reference_product: str, location: str, unit: str, database: str
) -> str:
    """Format a Brightway exchange name for SimaPro compatibility.

    :param name: Exchange name from the Brightway inventory.
    :type name: str
    :param reference_product: Reference product of the exchange.
    :type reference_product: str
    :param location: Location code associated with the exchange.
    :type location: str
    :param unit: Unit of the exchange.
    :type unit: str
    :param database: Target database used to select the formatting logic.
    :type database: str
    :return: Name formatted according to SimaPro conventions.
    :rtype: str
    """

    if database == "ecoinvent":
        # first letter of `name` should be capitalized
        reference_product = reference_product[0].upper() + reference_product[1:]
        name = name[0].upper() + name[1:]

        exchange_name = f"{reference_product} {{{location}}}| {name}"

        for i in ["market for", "market group for"]:
            if i in name.lower():
                exchange_name = f"{reference_product} {{{location}}}"
                reference_product = reference_product[0].lower() + reference_product[1:]

                if (
                    reference_product.lower() in ecoinvent_exceptions["market"]
                    and location == "GLO"
                ):
                    exchange_name += f"| {i}"
                else:
                    exchange_name += f"| {i} {reference_product}"

        exchange_name += " | Cut-off, U"

    else:
        # check first if name appears in ecoinvent-uvek mapping list
        if (name, location, unit, reference_product) in ecoinvent_uvek_mapping:
            return ecoinvent_uvek_mapping[(name, location, unit, reference_product)]
        # database to link to is uvek.
        exchange_name = f"{name}/{location} U"

    return exchange_name


def get_simapro_uncertainty_type(uncertainty_type: int) -> str:
    """Map Brightway uncertainty codes to the SimaPro string representation.

    :param uncertainty_type: Integer identifier of the uncertainty type.
    :type uncertainty_type: int
    :return: Human readable uncertainty label used by SimaPro.
    :rtype: str
    """

    UNCERTAINITY_TYPES = {
        0: "not defined",
        1: "not defined",
        2: "Lognormal",
        3: "Normal",
        4: "Uniform",
        5: "Triangular",
    }

    return UNCERTAINITY_TYPES.get(uncertainty_type, "not defined")


def is_blacklisted(exchange: dict, database: str) -> bool:
    """Check whether an exchange should be excluded during conversion.

    :param exchange: Exchange dictionary to inspect.
    :type exchange: dict
    :param database: Target database, ``"ecoinvent"`` or ``"uvek"``.
    :type database: str
    :return: ``True`` when the exchange must be ignored.
    :rtype: bool
    """

    if exchange["name"] in simapro_ecoinvent_blacklist:
        return True

    if database == "uvek":
        if exchange["name"] in simapro_uvek_blacklist:
            return True

    return False


def convert_sd_to_sd2(value: float, uncertainty_type: str) -> float:
    """Convert standard deviations according to SimaPro expectations.

    :param value: Standard deviation or lognormal sigma from Brightway.
    :type value: float
    :param uncertainty_type: Uncertainty distribution label.
    :type uncertainty_type: str
    :return: Converted standard deviation compatible with SimaPro.
    :rtype: float
    """

    if uncertainty_type == "Lognormal":
        return np.exp(value) ** 2

    if uncertainty_type == "Normal":
        # normal distribution
        return value**2

    if uncertainty_type in ["not defined", "Unspecified"]:
        # normal distribution
        return 0


def get_uvek_conversion_factors() -> dict:
    """Load conversion factors specific to the UVEK database.

    :return: Mapping of exchange names to conversion factors and units.
    :rtype: dict
    :raises FileNotFoundError: If the YAML file is missing.
    :raises yaml.YAMLError: If the YAML file cannot be parsed.
    """
    filename = "uvek_conversion_factors.yaml"
    filepath = DATA_DIR / "export" / filename

    # read YAML file
    with open(filepath, "r") as stream:
        try:
            data = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

    return data


def round_floats_in_string(s):
    """Round floating point numbers found inside a string to two decimals.

    :param s: Arbitrary text potentially containing floating point numbers.
    :type s: str
    :return: String where the embedded numbers have been rounded.
    :rtype: str
    """
    # Pattern to detect float numbers in a string
    pattern = re.compile(r"[-+]?\d*\.\d+")

    # Function to apply to each match
    def round_match(match):
        return str(round(float(match.group()), 2))

    # Apply function to each match
    return pattern.sub(round_match, s)


def get_subcategory(category: str) -> str:
    """Extract the SimaPro subcategory from a combined category string.

    :param category: Category string containing ``/``-separated components.
    :type category: str
    :return: Subcategory formatted with backslashes as required by SimaPro.
    :rtype: str
    """

    if len(category.split("/")) > 1:
        subcategory = category.split("/")[1:]
        # replace "/" with backslash
        subcategory = "\\".join(subcategory)
    else:
        subcategory = ""

    return subcategory


def flag_exchanges(activity: dict) -> dict:
    """Mark exchanges as unused before processing.

    :param activity: Activity whose exchanges should be flagged.
    :type activity: dict
    :return: Activity with a ``used`` flag initialised on each exchange.
    :rtype: dict
    """

    for exc in activity["exchanges"]:
        exc["used"] = False

    return activity


def print_unused_exchanges(inventories: list) -> None:
    """Display exchanges that were not converted.

    :param inventories: Converted activities to inspect.
    :type inventories: list[dict]
    """

    unused_exchanges = []
    exc_counter = 0
    for activity in inventories:
        for exc in activity["exchanges"]:
            exc_counter += 1
            if exc.get("used", False) is False and exc["amount"] != 0:
                unused_exchanges.append(
                    (
                        exc["name"],
                        exc["unit"],
                        exc.get("location", "GLO"),
                        exc.get(
                            "categories",
                        ),
                    )
                )

    if len(unused_exchanges) > 0:
        print("The following exchanges have not been used:")
        table = PrettyTable(
            (
                "Exchange",
                "Amount",
                "Location",
                "Categories",
            )
        )
        for row in list(set(unused_exchanges)):
            table.add_row(row)
        print(table)
    else:
        print(f"All {exc_counter} exchanges have been converted!")


def check_exchanges_for_conversion(exchanges: list, database: str) -> list:
    """Apply database-specific conversion factors to exchanges.

    :param exchanges: Exchanges that might require conversion.
    :type exchanges: list[dict]
    :param database: Target database identifier.
    :type database: str
    :return: Updated list of exchanges.
    :rtype: list[dict]
    """

    if database == "uvek":
        conversion_factors = get_uvek_conversion_factors()
        for exc in exchanges:
            if exc["name"] in conversion_factors:
                exc["amount"] *= conversion_factors[exc["name"]].get("factor", 1)
                exc["unit"] = conversion_factors[exc["name"]].get("unit", exc["unit"])

    return exchanges


def fetch_transport_distance(name: str, location: str) -> tuple:
    """Return default transport distances for a product and location.

    :param name: Name of the technosphere exchange.
    :type name: str
    :param location: Location code of the consuming activity.
    :type location: str
    :return: Distances for train, lorry and barge transport.
    :rtype: tuple[float, float, float]
    """

    if name in ecoinvent_transport_distances:
        if location == "CH":
            return (
                float(ecoinvent_transport_distances[name]["train - CH"]),
                float(ecoinvent_transport_distances[name]["lorry - CH"]),
                float(ecoinvent_transport_distances[name]["barge - CH"]),
            )
        else:
            return (
                float(ecoinvent_transport_distances[name]["train - RER"]),
                float(ecoinvent_transport_distances[name]["lorry - RER"]),
                float(ecoinvent_transport_distances[name]["barge - RER"]),
            )
    else:
        return 0.0, 0.0, 0.0


def add_distri_transport(activity: dict) -> dict:
    """Add distribution transport exchanges required by the UVEK database.

    :param activity: Activity that should receive additional transport
        exchanges.
    :type activity: dict
    :return: Activity enriched with transport exchanges.
    :rtype: dict
    """

    train_ch, lorry_ch, barge_ch = (0.0, 0.0, 0.0)
    train_rer, lorry_rer, barge_rer = (0.0, 0.0, 0.0)
    distance_train_ch, distance_lorry_ch, distance_barge_ch = (0.0, 0.0, 0.0)
    distance_train_rer, distance_lorry_rer, distance_barge_rer = (0.0, 0.0, 0.0)

    for exc in get_technosphere_exchanges(activity):
        if exc["unit"] == "kilogram":
            train, lorry, barge = fetch_transport_distance(
                exc["name"], activity["location"]
            )
            if activity["location"] == "CH":
                train_ch += train * exc["amount"] / 1000.0
                lorry_ch += lorry * exc["amount"] / 1000.0
                barge_ch += barge * exc["amount"] / 1000.0
                distance_train_ch += train
                distance_lorry_ch += lorry
                distance_barge_ch += barge
            else:
                train_rer += train * exc["amount"] / 1000.0
                lorry_rer += lorry * exc["amount"] / 1000.0
                barge_rer += barge * exc["amount"] / 1000.0
                distance_train_rer += train
                distance_lorry_rer += lorry
                distance_barge_rer += barge

    if train_ch > 0:
        activity["exchanges"].append(
            {
                "name": "market for transport, freight train",
                "reference product": "transport, freight train",
                "amount": train_ch,
                "unit": "ton kilometer",
                "type": "technosphere",
                "location": "CH",
                "uncertainty type": 2,
                "loc": np.log(train_ch),
                "scale": 0.396,
                "used": False,
                "comment": "Generic transport distances calculated based on "
                "Table 4.2 of the ecoinvent v.2 Methodology report. "
                f"Distribution: {np.round((train_ch/distance_train_ch)*1000, 2)} kg "
                f"over {np.round(distance_train_ch, 2)} km.",
            }
        )

    if lorry_ch > 0:
        activity["exchanges"].append(
            {
                "name": "market for transport, freight, lorry, unspecified",
                "reference product": "transport, freight, lorry, unspecified",
                "amount": lorry_ch,
                "unit": "ton kilometer",
                "type": "technosphere",
                "location": "CH",
                "uncertainty type": 2,
                "loc": np.log(lorry_ch),
                "scale": 0.396,
                "used": False,
                "comment": "Generic transport distances calculated based on "
                "Table 4.2 of the ecoinvent v.2 Methodology report. "
                f"Distribution: {np.round((lorry_ch/distance_lorry_ch)*1000, 2)} kg "
                f"over {np.round(distance_lorry_ch, 2)} km.",
            }
        )

    if barge_ch > 0:
        activity["exchanges"].append(
            {
                "name": "market for transport, freight, inland waterways, barge",
                "reference product": "transport, freight, inland waterways, barge",
                "amount": barge_ch,
                "unit": "ton kilometer",
                "type": "technosphere",
                "location": "RER",
                "uncertainty type": 2,
                "loc": np.log(barge_ch),
                "scale": 0.396,
                "used": False,
                "comment": "Generic transport distances calculated based on "
                "Table 4.2 of the ecoinvent v.2 Methodology report. "
                f"Distribution: {np.round((barge_ch/distance_barge_ch)*1000, 2)} kg "
                f"over {np.round(distance_barge_ch, 2)} km.",
            }
        )

    if train_rer > 0:
        activity["exchanges"].append(
            {
                "name": "market for transport, freight train",
                "reference product": "transport, freight train",
                "amount": train_rer,
                "unit": "ton kilometer",
                "type": "technosphere",
                "location": "Europe without Switzerland",
                "uncertainty type": 2,
                "loc": np.log(train_rer),
                "scale": 0.396,
                "used": False,
                "comment": "Generic transport distances calculated based on "
                "Table 4.2 of the ecoinvent v.2 Methodology report. "
                f"Distribution: {np.round((train_rer/distance_train_rer)*1000, 2)} kg "
                f"over {np.round(distance_train_rer, 2)} km.",
            }
        )

    if lorry_rer > 0:
        activity["exchanges"].append(
            {
                "name": "market for transport, freight, lorry, unspecified",
                "reference product": "transport, freight, lorry, unspecified",
                "amount": lorry_rer,
                "unit": "ton kilometer",
                "type": "technosphere",
                "location": "RER",
                "uncertainty type": 2,
                "loc": np.log(lorry_rer),
                "scale": 0.396,
                "used": False,
                "comment": "Generic transport distances calculated based on "
                "Table 4.2 of the ecoinvent v.2 Methodology report. "
                f"Distribution: {np.round((lorry_rer/distance_lorry_rer)*1000, 2)} kg "
                f"over {np.round(distance_lorry_rer, 2)} km.",
            }
        )

    if barge_rer > 0:
        activity["exchanges"].append(
            {
                "name": "market for transport, freight, inland waterways, barge",
                "reference product": "transport, freight, inland waterways, barge",
                "amount": barge_rer,
                "unit": "ton kilometer",
                "type": "technosphere",
                "location": "RER",
                "uncertainty type": 2,
                "loc": np.log(barge_rer),
                "scale": 0.396,
                "used": False,
                "comment": "Generic transport distances calculated based on "
                "Table 4.2 of the ecoinvent v.2 Methodology report. "
                f"Distribution: {np.round((barge_rer/distance_barge_rer)*1000, 2)} kg "
                f"over {np.round(distance_barge_rer, 2)} km.",
            }
        )

    return activity


def remove_duplicates(data):
    """Remove datasets that share the same name from a list of activities.

    :param data: Activities to deduplicate.
    :type data: list[dict]
    :return: New list containing only the first occurrence of each dataset.
    :rtype: list[dict]
    """
    a = []
    acts = []
    for x in data:
        if x["name"] not in a:
            a.append(x["name"])
            acts.append(x)
        else:
            logging.warning(f"Duplicate found: {x['name']}")
    return acts


def check_simapro_inventory(file):
    """Check a SimaPro CSV file for forbidden units.

    :param file: Path to the CSV inventory file.
    :type file: str
    :return: Path to the cleaned CSV file with forbidden units replaced.
    :rtype: str
    """
    # read CSV file
    new_file_data = []
    with open(file, "r", encoding="latin-1") as f:
        data = csv.reader(f, delimiter=";")
        for r, row in enumerate(data):
            row = search_for_forbidden_units(row)
            for v, val in enumerate(row):
                search_for_forbidden_units(val)
            new_file_data.append(row)

    # save new file
    with open(
        file.lower().replace(".csv", "_edited.csv"),
        mode="w",
        encoding="latin-1",
        newline="",
    ) as e:
        writer = csv.writer(e, delimiter=";")
        for row in new_file_data:
            writer.writerow(row)

    logging.info(
        f"New inventory file saved as: {file.lower().replace('.csv', '_edited.csv')}."
    )
    return file.lower().replace(".csv", "_edited.csv")


def search_for_forbidden_units(row: list) -> list:
    """Replace forbidden units found in a CSV row.

    :param row: Row values to inspect.
    :type row: list[str]
    :return: Row with forbidden units replaced by allowed ones.
    :rtype: list[str]
    """
    FORBIDDEN_UNITS = {
        "min": "minute",
    }

    for v, val in enumerate(row):
        if val in FORBIDDEN_UNITS:
            logging.warning(f"Unit {val} replaced by {FORBIDDEN_UNITS[val]}.")
            row[v] = FORBIDDEN_UNITS[val]

    return row


def load_biosphere_correspondence():
    """Load the correspondence between SimaPro and ecoinvent biosphere flows.

    :return: Mapping of biosphere flows grouped by compartment.
    :rtype: dict
    :raises FileNotFoundError: If the correspondence file cannot be found.
    :raises yaml.YAMLError: If the YAML file cannot be parsed.
    """
    filename = "correspondence_biosphere_flows.yaml"
    filepath = DATA_DIR / "export" / filename
    if not filepath.is_file():
        raise FileNotFoundError(
            "The dictionary of subcompartments match "
            "between ecoinvent and Simapro could not be found."
        )

    # read YAML file
    with open(filepath, "r") as stream:
        try:
            data = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

    return data


def load_ei_biosphere_flows():
    """Load the list of biosphere flows available in ecoinvent.

    :return: Unique set of tuples ``(name, category, subcategory)``.
    :rtype: list[tuple[str, str, str]]
    :raises FileNotFoundError: If the biosphere flow file cannot be found.
    """
    filename = "flows_biosphere_39.csv"
    filepath = DATA_DIR / "export" / filename
    if not filepath.is_file():
        raise FileNotFoundError(
            "The dictionary of subcompartments match "
            "between ecoinvent and Simapro could not be found."
        )

    with open(filepath, encoding="utf-8") as f:
        data = [[val.strip() for val in r.split(";")] for r in f.readlines()]

    return list(set([(r[0], r[1], r[2]) for r in data]))


def lower_cap_first_letter(s):
    """Lowercase the first character unless the input starts with an acronym.

    :param s: String to normalise.
    :type s: str
    :return: Adjusted string that preserves acronyms.
    :rtype: str
    """
    # Check if the string starts with an acronym (all uppercase letters
    # followed by a space, end of string, dash, or comma)
    if re.match(r"^[A-Z]+(\s|$|-|,)", s):
        return s  # Keep acronyms unchanged
    return s[0].lower() + s[1:] if s else s  # Lowercase first letter otherwise
