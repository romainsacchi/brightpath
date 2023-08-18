import csv

import bw2io

from . import DATA_DIR
from typing import Dict, Tuple
import json
import yaml
from bw2io.importers.excel import ExcelImporter
from pathlib import Path
from voluptuous import Schema, Required, Optional, Url
from prettytable import PrettyTable
import numpy as np
import re


def get_simapro_biosphere() -> Dict[str, str]:
    # Load the matching dictionary between ecoinvent and Simapro biosphere flows
    # for each ecoinvent biosphere flow name, it gives the corresponding Simapro name

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
    # Load the matching dictionary between ecoinvent and Simapro subcompartments
    # contained in simapro_subcompartments.yaml

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
    # Load the matching dictionary between ecoinvent and Simapro product flows

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
    # Load the list of Simapro biosphere flows that
    # should be excluded from the export

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
    # Load the list of Simapro uvek flows that
    # should be excluded from the export

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
    """
    Load ecoinvent_to_uvek_mapping.csv into a dictionary.
    :return: dictionary with tuples of ecoinvent flow name and location as keys
    """
    filename = "ecoinvent_to_uvek_mapping.csv"
    filepath = DATA_DIR / "export" / filename
    with open(filepath, 'r') as file:
        reader = csv.reader(file)
        next(reader)
        dictionary = {tuple(row[:4]): row[-1] for row in reader}

    return dictionary


def get_ecoinvent_transport_distances():
    """
    Load ei_transport.csv into a dictionary.
    :return: dictionary with tuples of ecoinvent flow name and location as keys
    """
    filename = "ei_transport.csv"
    filepath = DATA_DIR / "export" / filename
    with open(filepath, 'r') as file:
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
            } for row in reader}

    return dictionary


ecoinvent_uvek_mapping = get_ecoinvent_to_uvek_mapping()
ecoinvent_transport_distances = get_ecoinvent_transport_distances()


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

    # read YAML file
    with open(filepath, "r") as stream:
        try:
            data = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    return data


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

    # read YAML file
    with open(filepath, "r") as stream:
        try:
            data = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    return data


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

    # read YAML file
    with open(filepath, "r") as stream:
        try:
            data = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

    return data


def get_simapro_ecoinvent_exceptions():
    """
    Load the YAML file "simapro_ei_exceptions.yaml"
    and return it as a dictionary.
    :return:
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
    """
    Load the list of names that
    indicate that the input is a
    waste treatment.
    :return: list of name
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
    """
    Check that inventories, and the exchanges they contain
    have all the mandatory fields.
    :param data: list of activities
    :return: list of activities or error
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
    """
    Import inventories from a spreadsheet file.
    :param filepath:
    :return: list of inventories
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
    # metadata dictionary should conform to the following schema:
    # Define the validation schema
    system_description_schema = Schema({
        Required('name'): str,
        Optional('category'): str,
        Optional('description'): str,
        Optional('cut-off rules'): str,
        Optional('energy model'): str,
        Optional('transport model'): str,
        Optional('allocation rules'): str,
    })

    literature_reference_schema = Schema({
        Required('name'): str,
        Optional('documentation link'): Url(),
        Optional('comment'): str,
        Optional('category'): str,
        Optional('description'): str,
    })

    main_schema = Schema({
        Required('system description'): system_description_schema,
        Required('literature reference'): literature_reference_schema
    })

    # Validate against schema
    validated_data = main_schema(metadata)

    return validated_data


def load_inventory_metadata(filepath: str) -> dict:
    """
    Load the metadata of the inventory.
    :param filepath:
    :return: metadata
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
        "plant",
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
    raise ValueError(
        f"The activity {activity['name']} does "
        f"not have a production exchange."
    )


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
    return [
        exc
        for exc in activity["exchanges"]
        if exc["type"] == "biosphere"
           and exc.get("categories")[0] == category
           and exc["amount"] != 0
    ]


def format_exchange_name(name: str, reference_product: str, location: str, unit: str, database: str) -> str:
    """
    Format the name of the exchange.
    :param name: exchange name.
    :param reference_product: exchange reference product.
    :param location: exchange location.
    :param database: database to link to.
    :return:
    """

    if database == "ecoinvent":
        # first letter of `name` should be capitalized
        reference_product = reference_product[0].upper() + reference_product[1:]
        name = name[0].upper() + name[1:]

        exchange_name = (
            f"{reference_product} {{{location}}}| {name}"
        )

        for i in ["market for", "market group for"]:
            if i in name.lower():
                exchange_name = f"{reference_product} {{{location}}}"
                reference_product = reference_product[0].lower() + reference_product[1:]

                if reference_product.lower() in ecoinvent_exceptions["market"] and location == "GLO":
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
    """
    Brightway uses integers to define uncertianty distribution types.
    https://stats-arrays.readthedocs.io/en/latest/#mapping-parameter-array-columns-to-uncertainty-distributions
    Simapro uses strings.
    :param uncertainty_type:
    :return: uncertainty name
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
        return value ** 2

    if uncertainty_type in ["not defined", "Unspecified"]:
        # normal distribution
        return 0


def get_uvek_conversion_factors() -> dict:
    """
    Get conversion factors for uvek database.
    :return: dictionary
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


def print_unused_exchanges(inventories: list) -> None:
    """
    Print unused exchanges
    :param inventories:
    :return: None
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
                        exc.get("categories", ),
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
    """
    Check if some exchanges need to be converted.
    Specifically when linking to uvek.
    :param exchanges: exchanges to potentially convert.
    :param database: converted exchanges.
    :return: list of exchanges
    """

    if database == "uvek":
        conversion_factors = get_uvek_conversion_factors()
        for exc in exchanges:
            if exc["name"] in conversion_factors:
                exc["amount"] *= conversion_factors[exc["name"]].get("factor", 1)
                exc["unit"] = conversion_factors[exc["name"]].get("unit", exc["unit"])

    return exchanges


def fetch_transport_distance(name: str, location: str) -> tuple:
    """
    Depending on the exchange name `name`, return one or
    several exchanges representing additional transport.
    The uvek database does not have market datasets,
    hence transport has to be added manually.
    :param name: exchange name
    :param location: location of the consuming activity
    :return: one or several transport exchanges
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
    """
    Add transport exchanges for distribution.
    :param activity: activity
    :return: activity with added transport exchanges.
    """

    train_ch, lorry_ch, barge_ch = (0.0, 0.0, 0.0)
    train_rer, lorry_rer, barge_rer = (0.0, 0.0, 0.0)
    distance_train_ch, distance_lorry_ch, distance_barge_ch = (0.0, 0.0, 0.0)
    distance_train_rer, distance_lorry_rer, distance_barge_rer = (0.0, 0.0, 0.0)

    for exc in get_technosphere_exchanges(activity):
        if exc["unit"] == "kilogram":
            train, lorry, barge = fetch_transport_distance(exc["name"], activity["location"])
            if activity["location"] == "CH":
                train_ch += (train * exc["amount"] / 1000.0)
                lorry_ch += (lorry * exc["amount"] / 1000.0)
                barge_ch += (barge * exc["amount"] / 1000.0)
                distance_train_ch += train
                distance_lorry_ch += lorry
                distance_barge_ch += barge
            else:
                train_rer += (train * exc["amount"] / 1000.0)
                lorry_rer += (lorry * exc["amount"] / 1000.0)
                barge_rer += (barge * exc["amount"] / 1000.0)
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
                           f"over {np.round(distance_train_ch, 2)} km."

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
                           f"over {np.round(distance_lorry_ch, 2)} km."
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
                           f"over {np.round(distance_barge_ch, 2)} km."
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
                            f"over {np.round(distance_train_rer, 2)} km."
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
                            f"over {np.round(distance_lorry_rer, 2)} km."
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
                            f"over {np.round(distance_barge_rer, 2)} km."
            }
        )

    return activity
