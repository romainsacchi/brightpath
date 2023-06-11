import bw2io

from . import DATA_DIR
from typing import Dict, Tuple
import json
import yaml
from bw2io.importers.excel import ExcelImporter
from pathlib import Path
from voluptuous import Schema, Required, Optional, ALLOW_EXTRA
from prettytable import PrettyTable


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


def get_simapro_blacklist():
    # Load the list of Simapro biosphere flows that
    # should be excluded from the export

    filename = "simapro_blacklist.yaml"
    filepath = DATA_DIR / "export" / filename
    # read YAML file
    with open(filepath, 'r') as stream:
        try:
            data = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    return data


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
    with open(filepath, 'r') as stream:
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
    with open(filepath, 'r') as stream:
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
    with open(filepath, 'r') as stream:
        try:
            data = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

    return data


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
    with open(filepath, 'r') as stream:
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

    MANDATORY_TECH_EXC_KEYS = [
        "name",
        "reference product",
        "location",
        "unit"
    ]

    MANDATORY_BIO_EXC_KEYS = [
        "name",
        "categories",
        "unit"
    ]

    faulty_exchanges = []

    for activity in data:
        for exchange in activity["exchanges"]:
            if exchange["type"] in ["production", "technosphere"]:
                if not all(
                        key in exchange.keys()
                        for key in MANDATORY_TECH_EXC_KEYS
                ):
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
                if not all(
                        key in exchange.keys()
                        for key in MANDATORY_BIO_EXC_KEYS
                ):
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
        table.field_names = ["Name", "Reference product", "Location", "Categories", "Unit"]
        for exc in faulty_exchanges:
            table.add_row(exc)

        print(table)
        raise ValueError("Some exchanges do not have mandatory exchange "
                         "fields (marked 'None' in table above).")


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
        raise FileNotFoundError(
            "The file could not be found."
        )
    # check that suffix is .xlsx
    if filepath.suffix != ".xlsx":
        raise ValueError(
            "The file must be a .xlsx spreadsheet."
        )

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
    schema = Schema(
        {
            Required("system description"): [
                {
                    Required("name"): str,
                    Required("category"): str,
                    Optional("description"): str,
                    Optional("cut-off rules"): str,
                    Optional("energy model"): str,
                    Optional("transport model"): str,
                    Optional("allocation rules"): str,
                }
            ],
            Required("literature reference"): [
                {
                    Required("name"): str,
                    Required("category"): str,
                    Optional("documentation link"): str,
                    Optional("comment"): str,
                    Optional("description"): str,
                }
            ],
        },
        extra=ALLOW_EXTRA,
    )

    # Validate against schema
    validated_data = schema(metadata)

    # check that values for key `name`
    # are unique for each item in `system description`
    # and `literature reference`

    # check `system description`
    names = [item["name"] for item in validated_data["system description"]]
    if len(names) != len(set(names)):
        raise ValueError(
            "The values for key `name` must be unique for each item in `system description`."
        )

    # check `literature reference`
    names = [item["name"] for item in validated_data["literature reference"]]
    if len(names) != len(set(names)):
        raise ValueError(
            "The values for key `name` must be unique for each item in `literature reference`."
        )

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
        raise FileNotFoundError(
            "The file could not be found."
        )
    # check that suffix is .yaml
    if filepath.suffix != ".yaml":
        raise ValueError(
            "The file must be a .yaml file."
        )

    # read YAML file
    with open(filepath, 'r') as stream:
        try:
            data = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

    # check that metadata is valid
    data = check_metadata(data)

    return data


def is_activity_waste_treatment(activity: dict) -> bool:
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

    if is_a_waste_treatment(activity["name"]) is True:
        return True

    return False


def is_a_waste_treatment(name: str) -> bool:
    """
    Detect if name contains typical to waste treatment.
    :param name: exchange name
    :return: bool.
    """
    WASTE_TERMS = get_waste_exchange_names()

    if any(term.lower() in name.lower() for term in WASTE_TERMS):
        return True


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
    return [
        exc for exc in activity["exchanges"]
        if exc["type"] == "technosphere"
    ]


def get_biosphere_exchanges(activity: dict, category: str = None) -> list:
    """
    Get the technosphere exchanges of the given activity.
    :param activity: activity
    :param category: biosphere category
    :return: biosphere exchanges
    """
    return [
        exc for exc in activity["exchanges"]
        if exc["type"] == "biosphere"
        and exc.get("categories")[0] == category
    ]


def format_exchange_name(name: str, reference_product: str, location: str) -> str:
    exchange_name = f"{reference_product.capitalize()} {{{location}}}"

    for i in ["market for", "market group for"]:
        if i in name:
            exchange_name += f"| {i} {reference_product.lower()}"

    if "production" in name:
        if len(reference_product.split(", ")) > 1:
            for i, prod in enumerate(reference_product.split(", ")):
                if i == 0:
                    exchange_name += f"| {prod.lower()} production, "
                else:
                    exchange_name += f"{prod.lower()}"

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

def is_blacklisted(name: str) -> bool:
    """
    Check whether a name is blacklisted or not
    :param name: name
    :return: bool
    """

    return True if name in get_simapro_blacklist() else False