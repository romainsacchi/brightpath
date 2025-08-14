"""
This module contains the class SimaproConverter, which is used to convert
Simapro inventories to Brightway inventory files.
"""
from . import DATA_DIR
from .utils import (
    get_simapro_technosphere,
    get_simapro_biosphere,
    get_simapro_subcompartments,
    check_simapro_inventory,
    get_waste_exchange_names,
    load_ei_biosphere_flows,
    load_biosphere_correspondence,
    remove_duplicates,
    lower_cap_first_letter
)

from pathlib import Path
import bw2io
import logging
import csv
from datetime import datetime

WASTE_TERMS = get_waste_exchange_names()

def format_technosphere_exchange(txt: str):

    location_correction = {
        "WECC, US only": "US-WECC",
        "ASCC, US only" : "US-ASCC",
        "HICC, US only": "US-HICC",
        "MRO, US only": "US-MRO",
        "NPCC, US only": "US-NPCC",
        "PR, US only": "US-PR",
        "RFC, US only": "US-RFC",
        "SERC, US only": "US-SERC",
        "TRE, US only": "US-TRE",
        "HICC": "US-HICC",
        "SERC": "US-SERC",
        "RFC": "US-RFC",
        "ASCC": "US-ASCC",
        "TRE": "US-TRE",
        "FRCC": "US-FRCC",
        "SPP": "US-SPP",
    }

    correct_name = [
        "production",
        "production mix",
        "processing",
        "market for",
        "market group for",
        "production, at grid",
        "at market",
        "cut-off, U",
        "generic production",
        "market",
        "processing, mass based",
        "transport",
        "treatment of",
        "purification"
    ]

    reference_product, name, location = "", "", ""
    if len(txt.split("|")) == 1:
        reference_product = txt.split("|")[0]
        name = reference_product

    if len(txt.split("|")) == 2:
        reference_product, name = txt.split("|")

    if len(txt.split("|")) == 3:
        reference_product, name, _ = txt.split("|")

    if len(txt.split("|")) == 4:
        reference_product, name, _, _ = txt.split("|")

    reference_product = lower_cap_first_letter(reference_product)

    if "{" in reference_product:
        reference_product, location = reference_product.split("{")
        location, reference_product_ = location.split("}")
        if len(reference_product_) > 0:
            reference_product_ = reference_product_.strip()
            reference_product = f"{reference_product} {reference_product_}"
            reference_product = reference_product.replace("  ", " ")
    else:
        location = "GLO"

    if location in ["French Guiana", "French Guinana"]:
        location = "FG"

    name = name.replace("Cut-off, U", "",)
    name = name.replace("cut-off, U", "",)

    if name[-3:] in [", U", ", S"]:
        name = name[:-3]

    if "{" in name:
        name = name.split("{")[0]

    name = name.strip()

    if name == "":
        name = reference_product

    reference_product = reference_product.strip()
    if reference_product[-1] in [",", "."]:
        reference_product = reference_product[:-1]

    if name in correct_name:
        if name in ["market for", "market group for", "treatment of"]:
            name = f"{name} {reference_product}"
        elif name == "market":
            name = f"market for {reference_product}"
        elif name == "production":
            if ", " in reference_product:
                name = f"{reference_product.split(', ')[0]} production, {', '.join(reference_product.split(', ')[1:])}"
            else:
                name = f"{reference_product} production"
        elif name == "processing":
            name = reference_product
        else:
            name = f"{reference_product} {name}"

    if name.startswith("treatment of,"):
        name = f"treatment of {reference_product}, {name.split(', ')[-1]}"

    if name == "construction":
        if ", " in reference_product:
            name = f"{reference_product.split(', ')[0]} construction, {', '.join(reference_product.split(', ')[1:])}"
        else:
            name = f"{reference_product} construction"

    if name.startswith("production, from") or name.startswith("production from"):
        name = f"{reference_product} {name}"

    location = location.replace("}", "").strip()
    location = location_correction.get(location, location)

    return name, reference_product, location

def load_ecoinvent_activities(version: str) -> list:
    with open(DATA_DIR/ "export" / f"list_ei{version}_cutoff_activities.csv") as f:
        reader = csv.reader(f)
        next(reader)
        return [l for l in reader]


def format_biosphere_exchange(exc, ei_version, bio_flows, bio_mapping):
    if "in ground" in exc["name"]:
        if ei_version not in ["3.5", "3.6", "3.7", "3.8"]:
            exc["name"] = exc["name"].replace(", in ground", "")
        exc["categories"] = ("natural resource", "in ground")

    if exc["name"].startswith("Water, well"):
        exc["name"] = "Water, well, in ground"

    if exc["name"].startswith("Water, lake"):
        exc["name"] = "Water, lake"

    if exc["name"].startswith("Water, cooling"):
        exc["name"] = "Water, cooling, unspecified natural origin"

    if exc["name"].startswith("Water,"):
        if "in air" not in exc["name"]:
            exc["categories"] = ("natural resource", "in water")

    key = (
        exc["name"],
        exc["categories"][0],
        "unspecified"
        if len(exc["categories"]) == 1
        else exc["categories"][1]
    )

    if key not in bio_flows:
        try:
            if exc["name"] in bio_mapping[exc["categories"][0]]:
                exc["name"] = bio_mapping[exc["categories"][0]][exc["name"]]
                key = list(key)
                key[0] = exc["name"]
                key = tuple(key)
        except:
            logging.warning(f"Could not find biosphere flow for {exc['name']}.")
            pass

    if key not in bio_flows:
        if exc["categories"][0] == "natural resource":
            for i in [
                "in ground",
                "biotic",
                "in air",
                "land",

            ]:
                key = list(key)
                key[2] = i
                key = tuple(key)
                if key in bio_flows:
                    exc["categories"] = (exc["categories"][0], i)
                    break

        if exc["categories"][0] == "soil":
            for i in [
                "agricultural",
                "forestry",
                "industrial",
            ]:
                key = list(key)
                key[2] = i
                key = tuple(key)
                if key in bio_flows:
                    exc["categories"] = (exc["categories"][0], i)
                    break

        if exc["categories"][0] == "air":
            for i in [
                "low population density, long-term",
                "lower stratosphere + upper troposphere",
                "non-urban air or from high stacks",
                "urban air close to ground"
            ]:
                key = list(key)
                key[2] = i
                key = tuple(key)
                if key in bio_flows:
                    exc["categories"] = (exc["categories"][0], i)
                    break

    if exc["categories"] == ('natural resource', 'in ground'):
        if ei_version in ["3.5", "3.6", "3.7", "3.8"]:
            if "in ground" not in exc["name"]:
                exc["name"] += ", in ground"


    return exc


class SimaproConverter:
    def __init__(
            self,
            filepath: str,
            ecoinvent_version: str = "3.9",
            db_name: str = None
    ):
        """
        Initialize the SimaproConverter object.

        :param data: list of Simapro inventories
        :param ecoinvent_version: ecoinvent version to use
        """

        logging.basicConfig(
            level=logging.DEBUG,
            filename="brightpath.log",  # Log file to save the entries
            filemode="a",  # Append to the log file if it exists, 'w' to overwrite
            format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        if not Path(filepath).exists():
            raise FileNotFoundError(f"File {filepath} not found.")

        self.filepath = Path(check_simapro_inventory(filepath))
        self.db_name = db_name or self.filepath.stem
        self.i = bw2io.SimaProCSVImporter(filepath=self.filepath, name=self.db_name)
        self.i.apply_strategies()
        self.i.data = remove_duplicates(self.i.data)

        self.ecoinvent_version = ecoinvent_version
        self.biosphere = get_simapro_biosphere()
        self.technosphere = get_simapro_technosphere()
        self.subcompartments = get_simapro_subcompartments()

        self.ei_biosphere_flows = load_ei_biosphere_flows()
        self.biosphere_flows_correspondence = load_biosphere_correspondence()


        self.i.db_name = self.db_name


    def check_database_name(self):

        for act in self.i.data:
            act["database"] = self.i.db_name

            for exc in act["exchanges"]:
                if exc.get("type") in ["production", "technosphere"]:
                    if "input" in exc:
                        if exc["input"][0] is None:
                            exc["input"] = (self.i.db_name, exc["input"][1])

    def convert_to_brightway(self):

        print("- format exchanges")
        internal_datasets = []
        for ds in self.i.data:

            if "Comment" in ds.get("simapro metadata", {}).keys():
                # rename "Comment" to "comment"
                ds["comment"] = ds["simapro metadata"]["Comment"]
                del ds["simapro metadata"]["Comment"]

            ds["name"], ds["reference product"], ds["location"] = format_technosphere_exchange(ds["name"])
            internal_datasets.append(
                (
                    ds["name"], ds["reference product"], ds["location"]
                )
            )

            for exc in ds["exchanges"]:
                exc["simapro name"] = exc["name"]

                if exc["type"] == "production":
                    exc["name"] = ds["name"]
                    exc["product"] = ds["reference product"]
                    exc["reference product"] = ds["reference product"]
                    exc["location"] = ds["location"]

                    if any(x in exc["name"] for x in WASTE_TERMS):
                        logging.info(
                            msg=f"{exc['name']} considered waste treatment "
                                f"(input amount made negative)."
                        )
                        print(f"{exc['name']} considered waste treatment: sign of production exchange made negative.")
                        exc["amount"] *= -1

                if exc["type"] in ["technosphere", "substitution"]:
                    exc["name"], exc["product"], exc["location"] = format_technosphere_exchange(exc["name"])
                    exc["reference product"] = exc["product"]

                    if any(x in exc["name"] for x in WASTE_TERMS):
                        logging.info(
                            msg=f"{exc['name']} considered waste treatment "
                                f"(input amount made negative)."
                        )
                        exc["amount"] *= -1

                if exc["type"] == "substitution":
                    exc["type"] = "technosphere"
                    exc["amount"] *= -1

                if exc["type"] == "biosphere":
                    format_biosphere_exchange(
                        exc,
                        self.ecoinvent_version,
                        self.ei_biosphere_flows,
                        self.biosphere_flows_correspondence
                    )

        self.check_database_name()

        print("- remove empty datasets")
        self.remove_empty_datasets()
        print("- remove empty exchanges")
        self.remove_empty_exchanges()
        print("- check inventories")
        self.check_inventories()
        print("Done!")

    def remove_empty_datasets(self):
        self.i.data = [
            ds for ds in self.i.data
            if len(ds["exchanges"]) >= 1
        ]

    def remove_empty_exchanges(self):
        for ds in self.i.data:
            ds["exchanges"] = [e for e in ds["exchanges"] if e["amount"] != 0.0]

    def check_inventories(self):

        for ds in self.i.data:
            if len([x for x in ds["exchanges"] if x["type"] == "production"]) != 1:
                print(f"WARNING: {ds['name'], ds['reference product'], ds['location']} has more"
                      f"than one production flow.")

            for e in ds["exchanges"]:
                if e["type"] not in ["production", "technosphere", "biosphere"]:
                    print(f"WARNING: {ds['name'], ds['reference product'], ds['location']} has an"
                          f"unknown flow type {e['type']}.")

                if e["type"] == "production":
                    if (e["name"], e["product"], e["location"]) != (
                            ds["name"], ds["reference product"], ds["location"]
                    ):
                        print(f"WARNING: {ds['name'], ds['reference product'], ds['location']} has an"
                              f"incorrect production flow {e}.")
