"""
This module contains the class SimaproConverter, which is used to convert
Simapro inventories to Brightway inventory files.
"""

from .utils import (
    get_simapro_technosphere,
    get_simapro_biosphere,
    get_simapro_subcompartments,
    search_for_forbidden_units,
    get_waste_exchange_names,
    load_ei_biosphere_flows,
    load_biosphere_correspondence
)

from pathlib import Path
import bw2io
import logging

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
        "TRE": "US-TRE"
    }

    correct_name = [
        "production",
        "production mix",
        "processing",
        "market for",
        "production, at grid",
        "at market",
        "cut-off, U",
        "generic production",
        "market",
        "processing, mass based",
        "transport",
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

    name = name.strip()

    name = name.lower().replace("cut-off, u", "")
    if name == "":
        name = reference_product

    reference_product = reference_product.strip()
    if reference_product[-1] in [",", "."]:
        reference_product = reference_product[:-1]

    if name in correct_name:
        if name in ["market for", "market group for"]:
            name = f"{name} {reference_product}"
        elif name == "market":
            name = f"market for {reference_product}"
        else:
            name = f"{reference_product} ({name})"
    location = location.replace("}", "").strip()
    location = location_correction.get(location, location)

    return name, reference_product, location


def format_biosphere_exchange(exc, ei_version, bio_flows, bio_mapping):

    if ei_version == "3.9":
        if "in ground" in exc["name"]:
            exc["name"] = exc["name"].replace(", in ground", "")
            exc["categories"] = ("natural resource", "in ground")

    if exc["name"].startswith("Water, well"):
        exc["name"] = "Water, well, in ground"

    key = (
        exc["name"],
        exc["categories"][0],
        "unspecified"
        if len(exc["categories"]) == 1
        else exc["categories"][1]
    )

    if key not in bio_flows:
        if exc["name"] in bio_mapping[exc["categories"][0]]:
            exc["name"] = bio_mapping[exc["categories"][0]][exc["name"]]
            key = list(key)
            key[0] = exc["name"]
            key = tuple(key)

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
    return exc

class SimaproConverter:
    def __init__(
            self,
            filepath: str,
            ecoinvent_version: str = "3.9",
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

        self.filepath = search_for_forbidden_units(filepath)
        self.i = bw2io.SimaProCSVImporter(self.filepath)
        self.i.apply_strategies()
        self.ecoinvent_version = ecoinvent_version
        self.biosphere = get_simapro_biosphere()
        self.technosphere = get_simapro_technosphere()
        self.subcompartments = get_simapro_subcompartments()

        self.ei_biosphere_flows = load_ei_biosphere_flows()
        self.biosphere_flows_correspondence = load_biosphere_correspondence()

    def convert_to_brightway(self):

        print("- format exchanges")
        internal_datasets = []
        for ds in self.i.data:
            ds["name"], ds["reference product"], ds["location"] = format_technosphere_exchange(ds["name"])
            internal_datasets.append(
                (
                    ds["name"], ds["reference product"], ds["location"]
                )
            )

            for exc in ds["exchanges"]:
                if exc["type"] == "production":
                    exc["name"] = ds["name"]
                    exc["product"] = ds["reference product"]
                    exc["location"] = ds["location"]

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
