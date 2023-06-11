"""
This module contains the class Converter, which is used to convert
Brightway2 inventories to Simapro CSV files.
"""

from .utils import (
    get_simapro_blacklist,
    get_simapro_fields_list,
    get_simapro_units,
    get_simapro_headers,
    import_bw_inventories,
    get_simapro_technosphere,
    get_simapro_biosphere,
    load_inventory_metadata,
    is_activity_waste_treatment,
    find_production_exchange,
    get_technosphere_exchanges,
    is_a_waste_treatment,
    format_exchange_name,
    get_simapro_uncertainty_type, get_biosphere_exchanges,
    is_blacklisted
)

import datetime
import csv
from pathlib import Path


class Converter:
    """
    Convert Brightway2 inventories to Simapro CSV files.
    """

    def __init__(
            self,
            filepath: str,
            metadata: str = None,
            ecoinvent_version: str = "3.9",
            export_dir: str = None
    ):
        """
        :param filepath: path to the BW inventory spreadsheet file
        """
        self.filepath = filepath
        self.inventories = import_bw_inventories(filepath)
        self.simapro_blacklist = get_simapro_blacklist()
        self.simapro_fields = get_simapro_fields_list()
        self.simapro_units = get_simapro_units()
        self.simapro_headers = get_simapro_headers()
        self.simapro_technosphere = get_simapro_technosphere()
        self.simapro_biosphere = get_simapro_biosphere()
        self.ei_version = ecoinvent_version

        if metadata:
            self.metadata = load_inventory_metadata(metadata)

        # export directory is the current working
        # directory unless specified otherwise
        self.export_dir = Path.cwd() or Path(export_dir)

    def format_inventories_for_simapro(self):
        """
        Format inventories to Simapro format
        :return: list
        """

        rows = [
            [item.replace("today_date", datetime.datetime.today().strftime("%d.%m.%Y"))]
            if item.startswith("{date") else [item]
            for item in self.simapro_headers

        ]
        rows.append([])

        for activity in self.inventories:
            dataset_name = ""

            for field in self.simapro_fields:

                if (
                        is_activity_waste_treatment(activity) is True
                        and field == "Products"
                ):
                    continue

                if (
                        is_activity_waste_treatment(activity) is False
                        and field == "Waste treatment"
                ):
                    continue

                rows.append([field])
                if field in ["Process", "End"]:
                    rows.append([])
                    continue

                if field == "Process name":
                    dataset_name = f"{activity['name'].capitalize()} {{{activity.get('location', 'GLO')}}} | Cut-off U"
                    rows.extend(
                        [
                            [dataset_name],
                            [],
                        ]
                    )

                if field == "Type":
                    rows.extend(
                        [
                            ["Unit process"],
                            [],
                        ]
                    )

                if field == "Comment":
                    string = ""
                    if activity.get("comment"):
                        string = f"{activity['comment']} "
                    if activity.get("source"):
                        string += f"Source: {activity['source']} "
                    rows.extend(
                        [
                            [string],
                            [],
                        ]
                    )

                if field == "Category type":
                    if "classifications" in activity:
                        rows.append([activity["classifications"]])
                    else:
                        rows.append([])

                if field == "Geography":
                    rows.extend(
                        [
                            [activity["location"]],
                            [],
                        ]
                    )

                if field == "Date":
                    rows.extend(
                        [
                            [f"{datetime.datetime.today():%d.%m.%Y}"],
                            [],
                        ]
                    )

                if field in (
                        "Time Period",
                        "Record",
                        "Generator",
                        "Cut off rules",
                        "Capital goods",
                        "Technology",
                        "Representativeness",
                        "Boundary with nature",
                        "Infrastructure",
                        "External documents",
                        "System description",
                        "Allocation rules",
                        "Literature references",
                        "Collection method",
                        "Data treatment",
                        "Verification",
                        "Non material emission",
                        "Social issues",
                        "Economic issues",
                ):
                    if field.lower() in activity:
                        rows.extend(
                            [
                                [activity[field.lower()]],
                                [],
                            ]
                        )
                    else:
                        rows.append([])

                if field == "Waste treatment":
                    prod_exchange = find_production_exchange(activity)
                    rows.extend(
                        [
                            [
                                dataset_name,
                                prod_exchange["unit"],
                                prod_exchange["amount"],
                                "not defined",
                                prod_exchange.get("categories"),
                            ],
                            []
                        ]
                    )

                if field == "Products":
                    prod_exchange = find_production_exchange(activity)
                    rows.extend(
                        [
                            [
                                dataset_name,
                                self.simapro_units[prod_exchange["unit"]],
                                "{:.3E}".format(prod_exchange["amount"]),
                                "100%",
                                "not defined",
                                prod_exchange.get("categories"),
                            ],
                            []
                        ]
                    )

                if field == "Materials/fuels":
                    techno_excs = get_technosphere_exchanges(activity)
                    for exc in filter(lambda x: is_a_waste_treatment(x["name"]) is not True, techno_excs):

                        exchange_name = format_exchange_name(
                            exc["name"], exc["reference product"], exc.get("location", "GLO")
                        )

                        rows.append(
                            [
                                f"{exchange_name} | Cut-off, U",
                                self.simapro_units[exc["unit"]],
                                "{:.3E}".format(exc["amount"]),
                                get_simapro_uncertainty_type(exc.get("uncertainty type")),
                                "{:.3E}".format(exc.get("loc", 0)),
                                "{:.3E}".format(exc.get("scale", 0)),
                                exc.get("negative", 0),
                                exc.get("comment"),
                            ]
                        )

                    rows.append([])

                if field == "Resources":
                    biosphere_excs = get_biosphere_exchanges(activity, "natural resource")
                    for exc in filter(lambda x: is_blacklisted(x) is False, biosphere_excs):
                        rows.append(
                            [
                                f"{self.simapro_biosphere.get(exc['name'], exc['name'])}",
                                "",
                                self.simapro_units[exc["unit"]],
                                "{:.3E}".format(exc["amount"]),
                                get_simapro_uncertainty_type(exc.get("uncertainty type")),
                                "{:.3E}".format(exc.get("loc", 0)),
                                "{:.3E}".format(exc.get("scale", 0)),
                                exc.get("negative", 0),
                                exc.get("comment"),
                            ]
                        )
                        rows.append([])

                if field.startswith("Emissions to"):
                    biosphere_excs = get_biosphere_exchanges(activity, field.split(" ")[-1].lower())
                    for exc in filter(lambda x: is_blacklisted(x) is False, biosphere_excs):

                        if exc["name"].lower() == "water":
                            exc["unit"] = "kilogram"
                            exc["amount"] /= 1000

                        rows.append(
                            [
                                f"{self.simapro_biosphere.get(exc['name'], exc['name'])}",
                                "",
                                self.simapro_units[exc["unit"]],
                                "{:.3E}".format(exc["amount"]),
                                get_simapro_uncertainty_type(exc.get("uncertainty type")),
                                "{:.3E}".format(exc.get("loc", 0)),
                                "{:.3E}".format(exc.get("scale", 0)),
                                exc.get("negative", 0),
                                exc.get("comment"),
                            ]
                        )
                    rows.append([])

                if field == "Waste to treatment":
                    techno_excs = get_technosphere_exchanges(activity)
                    for exc in filter(lambda x: is_a_waste_treatment(x["name"]) is True, techno_excs):

                        # In SimaPro, waste inputs are positive numbers
                        if exc["amount"] < 0:
                            exc["amount"] *= -1

                        exchange_name = format_exchange_name(
                            exc["name"], exc["reference product"], exc.get("location", "GLO")
                        )

                        rows.append(
                            [
                                f"{exchange_name} | Cut-off, U",
                                self.simapro_units[exc["unit"]],
                                "{:.3E}".format(exc["amount"]),
                                get_simapro_uncertainty_type(exc.get("uncertainty type")),
                                "{:.3E}".format(exc.get("loc", 0)),
                                "{:.3E}".format(exc.get("scale", 0)),
                                exc.get("negative", 0),
                                exc.get("comment"),
                            ]
                        )

                    rows.append([])

                rows.append([])

            rows.append([])

        # Add metadata: system descriptions and literature references
        for field in (
                "System description",
                "Literature reference",
        ):
            if field.lower() in self.metadata:
                rows.extend([[field], []])
                for system in self.metadata[field.lower()]:
                    for key, val in system.items():
                        rows.extend(
                            [
                                [key],
                                [val],
                                [],
                            ]
                        )
                    rows.append([])
                rows.extend([["End"], []])

        return rows

    def convert_to_simapro(self):
        """
        Convert the inventories to Simapro CSV files.
        """

        data = self.format_inventories_for_simapro()

        # check that export direct exists
        # otherwise we create it
        self.export_dir.mkdir(parents=True, exist_ok=True)

        filepath = self.export_dir / f"simapro_{datetime.datetime.today().strftime('%d-%m-%Y')}"

        with open(filepath, "w", newline="", encoding="utf-8") as csvFile:
            writer = csv.writer(csvFile, delimiter=";")
            for row in data:
                writer.writerow(row)
        csvFile.close()

        return f"Inventories export to: {filepath}"
