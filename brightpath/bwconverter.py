"""
This module contains the class BrightwayConverter, which is used to convert
Brightway2 inventories to Simapro CSV files.
"""

import csv
import datetime
import logging
from copy import deepcopy
from pathlib import Path

from .utils import (
    add_distri_transport,
    check_exchanges_for_conversion,
    collect_unused_exchanges,
    convert_sd_to_sd2,
    escape_spreadsheet_formula,
    find_production_exchange,
    flag_exchanges,
    format_exchange_name,
    get_biosphere_exchanges,
    get_simapro_biosphere,
    get_simapro_ecoinvent_blacklist,
    get_simapro_fields_list,
    get_simapro_headers,
    get_simapro_subcompartments,
    get_simapro_technosphere,
    get_simapro_uncertainty_type,
    get_simapro_units,
    get_subcategory,
    get_technosphere_exchanges,
    import_bw_inventories,
    is_a_waste_treatment,
    is_activity_waste_treatment,
    is_blacklisted,
    load_inventory_metadata,
    round_floats_in_string,
    validate_brightway_inventory,
)

logger = logging.getLogger(__name__)


class BrightwayConverter:
    """
    Convert Brightway2 inventories to Simapro CSV files.
    """

    def __init__(
        self,
        filepath: str = None,
        data: list = None,
        metadata: str = None,
        ecoinvent_version: str = "3.9",
        export_dir: str = None,
    ):
        """
        :param filepath: path to the BW inventory spreadsheet file
        """
        if filepath and data is not None:
            raise ValueError("Provide either `filepath` or `data`, not both.")
        if not filepath and data is None:
            raise ValueError("Provide either `filepath` or `data`.")

        self.filepath = filepath
        self.inventories = import_bw_inventories(filepath) if self.filepath else deepcopy(data)
        validate_brightway_inventory(self.inventories)
        self.simapro_blacklist = get_simapro_ecoinvent_blacklist()
        self.simapro_fields = get_simapro_fields_list()
        self.simapro_units = get_simapro_units()
        self.simapro_headers = get_simapro_headers()
        self.simapro_technosphere = get_simapro_technosphere()
        self.simapro_biosphere = get_simapro_biosphere()
        self.simapro_subcompartment = get_simapro_subcompartments()
        self.ei_version = ecoinvent_version
        self.metadata = {}
        self.unused_exchanges = []

        if metadata:
            self.metadata = load_inventory_metadata(metadata)

        # export directory is the current working
        # directory unless specified otherwise
        self.export_dir = Path(export_dir) if export_dir else Path.cwd()

    def format_inventories_for_simapro(self, database: str):
        """
        Format inventories to Simapro format.
        :param database: name of the database to link to.
        :return: list
        """
        rows = self._header_rows()
        inventories = [self._prepare_activity(activity, database) for activity in self.inventories]

        for activity in inventories:
            rows.extend(self._format_activity_rows(activity, database))

        rows.extend(self._metadata_rows())
        self.unused_exchanges = collect_unused_exchanges(inventories)
        if self.unused_exchanges:
            logger.warning("%s exchanges were not converted.", len(self.unused_exchanges))
        else:
            logger.info("All exchanges have been converted.")

        return rows

    def _header_rows(self):
        rows = [
            (
                [item.replace("today_date", datetime.datetime.today().strftime("%d.%m.%Y"))]
                if item.startswith("{Date")
                else [item]
            )
            for item in self.simapro_headers
        ]
        rows.append([])
        return rows

    def _metadata_rows(self):
        rows = []
        for field in (
            "System description",
            "Literature reference",
        ):
            if field.lower() in self.metadata:
                rows.extend([[field], []])
                for key, val in self.metadata[field.lower()].items():
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

    def _prepare_activity(self, activity: dict, database: str):
        activity = deepcopy(activity)
        if database == "uvek":
            activity = add_distri_transport(activity)
        return flag_exchanges(activity)

    def _format_activity_rows(self, activity: dict, database: str):
        rows = []
        dataset_name = ""
        is_a_waste_treatment_activity = is_activity_waste_treatment(activity, database)

        for field in self.simapro_fields:
            if is_a_waste_treatment_activity is True and field == "Products":
                continue

            if is_a_waste_treatment_activity is False and field == "Waste treatment":
                continue

            rows.append([field])
            if field in ["Process", "End"]:
                rows.append([])
                continue

            if field == "Process name":
                dataset_name = format_exchange_name(
                    activity["name"], activity["reference product"], activity["location"], activity["unit"], database
                )
                rows.extend([[dataset_name], []])

            if field == "Type":
                rows.extend([["Unit process"], []])

            if field == "Comment":
                rows.extend([[self._comment_text(activity)], []])

            if field == "Category type":
                prod_exchange = find_production_exchange(activity)
                rows.extend([[prod_exchange["simapro category"].split("/")[0]], []])

            if field == "Geography":
                rows.extend([[activity["location"]], []])

            if field == "Date":
                rows.extend([[f"{datetime.datetime.today():%d.%m.%Y}"], []])

            if field in (
                "Time period",
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
            ):
                rows.extend(self._activity_metadata_field_rows(field, activity))

            if field in (
                "Final waste flows",
                "Non material emission",
                "Social issues",
                "Economic issues",
            ):
                rows.append([])

            if field in ("Waste treatment", "Products"):
                rows.extend(self._product_or_waste_rows(field, dataset_name, activity))

            if field in ["Materials/fuels", "Electricity/heat"]:
                rows.extend(
                    self._technosphere_rows(
                        field,
                        activity,
                        database,
                        is_a_waste_treatment_activity,
                    )
                )

            if field == "Resources":
                rows.extend(self._biosphere_rows(activity, database, "natural resource"))

            if field.startswith("Emissions to"):
                rows.extend(self._biosphere_rows(activity, database, field.split(" ")[-1].lower()))

            if field == "Waste to treatment":
                rows.extend(self._waste_to_treatment_rows(activity, database))

        rows.append([])
        return rows

    def _comment_text(self, activity: dict) -> str:
        string = ""
        if activity.get("comment"):
            string = f"{round_floats_in_string(activity['comment'])} "

        if activity.get("source"):
            string += f"Source: {activity['source']} "

        return string.replace("\n", " ")

    def _activity_metadata_field_rows(self, field: str, activity: dict):
        if field.lower() in activity:
            return [[activity[field.lower()]], []]

        if field == "Infrastructure":
            return [["No"], []]
        if field == "System description" and "system description" in self.metadata:
            return [[self.metadata["system description"]["name"]], []]
        if field == "Literature references" and "literature reference" in self.metadata:
            return [[self.metadata["literature reference"]["name"]], []]
        return [["Unspecified"], []]

    def _product_or_waste_rows(self, field: str, dataset_name: str, activity: dict):
        prod_exchange = find_production_exchange(activity)
        category = get_subcategory(prod_exchange["simapro category"])
        amount = prod_exchange["amount"]
        if field == "Waste treatment" and amount < 0:
            amount = abs(amount)

        prod_exchange["used"] = True
        return [
            [
                dataset_name,
                self.simapro_units[prod_exchange["unit"]],
                "{:.3E}".format(amount),
                "100",
                "not defined",
                category,
                "not defined",
            ],
            [],
        ]

    def _technosphere_rows(self, field: str, activity: dict, database: str, is_waste_treatment_activity: bool):
        rows = []
        energy_units = {"megajoule", "kilowatt hour"}
        want_energy = field == "Electricity/heat"

        for source_exc in get_technosphere_exchanges(activity):
            is_energy = source_exc["unit"] in energy_units
            if is_energy != want_energy:
                continue
            if is_blacklisted(source_exc, database):
                continue
            if is_a_waste_treatment(source_exc["name"], database=database):
                continue

            exc = check_exchanges_for_conversion([source_exc], database)[0]
            if is_waste_treatment_activity is True and exc["amount"] < 0:
                exc["amount"] = abs(exc["amount"])

            rows.append(self._technosphere_exchange_row(exc, database))
            source_exc["used"] = True

        rows.append([])
        return rows

    def _waste_to_treatment_rows(self, activity: dict, database: str):
        rows = []
        for source_exc in get_technosphere_exchanges(activity):
            if is_blacklisted(source_exc, database):
                continue
            if not is_a_waste_treatment(source_exc["name"], database=database):
                continue

            exc = check_exchanges_for_conversion([source_exc], database)[0]
            if exc["amount"] < 0:
                exc["amount"] = abs(exc["amount"])

            rows.append(self._technosphere_exchange_row(exc, database))
            source_exc["used"] = True

        rows.append([])
        return rows

    def _technosphere_exchange_row(self, exc: dict, database: str):
        exchange_name = format_exchange_name(
            exc["name"], exc["reference product"], exc.get("location", "GLO"), exc["unit"], database
        )
        u_type = get_simapro_uncertainty_type(exc.get("uncertainty type"))
        return [
            f"{exchange_name}",
            self.simapro_units[exc["unit"]],
            "{:.3E}".format(exc["amount"]),
            u_type,
            "{:.3E}".format(convert_sd_to_sd2(exc.get("scale", 1), u_type)),
            "{:.3E}".format(exc.get("min", 0)),
            "{:.3E}".format(exc.get("max", 0)),
            exc.get("comment"),
        ]

    def _biosphere_rows(self, activity: dict, database: str, category: str):
        rows = []
        for source_exc in get_biosphere_exchanges(activity, category):
            if is_blacklisted(source_exc, database):
                continue

            exc = deepcopy(source_exc)
            if category != "natural resource" and exc["name"].lower() == "water":
                exc["unit"] = "kilogram"
                exc["amount"] *= 1000

            rows.append(self._biosphere_exchange_row(exc))
            source_exc["used"] = True

        rows.append([])
        return rows

    def _biosphere_exchange_row(self, exc: dict):
        u_type = get_simapro_uncertainty_type(exc.get("uncertainty type"))
        return [
            f"{self.simapro_biosphere.get(exc['name'], exc['name'])}",
            self._subcompartment(exc),
            self.simapro_units[exc["unit"]],
            "{:.3E}".format(exc["amount"]),
            u_type,
            "{:.3E}".format(convert_sd_to_sd2(exc.get("scale", 1), u_type)),
            "{:.3E}".format(exc.get("min", 0)),
            "{:.3E}".format(exc.get("max", 0)),
            exc.get("comment"),
        ]

    def _subcompartment(self, exc: dict) -> str:
        if len(exc["categories"]) <= 1:
            return ""

        subcategory = exc["categories"][1]
        if subcategory not in self.simapro_subcompartment:
            raise ValueError(
                f"No SimaPro subcompartment mapping for {subcategory!r} " f"on biosphere exchange {exc.get('name')!r}."
            )
        return self.simapro_subcompartment[subcategory]

    def convert_to_simapro(self, database: str = "ecoinvent", format: str = "csv", filename: str = None) -> [str, list]:
        """
        Convert the inventories to Simapro CSV files.
        :param database: Name of the database to link to. Default is `ecoinvent`, but can be `uvek`.
        """

        if database not in ("ecoinvent", "uvek"):
            raise ValueError("Database must be either `ecoinvent` or `uvek`")

        if format not in ("csv", "data"):
            raise ValueError("Format must be either `csv` or `data`.")

        data = self.format_inventories_for_simapro(database)

        if format == "data":
            return data

        filepath = self._export_filepath(database, filename)
        filepath.parent.mkdir(parents=True, exist_ok=True)

        with open(filepath, "w", newline="", encoding="utf-8") as csvFile:
            writer = csv.writer(csvFile, delimiter=";")
            for row in data:
                writer.writerow([escape_spreadsheet_formula(value) for value in row])

        return f"Inventories export to: {filepath}"

    def _export_filepath(self, database: str, filename: str = None) -> Path:
        if filename:
            filepath = Path(filename)
            if filepath.suffix == "":
                filepath = filepath.with_suffix(".csv")
            if not filepath.is_absolute():
                filepath = self.export_dir / filepath
        else:
            stamp = datetime.datetime.today().strftime("%Y-%m-%d_%H%M%S")
            filepath = self.export_dir / f"simapro_{database}_{stamp}.csv"

        if not filepath.exists():
            return filepath

        stem = filepath.stem
        suffix = filepath.suffix
        parent = filepath.parent
        counter = 1
        while True:
            candidate = parent / f"{stem}_{counter}{suffix}"
            if not candidate.exists():
                return candidate
            counter += 1
