"""
This module contains the class BrightwayConverter, which is used to convert
Brightway2 inventories to Simapro CSV files.
"""

import csv
import datetime
from pathlib import Path

from .utils import (
    add_distri_transport,
    check_exchanges_for_conversion,
    convert_sd_to_sd2,
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
    print_unused_exchanges,
    round_floats_in_string,
)


class BrightwayConverter:
    """Convert Brightway2 inventories to SimaPro CSV files.

    The converter loads inventories exported from Brightway2 and prepares
    them for the SimaPro import format. The instance keeps references to the
    different lookup tables that are needed throughout the conversion.

    :param filepath: Path to the Brightway inventory spreadsheet.
    :type filepath: str | None
    :param data: Brightway inventories provided directly instead of loading
        them from ``filepath``.
    :type data: list | None
    :param metadata: Optional path to a YAML file containing additional
        metadata to append to the export.
    :type metadata: str | None
    :param ecoinvent_version: Version string of the ecoinvent database the
        inventories are linked to.
    :type ecoinvent_version: str
    :param export_dir: Directory where generated SimaPro CSV files are saved.
    :type export_dir: str | None
    :ivar filepath: Path to the Brightway inventory spreadsheet.
    :vartype filepath: str | None
    :ivar inventories: Brightway activities that are going to be converted.
    :vartype inventories: list[dict] | None
    :ivar simapro_blacklist: Exchanges that should not be exported.
    :vartype simapro_blacklist: dict
    :ivar simapro_fields: Order and structure of SimaPro sections.
    :vartype simapro_fields: list[str]
    :ivar simapro_units: Mapping of Brightway units to SimaPro units.
    :vartype simapro_units: dict[str, str]
    :ivar simapro_headers: Header rows used when writing the CSV file.
    :vartype simapro_headers: list[str]
    :ivar simapro_technosphere: Mapping from technosphere exchanges to
        SimaPro names.
    :vartype simapro_technosphere: dict[tuple[str, str], str]
    :ivar simapro_biosphere: Mapping from biosphere exchanges to SimaPro
        names.
    :vartype simapro_biosphere: dict[str, str]
    :ivar simapro_subcompartment: Mapping of biosphere subcompartments to
        SimaPro names.
    :vartype simapro_subcompartment: dict[str, str]
    :ivar ei_version: Version of the ecoinvent database in use.
    :vartype ei_version: str
    :ivar metadata: Optional metadata loaded from the YAML file.
    :vartype metadata: dict | None
    :ivar export_dir: Output directory for converted CSV files.
    :vartype export_dir: pathlib.Path
    """

    def __init__(
        self,
        filepath: str = None,
        data: list = None,
        metadata: str = None,
        ecoinvent_version: str = "3.9",
        export_dir: str = None,
    ):
        """Instantiate a converter that targets the SimaPro CSV format.

        When ``filepath`` is provided the inventories are loaded from the
        spreadsheet using :func:`brightpath.utils.import_bw_inventories`. If
        ``filepath`` is omitted, pre-loaded ``data`` can be supplied instead.
        Optional ``metadata`` is validated and attached to the export.

        :param filepath: Path to the Brightway inventory spreadsheet file.
        :type filepath: str | None
        :param data: Inventories loaded in memory. Used when ``filepath`` is
            ``None``.
        :type data: list | None
        :param metadata: Path to the metadata YAML file.
        :type metadata: str | None
        :param ecoinvent_version: Version of the linked ecoinvent database.
        :type ecoinvent_version: str
        :param export_dir: Directory where SimaPro exports will be written.
        :type export_dir: str | None
        :raises FileNotFoundError: If the metadata file does not exist.
        :raises ValueError: If the metadata file is not a YAML document.
        """
        self.filepath = filepath
        self.inventories = import_bw_inventories(filepath) if self.filepath else data
        self.simapro_blacklist = get_simapro_ecoinvent_blacklist()
        self.simapro_fields = get_simapro_fields_list()
        self.simapro_units = get_simapro_units()
        self.simapro_headers = get_simapro_headers()
        self.simapro_technosphere = get_simapro_technosphere()
        self.simapro_biosphere = get_simapro_biosphere()
        self.simapro_subcompartment = get_simapro_subcompartments()
        self.ei_version = ecoinvent_version

        if metadata:
            self.metadata = load_inventory_metadata(metadata)

        # export directory is the current working
        # directory unless specified otherwise
        self.export_dir = Path(export_dir) or Path.cwd()

    def format_inventories_for_simapro(self, database: str):
        """Transform the Brightway inventories into the SimaPro structure.

        This method orchestrates the conversion of each activity in the
        Brightway dataset into the row-based structure expected by SimaPro.
        The resulting structure is compatible with the CSV export performed
        by :meth:`convert_to_simapro`.

        :param database: Name of the target database to link to. Valid values
            are ``"ecoinvent"`` and ``"uvek"``.
        :type database: str
        :return: Rows ready to be written to a SimaPro CSV file.
        :rtype: list[list[str]]
        :raises ValueError: If required information is missing from the
            inventories.
        """

        rows = [
            (
                [
                    item.replace(
                        "today_date", datetime.datetime.today().strftime("%d.%m.%Y")
                    )
                ]
                if item.startswith("{Date")
                else [item]
            )
            for item in self.simapro_headers
        ]
        rows.append([])

        for activity in self.inventories:

            # first, add transport, if uvek
            if database == "uvek":
                activity = add_distri_transport(activity)
            # and flag exchanges
            activity = flag_exchanges(activity)
            dataset_name = ""
            is_a_waste_treatment_activity = is_activity_waste_treatment(
                activity, database
            )

            for field in self.simapro_fields:
                if is_a_waste_treatment_activity is True and field == "Products":
                    continue

                if (
                    is_a_waste_treatment_activity is False
                    and field == "Waste treatment"
                ):
                    continue

                rows.append([field])
                if field in ["Process", "End"]:
                    rows.append([])
                    continue

                if field == "Process name":
                    dataset_name = format_exchange_name(
                        activity["name"],
                        activity["reference product"],
                        activity["location"],
                        activity["unit"],
                        database,
                    )
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
                        string = f"{round_floats_in_string(activity['comment'])} "

                    if activity.get("source"):
                        string += f"Source: {activity['source']} "
                    # remove line breaks in string
                    string = string.replace("\n", " ")
                    rows.extend(
                        [
                            [string],
                            [],
                        ]
                    )

                if field == "Category type":
                    prod_exchange = find_production_exchange(activity)
                    if "simapro category" in prod_exchange:
                        rows.extend(
                            [
                                [prod_exchange["simapro category"].split("/")[0]],
                                [],
                            ]
                        )
                    else:
                        rows.extend([["Others"], []])

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
                    if field.lower() in activity:
                        rows.extend(
                            [
                                [activity[field.lower()]],
                                [],
                            ]
                        )
                    else:
                        if field == "Infrastructure":
                            rows.extend([["No"], []])
                        elif (
                            field == "System description"
                            and "system description" in self.metadata
                        ):
                            rows.extend(
                                [[self.metadata["system description"]["name"]], []]
                            )
                        elif (
                            field == "Literature references"
                            and "literature reference" in self.metadata
                        ):
                            rows.extend(
                                [[self.metadata["literature reference"]["name"]], []]
                            )
                        else:
                            rows.extend([["Unspecified"], []])

                if field in (
                    "Final waste flows",
                    "Non material emission",
                    "Social issues",
                    "Economic issues",
                ):
                    rows.append([])

                if field == "Waste treatment":
                    prod_exchange = find_production_exchange(activity)
                    category = get_subcategory(prod_exchange["simapro category"])

                    if prod_exchange["amount"] < 0:
                        prod_exchange["amount"] *= -1

                    rows.extend(
                        [
                            [
                                dataset_name,
                                self.simapro_units[prod_exchange["unit"]],
                                "{:.3E}".format(prod_exchange["amount"]),
                                "100",
                                "not defined",
                                category,
                                "not defined",
                            ],
                            [],
                        ]
                    )
                    prod_exchange["used"] = True

                if field == "Products":
                    prod_exchange = find_production_exchange(activity)
                    category = get_subcategory(prod_exchange["simapro category"])
                    rows.extend(
                        [
                            [
                                dataset_name,
                                self.simapro_units[prod_exchange["unit"]],
                                "{:.3E}".format(prod_exchange["amount"]),
                                "100",
                                "not defined",
                                category,
                                "not defined",
                            ],
                            [],
                        ]
                    )
                    prod_exchange["used"] = True

                if field in ["Materials/fuels", "Electricity/heat"]:

                    if field == "Materials/fuels":
                        techno_excs = list(
                            filter(
                                lambda x: x["unit"]
                                not in ["megajoule", "kilowatt hour"],
                                get_technosphere_exchanges(activity),
                            )
                        )
                    else:
                        techno_excs = list(
                            filter(
                                lambda x: x["unit"] in ["megajoule", "kilowatt hour"],
                                get_technosphere_exchanges(activity),
                            )
                        )

                    techno_excs = list(
                        filter(
                            lambda x: is_blacklisted(x, database) is False,
                            list(techno_excs),
                        )
                    )
                    techno_excs = check_exchanges_for_conversion(techno_excs, database)

                    for exc in filter(
                        lambda x: is_a_waste_treatment(x["name"], database=database)
                        is not True,
                        techno_excs,
                    ):
                        exchange_name = format_exchange_name(
                            exc["name"],
                            exc["reference product"],
                            exc.get("location", "GLO"),
                            exc["unit"],
                            database,
                        )

                        u_type = get_simapro_uncertainty_type(
                            exc.get("uncertainty type")
                        )

                        if is_a_waste_treatment_activity is True and exc["amount"] < 0:
                            exc["amount"] *= -1

                        rows.append(
                            [
                                f"{exchange_name}",
                                self.simapro_units[exc["unit"]],
                                "{:.3E}".format(exc["amount"]),
                                u_type,
                                "{:.3E}".format(
                                    convert_sd_to_sd2(exc.get("scale", 1), u_type)
                                ),
                                "{:.3E}".format(exc.get("min", 0)),
                                "{:.3E}".format(exc.get("max", 0)),
                                exc.get("comment"),
                            ]
                        )
                        exc["used"] = True

                    rows.append([])

                if field == "Resources":
                    biosphere_excs = get_biosphere_exchanges(
                        activity, "natural resource"
                    )
                    for exc in filter(
                        lambda x: is_blacklisted(x, database) is False, biosphere_excs
                    ):

                        u_type = get_simapro_uncertainty_type(
                            exc.get("uncertainty type")
                        )

                        if len(exc["categories"]) > 1:
                            sub_compartment = self.simapro_subcompartment[
                                exc["categories"][1]
                            ]
                        else:
                            sub_compartment = ""

                        rows.append(
                            [
                                f"{self.simapro_biosphere.get(exc['name'], exc['name'])}",
                                sub_compartment,
                                self.simapro_units[exc["unit"]],
                                "{:.3E}".format(exc["amount"]),
                                u_type,
                                "{:.3E}".format(
                                    convert_sd_to_sd2(exc.get("scale", 1), u_type)
                                ),
                                "{:.3E}".format(exc.get("min", 0)),
                                "{:.3E}".format(exc.get("max", 0)),
                                exc.get("comment"),
                            ]
                        )
                        exc["used"] = True
                    rows.append([])

                if field.startswith("Emissions to"):
                    biosphere_excs = get_biosphere_exchanges(
                        activity, field.split(" ")[-1].lower()
                    )
                    for exc in filter(
                        lambda x: is_blacklisted(x, database) is False, biosphere_excs
                    ):

                        if exc["name"].lower() == "water":
                            exc["unit"] = "kilogram"
                            exc["amount"] *= 1000

                        u_type = get_simapro_uncertainty_type(
                            exc.get("uncertainty type")
                        )

                        if len(exc["categories"]) > 1:
                            sub_compartment = self.simapro_subcompartment[
                                exc["categories"][1]
                            ]
                        else:
                            sub_compartment = ""

                        rows.append(
                            [
                                f"{self.simapro_biosphere.get(exc['name'], exc['name'])}",
                                sub_compartment,
                                self.simapro_units[exc["unit"]],
                                "{:.3E}".format(exc["amount"]),
                                u_type,
                                "{:.3E}".format(
                                    convert_sd_to_sd2(exc.get("scale", 1), u_type)
                                ),
                                "{:.3E}".format(exc.get("min", 0)),
                                "{:.3E}".format(exc.get("max", 0)),
                                exc.get("comment"),
                            ]
                        )
                        exc["used"] = True
                    rows.append([])

                if field == "Waste to treatment":
                    techno_excs = get_technosphere_exchanges(activity)
                    techno_excs = list(
                        filter(
                            lambda x: is_blacklisted(x, database) is False, techno_excs
                        )
                    )
                    techno_excs = check_exchanges_for_conversion(techno_excs, database)

                    for exc in filter(
                        lambda x: is_a_waste_treatment(x["name"], database=database)
                        is True,
                        techno_excs,
                    ):

                        # In SimaPro, waste inputs are positive numbers
                        if exc["amount"] < 0:
                            exc["amount"] *= -1

                        exchange_name = format_exchange_name(
                            exc["name"],
                            exc["reference product"],
                            exc.get("location", "GLO"),
                            exc["unit"],
                            database,
                        )

                        u_type = get_simapro_uncertainty_type(
                            exc.get("uncertainty type")
                        )

                        rows.append(
                            [
                                f"{exchange_name}",
                                self.simapro_units[exc["unit"]],
                                "{:.3E}".format(exc["amount"]),
                                u_type,
                                "{:.3E}".format(
                                    convert_sd_to_sd2(exc.get("scale", 1), u_type)
                                ),
                                "{:.3E}".format(exc.get("min", 0)),
                                "{:.3E}".format(exc.get("max", 0)),
                                exc.get("comment"),
                            ]
                        )
                        exc["used"] = True

                    rows.append([])

            rows.append([])

        # Add metadata: system descriptions and literature references
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

        # check that all exchanges of all activities
        # have been used
        # print it in a prettytable
        print_unused_exchanges(self.inventories)

        return rows

    def convert_to_simapro(
        self, database: str = "ecoinvent", format: str = "csv"
    ) -> [str, list]:
        """Export the converted inventories.

        The inventories are formatted using
        :meth:`format_inventories_for_simapro` and either returned as raw data
        or written to disk as a CSV file, depending on ``format``.

        :param database: Database to use when resolving exchanges. Accepted
            values are ``"ecoinvent"`` and ``"uvek"``.
        :type database: str
        :param format: Output mode. Use ``"data"`` to receive the converted
            rows instead of writing a CSV file.
        :type format: str
        :return: The CSV filepath when ``format`` is ``"csv"`` or the raw
            SimaPro data rows when ``format`` is ``"data"``.
        :rtype: str | list[list[str]]
        :raises ValueError: If an unsupported ``database`` value is supplied.
        """

        if database not in ("ecoinvent", "uvek"):
            raise ValueError("Database must be either `ecoinvent` or `uvek`")

        data = self.format_inventories_for_simapro(database)

        if format == "data":
            return data

        # check that export direct exists
        # otherwise we create it
        self.export_dir.mkdir(parents=True, exist_ok=True)

        filepath = (
            self.export_dir
            / f"simapro_{database}_{datetime.datetime.today().strftime('%d-%m-%Y')}.csv"
        )

        with open(filepath, "w", newline="", encoding="utf-8") as csvFile:
            writer = csv.writer(csvFile, delimiter=";")
            for row in data:
                writer.writerow(row)
        csvFile.close()

        return f"Inventories export to: {filepath}"
