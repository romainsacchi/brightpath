"""
This module contains the class SimaproConverter, which is used to convert
Simapro inventories to Brightway inventory files.
"""

import csv
import logging
import numbers
import re
import tempfile
from copy import deepcopy
from pathlib import Path

import bw2io
import xlsxwriter

from . import DATA_DIR
from .utils import (
    ALLOWED_BIOSPHERE_CATEGORIES,
    check_simapro_inventory,
    ensure_unique_datasets,
    get_simapro_biosphere,
    get_simapro_subcompartments,
    get_simapro_technosphere,
    get_waste_exchange_names,
    load_biosphere_correspondence,
    load_ei_biosphere_flows,
    load_simapro_brightway_biosphere_mapping,
    lower_cap_first_letter,
)

WASTE_TERMS = get_waste_exchange_names()
logger = logging.getLogger(__name__)

EXCEL_ACTIVITY_SKIP_FIELDS = {
    "database",
    "exchanges",
    "name",
    "parameters",
    "simapro metadata",
}
EXCEL_EXCHANGE_SKIP_FIELDS = {"input", "output"}
EXCEL_ACTIVITY_FIELD_ORDER = [
    "reference product",
    "unit",
    "location",
    "code",
    "comment",
    "type",
    "categories",
]
EXCEL_EXCHANGE_FIELD_ORDER = [
    "name",
    "amount",
    "database",
    "reference product",
    "product",
    "location",
    "unit",
    "categories",
    "type",
    "formula",
    "uncertainty type",
    "loc",
    "scale",
    "shape",
    "minimum",
    "maximum",
    "simapro name",
    "comment",
]


def _version_tuple(version: str) -> tuple:
    try:
        return tuple(int(part) for part in version.split("."))
    except ValueError:
        return tuple()


def _is_version_at_least(version: str, minimum: str) -> bool:
    version_parts = _version_tuple(version)
    minimum_parts = _version_tuple(minimum)
    if not version_parts or not minimum_parts:
        return False

    length = max(len(version_parts), len(minimum_parts))
    return version_parts + (0,) * (length - len(version_parts)) >= (
        minimum_parts + (0,) * (length - len(minimum_parts))
    )


def format_technosphere_exchange(txt: str):
    if not isinstance(txt, str) or not txt.strip():
        raise ValueError("Cannot parse an empty SimaPro technosphere exchange name.")

    parts = [part.strip() for part in txt.split("|")]
    if len(parts) > 4:
        raise ValueError(f"Cannot parse SimaPro technosphere exchange name with too many fields: {txt!r}.")

    location_correction = {
        "WECC, US only": "US-WECC",
        "ASCC, US only": "US-ASCC",
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
        "Europe, without Russia and Turkey": "Europe, without Russia and Türkiye",
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
        "purification",
    ]

    reference_product, name, location = "", "", ""
    if len(parts) == 1:
        reference_product = parts[0]
        name = reference_product

    if len(parts) == 2:
        reference_product, name = parts

    if len(parts) == 3:
        reference_product, name, _ = parts

    if len(parts) == 4:
        reference_product, name, _, _ = parts

    _validate_braces(reference_product, txt)
    _validate_braces(name, txt)

    reference_product = lower_cap_first_letter(reference_product)

    if "{" in reference_product:
        reference_product, location = reference_product.split("{", 1)
        location, reference_product_ = location.split("}", 1)
        if not location.strip():
            raise ValueError(f"Cannot parse SimaPro exchange name with empty location: {txt!r}.")
        if len(reference_product_) > 0:
            reference_product_ = reference_product_.strip()
            reference_product = f"{reference_product} {reference_product_}"
            reference_product = reference_product.replace("  ", " ")
    else:
        location = "GLO"

    if location in ["French Guiana", "French Guinana"]:
        location = "FG"

    name = name.replace(
        "Cut-off, U",
        "",
    )
    name = name.replace(
        "cut-off, U",
        "",
    )

    if name[-3:] in [", U", ", S"]:
        name = name[:-3]

    if "{" in name:
        name = name.split("{")[0]

    name = name.strip()

    if name == "":
        name = reference_product

    reference_product = reference_product.strip()
    if reference_product and reference_product[-1] in [",", "."]:
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

    if not name.strip() or not reference_product.strip():
        raise ValueError(f"Could not parse non-empty name and reference product from {txt!r}.")

    return name, reference_product, location


def _validate_braces(value: str, original: str) -> None:
    if value.count("{") != value.count("}"):
        raise ValueError(f"Cannot parse malformed SimaPro location braces in {original!r}.")
    if value.count("{") > 1 or value.count("}") > 1:
        raise ValueError(f"Cannot parse multiple SimaPro location blocks in {original!r}.")


def _exchange_category_text(exchange: dict) -> str:
    categories = exchange.get("categories") or exchange.get("simapro category") or ""
    if isinstance(categories, (tuple, list)):
        return "/".join(str(item) for item in categories)
    return str(categories)


def is_simapro_final_waste_flow(exchange: dict) -> bool:
    return exchange.get("type") in {"technosphere", "substitution"} and "Final waste flows" in _exchange_category_text(
        exchange
    )


def _safe_excel_filename(name: str) -> str:
    safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", name).strip("_")
    return safe_name or "brightway-inventory"


def _valid_worksheet_name(name: str) -> str:
    cleaned = name
    for char in {"\\", "/", "*", "[", "]", ":", "?"}:
        cleaned = cleaned.replace(char, "#")
    if cleaned == "History":
        cleaned = "History-worksheet"
    return (cleaned or "Inventory")[:30]


def _is_excel_exportable(value) -> bool:
    if value is None:
        return True
    if isinstance(value, (str, bool, numbers.Number)):
        return True
    if isinstance(value, (tuple, list)):
        return all(_is_excel_exportable(item) for item in value)
    return False


def _format_excel_value(value):
    if isinstance(value, (tuple, list)):
        return "::".join("" if item is None else str(_format_excel_value(item)) for item in value)
    return value


def _ordered_export_fields(data: list[dict], preferred: list[str], skipped: set[str]) -> list[str]:
    fields = {
        field for row in data for field, value in row.items() if field not in skipped and _is_excel_exportable(value)
    }
    ordered = [field for field in preferred if field in fields]
    ordered.extend(sorted(fields.difference(ordered)))
    return ordered


def _write_excel_cell(sheet, row: int, column: int, value, cell_format=None) -> None:
    value = _format_excel_value(value)
    if value is None:
        return
    if isinstance(value, bool):
        sheet.write_boolean(row, column, value, cell_format)
    elif isinstance(value, numbers.Number):
        sheet.write_number(row, column, value, cell_format)
    else:
        sheet.write_string(row, column, str(value), cell_format)


def load_ecoinvent_activities(version: str) -> list:
    if not re.fullmatch(r"\d+(?:\.\d+){1,2}", version):
        raise ValueError(f"Unsupported ecoinvent version format: {version!r}.")

    with open(DATA_DIR / "export" / f"list_ei{version}_cutoff_activities.csv", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        return [row for row in reader]


def _biosphere_key(exc) -> tuple:
    return (exc["name"], exc["categories"][0], "unspecified" if len(exc["categories"]) == 1 else exc["categories"][1])


def _mapping_for_category(mapping: dict, category: str) -> dict:
    mapped = {}
    mapped.update(mapping.get("global", {}))
    mapped.update(mapping.get(category, {}))
    return mapped


def _apply_simapro_biosphere_name_normalizers(exc, ei_version, version_mapping):
    if not _is_version_at_least(ei_version, "3.10"):
        return

    name = exc["name"]
    category = exc["categories"][0]
    name_mapping = _mapping_for_category(version_mapping, category)
    name = name_mapping.get(name, name)

    if re.search(r"/m3, .+$", name):
        name = name.split("/m3", 1)[0]

    if name.endswith("/kg"):
        name = name.removesuffix("/kg")

    name = re.sub(r"\s+\(([IVX]+)\)$", r" \1", name)

    if name.endswith(", ion"):
        name = name.removesuffix(", ion") + " ion"

    exc["name"] = name_mapping.get(name, name)


def format_biosphere_exchange(
    exc,
    ei_version,
    bio_flows,
    bio_mapping,
    copy: bool = True,
    version_mapping=None,
):
    exc = deepcopy(exc) if copy else exc
    version_mapping = version_mapping or {}
    categories = exc.get("categories")
    if not categories:
        raise ValueError(f"Biosphere exchange {exc.get('name')} is missing categories.")
    if not isinstance(categories, (tuple, list)):
        raise ValueError(f"Biosphere exchange {exc.get('name')} categories must be a tuple or list.")
    if categories[0] not in ALLOWED_BIOSPHERE_CATEGORIES:
        raise ValueError(f"Biosphere exchange {exc.get('name')} has unsupported category {categories[0]!r}.")

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

    _apply_simapro_biosphere_name_normalizers(exc, ei_version, version_mapping)

    key = _biosphere_key(exc)

    if key not in bio_flows:
        try:
            if exc["name"] in bio_mapping[exc["categories"][0]]:
                exc["name"] = bio_mapping[exc["categories"][0]][exc["name"]]
                key = _biosphere_key(exc)
        except KeyError:
            logger.warning("Could not find biosphere flow mapping for %s.", exc["name"])

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
                "urban air close to ground",
            ]:
                key = list(key)
                key[2] = i
                key = tuple(key)
                if key in bio_flows:
                    exc["categories"] = (exc["categories"][0], i)
                    break

    if exc["categories"] == ("natural resource", "in ground"):
        if ei_version in ["3.5", "3.6", "3.7", "3.8"]:
            if "in ground" not in exc["name"]:
                exc["name"] += ", in ground"

    return exc


class SimaproConverter:
    def __init__(self, filepath: str, ecoinvent_version: str = "3.9", db_name: str = None):
        """
        Initialize the SimaproConverter object.

        :param data: list of Simapro inventories
        :param ecoinvent_version: ecoinvent version to use
        """

        source = Path(filepath)
        if not source.exists():
            raise FileNotFoundError(f"File {filepath} not found.")
        if source.suffix.lower() != ".csv":
            raise ValueError("SimaPro inventories must be CSV files.")

        self.original_filepath = source
        self._tempdir = tempfile.TemporaryDirectory(prefix="brightpath_")
        cleaned_path = Path(self._tempdir.name) / f"{source.stem}_edited{source.suffix}"
        self.filepath = Path(check_simapro_inventory(source, output_path=cleaned_path))
        self.db_name = db_name or source.stem
        self.i = bw2io.SimaProCSVImporter(filepath=self.filepath, name=self.db_name)
        self.i.apply_strategies()

        self.ecoinvent_version = ecoinvent_version
        self.biosphere = get_simapro_biosphere()
        self.technosphere = get_simapro_technosphere()
        self.subcompartments = get_simapro_subcompartments()

        self.ei_biosphere_flows = load_ei_biosphere_flows()
        self.biosphere_flows_correspondence = load_biosphere_correspondence()
        self.simapro_brightway_biosphere_mapping = load_simapro_brightway_biosphere_mapping(ecoinvent_version)

        self.i.db_name = self.db_name

    def check_database_name(self):

        for act in self.i.data:
            act["database"] = self.i.db_name

            for exc in act["exchanges"]:
                if exc.get("type") in ["production", "technosphere"]:
                    if "input" in exc:
                        if exc["input"][0] is None:
                            exc["input"] = (self.i.db_name, exc["input"][1])

    def convert_to_brightway(self, format: str = "data", filename=None):
        if format not in ("data", "excel"):
            raise ValueError("Format must be either `data` or `excel`.")

        logger.info("Formatting exchanges.")
        for ds in self.i.data:

            if "Comment" in ds.get("simapro metadata", {}).keys():
                # rename "Comment" to "comment"
                ds["comment"] = ds["simapro metadata"]["Comment"]
                del ds["simapro metadata"]["Comment"]

            ds["name"], ds["reference product"], ds["location"] = format_technosphere_exchange(ds["name"])

            for exc in ds["exchanges"]:
                exc["simapro name"] = exc["name"]

                if is_simapro_final_waste_flow(exc):
                    logger.info(
                        "Dropping SimaPro final waste indicator %s.",
                        exc["name"],
                    )
                    exc["amount"] = 0.0
                    continue

                if exc["type"] == "production":
                    exc["name"] = ds["name"]
                    exc["product"] = ds["reference product"]
                    exc["reference product"] = ds["reference product"]
                    exc["location"] = ds["location"]

                    if any(x in exc["name"] for x in WASTE_TERMS):
                        logger.info(
                            "%s considered waste treatment (input amount made negative).",
                            exc["name"],
                        )
                        exc["amount"] *= -1

                if exc["type"] in ["technosphere", "substitution"]:
                    exc["name"], exc["product"], exc["location"] = format_technosphere_exchange(exc["name"])
                    exc["reference product"] = exc["product"]

                    if any(x in exc["name"] for x in WASTE_TERMS):
                        logger.info(
                            "%s considered waste treatment (input amount made negative).",
                            exc["name"],
                        )
                        exc["amount"] *= -1

                if exc["type"] == "substitution":
                    exc["type"] = "technosphere"
                    exc["amount"] *= -1

                if exc["type"] == "biosphere":
                    exc.update(
                        format_biosphere_exchange(
                            exc,
                            self.ecoinvent_version,
                            self.ei_biosphere_flows,
                            self.biosphere_flows_correspondence,
                            version_mapping=getattr(
                                self,
                                "simapro_brightway_biosphere_mapping",
                                {},
                            ),
                        )
                    )

        ensure_unique_datasets(self.i.data)
        self.check_database_name()

        logger.info("Removing empty datasets.")
        self.remove_empty_datasets()
        logger.info("Removing empty exchanges.")
        self.remove_empty_exchanges()
        logger.info("Checking inventories.")
        self.check_inventories()
        logger.info("SimaPro conversion completed.")

        if format == "excel":
            return self.write_brightway_excel(filename)

        return self.i.data

    def write_brightway_excel(self, filename=None) -> Path:
        filepath = self._excel_filepath(filename)
        filepath.parent.mkdir(parents=True, exist_ok=True)

        workbook = xlsxwriter.Workbook(filepath)
        bold = workbook.add_format({"bold": True})
        bold.set_font_size(12)
        sheet = workbook.add_worksheet(_valid_worksheet_name(self.db_name))

        row = 0
        row = self._write_database_excel_section(sheet, row, bold)
        row += 1

        for activity in self.i.data:
            row = self._write_activity_excel_section(sheet, row, activity, bold)
            row += 1

        workbook.close()
        return filepath

    def _excel_filepath(self, filename=None) -> Path:
        if filename is None:
            return Path.cwd() / f"lci-{_safe_excel_filename(self.db_name)}.xlsx"

        filepath = Path(filename).expanduser()
        if filepath.suffix == "":
            filepath = filepath.with_suffix(".xlsx")
        if filepath.suffix.lower() != ".xlsx":
            raise ValueError("Brightway Excel exports must use a .xlsx filename.")
        return filepath.resolve()

    def _write_database_excel_section(self, sheet, row: int, bold) -> int:
        _write_excel_cell(sheet, row, 0, "Database", bold)
        _write_excel_cell(sheet, row, 1, self.db_name)
        return row + 1

    def _write_activity_excel_section(self, sheet, row: int, activity: dict, bold) -> int:
        _write_excel_cell(sheet, row, 0, "Activity", bold)
        _write_excel_cell(sheet, row, 1, activity["name"])
        row += 1

        for field in _ordered_export_fields([activity], EXCEL_ACTIVITY_FIELD_ORDER, EXCEL_ACTIVITY_SKIP_FIELDS):
            _write_excel_cell(sheet, row, 0, field)
            _write_excel_cell(sheet, row, 1, activity.get(field))
            row += 1

        exchanges = activity.get("exchanges", [])
        _write_excel_cell(sheet, row, 0, "Exchanges", bold)
        row += 1

        if exchanges:
            columns = _ordered_export_fields(exchanges, EXCEL_EXCHANGE_FIELD_ORDER, EXCEL_EXCHANGE_SKIP_FIELDS)
            for column, field in enumerate(columns):
                _write_excel_cell(sheet, row, column, field)
            row += 1

            for exchange in exchanges:
                for column, field in enumerate(columns):
                    _write_excel_cell(sheet, row, column, exchange.get(field))
                row += 1

        return row

    def remove_empty_datasets(self):
        self.i.data = [ds for ds in self.i.data if len(ds["exchanges"]) >= 1]

    def remove_empty_exchanges(self):
        for ds in self.i.data:
            ds["exchanges"] = [e for e in ds["exchanges"] if e["amount"] != 0.0]

    def check_inventories(self):

        errors = []
        for ds in self.i.data:
            if len([x for x in ds["exchanges"] if x["type"] == "production"]) != 1:
                errors.append(
                    f"{ds['name'], ds['reference product'], ds['location']} must have exactly one production flow."
                )

            for e in ds["exchanges"]:
                if e["type"] not in ["production", "technosphere", "biosphere"]:
                    errors.append(
                        f"{ds['name'], ds['reference product'], ds['location']} has an unknown flow type {e['type']}."
                    )

                if e["type"] == "production":
                    if (e["name"], e["product"], e["location"]) != (
                        ds["name"],
                        ds["reference product"],
                        ds["location"],
                    ):
                        errors.append(
                            f"{ds['name'], ds['reference product'], ds['location']} has an incorrect production flow {e}."
                        )

        if errors:
            raise ValueError("Inventory validation failed:\n" + "\n".join(errors))
