from __future__ import annotations

import csv
import datetime
import logging
import re
import tempfile
from copy import deepcopy
from dataclasses import dataclass, field
from numbers import Real
from pathlib import Path

import bw2io

from brightpath.background.catalogs import CatalogProvider
from brightpath.core.context import BackgroundContext, BiosphereProfile, FormatProfile, InventoryContext
from brightpath.exceptions import SimaProSerializationError
from brightpath.models import (
    BackgroundProfile,
    InventoryDocument,
    InventoryFormat,
    Issue,
    default_biosphere_profile,
)
from brightpath.profiles import format_simapro_technosphere_name, parse_simapro_technosphere_name
from brightpath.utils import (
    ALLOWED_BIOSPHERE_CATEGORIES,
    check_simapro_inventory,
    collect_unused_exchanges,
    convert_sd_to_sd2,
    escape_spreadsheet_formula,
    find_production_exchange,
    flag_exchanges,
    get_biosphere_exchanges,
    get_simapro_biosphere,
    get_simapro_fields_list,
    get_simapro_headers,
    get_simapro_subcompartments,
    get_simapro_units,
    get_subcategory,
    get_technosphere_exchanges,
    get_waste_exchange_names,
    inspect_brightway_inventory,
    is_a_waste_treatment,
    is_activity_waste_treatment,
    is_blacklisted,
    load_biosphere_correspondence,
    load_simapro_brightway_biosphere_mapping,
    round_floats_in_string,
)

logger = logging.getLogger(__name__)
_WASTE_TERMS = tuple(get_waste_exchange_names())
_INVENTORY_PATH_PATTERN = re.compile(r"^(?P<path>activity\[\d+\](?:\.exchanges\[\d+\])?):")
_DETECTED_SYSTEM_MODELS_KEY = "simapro detected system models"


class _Formula(str):
    pass


@dataclass
class SimaProRenderResult:
    """In-memory SimaPro rows and any format-specific rendering issues."""

    rows: list[list]
    issues: list[Issue] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        """Whether rendering produced at least one error issue."""

        return any(issue.severity == "error" for issue in self.issues)


def load_simapro_csv(
    path: str | Path,
    *,
    background_profile: BackgroundProfile | None = None,
    biosphere_profile: BiosphereProfile | None = None,
    context: InventoryContext | None = None,
    database_name: str | None = None,
    catalog_provider: CatalogProvider | None = None,
) -> InventoryDocument:
    """Load and normalize a SimaPro CSV export into canonical inventory data.

    ``catalog_provider`` must supply the exact declared biosphere profile. This
    keeps SimaPro name normalization tied to the inventory context instead of
    applying a fixed ecoinvent release to every file.
    """

    source = Path(path).expanduser()
    if not source.is_file():
        raise FileNotFoundError(f"SimaPro CSV file not found: {source}")
    if source.suffix.lower() != ".csv":
        raise ValueError("SimaPro inventories must use a .csv filename.")

    if context is not None:
        if context.format.format_id != InventoryFormat.SIMAPRO_CSV.value:
            raise ValueError("Explicit context format must be simapro_csv.")
        profile = BackgroundProfile.from_technosphere_profile(context.background.technosphere)
        if background_profile is not None and background_profile.normalized() != profile:
            raise ValueError("background_profile conflicts with context.technosphere.")
        if biosphere_profile is not None and biosphere_profile != context.background.biosphere:
            raise ValueError("biosphere_profile conflicts with context.biosphere.")
    else:
        if background_profile is None:
            raise TypeError("background_profile or context must be provided.")
        profile = background_profile.normalized()
        technosphere = profile.to_technosphere_profile()
        context = InventoryContext(
            format=FormatProfile(InventoryFormat.SIMAPRO_CSV.value, encoding="latin-1"),
            background=BackgroundContext(
                technosphere=technosphere,
                biosphere=biosphere_profile or default_biosphere_profile(technosphere),
            ),
        )
    name = database_name or source.stem
    with tempfile.TemporaryDirectory(prefix="brightpath_") as directory:
        cleaned_path = Path(directory) / f"{source.stem}_cleaned.csv"
        cleaned = check_simapro_inventory(source, output_path=cleaned_path)
        global_parameter_scopes = _read_global_parameter_scopes(cleaned)
        if "default-units" not in bw2io.migrations:
            bw2io.create_core_migrations()
        importer = bw2io.SimaProCSVImporter(filepath=cleaned, name=name)
        importer.apply_strategies()
        detected_system_models = detect_simapro_system_models(importer.data)
        global_parameters = deepcopy(getattr(importer, "global_parameters", None))
        database_parameters, project_parameters = _split_global_parameters(
            global_parameters,
            global_parameter_scopes,
        )
        global_parameter_names = _normalize_parameter_identifiers(
            [*(database_parameters or []), *(project_parameters or [])]
        )
        try:
            data = normalize_simapro_import_data(
                importer.data,
                background_profile=profile,
                biosphere_profile=context.background.biosphere,
                database_name=name,
                catalog_provider=catalog_provider,
                parameter_name_mapping=global_parameter_names,
            )
        except Exception as exc:
            if not hasattr(exc, "partial_data"):
                _attach_partial_data(exc, importer.data)
            raise

    metadata = deepcopy(getattr(importer, "metadata", {}) or {})
    if detected_system_models:
        metadata[_DETECTED_SYSTEM_MODELS_KEY] = sorted(detected_system_models)

    return InventoryDocument(
        data=data,
        context=context,
        database_name=name,
        metadata=metadata,
        database_parameters=database_parameters,
        project_parameters=project_parameters,
    )


def normalize_simapro_import_data(
    data: list[dict],
    *,
    background_profile: BackgroundProfile,
    biosphere_profile: BiosphereProfile | None = None,
    database_name: str,
    biosphere_flows=None,
    biosphere_correspondence=None,
    version_mapping=None,
    parameter_name_mapping=None,
    catalog_provider: CatalogProvider | None = None,
) -> list[dict]:
    """Return canonical data parsed from `bw2io.SimaProCSVImporter` output."""

    normalized = deepcopy(data)
    profile = background_profile.normalized()
    selected_biosphere = biosphere_profile or default_biosphere_profile(profile)
    reference_version = selected_biosphere.version
    if biosphere_flows is None:
        if catalog_provider is None:
            raise TypeError(
                "catalog_provider or biosphere_flows must be provided for exact SimaPro biosphere normalization."
            )
        catalog = catalog_provider.load_biosphere(selected_biosphere)
        biosphere_flows = _simapro_biosphere_reference(catalog.identities)
    if biosphere_correspondence is None:
        biosphere_correspondence = load_biosphere_correspondence()
    if version_mapping is None:
        version_mapping = load_simapro_brightway_biosphere_mapping(reference_version)
    parameter_name_mapping = parameter_name_mapping or {}

    try:
        for dataset in normalized:
            dataset.pop("filename", None)
            simapro_metadata = dataset.setdefault("simapro metadata", {})
            comment = simapro_metadata.pop("Comment", None)
            if comment not in (None, ""):
                dataset["comment"] = comment
            for key, value in simapro_metadata.items():
                if key not in {"Process name", "Category type"}:
                    dataset.setdefault(key.lower(), value)

            raw_dataset_name = (
                dataset.get("simapro name") or simapro_metadata.get("Process name") or dataset.get("name", "")
            )
            dataset["simapro name"] = raw_dataset_name
            dataset["name"], dataset["reference product"], dataset["location"] = parse_simapro_technosphere_name(
                raw_dataset_name,
                unit=str(dataset.get("unit") or ""),
                profile=profile,
            )
            dataset["database"] = database_name
            local_parameter_names = _normalize_parameter_identifiers(dataset.get("parameters") or [])
            formula_name_mapping = {**parameter_name_mapping, **local_parameter_names}
            for parameter in dataset.get("parameters") or []:
                if isinstance(parameter, dict) and parameter.get("formula"):
                    parameter["formula"] = _replace_parameter_identifiers(
                        str(parameter["formula"]),
                        formula_name_mapping,
                    )

            converted_exchanges = []
            for exchange in dataset.get("exchanges", []):
                if exchange.get("formula"):
                    exchange["formula"] = _replace_parameter_identifiers(
                        str(exchange["formula"]),
                        formula_name_mapping,
                    )
                raw_exchange_name = exchange.get("simapro name") or exchange.get("name", "")
                exchange["simapro name"] = raw_exchange_name
                if is_simapro_final_waste_flow(exchange):
                    continue

                exchange_type = exchange.get("type")
                if exchange_type == "production":
                    exchange["name"] = dataset["name"]
                    exchange["product"] = dataset["reference product"]
                    exchange["reference product"] = dataset["reference product"]
                    exchange["location"] = dataset["location"]
                    exchange["database"] = database_name
                    _restore_simapro_category(dataset, exchange)
                    if _is_waste_name(exchange["name"]):
                        exchange["amount"] *= -1

                elif exchange_type in {"technosphere", "substitution"}:
                    name, product, location = parse_simapro_technosphere_name(
                        raw_exchange_name,
                        unit=str(exchange.get("unit") or ""),
                        profile=profile,
                    )
                    exchange["name"] = name
                    exchange["product"] = product
                    exchange["reference product"] = product
                    exchange["location"] = location
                    exchange.pop("input", None)
                    if _is_waste_name(name):
                        exchange["amount"] *= -1
                    if exchange_type == "substitution":
                        exchange["type"] = "technosphere"
                        exchange["amount"] *= -1

                elif exchange_type == "biosphere":
                    exchange.update(
                        format_biosphere_exchange(
                            exchange,
                            reference_version,
                            biosphere_flows,
                            biosphere_correspondence,
                            version_mapping=version_mapping,
                        )
                    )
                    exchange.pop("input", None)

                converted_exchanges.append(exchange)
            dataset["exchanges"] = converted_exchanges

        return normalized
    except Exception as exc:
        _attach_partial_data(exc, normalized)
        raise


def _simapro_biosphere_reference(identities) -> frozenset[tuple[str, str, str]]:
    """Project exact catalog identities into the SimaPro matching key."""

    return frozenset(
        (
            name,
            categories[0],
            categories[1] if len(categories) > 1 else "unspecified",
        )
        for name, categories, _unit in identities
        if categories
    )


def write_simapro_csv(
    document: InventoryDocument,
    path: str | Path,
) -> tuple[Path, SimaProRenderResult]:
    destination = Path(path).expanduser()
    if destination.suffix == "":
        destination = destination.with_suffix(".csv")
    if destination.suffix.lower() != ".csv":
        raise ValueError("SimaPro exports must use a .csv filename.")
    destination = destination.resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)

    result = render_simapro_rows(document)
    if result.has_errors:
        detail = "\n".join(issue.message for issue in result.issues if issue.severity == "error")
        raise SimaProSerializationError(f"SimaPro rendering failed:\n{detail}")

    try:
        with destination.open("w", newline="", encoding="latin-1") as handle:
            writer = csv.writer(handle, delimiter=";")
            for row in result.rows:
                writer.writerow(
                    [value if isinstance(value, _Formula) else escape_spreadsheet_formula(value) for value in row]
                )
    except UnicodeEncodeError as exc:
        raise SimaProSerializationError(
            "SimaPro CSV uses Latin-1 encoding and the inventory contains unsupported characters."
        ) from exc
    return destination, result


def render_simapro_rows(document: InventoryDocument) -> SimaProRenderResult:
    return _SimaProRenderer(document).render()


def is_simapro_final_waste_flow(exchange: dict) -> bool:
    categories = exchange.get("categories") or exchange.get("simapro category") or ""
    if isinstance(categories, (tuple, list)):
        categories = "/".join(str(item) for item in categories)
    return exchange.get("type") in {"technosphere", "substitution"} and "Final waste flows" in str(categories)


def detect_simapro_system_models(data: list[dict]) -> frozenset[str]:
    """Return ecoinvent system-model markers found in preserved SimaPro names."""

    detected = set()
    for dataset in data:
        if not isinstance(dataset, dict):
            continue
        metadata = dataset.get("simapro metadata") or {}
        names = [
            dataset.get("simapro name"),
            metadata.get("Process name") if isinstance(metadata, dict) else None,
            dataset.get("name"),
        ]
        names.extend(
            exchange.get("simapro name") or exchange.get("name")
            for exchange in dataset.get("exchanges", [])
            if isinstance(exchange, dict)
        )
        for name in names:
            normalized = str(name or "").lower()
            if "cut-off, u" in normalized or "cutoff, u" in normalized:
                detected.add("cutoff")
            if "consequential, u" in normalized:
                detected.add("consequential")
    return frozenset(detected)


def format_biosphere_exchange(
    exchange,
    ecoinvent_version,
    biosphere_flows,
    biosphere_mapping,
    copy: bool = True,
    version_mapping=None,
):
    exchange = deepcopy(exchange) if copy else exchange
    version_mapping = version_mapping or {}
    categories = exchange.get("categories")
    if not categories:
        raise ValueError(f"Biosphere exchange {exchange.get('name')} is missing categories.")
    if not isinstance(categories, (tuple, list)):
        raise ValueError(f"Biosphere exchange {exchange.get('name')} categories must be a tuple or list.")
    if categories[0] not in ALLOWED_BIOSPHERE_CATEGORIES:
        raise ValueError(f"Biosphere exchange {exchange.get('name')} has unsupported category {categories[0]!r}.")

    if "in ground" in exchange["name"]:
        if ecoinvent_version not in {"3.5", "3.6", "3.7", "3.8"}:
            exchange["name"] = exchange["name"].replace(", in ground", "")
        exchange["categories"] = ("natural resource", "in ground")
    if exchange["name"].startswith("Water, well"):
        exchange["name"] = "Water, well, in ground"
    if exchange["name"].startswith("Water, lake"):
        exchange["name"] = "Water, lake"
    if exchange["name"].startswith("Water, cooling"):
        exchange["name"] = "Water, cooling, unspecified natural origin"
    if exchange["name"].startswith("Water,") and "in air" not in exchange["name"]:
        exchange["categories"] = ("natural resource", "in water")

    _apply_biosphere_name_normalizers(exchange, ecoinvent_version, version_mapping)
    key = _biosphere_key(exchange)
    if key not in biosphere_flows:
        category_mapping = biosphere_mapping.get(exchange["categories"][0], {})
        if exchange["name"] in category_mapping:
            exchange["name"] = category_mapping[exchange["name"]]
            key = _biosphere_key(exchange)

    if key not in biosphere_flows:
        alternatives = {
            "natural resource": ("in ground", "biotic", "in air", "land"),
            "soil": ("agricultural", "forestry", "industrial"),
            "air": (
                "low population density, long-term",
                "lower stratosphere + upper troposphere",
                "non-urban air or from high stacks",
                "urban air close to ground",
            ),
        }
        for subcategory in alternatives.get(exchange["categories"][0], ()):
            candidate = (key[0], key[1], subcategory)
            if candidate in biosphere_flows:
                exchange["categories"] = (exchange["categories"][0], subcategory)
                break

    if exchange["categories"] == ("natural resource", "in ground") and ecoinvent_version in {
        "3.5",
        "3.6",
        "3.7",
        "3.8",
    }:
        if "in ground" not in exchange["name"]:
            exchange["name"] += ", in ground"
    return exchange


class _SimaProRenderer:
    def __init__(self, document: InventoryDocument) -> None:
        self.document = document
        self.profile = document.background_profile.normalized()
        self.fields = get_simapro_fields_list()
        self.units = get_simapro_units()
        self.headers = get_simapro_headers()
        self.biosphere = get_simapro_biosphere()
        self.subcompartments = get_simapro_subcompartments()

    def render(self) -> SimaProRenderResult:
        issues = self._preflight_issues()
        if any(issue.severity == "error" for issue in issues):
            return SimaProRenderResult(rows=[], issues=issues)

        try:
            rows = self._header_rows()
            inventories = [flag_exchanges(activity) for activity in self.document.data]
            for activity in inventories:
                rows.extend(self._activity_rows(activity))
            rows.extend(self._global_parameter_rows())
            rows.extend(self._metadata_rows())
        except (KeyError, SimaProSerializationError, TypeError, ValueError) as exc:
            issues.append(
                Issue(
                    severity="error",
                    code="simapro_serialization_failed",
                    message=str(exc),
                )
            )
            return SimaProRenderResult(rows=[], issues=issues)

        for item in collect_unused_exchanges(inventories):
            issues.append(
                Issue(
                    severity="warning",
                    code="simapro_exchange_unused",
                    message=(
                        f"Exchange {item['exchange']!r} in activity {item['activity']!r} "
                        "was not represented in SimaPro output."
                    ),
                )
            )
        return SimaProRenderResult(rows=rows, issues=issues)

    def _preflight_issues(self) -> list[Issue]:
        issues = []
        if self.profile.family not in {"ecoinvent", "uvek"}:
            issues.append(
                Issue(
                    severity="error",
                    code="simapro_background_unsupported",
                    message=f"SimaPro output does not support background family {self.profile.family!r}.",
                )
            )
        elif self.profile.family == "ecoinvent" and self.profile.system_model not in {"cutoff", "consequential"}:
            issues.append(
                Issue(
                    severity="error",
                    code="simapro_system_model_unsupported",
                    message=(
                        "SimaPro ecoinvent names support only cut-off and consequential system models; "
                        f"got {self.profile.system_model!r}."
                    ),
                )
            )

        errors, _warnings = inspect_brightway_inventory(
            self.document.data,
            require_simapro_category=True,
            validate_units=True,
        )
        for message in errors:
            match = _INVENTORY_PATH_PATTERN.match(message)
            if "simapro category" in message:
                code = "simapro_category_missing"
            elif "unknown activity unit" in message or "unknown exchange unit" in message:
                code = "simapro_unit_unsupported"
            else:
                code = "simapro_inventory_invalid"
            issues.append(
                Issue(
                    severity="error",
                    code=code,
                    message=message,
                    path=match.group("path") if match else "",
                )
            )

        issues.extend(self._parameter_issues(self.document.database_parameters, "database_parameters"))
        issues.extend(self._parameter_issues(self.document.project_parameters, "project_parameters"))
        for activity_index, activity in enumerate(self.document.data):
            if isinstance(activity, dict):
                issues.extend(
                    self._parameter_issues(
                        activity.get("parameters"),
                        f"activity[{activity_index}].parameters",
                    )
                )
        return issues

    def _header_rows(self):
        project_name = str(self.document.metadata.get("Project") or self.document.database_name or "brightpath")
        rows = [
            (
                [
                    item.replace("today_date", datetime.datetime.today().strftime("%d.%m.%Y")).replace(
                        "project_name", project_name
                    )
                ]
                if item.startswith("{Date")
                else [item.replace("project_name", project_name)]
            )
            for item in self.headers
        ]
        rows.append([])
        return rows

    def _metadata_rows(self):
        rows = []
        metadata = self.document.metadata
        for metadata_field in ("System description", "Literature reference"):
            value = metadata.get(metadata_field.lower())
            if not isinstance(value, dict):
                continue
            rows.extend([[metadata_field], []])
            for key, item in value.items():
                rows.extend([[key], [item], []])
            rows.extend([[], ["End"], []])
        return rows

    def _global_parameter_rows(self):
        rows = self._parameter_rows(self.document.database_parameters, scope="Database")
        rows.extend(self._parameter_rows(self.document.project_parameters, scope="Project"))
        return rows

    def _activity_rows(self, activity: dict):
        rows = []
        dataset_name = ""
        is_waste = is_activity_waste_treatment(activity, self.profile.family)
        for field_name in self.fields:
            if is_waste and field_name == "Products":
                continue
            if not is_waste and field_name == "Waste treatment":
                continue

            if field_name == "End":
                rows.extend(self._parameter_rows(activity.get("parameters")))
                rows.extend([[field_name], []])
                continue

            rows.append([field_name])
            if field_name == "Process":
                rows.append([])
                continue
            if field_name == "Process name":
                dataset_name = self._format_technosphere(activity)
                rows.extend([[dataset_name], []])
            elif field_name == "Type":
                rows.extend([["Unit process"], []])
            elif field_name == "Comment":
                rows.extend([[self._comment(activity)], []])
            elif field_name == "Category type":
                production = find_production_exchange(activity)
                rows.extend([[production["simapro category"].split("/")[0]], []])
            elif field_name == "Geography":
                rows.extend([[activity["location"]], []])
            elif field_name == "Date":
                rows.extend([[f"{datetime.datetime.today():%d.%m.%Y}"], []])
            elif field_name in _ACTIVITY_METADATA_FIELDS:
                rows.extend(self._activity_metadata_rows(field_name, activity))
            elif field_name in _EMPTY_SECTIONS:
                rows.append([])
            elif field_name in {"Waste treatment", "Products"}:
                rows.extend(self._product_rows(dataset_name, activity, field_name))
            elif field_name in {"Materials/fuels", "Electricity/heat"}:
                rows.extend(self._technosphere_rows(field_name, activity, is_waste))
            elif field_name == "Resources":
                rows.extend(self._biosphere_rows(activity, "natural resource"))
            elif field_name.startswith("Emissions to"):
                rows.extend(self._biosphere_rows(activity, field_name.split(" ")[-1].lower()))
            elif field_name == "Waste to treatment":
                rows.extend(self._waste_rows(activity))
        rows.append([])
        return rows

    def _parameter_rows(self, parameters, *, scope: str = ""):
        if not parameters:
            return []

        inputs = [parameter for parameter in parameters if not parameter.get("formula")]
        calculated = [parameter for parameter in parameters if parameter.get("formula")]
        rows = []
        if inputs:
            rows.append([f"{scope} Input parameters".strip()])
            rows.extend(self._input_parameter_row(parameter) for parameter in inputs)
            rows.append([])
        if calculated:
            rows.append([f"{scope} Calculated parameters".strip()])
            rows.extend(
                [parameter["name"], _Formula(str(parameter["formula"])), parameter.get("comment")]
                for parameter in calculated
            )
            rows.append([])
        return rows

    def _input_parameter_row(self, parameter: dict):
        uncertainty = _simapro_uncertainty_type(parameter.get("uncertainty type"))
        amount = parameter.get("amount", parameter.get("loc"))
        return [
            parameter["name"],
            f"{amount:.12g}",
            uncertainty,
            f"{convert_sd_to_sd2(parameter.get('scale', 1), uncertainty):.12g}",
            f"{parameter.get('min', parameter.get('minimum', 0)):.12g}",
            f"{parameter.get('max', parameter.get('maximum', 0)):.12g}",
            "Yes" if parameter.get("hidden") else "No",
            parameter.get("comment"),
        ]

    @staticmethod
    def _parameter_issues(parameters, path: str) -> list[Issue]:
        if parameters is None:
            return []
        if not isinstance(parameters, list):
            return [
                Issue(
                    severity="error",
                    code="simapro_parameters_invalid",
                    message="SimaPro parameters must be a list of dictionaries.",
                    path=path,
                )
            ]

        issues = []
        for index, parameter in enumerate(parameters):
            parameter_path = f"{path}[{index}]"
            if not isinstance(parameter, dict):
                issues.append(
                    Issue(
                        severity="error",
                        code="simapro_parameters_invalid",
                        message="SimaPro parameter must be a dictionary.",
                        path=parameter_path,
                    )
                )
                continue
            if not str(parameter.get("name") or "").strip():
                issues.append(
                    Issue(
                        severity="error",
                        code="simapro_parameters_invalid",
                        message="SimaPro parameter must define a non-empty name.",
                        path=parameter_path,
                    )
                )
            if not parameter.get("formula") and not isinstance(parameter.get("amount", parameter.get("loc")), Real):
                issues.append(
                    Issue(
                        severity="error",
                        code="simapro_parameters_invalid",
                        message="SimaPro input parameter must define a numeric amount.",
                        path=parameter_path,
                    )
                )
        return issues

    def _format_technosphere(self, value: dict) -> str:
        return format_simapro_technosphere_name(
            name=value["name"],
            reference_product=value["reference product"],
            location=value.get("location", "GLO"),
            unit=value["unit"],
            profile=self.profile,
        )

    def _comment(self, activity: dict) -> str:
        result = ""
        if activity.get("comment"):
            result = f"{round_floats_in_string(str(activity['comment']))} "
        if activity.get("source"):
            result += f"Source: {activity['source']} "
        return result.replace("\n", " ")

    def _activity_metadata_rows(self, field_name: str, activity: dict):
        if field_name.lower() in activity:
            return [[activity[field_name.lower()]], []]
        metadata = activity.get("simapro metadata", {})
        if field_name in metadata:
            return [[metadata[field_name]], []]
        if field_name == "Infrastructure":
            return [["No"], []]
        document_metadata = self.document.metadata
        if field_name == "System description" and "system description" in document_metadata:
            return [[document_metadata["system description"]["name"]], []]
        if field_name == "Literature references" and "literature reference" in document_metadata:
            return [[document_metadata["literature reference"]["name"]], []]
        return [["Unspecified"], []]

    def _product_rows(self, dataset_name: str, activity: dict, field_name: str):
        production = find_production_exchange(activity)
        amount = production["amount"]
        if field_name == "Waste treatment" and amount < 0:
            amount = abs(amount)
        production["used"] = True
        return [
            [
                dataset_name,
                self.units[production["unit"]],
                f"{amount:.3E}",
                "100",
                "not defined",
                get_subcategory(production["simapro category"]),
                "not defined",
            ],
            [],
        ]

    def _technosphere_rows(self, field_name: str, activity: dict, is_waste_activity: bool):
        rows = []
        want_energy = field_name == "Electricity/heat"
        for exchange in get_technosphere_exchanges(activity):
            if (exchange["unit"] in {"megajoule", "kilowatt hour"}) != want_energy:
                continue
            if is_blacklisted(exchange, self.profile.family):
                continue
            if is_a_waste_treatment(exchange["name"], self.profile.family):
                continue
            amount = abs(exchange["amount"]) if is_waste_activity and exchange["amount"] < 0 else exchange["amount"]
            rows.append(self._technosphere_exchange_row(exchange, amount))
            exchange["used"] = True
        rows.append([])
        return rows

    def _waste_rows(self, activity: dict):
        rows = []
        for exchange in get_technosphere_exchanges(activity):
            if is_blacklisted(exchange, self.profile.family):
                continue
            if not is_a_waste_treatment(exchange["name"], self.profile.family):
                continue
            rows.append(self._technosphere_exchange_row(exchange, abs(exchange["amount"])))
            exchange["used"] = True
        rows.append([])
        return rows

    def _technosphere_exchange_row(self, exchange: dict, amount: float):
        uncertainty = _simapro_uncertainty_type(exchange.get("uncertainty type"))
        return [
            self._format_technosphere(exchange),
            self.units[exchange["unit"]],
            f"{amount:.3E}",
            uncertainty,
            f"{convert_sd_to_sd2(exchange.get('scale', 1), uncertainty):.3E}",
            f"{exchange.get('min', exchange.get('minimum', 0)):.3E}",
            f"{exchange.get('max', exchange.get('maximum', 0)):.3E}",
            exchange.get("comment"),
        ]

    def _biosphere_rows(self, activity: dict, category: str):
        rows = []
        for exchange in get_biosphere_exchanges(activity, category):
            if is_blacklisted(exchange, self.profile.family):
                continue
            rendered = deepcopy(exchange)
            if category != "natural resource" and rendered["name"].lower() == "water":
                rendered["unit"] = "kilogram"
                rendered["amount"] *= 1000
            rows.append(self._biosphere_exchange_row(rendered))
            exchange["used"] = True
        rows.append([])
        return rows

    def _biosphere_exchange_row(self, exchange: dict):
        uncertainty = _simapro_uncertainty_type(exchange.get("uncertainty type"))
        categories = exchange["categories"]
        subcompartment = ""
        if len(categories) > 1:
            try:
                subcompartment = self.subcompartments[categories[1]]
            except KeyError as exc:
                raise SimaProSerializationError(
                    f"No SimaPro subcompartment mapping for {categories[1]!r} on {exchange['name']!r}."
                ) from exc
        return [
            self.biosphere.get(exchange["name"], exchange["name"]),
            subcompartment,
            self.units[exchange["unit"]],
            f"{exchange['amount']:.3E}",
            uncertainty,
            f"{convert_sd_to_sd2(exchange.get('scale', 1), uncertainty):.3E}",
            f"{exchange.get('min', exchange.get('minimum', 0)):.3E}",
            f"{exchange.get('max', exchange.get('maximum', 0)):.3E}",
            exchange.get("comment"),
        ]


_ACTIVITY_METADATA_FIELDS = {
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
}
_EMPTY_SECTIONS = {"Final waste flows", "Non material emission", "Social issues", "Economic issues"}


def _restore_simapro_category(dataset: dict, production: dict) -> None:
    category_type = str(dataset.get("simapro metadata", {}).get("Category type") or "").strip()
    categories = production.get("categories") or ()
    if isinstance(categories, str):
        categories = tuple(part for part in re.split(r"[/\\]", categories) if part)
    category_parts = [category_type, *(str(part) for part in categories)]
    production["simapro category"] = "/".join(part for part in category_parts if part)


def _is_waste_name(name: str) -> bool:
    return any(term in name for term in _WASTE_TERMS)


def _biosphere_key(exchange: dict) -> tuple:
    categories = exchange["categories"]
    return (exchange["name"], categories[0], "unspecified" if len(categories) == 1 else categories[1])


def _apply_biosphere_name_normalizers(exchange: dict, version: str, version_mapping: dict) -> None:
    if _version_tuple(version) < (3, 10):
        return
    category = exchange["categories"][0]
    mapping = {**version_mapping.get("global", {}), **version_mapping.get(category, {})}
    name = mapping.get(exchange["name"], exchange["name"])
    if re.search(r"/m3, .+$", name):
        name = name.split("/m3", 1)[0]
    if name.endswith("/kg"):
        name = name.removesuffix("/kg")
    name = re.sub(r"\s+\(([IVX]+)\)$", r" \1", name)
    if name.endswith(", ion"):
        name = name.removesuffix(", ion") + " ion"
    exchange["name"] = mapping.get(name, name)


def _version_tuple(version: str) -> tuple[int, ...]:
    try:
        return tuple(int(part) for part in version.split("."))
    except ValueError:
        return tuple()


def _simapro_uncertainty_type(value) -> str:
    return {
        0: "Undefined",
        1: "Undefined",
        2: "Lognormal",
        3: "Normal",
        4: "Uniform",
        5: "Triangle",
    }.get(value, "Undefined")


def _attach_partial_data(exc: Exception, data: list[dict]) -> None:
    try:
        exc.partial_data = deepcopy(data)
    except (AttributeError, TypeError):
        pass


def _read_global_parameter_scopes(path: Path) -> dict[str, str]:
    scopes = {}
    current_scope = ""
    section_scopes = {
        "Database Input parameters": "database",
        "Database Calculated parameters": "database",
        "Project Input parameters": "project",
        "Project Calculated parameters": "project",
    }
    with path.open(encoding="latin-1", newline="") as handle:
        for row in csv.reader(handle, delimiter=";"):
            first = str(row[0] if row else "").strip()
            if first in section_scopes:
                current_scope = section_scopes[first]
            elif not first:
                current_scope = ""
            elif current_scope:
                scopes[first.lower()] = current_scope
    return scopes


def _split_global_parameters(parameters, scopes: dict[str, str]) -> tuple[list[dict] | None, list[dict] | None]:
    if not parameters:
        return None, None
    if isinstance(parameters, dict):
        database_parameters = []
        project_parameters = []
        for name, value in parameters.items():
            parameter = (
                {"name": name, **deepcopy(value)}
                if isinstance(value, dict)
                else {"name": name, "amount": deepcopy(value)}
            )
            target = project_parameters if scopes.get(str(name).lower()) == "project" else database_parameters
            target.append(parameter)
        return database_parameters or None, project_parameters or None
    if isinstance(parameters, list):
        return deepcopy(parameters), None
    return None, None


def _normalize_parameter_identifiers(parameters: list[dict]) -> dict[str, str]:
    mapping = {
        str(parameter["name"]): str(parameter["name"]).lower()
        for parameter in parameters
        if isinstance(parameter, dict) and parameter.get("name")
    }
    for parameter in parameters:
        if not isinstance(parameter, dict):
            continue
        name = str(parameter.get("name") or "")
        if name:
            parameter["name"] = mapping[name]
        if parameter.get("formula"):
            parameter["formula"] = _replace_parameter_identifiers(str(parameter["formula"]), mapping)
    return mapping


def _replace_parameter_identifiers(formula: str, mapping: dict[str, str]) -> str:
    result = formula
    for source, target in sorted(mapping.items(), key=lambda item: len(item[0]), reverse=True):
        result = re.sub(
            rf"(?<![A-Za-z0-9_]){re.escape(source)}(?![A-Za-z0-9_])",
            target,
            result,
            flags=re.IGNORECASE,
        )
    return result
