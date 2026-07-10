from __future__ import annotations

import json
import math
import numbers
import re
from pathlib import Path
from typing import Iterable

import bw2io
import xlsxwriter
from bw2io.importers.excel import ExcelImporter

from brightpath.exceptions import ExcelSerializationError
from brightpath.models import BackgroundProfile, InventoryDocument, InventoryFormat

_JSON_PREFIX = "__brightpath_json__:"
_MISSING = object()
_PROFILE_METADATA_FIELDS = {
    "family": "brightpath background family",
    "version": "brightpath background version",
    "system_model": "brightpath background system model",
}
_ACTIVITY_SKIP_FIELDS = {
    "database",
    "exchanges",
    "name",
    "parameters",
    "worksheet name",
}
_EXCHANGE_SKIP_FIELDS = {"input", "output"}
_ACTIVITY_FIELD_ORDER = [
    "reference product",
    "unit",
    "location",
    "code",
    "comment",
    "type",
    "categories",
]
_EXCHANGE_FIELD_ORDER = [
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
    "comment",
]
_PARAMETER_FIELD_ORDER = [
    "name",
    "amount",
    "formula",
    "uncertainty type",
    "loc",
    "scale",
    "shape",
    "minimum",
    "maximum",
]


def load_brightway_excel(
    path: str | Path,
    *,
    background_profile: BackgroundProfile | None = None,
) -> InventoryDocument:
    """Load a Brightway Excel workbook into a software-neutral document."""

    source = Path(path).expanduser()
    if not source.is_file():
        raise FileNotFoundError(f"Brightway Excel workbook not found: {source}")
    if source.suffix.lower() != ".xlsx":
        raise ValueError("Brightway Excel workbooks must use a .xlsx filename.")

    importer = ExcelImporter(source)
    if "biosphere-2-3-categories" not in bw2io.migrations:
        bw2io.create_core_migrations()
    importer.apply_strategies()

    data = _decode_tagged_values(importer.data)
    metadata = _decode_tagged_values(getattr(importer, "metadata", {}) or {})
    embedded_profile = _profile_from_metadata(metadata)
    profile = (background_profile or embedded_profile).normalized()

    return InventoryDocument(
        data=data,
        background_profile=profile,
        inventory_format=InventoryFormat.BRIGHTWAY_EXCEL,
        database_name=getattr(importer, "db_name", ""),
        metadata=metadata,
        database_parameters=_decode_tagged_values(getattr(importer, "database_parameters", None)),
        project_parameters=_decode_tagged_values(getattr(importer, "project_parameters", None)),
    )


def write_brightway_excel(
    document: InventoryDocument,
    path: str | Path,
) -> Path:
    """Write an inventory document using the generic Brightway Excel layout."""

    destination = Path(path).expanduser()
    if destination.suffix == "":
        destination = destination.with_suffix(".xlsx")
    if destination.suffix.lower() != ".xlsx":
        raise ValueError("Brightway Excel exports must use a .xlsx filename.")
    destination = destination.resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)

    workbook = xlsxwriter.Workbook(destination)
    try:
        bold = workbook.add_format({"bold": True})
        bold.set_font_size(12)
        sheet = workbook.add_worksheet(_valid_worksheet_name(document.database_name))
        row = 0

        row = _write_parameter_section(
            sheet,
            row,
            "Project parameters",
            document.project_parameters,
            bold,
        )

        _write_cell(sheet, row, 0, "Database", bold)
        _write_cell(sheet, row, 1, document.database_name or "brightpath-inventory")
        row += 1

        metadata = document.metadata
        profile = document.background_profile.normalized()
        if profile.family:
            metadata[_PROFILE_METADATA_FIELDS["family"]] = profile.family
        if profile.version:
            metadata[_PROFILE_METADATA_FIELDS["version"]] = profile.version
        if profile.system_model:
            metadata[_PROFILE_METADATA_FIELDS["system_model"]] = profile.system_model

        for field in sorted(metadata):
            _write_cell(sheet, row, 0, field)
            _write_cell(sheet, row, 1, metadata[field], field=field)
            row += 1
        row += 1

        row = _write_parameter_section(
            sheet,
            row,
            "Database parameters",
            document.database_parameters,
            bold,
        )

        for activity in document.data:
            row = _write_activity(sheet, row, activity, bold)
            row += 1
    finally:
        workbook.close()

    return destination


def _profile_from_metadata(metadata: dict) -> BackgroundProfile:
    return BackgroundProfile(
        family=str(metadata.get(_PROFILE_METADATA_FIELDS["family"], "") or ""),
        version=str(metadata.get(_PROFILE_METADATA_FIELDS["version"], "") or ""),
        system_model=str(metadata.get(_PROFILE_METADATA_FIELDS["system_model"], "") or ""),
    )


def _write_activity(sheet, row: int, activity: dict, bold) -> int:
    if not isinstance(activity, dict):
        raise ExcelSerializationError("Every inventory dataset must be a dictionary.")

    _write_cell(sheet, row, 0, "Activity", bold)
    _write_cell(sheet, row, 1, activity.get("name", ""), field="name")
    row += 1

    fields = _ordered_fields([activity], _ACTIVITY_FIELD_ORDER, _ACTIVITY_SKIP_FIELDS)
    for field in fields:
        _write_cell(sheet, row, 0, field)
        _write_cell(sheet, row, 1, activity.get(field), field=field)
        row += 1

    parameters = activity.get("parameters")
    if parameters:
        if not isinstance(parameters, list) or not all(isinstance(parameter, dict) for parameter in parameters):
            raise ExcelSerializationError(
                f"Activity {activity.get('name')!r} parameters must be a list of dictionaries."
            )
        groups = {str(parameter.get("group") or "") for parameter in parameters}
        if len(groups) > 1:
            raise ExcelSerializationError(
                f"Activity {activity.get('name')!r} has multiple parameter groups, which "
                "cannot be represented in one Brightway Excel activity section."
            )
        group = next(iter(groups), "")
        _write_cell(sheet, row, 0, "Parameters", bold)
        _write_cell(sheet, row, 1, group)
        row += 1
        row = _write_labelled_rows(
            sheet,
            row,
            parameters,
            preferred=_PARAMETER_FIELD_ORDER,
            skipped={"group"},
        )

    exchanges = activity.get("exchanges", [])
    if not isinstance(exchanges, list):
        raise ExcelSerializationError(f"Activity {activity.get('name')!r} exchanges must be a list.")

    _write_cell(sheet, row, 0, "Exchanges", bold)
    row += 1
    if exchanges:
        if not all(isinstance(exchange, dict) for exchange in exchanges):
            raise ExcelSerializationError(f"Activity {activity.get('name')!r} contains a non-dictionary exchange.")
        row = _write_labelled_rows(
            sheet,
            row,
            exchanges,
            preferred=_EXCHANGE_FIELD_ORDER,
            skipped=_EXCHANGE_SKIP_FIELDS,
        )
    return row


def _write_parameter_section(
    sheet,
    row: int,
    label: str,
    parameters: list[dict] | None,
    bold,
) -> int:
    if not parameters:
        return row
    if not isinstance(parameters, list) or not all(isinstance(parameter, dict) for parameter in parameters):
        raise ExcelSerializationError(f"{label} must be a list of dictionaries.")

    _write_cell(sheet, row, 0, label, bold)
    row += 1
    row = _write_labelled_rows(
        sheet,
        row,
        parameters,
        preferred=_PARAMETER_FIELD_ORDER,
        skipped=set(),
    )
    return row + 1


def _write_labelled_rows(
    sheet,
    row: int,
    values: list[dict],
    *,
    preferred: list[str],
    skipped: set[str],
) -> int:
    columns = _ordered_fields(values, preferred, skipped)
    for column, field in enumerate(columns):
        _write_cell(sheet, row, column, field)
    row += 1
    for value in values:
        for column, field in enumerate(columns):
            _write_cell(sheet, row, column, value.get(field, _MISSING), field=field)
        row += 1
    return row


def _ordered_fields(
    values: Iterable[dict],
    preferred: list[str],
    skipped: set[str],
) -> list[str]:
    found = {field for value in values for field in value if field not in skipped}
    ordered = [field for field in preferred if field in found]
    ordered.extend(sorted(found.difference(ordered)))
    return ordered


def _write_cell(sheet, row: int, column: int, value, cell_format=None, *, field="") -> None:
    if value is _MISSING:
        return
    serialized = _serialize_value(value, field=field)
    if isinstance(serialized, bool):
        sheet.write_boolean(row, column, serialized, cell_format)
    elif isinstance(serialized, numbers.Real) and not isinstance(serialized, bool):
        sheet.write_number(row, column, serialized, cell_format)
    else:
        sheet.write_string(row, column, serialized, cell_format)


def _serialize_value(value, *, field: str) -> str | bool | numbers.Real:
    if value is None:
        return _JSON_PREFIX + "null"
    if isinstance(value, bool):
        return value
    if isinstance(value, numbers.Real):
        if isinstance(value, float) and not math.isfinite(value):
            return _json_value(value, field=field)
        return value
    if isinstance(value, str):
        if field == _PROFILE_METADATA_FIELDS["version"]:
            return _json_value(value, field=field)
        return value
    if isinstance(value, tuple):
        if all(item is None or isinstance(item, (str, bool, numbers.Real)) for item in value):
            return "::".join("" if item is None else str(item) for item in value)
        return _json_value(value, field=field)
    if isinstance(value, (list, dict)):
        return _json_value(value, field=field)
    raise ExcelSerializationError(
        f"Field {field or '<unknown>'!r} contains unsupported value type " f"{type(value).__name__!r}."
    )


def _json_value(value, *, field: str) -> str:
    try:
        return _JSON_PREFIX + json.dumps(value, ensure_ascii=False, allow_nan=True)
    except (TypeError, ValueError) as exc:
        raise ExcelSerializationError(
            f"Field {field or '<unknown>'!r} contains a value that cannot be serialized."
        ) from exc


def _decode_tagged_values(value):
    if isinstance(value, str) and value.startswith(_JSON_PREFIX):
        try:
            return json.loads(value[len(_JSON_PREFIX) :])
        except json.JSONDecodeError:
            return value
    if isinstance(value, dict):
        return {key: _decode_tagged_values(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_decode_tagged_values(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_decode_tagged_values(item) for item in value)
    return value


def _valid_worksheet_name(name: str) -> str:
    cleaned = re.sub(r"[\\/*\[\]:?]", "#", name or "Inventory")
    if cleaned == "History":
        cleaned = "History-worksheet"
    return cleaned[:30]
