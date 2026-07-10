"""Brightway block-layout CSV and TSV syntax codecs.

The delimited layouts use the same section grammar as the generic ``bw2io``
Excel importer.  They remain separate format identifiers because a delimiter
is part of the syntax contract and CSV filenames are otherwise ambiguous with
SimaPro exports.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

import bw2io
from bw2io import CSVImporter

from brightpath.core.context import BackgroundContext, BiosphereProfile, FormatProfile, InventoryContext
from brightpath.exceptions import ExcelSerializationError
from brightpath.models import BackgroundProfile, InventoryDocument, InventoryFormat, default_biosphere_profile

from .brightway_excel import (
    _ACTIVITY_FIELD_ORDER,
    _ACTIVITY_SKIP_FIELDS,
    _BIOSPHERE_METADATA_FIELDS,
    _EXCHANGE_FIELD_ORDER,
    _EXCHANGE_SKIP_FIELDS,
    _FORMAT_METADATA_FIELDS,
    _JSON_PREFIX,
    _MISSING,
    _PARAMETER_FIELD_ORDER,
    _PROFILE_METADATA_FIELDS,
    _decode_tagged_values,
    _ordered_fields,
    _serialize_value,
)

_DELIMITERS = {".csv": ",", ".tsv": "\t"}
_FORMAT_IDS = {
    ".csv": InventoryFormat.BRIGHTWAY_CSV.value,
    ".tsv": InventoryFormat.BRIGHTWAY_TSV.value,
}
_FORMULA_PREFIXES = ("=", "+", "-", "@")


class _TSVExtractor:
    """Read TSV rows without changing ``CSVImporter.extractor`` globally."""

    @classmethod
    def extract(cls, filepath, encoding="utf-8-sig"):
        source = Path(filepath)
        if not source.is_file():
            raise FileNotFoundError(f"Brightway TSV file not found: {source}")
        with source.open(encoding=encoding, newline="") as handle:
            rows = list(csv.reader(handle, delimiter="\t"))
        return [source.name, rows]


class _TSVImporter(CSVImporter):
    extractor = _TSVExtractor


def load_brightway_delimited(
    path: str | Path,
    *,
    delimiter: str | None = None,
    background_profile: BackgroundProfile | None = None,
    biosphere_profile: BiosphereProfile | None = None,
    context: InventoryContext | None = None,
) -> InventoryDocument:
    """Load a Brightway block-layout CSV or TSV inventory.

    :param path: Existing ``.csv`` or ``.tsv`` inventory file.
    :param delimiter: Optional explicit delimiter. It must agree with the file
        suffix; only comma and tab layouts are supported.
    :param background_profile: Legacy technosphere profile used when no full
        *context* is supplied. Embedded BrightPath metadata is the fallback.
    :param biosphere_profile: Explicit biosphere profile for legacy calls.
    :param context: Complete source context. Its format must agree with the
        selected CSV or TSV syntax.
    :return: A software-neutral inventory document.
    """

    source = Path(path).expanduser()
    if not source.is_file():
        raise FileNotFoundError(f"Brightway delimited inventory not found: {source}")
    selected_delimiter, format_id, _ = _resolve_layout(source, delimiter=delimiter, for_write=False)

    importer = CSVImporter(source) if selected_delimiter == "," else _TSVImporter(source)
    if "biosphere-2-3-categories" not in bw2io.migrations:
        bw2io.create_core_migrations()
    importer.apply_strategies()

    data = _decode_tagged_keys_and_values(importer.data)
    metadata = _decode_tagged_keys_and_values(getattr(importer, "metadata", {}) or {})
    embedded_profile = _profile_from_metadata(metadata)
    embedded_biosphere = _biosphere_profile_from_metadata(metadata)
    if context is not None and context.format.format_id != format_id:
        raise ValueError(f"Explicit context format must be {format_id}.")
    if context is None:
        profile = (background_profile or embedded_profile).normalized()
        technosphere = profile.to_technosphere_profile()
        context = InventoryContext(
            format=_format_profile_from_metadata(metadata, format_id),
            background=BackgroundContext(
                technosphere=technosphere,
                biosphere=(biosphere_profile or embedded_biosphere or default_biosphere_profile(technosphere)),
            ),
        )

    return InventoryDocument(
        data=data,
        context=context,
        database_name=_decode_tagged_keys_and_values(getattr(importer, "db_name", "")),
        metadata=metadata,
        database_parameters=_decode_tagged_keys_and_values(getattr(importer, "database_parameters", None)),
        project_parameters=_decode_tagged_keys_and_values(getattr(importer, "project_parameters", None)),
    )


def write_brightway_delimited(
    document: InventoryDocument,
    path: str | Path,
    delimiter: str | None = None,
) -> Path:
    """Write a deterministic UTF-8 Brightway block-layout CSV or TSV file.

    If *path* has no suffix, *delimiter* is required and selects ``.csv`` or
    ``.tsv``. Formula-like text is encoded as a tagged JSON string to prevent
    spreadsheet formula execution and is restored by
    :func:`load_brightway_delimited`.

    :return: The absolute destination path.
    """

    destination = Path(path).expanduser()
    selected_delimiter, _, destination = _resolve_layout(destination, delimiter=delimiter, for_write=True)
    destination = destination.resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)

    with destination.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter=selected_delimiter, lineterminator="\n")
        writer.writerows(_render_rows(document))

    return destination


def _resolve_layout(path: Path, *, delimiter: str | None, for_write: bool) -> tuple[str, str, Path]:
    if delimiter is not None and delimiter not in set(_DELIMITERS.values()):
        raise ValueError("Brightway delimited inventories support only comma or tab delimiters.")

    suffix = path.suffix.lower()
    if not suffix:
        if not for_write:
            raise ValueError("Brightway delimited inventories must use a .csv or .tsv filename.")
        if delimiter is None:
            raise ValueError("A delimiter is required when the destination has no .csv or .tsv suffix.")
        suffix = ".csv" if delimiter == "," else ".tsv"
        path = path.with_suffix(suffix)
    if suffix not in _DELIMITERS:
        raise ValueError("Brightway delimited inventories must use a .csv or .tsv filename.")

    inferred = _DELIMITERS[suffix]
    if delimiter is not None and delimiter != inferred:
        display = "comma" if inferred == "," else "tab"
        raise ValueError(f"The {suffix} suffix requires the {display} delimiter.")
    return inferred, _FORMAT_IDS[suffix], path


def _render_rows(document: InventoryDocument) -> list[list]:
    rows: list[list] = []
    _append_parameter_section(rows, "Project parameters", document.project_parameters)

    rows.append(["Database", _serialize_cell(document.database_name or "brightpath-inventory", field="database")])
    metadata = document.metadata
    profile = document.background_profile.normalized()
    if profile.family:
        metadata[_PROFILE_METADATA_FIELDS["family"]] = profile.family
    if profile.version:
        metadata[_PROFILE_METADATA_FIELDS["version"]] = profile.version
    if profile.system_model:
        metadata[_PROFILE_METADATA_FIELDS["system_model"]] = profile.system_model
    biosphere = document.biosphere_profile
    metadata[_BIOSPHERE_METADATA_FIELDS["family"]] = biosphere.family
    metadata[_BIOSPHERE_METADATA_FIELDS["version"]] = biosphere.version
    format_profile = document.context.format
    for attribute, field in _FORMAT_METADATA_FIELDS.items():
        value = getattr(format_profile, attribute)
        if value:
            metadata[field] = value

    for field in sorted(metadata):
        rows.append([_serialize_cell(field, field="metadata field"), _serialize_cell(metadata[field], field=field)])
    rows.append([])

    _append_parameter_section(rows, "Database parameters", document.database_parameters)
    for activity in document.data:
        _append_activity(rows, activity)
        rows.append([])
    return rows


def _append_activity(rows: list[list], activity: dict) -> None:
    if not isinstance(activity, dict):
        raise ExcelSerializationError("Every inventory dataset must be a dictionary.")

    rows.append(["Activity", _serialize_cell(activity.get("name", ""), field="name")])
    fields = _ordered_fields([activity], _ACTIVITY_FIELD_ORDER, _ACTIVITY_SKIP_FIELDS)
    for field in fields:
        rows.append(
            [
                _serialize_cell(field, field="activity field"),
                _serialize_cell(activity.get(field, _MISSING), field=field),
            ]
        )

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
                "cannot be represented in one Brightway delimited activity section."
            )
        rows.append(["Parameters", _serialize_cell(next(iter(groups), ""), field="parameter group")])
        _append_labelled_rows(
            rows,
            parameters,
            preferred=_PARAMETER_FIELD_ORDER,
            skipped={"group"},
        )

    exchanges = activity.get("exchanges", [])
    if not isinstance(exchanges, list):
        raise ExcelSerializationError(f"Activity {activity.get('name')!r} exchanges must be a list.")
    rows.append(["Exchanges"])
    if exchanges:
        if not all(isinstance(exchange, dict) for exchange in exchanges):
            raise ExcelSerializationError(f"Activity {activity.get('name')!r} contains a non-dictionary exchange.")
        _append_labelled_rows(
            rows,
            exchanges,
            preferred=_EXCHANGE_FIELD_ORDER,
            skipped=_EXCHANGE_SKIP_FIELDS,
        )


def _append_parameter_section(rows: list[list], label: str, parameters: list[dict] | None) -> None:
    if not parameters:
        return
    if not isinstance(parameters, list) or not all(isinstance(parameter, dict) for parameter in parameters):
        raise ExcelSerializationError(f"{label} must be a list of dictionaries.")
    rows.append([label])
    _append_labelled_rows(rows, parameters, preferred=_PARAMETER_FIELD_ORDER, skipped=set())
    rows.append([])


def _append_labelled_rows(
    rows: list[list],
    values: list[dict],
    *,
    preferred: list[str],
    skipped: set[str],
) -> None:
    columns = _ordered_fields(values, preferred, skipped)
    rows.append([_serialize_cell(field, field="column label") for field in columns])
    for value in values:
        rows.append([_serialize_cell(value.get(field, _MISSING), field=field) for field in columns])


def _serialize_cell(value, *, field: str):
    if value is _MISSING:
        return ""
    serialized = _serialize_value(value, field=field)
    if isinstance(serialized, str) and _looks_like_formula(serialized):
        return _JSON_PREFIX + json.dumps(serialized, ensure_ascii=False)
    return serialized


def _looks_like_formula(value: str) -> bool:
    return value.lstrip(" \r\n\t").startswith(_FORMULA_PREFIXES)


def _decode_tagged_keys_and_values(value):
    decoded = _decode_tagged_values(value)
    if isinstance(decoded, dict):
        return {_decode_tagged_values(key): _decode_tagged_keys_and_values(item) for key, item in decoded.items()}
    if isinstance(decoded, list):
        return [_decode_tagged_keys_and_values(item) for item in decoded]
    if isinstance(decoded, tuple):
        return tuple(_decode_tagged_keys_and_values(item) for item in decoded)
    return decoded


def _profile_from_metadata(metadata: dict) -> BackgroundProfile:
    return BackgroundProfile(
        family=str(metadata.get(_PROFILE_METADATA_FIELDS["family"], "") or ""),
        version=str(metadata.get(_PROFILE_METADATA_FIELDS["version"], "") or ""),
        system_model=str(metadata.get(_PROFILE_METADATA_FIELDS["system_model"], "") or ""),
    )


def _biosphere_profile_from_metadata(metadata: dict) -> BiosphereProfile | None:
    family = str(metadata.get(_BIOSPHERE_METADATA_FIELDS["family"], "") or "")
    version = str(metadata.get(_BIOSPHERE_METADATA_FIELDS["version"], "") or "")
    if not family or not version:
        return None
    return BiosphereProfile(family, version)


def _format_profile_from_metadata(metadata: dict, format_id: str) -> FormatProfile:
    return FormatProfile(
        format_id,
        format_version=str(metadata.get(_FORMAT_METADATA_FIELDS["format_version"], "") or ""),
        dialect=str(metadata.get(_FORMAT_METADATA_FIELDS["dialect"], "") or ""),
        encoding=str(metadata.get(_FORMAT_METADATA_FIELDS["encoding"], "") or ""),
    )
