from __future__ import annotations

import ast
import csv
import logging
import os
import re
from copy import deepcopy
from pathlib import Path

import bw2io
from bw2io import CSVImporter
from bw2io.importers.excel import ExcelImporter

from brightpath.catalogs import available_catalog_profiles, load_background_catalog
from brightpath.models import AnalysisResult, BackgroundProfile, CandidateSummary, Issue
from brightpath.simaproconverter import SimaproConverter
from brightpath.utils import inspect_brightway_inventory


SOURCE_FORMAT_BRIGHTWAY_EXCEL = "brightway_excel"
SOURCE_FORMAT_BRIGHTWAY_CSV = "brightway_csv"
SOURCE_FORMAT_BRIGHTWAY_TSV = "brightway_tsv"
SOURCE_FORMAT_SIMAPRO_CSV = "simapro_csv"
SOFTWARE_BRIGHTWAY = "brightway"
SOFTWARE_SIMAPRO = "simapro"

_ACTIVITY_PATH_PATTERN = re.compile(
    r"^(?P<path>activity\[(?P<index>\d+)\](?:\.exchanges\[(?P<exchange_index>\d+)\])?):\s*(?P<message>.+)$"
)
_SIMAPRO_IDENTITY_PATTERN = re.compile(
    r"^(?P<identity>\(.+?\))\s+(?P<message>.+)$"
)
_TUPLE_PATTERN = re.compile(r"\([^()]+\)")
_TRAILING_SOURCE_PATTERN = re.compile(
    r"(?is)(?:^|(?<=[.!?\n]))\s*(?P<label>sources?)\s*:\s*(?P<source>.+?)\s*$"
)
_ROOT_LOGGER = "brightpath"


class _CollectingHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


class InventoryValidationError(ValueError):
    def __init__(self, result: AnalysisResult) -> None:
        self.result = result
        super().__init__(_format_error_summary(result))


class _TSVExtractor:
    @classmethod
    def extract(cls, filepath, encoding="utf-8-sig"):
        assert os.path.exists(filepath), f"Can't find file at path {filepath}"
        with open(filepath, encoding=encoding, newline="") as handle:
            reader = csv.reader(handle, delimiter="\t")
            data = [row for row in reader]
        return [os.path.basename(filepath), data]


class _TSVImporter(CSVImporter):
    extractor = _TSVExtractor


def infer_source_format(path: str | Path) -> str:
    suffix = Path(path).suffix.lower()
    if suffix == ".xlsx":
        return SOURCE_FORMAT_BRIGHTWAY_EXCEL
    if suffix == ".tsv":
        return SOURCE_FORMAT_BRIGHTWAY_TSV
    if suffix == ".csv":
        return SOURCE_FORMAT_SIMAPRO_CSV
    if suffix == ".xls":
        raise ValueError(
            "BrightPath analysis currently supports Brightway .xlsx workbooks, not .xls files."
        )
    raise ValueError(
        f"Unsupported inventory source format for analysis: {suffix or 'no extension'}."
    )


def analyze_inventory(
    *,
    path: str | Path,
    source_format: str | None = None,
    source_profile: BackgroundProfile | None = None,
) -> AnalysisResult:
    resolved_path = Path(path)
    resolved_format = source_format or infer_source_format(resolved_path)
    profile = (source_profile or BackgroundProfile()).normalized()

    if resolved_format == SOURCE_FORMAT_BRIGHTWAY_EXCEL:
        return _analyze_brightway_excel(
            path=resolved_path,
            source_profile=profile,
        )
    if resolved_format == SOURCE_FORMAT_BRIGHTWAY_CSV:
        return _analyze_brightway_delimited(
            path=resolved_path,
            source_profile=profile,
            detected_format=SOURCE_FORMAT_BRIGHTWAY_CSV,
        )
    if resolved_format == SOURCE_FORMAT_BRIGHTWAY_TSV:
        return _analyze_brightway_delimited(
            path=resolved_path,
            source_profile=profile,
            detected_format=SOURCE_FORMAT_BRIGHTWAY_TSV,
        )
    if resolved_format == SOURCE_FORMAT_SIMAPRO_CSV:
        return _analyze_simapro_csv(
            path=resolved_path,
            source_profile=profile,
        )

    raise ValueError(f"Unsupported source format: {resolved_format!r}.")


def validate_inventory(
    *,
    path: str | Path,
    source_format: str | None = None,
    source_profile: BackgroundProfile | None = None,
) -> AnalysisResult:
    result = analyze_inventory(
        path=path,
        source_format=source_format,
        source_profile=source_profile,
    )
    if result.has_errors:
        raise InventoryValidationError(result)
    return result


def _analyze_brightway_excel(
    *,
    path: Path,
    source_profile: BackgroundProfile,
) -> AnalysisResult:
    return _analyze_brightway_inventory_data(
        path=path,
        source_profile=source_profile,
        detected_format=SOURCE_FORMAT_BRIGHTWAY_EXCEL,
        loader=_load_brightway_excel_without_validation,
        parse_error_code="brightway_excel_parse_failed",
    )


def _analyze_brightway_delimited(
    *,
    path: Path,
    source_profile: BackgroundProfile,
    detected_format: str,
) -> AnalysisResult:
    return _analyze_brightway_inventory_data(
        path=path,
        source_profile=source_profile,
        detected_format=detected_format,
        loader=_load_brightway_delimited_without_validation,
        parse_error_code="brightway_tabular_parse_failed",
    )


def _analyze_brightway_inventory_data(
    *,
    path: Path,
    source_profile: BackgroundProfile,
    detected_format: str,
    loader,
    parse_error_code: str,
) -> AnalysisResult:
    result = AnalysisResult(
        detected_software=SOFTWARE_BRIGHTWAY,
        detected_format=detected_format,
        source_profile=source_profile,
    )
    warnings: list[Issue] = []

    with _capture_warnings() as collector:
        try:
            inventory_data = loader(path)
        except Exception as exc:
            result.file_issues.extend(
                _exception_to_file_issues(
                    exc,
                    default_code=parse_error_code,
                )
            )
            inventory_data = []

    warnings.extend(_warning_issues(collector.messages))
    result.inventory_data = deepcopy(inventory_data)
    result.candidates = _build_candidates(inventory_data)
    result.source_profile, profile_issues = _resolve_background_profile(
        inventory_data,
        source_profile,
    )
    result.file_issues.extend(profile_issues)

    if inventory_data:
        validation_errors, validation_warnings = inspect_brightway_inventory(
            inventory_data,
            require_simapro_category=False,
        )
        if validation_errors:
            _attach_activity_issues(
                candidates=result.candidates,
                candidate_issues=_issues_from_brightway_validation_messages(
                    validation_errors,
                    severity="error",
                    code="inventory_validation_error",
                ),
                file_issues=result.file_issues,
            )
        if validation_warnings:
            _attach_activity_issues(
                candidates=result.candidates,
                candidate_issues=_issues_from_brightway_validation_messages(
                    validation_warnings,
                    severity="warning",
                    code="inventory_validation_warning",
                ),
                file_issues=result.file_issues,
            )
        background_issues, background_file_issues = _validate_background_links(
            inventory_data,
            result.source_profile,
        )
        _attach_activity_issues(
            candidates=result.candidates,
            candidate_issues=background_issues,
            file_issues=result.file_issues,
        )
        result.file_issues.extend(background_file_issues)

    result.file_issues.extend(warnings)
    return result


def _analyze_simapro_csv(
    *,
    path: Path,
    source_profile: BackgroundProfile,
) -> AnalysisResult:
    result = AnalysisResult(
        detected_software=SOFTWARE_SIMAPRO,
        detected_format=SOURCE_FORMAT_SIMAPRO_CSV,
        source_profile=source_profile,
    )
    converter: SimaproConverter | None = None
    inventory_data: list[dict] = []

    with _capture_warnings() as collector:
        try:
            converter = SimaproConverter(
                filepath=str(path),
                ecoinvent_version=_reference_ecoinvent_version(source_profile),
            )
            inventory_data = converter.convert_to_brightway(format="data")
        except Exception as exc:
            if converter is not None and getattr(converter, "i", None) is not None:
                inventory_data = deepcopy(getattr(converter.i, "data", []))
            result.file_issues.extend(
                _issues_from_simapro_exception(
                    exc,
                    inventory_data=inventory_data,
                )
            )

    result.inventory_data = deepcopy(inventory_data)
    result.candidates = _build_candidates(inventory_data)
    result.source_profile, profile_issues = _resolve_background_profile(
        inventory_data,
        source_profile,
    )
    result.file_issues.extend(profile_issues)
    _attach_identity_issues(
        candidates=result.candidates,
        file_issues=result.file_issues,
    )
    if inventory_data:
        validation_errors, validation_warnings = inspect_brightway_inventory(
            inventory_data,
            require_simapro_category=False,
        )
        if validation_errors:
            _attach_activity_issues(
                candidates=result.candidates,
                candidate_issues=_issues_from_brightway_validation_messages(
                    validation_errors,
                    severity="error",
                    code="inventory_validation_error",
                ),
                file_issues=result.file_issues,
            )
        if validation_warnings:
            _attach_activity_issues(
                candidates=result.candidates,
                candidate_issues=_issues_from_brightway_validation_messages(
                    validation_warnings,
                    severity="warning",
                    code="inventory_validation_warning",
                ),
                file_issues=result.file_issues,
            )
        background_issues, background_file_issues = _validate_background_links(
            inventory_data,
            result.source_profile,
        )
        _attach_activity_issues(
            candidates=result.candidates,
            candidate_issues=background_issues,
            file_issues=result.file_issues,
        )
        result.file_issues.extend(background_file_issues)
    result.file_issues.extend(_warning_issues(collector.messages))
    return result


def _load_brightway_excel_without_validation(path: Path) -> list[dict]:
    if not path.is_file():
        raise FileNotFoundError("The file could not be found.")
    if path.suffix.lower() != ".xlsx":
        raise ValueError("Brightway Excel analysis requires a .xlsx workbook.")

    importer = ExcelImporter(path)
    if "biosphere-2-3-categories" not in bw2io.migrations:
        bw2io.create_core_migrations()
    importer.apply_strategies()
    return importer.data


def _load_brightway_delimited_without_validation(path: Path) -> list[dict]:
    if not path.is_file():
        raise FileNotFoundError("The file could not be found.")

    suffix = path.suffix.lower()
    if suffix == ".csv":
        importer = CSVImporter(path)
    elif suffix == ".tsv":
        importer = _TSVImporter(path)
    else:
        raise ValueError("Brightway delimited analysis requires a .csv or .tsv file.")

    if "biosphere-2-3-categories" not in bw2io.migrations:
        bw2io.create_core_migrations()
    importer.apply_strategies()
    return importer.data


def _build_candidates(inventory_data: list[dict]) -> list[CandidateSummary]:
    candidates: list[CandidateSummary] = []
    for index, dataset in enumerate(inventory_data):
        description_hint, source_hint = _extract_candidate_metadata_hints(dataset)
        candidates.append(
            CandidateSummary(
                index=index,
                name=str(dataset.get("name") or ""),
                reference_product=str(dataset.get("reference product") or ""),
                location=str(dataset.get("location") or ""),
                unit=str(dataset.get("unit") or ""),
                description_hint=description_hint,
                source_hint=source_hint,
            )
        )
    return candidates


def _extract_candidate_metadata_hints(dataset: dict) -> tuple[str, str]:
    description_hint = _stringify_metadata_hint(dataset.get("comment"))
    source_hint = _stringify_metadata_hint(dataset.get("source"))
    if source_hint:
        return description_hint, source_hint

    split_description, extracted_source = _split_trailing_source_section(description_hint)
    if extracted_source:
        return split_description, extracted_source
    return description_hint, ""


def _split_trailing_source_section(comment: str) -> tuple[str, str]:
    normalized_comment = (comment or "").strip()
    if not normalized_comment:
        return "", ""

    last_match = None
    for match in _TRAILING_SOURCE_PATTERN.finditer(normalized_comment):
        last_match = match
    if last_match is None:
        return normalized_comment, ""
    if last_match.start() == 0:
        return normalized_comment, ""

    description = normalized_comment[: last_match.start()].rstrip()
    source = last_match.group("source").strip()
    return description, source


def _stringify_metadata_hint(value) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float, bool)):
        return str(value).strip()
    if isinstance(value, dict):
        lines = []
        for key, item in value.items():
            normalized_item = _stringify_metadata_hint(item)
            if not normalized_item:
                continue
            label = str(key).strip()
            if label:
                lines.append(f"{label}: {normalized_item}")
            else:
                lines.append(normalized_item)
        return "\n".join(lines).strip()
    if isinstance(value, (list, tuple, set)):
        parts = [_stringify_metadata_hint(item) for item in value]
        return "\n".join(part for part in parts if part).strip()
    return str(value).strip()


def _issues_from_brightway_validation_exception(exc: Exception) -> list[Issue]:
    return _issues_from_brightway_validation_messages(
        _exception_lines(exc),
        severity="error",
        code="inventory_validation_error",
    )


def _issues_from_brightway_validation_messages(
    messages: list[str],
    *,
    severity: str,
    code: str,
) -> list[Issue]:
    issues: list[Issue] = []
    for line in messages:
        match = _ACTIVITY_PATH_PATTERN.match(line)
        if match:
            issues.append(
                Issue(
                    severity=severity,
                    code=code,
                    message=line,
                    path=match.group("path"),
                )
            )
            continue
        issues.append(
            Issue(
                severity=severity,
                code=code,
                message=line,
            )
        )
    return issues


def _issues_from_simapro_exception(
    exc: Exception,
    *,
    inventory_data: list[dict],
) -> list[Issue]:
    issues: list[Issue] = []
    for line in _exception_lines(exc):
        identity_match = _SIMAPRO_IDENTITY_PATTERN.match(line)
        if identity_match:
            issues.append(
                Issue(
                    severity="error",
                    code="inventory_validation_error",
                    message=identity_match.group("message"),
                    path=identity_match.group("identity"),
                )
            )
            continue
        if "Duplicate datasets found after SimaPro name parsing:" in line:
            for duplicate_identity in _extract_duplicate_identities(line):
                issues.append(
                    Issue(
                        severity="error",
                        code="duplicate_dataset_identity",
                        message=(
                            "Dataset identity must be unique after SimaPro name parsing."
                        ),
                        path=duplicate_identity,
                    )
                )
            continue
        issues.append(
            Issue(
                severity="error",
                code="simapro_csv_analysis_failed",
                message=line,
            )
        )

    if issues:
        return issues

    if inventory_data:
        return [
            Issue(
                severity="error",
                code="simapro_csv_analysis_failed",
                message="SimaPro CSV analysis failed unexpectedly.",
            )
        ]

    return _exception_to_file_issues(
        exc,
        default_code="simapro_csv_parse_failed",
    )


def _attach_activity_issues(
    *,
    candidates: list[CandidateSummary],
    candidate_issues: list[Issue],
    file_issues: list[Issue],
) -> None:
    for issue in candidate_issues:
        candidate_index = _candidate_index_from_activity_path(issue.path)
        if candidate_index is None or candidate_index >= len(candidates):
            file_issues.append(issue)
            continue
        candidates[candidate_index].issues.append(issue)


def _attach_identity_issues(
    *,
    candidates: list[CandidateSummary],
    file_issues: list[Issue],
) -> None:
    remaining_file_issues: list[Issue] = []
    for issue in file_issues:
        indexes = _candidate_indexes_from_identity(issue.path, candidates)
        if not indexes:
            remaining_file_issues.append(issue)
            continue
        for candidate_index in indexes:
            candidates[candidate_index].issues.append(issue)
    file_issues[:] = remaining_file_issues


def _candidate_index_from_activity_path(path: str) -> int | None:
    match = _ACTIVITY_PATH_PATTERN.match(f"{path}: placeholder")
    if not match:
        return None
    return int(match.group("index"))


def _candidate_indexes_from_identity(
    path: str,
    candidates: list[CandidateSummary],
) -> list[int]:
    if not path.startswith("("):
        return []
    try:
        identity = ast.literal_eval(path)
    except (SyntaxError, ValueError):
        return []
    if not isinstance(identity, tuple) or len(identity) != 3:
        return []

    normalized_identity = tuple("" if value is None else str(value) for value in identity)
    return [
        candidate.index
        for candidate in candidates
        if (
            candidate.name,
            candidate.reference_product,
            candidate.location,
        ) == normalized_identity
    ]


def _extract_duplicate_identities(message: str) -> list[str]:
    identities: list[str] = []
    for candidate in _TUPLE_PATTERN.findall(message):
        try:
            parsed = ast.literal_eval(candidate)
        except (SyntaxError, ValueError):
            continue
        if not isinstance(parsed, tuple) or len(parsed) != 3:
            continue
        identities.append(repr(tuple("" if value is None else str(value) for value in parsed)))
    return identities


def _exception_lines(exc: Exception) -> list[str]:
    raw_lines = [line.strip() for line in str(exc).splitlines() if line.strip()]
    if raw_lines and raw_lines[0].lower().startswith("inventory validation failed"):
        raw_lines = raw_lines[1:]
    return raw_lines or [str(exc)]


def _exception_to_file_issues(
    exc: Exception,
    *,
    default_code: str,
) -> list[Issue]:
    return [
        Issue(
            severity="error",
            code=default_code,
            message=line,
        )
        for line in _exception_lines(exc)
    ]


def _reference_ecoinvent_version(source_profile: BackgroundProfile) -> str:
    if source_profile.family == "ecoinvent" and source_profile.version:
        return source_profile.version
    return "3.9"


def _resolve_background_profile(
    inventory_data: list[dict],
    source_profile: BackgroundProfile,
) -> tuple[BackgroundProfile, list[Issue]]:
    normalized = source_profile.normalized()
    issues: list[Issue] = []

    if not inventory_data:
        return normalized, issues

    if normalized.family and normalized.version and normalized.system_model:
        return normalized, issues

    candidate_profiles = available_catalog_profiles(family=normalized.family)
    if not candidate_profiles:
        return normalized, issues

    exchange_keys = _collect_technosphere_targets(inventory_data)
    if not exchange_keys:
        return normalized, issues

    scored_profiles: list[tuple[int, BackgroundProfile]] = []
    for profile in candidate_profiles:
        try:
            catalog = load_background_catalog(profile)
        except FileNotFoundError:
            continue
        if normalized.version and profile.version != normalized.version:
            continue
        if normalized.system_model and profile.system_model != normalized.system_model:
            continue
        score = sum(1 for key in exchange_keys if key in catalog.technosphere)
        if score > 0:
            scored_profiles.append((score, profile))

    if not scored_profiles:
        return normalized, issues

    scored_profiles.sort(
        key=lambda item: (item[0], _profile_preference_key(item[1])),
        reverse=True,
    )
    best_score, best_profile = scored_profiles[0]
    tied = [profile for score, profile in scored_profiles if score == best_score]
    if len(tied) != 1:
        preferred_profile = max(tied, key=_profile_preference_key)
        resolved = BackgroundProfile(
            family=normalized.family or preferred_profile.family,
            version=normalized.version or preferred_profile.version,
            system_model=normalized.system_model or preferred_profile.system_model,
        ).normalized()
        issues.append(
            Issue(
                severity="warning",
                code="background_profile_assumed",
                message=(
                    f"Several local reference catalogs matched equally well. BrightPath selected "
                    f"{resolved.family} {resolved.version} {resolved.system_model} using its "
                    "default tie-breaker: most recent version, then cut-off when available."
                ),
                suggested_fix=(
                    "Review the chosen background version and system model and override them in "
                    "the calling workflow if needed."
                ),
            )
        )
        return resolved, issues

    resolved = BackgroundProfile(
        family=normalized.family or best_profile.family,
        version=normalized.version or best_profile.version,
        system_model=normalized.system_model or best_profile.system_model,
    ).normalized()
    issues.append(
        Issue(
            severity="info",
            code="background_profile_inferred",
            message=(
                f"Inferred background profile {resolved.family} {resolved.version} "
                f"{resolved.system_model} from technosphere exchange matches."
            ),
        )
    )
    return resolved, issues


def _profile_preference_key(profile: BackgroundProfile) -> tuple[tuple[int, ...], int, str, str]:
    return (
        _version_sort_key(profile.version),
        1 if profile.system_model == "cutoff" else 0,
        profile.family,
        profile.system_model,
    )


def _version_sort_key(version: str) -> tuple[int, ...]:
    if not version:
        return tuple()
    parts: list[int] = []
    for token in version.split("."):
        try:
            parts.append(int(token))
        except ValueError:
            parts.append(-1)
    return tuple(parts)


def _collect_technosphere_targets(inventory_data: list[dict]) -> list[tuple[str, str, str, str]]:
    collected: list[tuple[str, str, str, str]] = []
    for activity in inventory_data:
        for exchange in activity.get("exchanges", []):
            if exchange.get("type") != "technosphere":
                continue
            collected.append(
                (
                    str(exchange.get("name") or ""),
                    str(exchange.get("reference product") or ""),
                    str(exchange.get("location") or ""),
                    str(exchange.get("unit") or ""),
                )
            )
    return collected


def _validate_background_links(
    inventory_data: list[dict],
    source_profile: BackgroundProfile,
) -> tuple[list[Issue], list[Issue]]:
    normalized = source_profile.normalized()
    if not (normalized.family and normalized.version and normalized.system_model):
        return [], []

    requires_catalog = any(
        exchange.get("type") in {"technosphere", "biosphere"}
        for activity in inventory_data
        for exchange in activity.get("exchanges", [])
    )
    if not requires_catalog:
        return [], []

    try:
        catalog = load_background_catalog(normalized)
    except FileNotFoundError:
        return [], [
            Issue(
                severity="error",
                code="background_catalog_missing",
                message=(
                    "No local reference catalog is available for "
                    f"{normalized.family} {normalized.version} {normalized.system_model}."
                ),
                suggested_fix=(
                    "Generate or install the matching BrightPath reference catalog before "
                    "validating exchange links against this background profile."
                ),
            )
        ]

    internal_targets = {
        (
            str(activity.get("name") or ""),
            str(activity.get("reference product") or ""),
            str(activity.get("location") or ""),
            str(activity.get("unit") or ""),
        )
        for activity in inventory_data
    }
    issues: list[Issue] = []
    unknown_technosphere_by_activity: dict[int, list[tuple[str, str, str, str]]] = {}
    for activity_index, activity in enumerate(inventory_data):
        for exchange_index, exchange in enumerate(activity.get("exchanges", [])):
            exchange_type = exchange.get("type")
            if exchange_type == "technosphere":
                key = (
                    str(exchange.get("name") or ""),
                    str(exchange.get("reference product") or ""),
                    str(exchange.get("location") or ""),
                    str(exchange.get("unit") or ""),
                )
                if key not in internal_targets and key not in catalog.technosphere:
                    unknown_technosphere_by_activity.setdefault(activity_index, []).append(key)
            elif exchange_type == "biosphere":
                key = (
                    str(exchange.get("name") or ""),
                    tuple(str(item) for item in exchange.get("categories", ())),
                    str(exchange.get("unit") or ""),
                )
                if key not in catalog.biosphere:
                    issues.append(
                        Issue(
                            severity="error",
                            code="unknown_biosphere_flow",
                            message=(
                                "Biosphere exchange does not match the selected background biosphere reference catalog."
                            ),
                            path=_context_path(activity_index, exchange_index),
                        )
                    )
    for activity_index, unknown_exchanges in unknown_technosphere_by_activity.items():
        issues.append(
            Issue(
                severity="error",
                code="unknown_technosphere_target",
                message=_format_unknown_technosphere_message(unknown_exchanges),
                path=f"activity[{activity_index}]",
                suggested_fix=(
                    "Check whether these technosphere exchanges belong to another background "
                    "version or system model, or correct their name, reference product, "
                    "location, or unit."
                ),
            )
        )
    return issues, []


def _context_path(activity_index: int, exchange_index: int) -> str:
    return f"activity[{activity_index}].exchanges[{exchange_index}]"


def _format_unknown_technosphere_message(
    exchanges: list[tuple[str, str, str, str]],
) -> str:
    lines = [
        "Technosphere exchanges do not match an uploaded dataset or the selected background reference catalog."
    ]
    for name, reference_product, location, unit in exchanges:
        lines.append(
            f"- {name or '?'} | {reference_product or '?'} | {location or '?'} | {unit or '?'}"
        )
    return "\n".join(lines)


def _warning_issues(messages: list[str]) -> list[Issue]:
    seen: set[str] = set()
    issues: list[Issue] = []
    for message in messages:
        if message in seen:
            continue
        seen.add(message)
        issues.append(
            Issue(
                severity="warning",
                code="brightpath_warning",
                message=message,
            )
        )
    return issues


def _format_error_summary(result: AnalysisResult) -> str:
    messages: list[str] = []
    messages.extend(issue.message for issue in result.file_issues if issue.severity == "error")
    for candidate in result.candidates:
        for issue in candidate.issues:
            if issue.severity == "error":
                label = candidate.name or f"activity[{candidate.index}]"
                messages.append(f"{label}: {issue.message}")
    if not messages:
        return "Inventory validation failed."
    return "Inventory validation failed:\n" + "\n".join(messages)


class _capture_warnings:
    def __enter__(self) -> _CollectingHandler:
        self.logger = logging.getLogger(_ROOT_LOGGER)
        self.previous_level = self.logger.level
        self.handler = _CollectingHandler()
        self.logger.addHandler(self.handler)
        if self.logger.level == logging.NOTSET or self.logger.level > logging.WARNING:
            self.logger.setLevel(logging.WARNING)
        return self.handler

    def __exit__(self, exc_type, exc, tb) -> None:
        self.logger.removeHandler(self.handler)
        self.logger.setLevel(self.previous_level)
