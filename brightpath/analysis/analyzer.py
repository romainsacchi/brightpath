from __future__ import annotations

import ast
import logging
import re
from copy import deepcopy
from pathlib import Path

import bw2io
from bw2io.importers.excel import ExcelImporter

from brightpath.models import AnalysisResult, BackgroundProfile, CandidateSummary, Issue
from brightpath.simaproconverter import SimaproConverter
from brightpath.utils import inspect_brightway_inventory


SOURCE_FORMAT_BRIGHTWAY_EXCEL = "brightway_excel"
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
_ROOT_LOGGER = "brightpath"


class _CollectingHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


def infer_source_format(path: str | Path) -> str:
    suffix = Path(path).suffix.lower()
    if suffix == ".xlsx":
        return SOURCE_FORMAT_BRIGHTWAY_EXCEL
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
    if resolved_format == SOURCE_FORMAT_SIMAPRO_CSV:
        return _analyze_simapro_csv(
            path=resolved_path,
            source_profile=profile,
        )

    raise ValueError(f"Unsupported source format: {resolved_format!r}.")


def _analyze_brightway_excel(
    *,
    path: Path,
    source_profile: BackgroundProfile,
) -> AnalysisResult:
    result = AnalysisResult(
        detected_software=SOFTWARE_BRIGHTWAY,
        detected_format=SOURCE_FORMAT_BRIGHTWAY_EXCEL,
        source_profile=source_profile,
    )
    warnings: list[Issue] = []

    with _capture_warnings() as collector:
        try:
            inventory_data = _load_brightway_excel_without_validation(path)
        except Exception as exc:
            result.file_issues.extend(
                _exception_to_file_issues(
                    exc,
                    default_code="brightway_excel_parse_failed",
                )
            )
            inventory_data = []

    warnings.extend(_warning_issues(collector.messages))
    result.inventory_data = deepcopy(inventory_data)
    result.candidates = _build_candidates(inventory_data)

    if inventory_data:
        validation_errors, _validation_warnings = inspect_brightway_inventory(
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
    _attach_identity_issues(
        candidates=result.candidates,
        file_issues=result.file_issues,
    )
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


def _build_candidates(inventory_data: list[dict]) -> list[CandidateSummary]:
    candidates: list[CandidateSummary] = []
    for index, dataset in enumerate(inventory_data):
        candidates.append(
            CandidateSummary(
                index=index,
                name=str(dataset.get("name") or ""),
                reference_product=str(dataset.get("reference product") or ""),
                location=str(dataset.get("location") or ""),
                unit=str(dataset.get("unit") or ""),
            )
        )
    return candidates


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
