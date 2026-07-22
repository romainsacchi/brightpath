from __future__ import annotations

import ast
import csv
import logging
import os
import re
from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Iterable, Optional

import bw2io
from bw2io import CSVImporter
from bw2io.importers.excel import ExcelImporter

from brightpath.adapters import default_adapter_registry
from brightpath.background import (
    BiosphereCatalog,
    CatalogIntegrityError,
    CatalogNotFoundError,
    CatalogProvider,
    InMemoryCatalogProvider,
    catalog_provider_from_environment,
)
from brightpath.background import validate_background_links as validate_exact_background_links
from brightpath.catalogs import available_catalog_profiles, load_background_catalog
from brightpath.core import (
    BackgroundContext,
    BiosphereProfile,
    FormatProfile,
    InventoryContext,
)
from brightpath.exceptions import InventoryValidationError
from brightpath.formats.simapro_csv import format_biosphere_exchange
from brightpath.models import AnalysisResult, BackgroundProfile, CandidateSummary, Issue
from brightpath.simapro import SimaProInventory
from brightpath.utils import (
    inspect_brightway_inventory,
    load_biosphere_correspondence,
    load_ei_biosphere_flows,
)

SOURCE_FORMAT_BRIGHTWAY_EXCEL = "brightway_excel"
SOURCE_FORMAT_BRIGHTWAY_CSV = "brightway_csv"
SOURCE_FORMAT_BRIGHTWAY_TSV = "brightway_tsv"
SOURCE_FORMAT_SIMAPRO_CSV = "simapro_csv"
SOFTWARE_BRIGHTWAY = "brightway"
SOFTWARE_SIMAPRO = "simapro"

_ACTIVITY_PATH_PATTERN = re.compile(
    r"^(?P<path>activity\[(?P<index>\d+)\](?:\.exchanges\[(?P<exchange_index>\d+)\])?):\s*(?P<message>.+)$"
)
_SIMAPRO_IDENTITY_PATTERN = re.compile(r"^(?P<identity>\(.+?\))\s+(?P<message>.+)$")
_TUPLE_PATTERN = re.compile(r"\([^()]+\)")
_TRAILING_SOURCE_PATTERN = re.compile(r"(?is)(?:^|(?<=[.!?\n]))\s*(?P<label>sources?)\s*:\s*(?P<source>.+?)\s*$")
_ROOT_LOGGER = "brightpath"
_CATALOG_UNSET = object()
_SUPPLEMENTAL_BIOSPHERE_NAME_ALIASES = {
    "air": {
        "Ethane, 1,1,1,2-tetrafluoro-, HFC-134a": "1,1,1,2-Tetrafluoroethane",
        "Propene": "Propylene",
        "Xylene": "Xylenes, unspecified",
    },
    "water": {
        "AOX, Adsorbable Organic Halogen": "AOX, Adsorbable Organic Halides",
        "Sodium": "Sodium I",
    },
}


@dataclass(frozen=True)
class _AnalysisCatalog:
    technosphere: frozenset[tuple[str, str, str, str]]
    biosphere: frozenset[tuple[str, tuple[str, ...], str]]


@dataclass(frozen=True)
class _BiosphereAnalysisCandidate:
    profile: BiosphereProfile
    result: AnalysisResult
    total_links: int
    unresolved_links: int
    file_errors: int

    @property
    def resolved_links(self) -> int:
        return max(self.total_links - self.unresolved_links, 0)

    @property
    def coverage(self) -> float:
        return self.resolved_links / self.total_links if self.total_links else 0.0

    @property
    def rank(self) -> tuple[int, float, int, int]:
        return (
            int(self.file_errors == 0),
            self.coverage,
            self.resolved_links,
            -self.unresolved_links,
        )


class _CollectingHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


class _TSVExtractor:
    @classmethod
    def extract(cls, filepath, encoding="utf-8-sig", sheet_name=None):
        if sheet_name is not None:
            raise ValueError("TSV files do not contain worksheets")
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Can't find file at path {filepath}")
        with open(filepath, encoding=encoding, newline="") as handle:
            reader = csv.reader(handle, delimiter="\t")
            data = [row for row in reader]
        return [os.path.basename(filepath), data]


class _TSVImporter(CSVImporter):
    extractor = _TSVExtractor


def infer_source_format(path: str | Path) -> str:
    """Infer the upload-analysis format from content when a file exists.

    The built-in adapter registry probes existing artifacts. For a path that
    does not exist yet, only the unambiguous ``.xlsx`` and ``.tsv`` suffixes
    can be inferred. A bare ``.csv`` filename is deliberately ambiguous.

    :raises ValueError: If the suffix is unsupported.
    """

    source = Path(path)
    if source.is_file():
        detection = default_adapter_registry().detect(source)
        if detection.detected_format is not None and not detection.has_errors:
            return detection.detected_format.format_id
        detail = "; ".join(issue.message for issue in detection.issues)
        raise ValueError(detail or f"Could not detect inventory source format: {source}.")

    suffix = source.suffix.lower()
    if suffix == ".xlsx":
        return SOURCE_FORMAT_BRIGHTWAY_EXCEL
    if suffix == ".tsv":
        return SOURCE_FORMAT_BRIGHTWAY_TSV
    if suffix == ".csv":
        raise ValueError("CSV format is ambiguous; provide an existing file or an explicit source_format.")
    if suffix == ".xls":
        raise ValueError("BrightPath analysis currently supports Brightway .xlsx workbooks, not .xls files.")
    raise ValueError(f"Unsupported inventory source format for analysis: {suffix or 'no extension'}.")


def analyze_inventory(
    *,
    path: str | Path,
    source_format: Optional[str] = None,
    source_profile: Optional[BackgroundProfile] = None,
    source_context: InventoryContext | None = None,
    catalog_provider: CatalogProvider | None = None,
    additional_foreground_targets: Optional[Iterable[tuple[str, str, str, str]]] = None,
) -> AnalysisResult:
    """Parse an inventory upload and return structured intake information.

    :param path: Inventory file to analyze.
    :param source_format: Optional explicit format constant. Existing files
        are otherwise detected from content; ambiguous CSV is never guessed.
    :param source_profile: Optional compatibility profile. Brightway analysis
        can infer missing fields. SimaPro analysis uses a complete profile as
        the exact technosphere axis while inferring only the biosphere profile.
    :param source_context: Exact format, technosphere, and biosphere context
        used directly when parsing SimaPro CSV.
    :param catalog_provider: Exact catalogs used for SimaPro parsing and link
        validation. The application provider is used when omitted.
    :param additional_foreground_targets: External foreground identities that
        should be accepted during link validation.
    :return: Detected format, resolved profile, normalized inventory data,
        file issues, and per-dataset candidate summaries.

    Parsing and validation failures are captured as issues whenever possible;
    this function is intended for inspectable upload workflows.
    """

    if source_context is not None and not isinstance(source_context, InventoryContext):
        raise TypeError("source_context must be an InventoryContext.")
    if catalog_provider is not None and not isinstance(catalog_provider, CatalogProvider):
        raise TypeError("catalog_provider must implement CatalogProvider.")

    resolved_path = Path(path)
    context_profile = (
        BackgroundProfile.from_technosphere_profile(source_context.background.technosphere)
        if source_context is not None
        else None
    )
    profile = (context_profile or source_profile or BackgroundProfile()).normalized()
    try:
        resolved_format = source_format or infer_source_format(resolved_path)
    except ValueError as error:
        return AnalysisResult(
            detected_software="",
            detected_format="",
            source_profile=profile,
            file_issues=[
                Issue(
                    severity="error",
                    code="format_detection_failed",
                    message=str(error),
                )
            ],
        )
    normalized_foreground_targets = _normalize_foreground_targets(additional_foreground_targets)

    if resolved_format == SOURCE_FORMAT_BRIGHTWAY_EXCEL:
        return _analyze_brightway_excel(
            path=resolved_path,
            source_profile=profile,
            additional_foreground_targets=normalized_foreground_targets,
        )
    if resolved_format == SOURCE_FORMAT_BRIGHTWAY_CSV:
        return _analyze_brightway_delimited(
            path=resolved_path,
            source_profile=profile,
            detected_format=SOURCE_FORMAT_BRIGHTWAY_CSV,
            additional_foreground_targets=normalized_foreground_targets,
        )
    if resolved_format == SOURCE_FORMAT_BRIGHTWAY_TSV:
        return _analyze_brightway_delimited(
            path=resolved_path,
            source_profile=profile,
            detected_format=SOURCE_FORMAT_BRIGHTWAY_TSV,
            additional_foreground_targets=normalized_foreground_targets,
        )
    if resolved_format == SOURCE_FORMAT_SIMAPRO_CSV:
        return _analyze_simapro_csv(
            path=resolved_path,
            source_context=source_context,
            legacy_source_profile=source_profile,
            catalog_provider=catalog_provider,
            additional_foreground_targets=normalized_foreground_targets,
        )

    raise ValueError(f"Unsupported source format: {resolved_format!r}.")


def validate_inventory(
    *,
    path: str | Path,
    source_format: Optional[str] = None,
    source_profile: Optional[BackgroundProfile] = None,
    source_context: InventoryContext | None = None,
    catalog_provider: CatalogProvider | None = None,
    additional_foreground_targets: Optional[Iterable[tuple[str, str, str, str]]] = None,
) -> AnalysisResult:
    """Analyze an upload and raise if any returned issue is an error.

    :return: The successful :class:`~brightpath.models.AnalysisResult`.
    :raises InventoryValidationError: If file-level or candidate-level errors
        are present. The exception exposes the result through ``.result``.
    """

    result = analyze_inventory(
        path=path,
        source_format=source_format,
        source_profile=source_profile,
        source_context=source_context,
        catalog_provider=catalog_provider,
        additional_foreground_targets=additional_foreground_targets,
    )
    if result.has_errors:
        raise InventoryValidationError(
            result=result,
            message=_format_error_summary(result),
        )
    return result


def _analyze_brightway_excel(
    *,
    path: Path,
    source_profile: BackgroundProfile,
    additional_foreground_targets: frozenset[tuple[str, str, str, str]],
) -> AnalysisResult:
    return _analyze_brightway_inventory_data(
        path=path,
        source_profile=source_profile,
        detected_format=SOURCE_FORMAT_BRIGHTWAY_EXCEL,
        loader=_load_brightway_excel_without_validation,
        parse_error_code="brightway_excel_parse_failed",
        additional_foreground_targets=additional_foreground_targets,
    )


def _analyze_brightway_delimited(
    *,
    path: Path,
    source_profile: BackgroundProfile,
    detected_format: str,
    additional_foreground_targets: frozenset[tuple[str, str, str, str]],
) -> AnalysisResult:
    return _analyze_brightway_inventory_data(
        path=path,
        source_profile=source_profile,
        detected_format=detected_format,
        loader=_load_brightway_delimited_without_validation,
        parse_error_code="brightway_tabular_parse_failed",
        additional_foreground_targets=additional_foreground_targets,
    )


def _analyze_brightway_inventory_data(
    *,
    path: Path,
    source_profile: BackgroundProfile,
    detected_format: str,
    loader,
    parse_error_code: str,
    additional_foreground_targets: frozenset[tuple[str, str, str, str]],
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
    result.source_profile, profile_issues = _resolve_background_profile(
        inventory_data,
        source_profile,
    )
    inventory_data = _normalize_inventory_for_validation(
        inventory_data,
        result.source_profile,
        additional_foreground_targets=additional_foreground_targets,
    )
    result.inventory_data = deepcopy(inventory_data)
    result.candidates = _build_candidates(inventory_data)
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
            additional_foreground_targets=additional_foreground_targets,
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
    source_context: InventoryContext | None,
    legacy_source_profile: BackgroundProfile | None,
    catalog_provider: CatalogProvider | None,
    additional_foreground_targets: frozenset[tuple[str, str, str, str]],
) -> AnalysisResult:
    normalized_legacy_profile = (legacy_source_profile or BackgroundProfile()).normalized()
    source_profile = (
        BackgroundProfile.from_technosphere_profile(source_context.background.technosphere)
        if source_context is not None
        else normalized_legacy_profile
    )
    result = AnalysisResult(
        detected_software=SOFTWARE_SIMAPRO,
        detected_format=SOURCE_FORMAT_SIMAPRO_CSV,
        source_profile=source_profile,
        source_context=source_context,
    )
    if source_context is None:
        if all(
            (
                source_profile.family,
                source_profile.version,
                source_profile.system_model,
            )
        ):
            return _analyze_simapro_with_inferred_biosphere(
                path=path,
                source_profile=source_profile,
                catalog_provider=catalog_provider,
                additional_foreground_targets=additional_foreground_targets,
            )
        result.file_issues.append(
            Issue(
                severity="error",
                code="simapro_source_context_required",
                message=(
                    "SimaPro analysis requires either an exact source InventoryContext or a complete "
                    "technosphere source_profile for biosphere inference."
                ),
                suggested_fix=(
                    "Provide an exact source_context, or provide the technosphere family, version, and system model."
                ),
            )
        )
        return result
    if source_context.format.format_id != SOURCE_FORMAT_SIMAPRO_CSV:
        result.file_issues.append(
            Issue(
                severity="error",
                code="simapro_source_context_format_mismatch",
                message=(
                    "SimaPro analysis requires source_context.format.format_id to be "
                    f"{SOURCE_FORMAT_SIMAPRO_CSV!r}, not {source_context.format.format_id!r}."
                ),
                suggested_fix="Use a SimaPro CSV format profile for this source artifact.",
            )
        )
        return result

    conflict = _simapro_profile_conflict(legacy_source_profile, source_profile)
    if conflict:
        result.file_issues.append(
            Issue(
                severity="error",
                code="simapro_source_profile_conflict",
                message=conflict,
                suggested_fix="Remove source_profile or make its specified fields match source_context.technosphere.",
            )
        )
        return result

    provider, provider_issue = _resolve_simapro_catalog_provider(catalog_provider)
    if provider_issue is not None:
        result.file_issues.append(provider_issue)
        return result
    if provider is None:  # Defensive guard for custom provider implementations.
        result.file_issues.append(
            Issue(
                severity="error",
                code="simapro_biosphere_catalog_failed",
                message="Constructing the catalog provider returned no provider.",
            )
        )
        return result
    biosphere_catalog, catalog_issue = _load_exact_simapro_biosphere_catalog(source_context, provider)
    if catalog_issue is not None:
        result.file_issues.append(catalog_issue)
        return result
    if biosphere_catalog is None:  # Defensive guard for custom provider implementations.
        result.file_issues.append(
            Issue(
                severity="error",
                code="simapro_biosphere_catalog_failed",
                message="The catalog provider did not return an exact biosphere catalog.",
            )
        )
        return result

    parsing_provider = InMemoryCatalogProvider(biosphere=(biosphere_catalog,))
    analysis_catalog = _exact_analysis_catalog(source_context, provider, biosphere_catalog)
    inventory_data: list[dict] = []

    with _capture_warnings() as collector:
        try:
            inventory = SimaProInventory.from_csv(
                path,
                context=source_context,
                catalog_provider=parsing_provider,
            )
            inventory_data = inventory.data
            result.file_issues.extend(
                issue
                for issue in inventory.validate(check_background_links=False).issues
                if issue.code == "duplicate_dataset_identity"
            )
        except Exception as exc:
            inventory_data = deepcopy(getattr(exc, "partial_data", []))
            result.file_issues.extend(
                _issues_from_simapro_exception(
                    exc,
                    inventory_data=inventory_data,
                )
            )

    inventory_data = _normalize_inventory_for_validation(
        inventory_data,
        source_profile,
        additional_foreground_targets=additional_foreground_targets,
        catalog=analysis_catalog,
        normalize_biosphere=False,
    )
    result.inventory_data = deepcopy(inventory_data)
    result.candidates = _build_candidates(inventory_data)
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
        background_issues, background_file_issues = _validate_exact_analysis_background_links(
            inventory_data,
            source_context,
            provider,
            additional_foreground_targets=additional_foreground_targets,
        )
        _attach_activity_issues(
            candidates=result.candidates,
            candidate_issues=background_issues,
            file_issues=result.file_issues,
        )
        result.file_issues.extend(background_file_issues)
    result.file_issues.extend(_warning_issues(collector.messages))
    return result


def _analyze_simapro_with_inferred_biosphere(
    *,
    path: Path,
    source_profile: BackgroundProfile,
    catalog_provider: CatalogProvider | None,
    additional_foreground_targets: frozenset[tuple[str, str, str, str]],
) -> AnalysisResult:
    base_result = AnalysisResult(
        detected_software=SOFTWARE_SIMAPRO,
        detected_format=SOURCE_FORMAT_SIMAPRO_CSV,
        source_profile=source_profile,
    )
    provider, provider_issue = _resolve_simapro_catalog_provider(catalog_provider)
    if provider_issue is not None:
        base_result.file_issues.append(provider_issue)
        return base_result
    if provider is None:  # Defensive guard for custom provider implementations.
        base_result.file_issues.append(
            Issue(
                severity="error",
                code="simapro_biosphere_inference_failed",
                message="Constructing the catalog provider returned no provider.",
            )
        )
        return base_result

    try:
        profiles = provider.biosphere_profiles()
    except Exception as error:
        base_result.file_issues.append(
            Issue(
                severity="error",
                code="simapro_biosphere_inference_failed",
                message=f"Listing available biosphere catalogs failed: {error}",
                suggested_fix="Check the configured catalog provider before retrying biosphere inference.",
            )
        )
        return base_result
    if not profiles:
        base_result.file_issues.append(
            Issue(
                severity="error",
                code="simapro_biosphere_inference_unavailable",
                message="No biosphere catalogs are available for SimaPro profile inference.",
                suggested_fix="Install or inject biosphere catalogs, or provide an exact source_context.",
            )
        )
        return base_result

    technosphere = source_profile.to_technosphere_profile()
    candidates = []
    for biosphere_profile in profiles:
        context = InventoryContext(
            format=FormatProfile(SOURCE_FORMAT_SIMAPRO_CSV, encoding="latin-1"),
            background=BackgroundContext(
                technosphere=technosphere,
                biosphere=biosphere_profile,
            ),
        )
        analysis = _analyze_simapro_csv(
            path=path,
            source_context=context,
            legacy_source_profile=source_profile,
            catalog_provider=provider,
            additional_foreground_targets=additional_foreground_targets,
        )
        total_links = sum(
            1
            for activity in analysis.inventory_data
            for exchange in activity.get("exchanges", [])
            if exchange.get("type") == "biosphere"
        )
        unresolved_links = sum(
            1
            for candidate in analysis.candidates
            for issue in candidate.issues
            if issue.code == "unknown_biosphere_flow"
        )
        candidates.append(
            _BiosphereAnalysisCandidate(
                profile=biosphere_profile,
                result=analysis,
                total_links=total_links,
                unresolved_links=unresolved_links,
                file_errors=sum(1 for issue in analysis.file_issues if issue.severity == "error"),
            )
        )

    eligible = [candidate for candidate in candidates if candidate.resolved_links > 0]
    if not eligible:
        base_result.file_issues.append(
            Issue(
                severity="error",
                code="simapro_biosphere_profile_not_inferred",
                message=(
                    f"None of the {len(profiles)} available biosphere catalogs resolved a "
                    "biosphere exchange uniquely enough to infer a profile."
                ),
                suggested_fix="Provide an exact biosphere profile or use an application fallback.",
            )
        )
        return base_result

    best_rank = max(candidate.rank for candidate in eligible)
    best_candidates = [candidate for candidate in eligible if candidate.rank == best_rank]
    if len(best_candidates) != 1:
        labels = ", ".join(candidate.profile.label() for candidate in best_candidates)
        base_result.file_issues.append(
            Issue(
                severity="error",
                code="simapro_biosphere_profile_ambiguous",
                message=("Multiple biosphere catalogs produced the same best exchange coverage " f"({labels})."),
                suggested_fix="Provide an exact biosphere profile or use an application fallback.",
            )
        )
        return base_result

    selected = best_candidates[0]
    selected.result.file_issues.append(
        Issue(
            severity="info",
            code="simapro_biosphere_profile_inferred",
            message=(
                f"BrightPath inferred {selected.profile.label()} by resolving "
                f"{selected.resolved_links} of {selected.total_links} biosphere exchanges "
                f"across {len(profiles)} available biosphere catalogs."
            ),
            suggested_fix="Review the inferred profile and override it if the source context is known.",
        )
    )
    return selected.result


def _resolve_simapro_catalog_provider(
    catalog_provider: CatalogProvider | None,
) -> tuple[CatalogProvider | None, Issue | None]:
    if catalog_provider is not None:
        return catalog_provider, None
    try:
        return catalog_provider_from_environment(), None
    except CatalogIntegrityError as error:
        return None, Issue(
            severity="error",
            code="simapro_biosphere_catalog_invalid",
            message=f"Constructing the catalog provider failed integrity checks: {error}",
            suggested_fix="Repair the configured catalog manifest or inject a valid exact catalog provider.",
        )
    except Exception as error:
        return None, Issue(
            severity="error",
            code="simapro_biosphere_catalog_failed",
            message=f"Constructing the catalog provider failed: {error}",
            suggested_fix="Check the catalog configuration or inject a valid exact catalog provider.",
        )


def _simapro_profile_conflict(
    legacy: BackgroundProfile | None,
    exact: BackgroundProfile,
) -> str:
    """Describe specified legacy fields that contradict an exact context."""

    if legacy is None:
        return ""
    normalized = legacy.normalized()
    conflicts = [
        field_name
        for field_name in ("family", "version", "system_model")
        if getattr(normalized, field_name) and getattr(normalized, field_name) != getattr(exact, field_name)
    ]
    if not conflicts:
        return ""
    fields = ", ".join(conflicts)
    return f"source_profile conflicts with source_context.technosphere for: {fields}."


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
                        message=("Dataset identity must be unique after SimaPro name parsing."),
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
        )
        == normalized_identity
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


def _load_exact_simapro_biosphere_catalog(
    source_context: InventoryContext,
    provider: CatalogProvider,
) -> tuple[BiosphereCatalog | None, Issue | None]:
    profile = source_context.background.biosphere
    try:
        catalog = provider.load_biosphere(profile)
    except CatalogNotFoundError as error:
        return None, Issue(
            severity="error",
            code="simapro_biosphere_catalog_missing",
            message=f"No exact biosphere catalog is available for {profile.label()}: {error}",
            suggested_fix="Install or inject the exact biosphere catalog before analyzing this SimaPro CSV.",
        )
    except CatalogIntegrityError as error:
        return None, Issue(
            severity="error",
            code="simapro_biosphere_catalog_invalid",
            message=f"The exact biosphere catalog for {profile.label()} is invalid: {error}",
            suggested_fix="Repair or replace the exact biosphere catalog before analyzing this SimaPro CSV.",
        )
    except Exception as error:
        return None, Issue(
            severity="error",
            code="simapro_biosphere_catalog_failed",
            message=f"Loading the exact biosphere catalog for {profile.label()} failed: {error}",
            suggested_fix="Check the injected catalog provider before analyzing this SimaPro CSV.",
        )

    if not isinstance(catalog, BiosphereCatalog) or catalog.profile != profile:
        actual = catalog.profile.label() if isinstance(catalog, BiosphereCatalog) else type(catalog).__name__
        return None, Issue(
            severity="error",
            code="simapro_biosphere_catalog_invalid",
            message=f"The provider returned {actual}, not the requested exact biosphere profile {profile.label()}.",
            suggested_fix="Correct the catalog provider so exact profile requests return matching catalogs.",
        )
    return catalog, None


def _exact_analysis_catalog(
    source_context: InventoryContext,
    provider: CatalogProvider,
    biosphere_catalog: BiosphereCatalog,
) -> _AnalysisCatalog:
    technosphere = frozenset()
    profile = source_context.background.technosphere
    try:
        catalog = provider.load_technosphere(profile)
    except (CatalogNotFoundError, CatalogIntegrityError):
        pass
    else:
        if catalog.profile == profile:
            technosphere = catalog.identities
    return _AnalysisCatalog(technosphere=technosphere, biosphere=biosphere_catalog.identities)


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


def _normalize_foreground_targets(
    foreground_targets: Optional[Iterable[tuple[str, str, str, str]]],
) -> frozenset[tuple[str, str, str, str]]:
    normalized: set[tuple[str, str, str, str]] = set()
    for target in foreground_targets or ():
        if len(target) != 4:
            continue
        key = tuple(str(part or "").strip() for part in target)
        if all(key):
            normalized.add(key)
    return frozenset(normalized)


def _canonicalize_text(value: str) -> str:
    return " ".join(str(value or "").split()).casefold()


def _canonicalize_technosphere_key(
    key: tuple[str, str, str, str],
) -> tuple[str, str, str, str]:
    name, reference_product, location, unit = key
    return (
        _canonicalize_text(name),
        _canonicalize_text(reference_product),
        _canonicalize_text(location),
        _canonicalize_unit(unit),
    )


def _canonicalize_technosphere_triplet(
    key: tuple[str, str, str],
) -> tuple[str, str, str]:
    name, location, unit = key
    return (
        _canonicalize_text(name),
        _canonicalize_text(location),
        _canonicalize_unit(unit),
    )


def _find_unique_canonical_match(
    key: tuple[str, str, str, str],
    canonical_index: dict[
        tuple[str, str, str, str],
        frozenset[tuple[str, str, str, str]],
    ],
) -> tuple[str, str, str, str] | None:
    matches = canonical_index.get(_canonicalize_technosphere_key(key), frozenset())
    if len(matches) == 1:
        return next(iter(matches))
    return None


def _build_canonical_target_index(
    targets: Iterable[tuple[str, str, str, str]],
) -> dict[tuple[str, str, str, str], frozenset[tuple[str, str, str, str]]]:
    indexed: defaultdict[
        tuple[str, str, str, str],
        set[tuple[str, str, str, str]],
    ] = defaultdict(set)
    for target in targets:
        indexed[_canonicalize_technosphere_key(target)].add(target)
    return {canonical_key: frozenset(matches) for canonical_key, matches in indexed.items()}


def _canonicalize_unit(value: str) -> str:
    normalized = " ".join(str(value or "").split()).casefold()
    unit_aliases = {
        "kg": "kilogram",
        "m3": "cubic meter",
        "kwh": "kilowatt hour",
        "km": "kilometer",
        "tkm": "ton kilometer",
        "mj": "megajoule",
        "m2": "square meter",
        "kw": "kilowatt",
        "hr": "hour",
        "m2a": "square meter-year",
        "m": "meter",
        "vkm": "vehicle-kilometer",
        "personkm": "person-kilometer",
        "person kilometer": "person-kilometer",
        "vehicle kilometer": "vehicle-kilometer",
        "my": "meter-year",
        "meter year": "meter-year",
        "square meter year": "square meter-year",
        "ha": "hectare",
    }
    return unit_aliases.get(normalized, normalized)


def _normalize_inventory_for_validation(
    inventory_data: list[dict],
    source_profile: BackgroundProfile,
    *,
    additional_foreground_targets: frozenset[tuple[str, str, str, str]],
    catalog=_CATALOG_UNSET,
    normalize_biosphere: bool = True,
) -> list[dict]:
    normalized_inventory = deepcopy(inventory_data)
    selected_catalog = _load_catalog_if_available(source_profile) if catalog is _CATALOG_UNSET else catalog
    _promote_legacy_product_fields(normalized_inventory)
    _synchronize_production_exchanges_with_activity(normalized_inventory)
    _fill_missing_technosphere_reference_products(
        normalized_inventory,
        selected_catalog,
        additional_foreground_targets=additional_foreground_targets,
    )
    _harmonize_technosphere_exchange_identities(
        normalized_inventory,
        selected_catalog,
        additional_foreground_targets=additional_foreground_targets,
    )
    if normalize_biosphere:
        _normalize_biosphere_exchanges(
            normalized_inventory,
            source_profile,
            selected_catalog,
        )
    return normalized_inventory


def _load_catalog_if_available(source_profile: BackgroundProfile):
    normalized = source_profile.normalized()
    if not (normalized.family and normalized.version and normalized.system_model):
        return None
    try:
        return load_background_catalog(normalized)
    except FileNotFoundError:
        return None


def _promote_legacy_product_fields(inventory_data: list[dict]) -> None:
    for activity in inventory_data:
        if not str(activity.get("reference product") or "").strip():
            legacy_product = str(activity.get("product") or "").strip()
            if legacy_product:
                activity["reference product"] = legacy_product
        for exchange in activity.get("exchanges", []):
            if exchange.get("type") not in {"production", "technosphere"}:
                continue
            if str(exchange.get("reference product") or "").strip():
                continue
            legacy_product = str(exchange.get("product") or "").strip()
            if legacy_product:
                exchange["reference product"] = legacy_product


def _fill_missing_technosphere_reference_products(
    inventory_data: list[dict],
    catalog,
    *,
    additional_foreground_targets: frozenset[tuple[str, str, str, str]],
) -> None:
    foreground_candidates: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    foreground_canonical_candidates: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    for activity in inventory_data:
        name = str(activity.get("name") or "").strip()
        reference_product = str(activity.get("reference product") or "").strip()
        location = str(activity.get("location") or "").strip()
        unit = str(activity.get("unit") or "").strip()
        if all((name, reference_product, location, unit)):
            foreground_candidates[(name, location, unit)].add(reference_product)
            foreground_canonical_candidates[_canonicalize_technosphere_triplet((name, location, unit))].add(
                reference_product
            )
    for name, reference_product, location, unit in additional_foreground_targets:
        foreground_candidates[(name, location, unit)].add(reference_product)
        foreground_canonical_candidates[_canonicalize_technosphere_triplet((name, location, unit))].add(
            reference_product
        )

    catalog_candidates: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    catalog_canonical_candidates: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    if catalog is not None:
        for name, reference_product, location, unit in catalog.technosphere:
            catalog_candidates[(name, location, unit)].add(reference_product)
            catalog_canonical_candidates[_canonicalize_technosphere_triplet((name, location, unit))].add(
                reference_product
            )

    for activity in inventory_data:
        for exchange in activity.get("exchanges", []):
            if exchange.get("type") != "technosphere":
                continue
            if str(exchange.get("reference product") or "").strip():
                continue

            key = (
                str(exchange.get("name") or "").strip(),
                str(exchange.get("location") or "").strip(),
                str(exchange.get("unit") or "").strip(),
            )
            foreground_matches = foreground_candidates.get(key, set())
            if len(foreground_matches) == 1:
                exchange["reference product"] = next(iter(foreground_matches))
                continue
            if foreground_matches:
                continue
            foreground_canonical_matches = foreground_canonical_candidates.get(
                _canonicalize_technosphere_triplet(key),
                set(),
            )
            if len(foreground_canonical_matches) == 1:
                exchange["reference product"] = next(iter(foreground_canonical_matches))
                continue
            if foreground_canonical_matches:
                continue

            catalog_matches = catalog_candidates.get(key, set())
            if len(catalog_matches) == 1:
                exchange["reference product"] = next(iter(catalog_matches))
                continue

            catalog_canonical_matches = catalog_canonical_candidates.get(
                _canonicalize_technosphere_triplet(key),
                set(),
            )
            if len(catalog_canonical_matches) == 1:
                exchange["reference product"] = next(iter(catalog_canonical_matches))


def _harmonize_technosphere_exchange_identities(
    inventory_data: list[dict],
    catalog,
    *,
    additional_foreground_targets: frozenset[tuple[str, str, str, str]],
) -> None:
    internal_targets = {
        (
            str(activity.get("name") or "").strip(),
            str(activity.get("reference product") or "").strip(),
            str(activity.get("location") or "").strip(),
            str(activity.get("unit") or "").strip(),
        )
        for activity in inventory_data
        if all(
            (
                str(activity.get("name") or "").strip(),
                str(activity.get("reference product") or "").strip(),
                str(activity.get("location") or "").strip(),
                str(activity.get("unit") or "").strip(),
            )
        )
    }
    catalog_targets = catalog.technosphere if catalog is not None else frozenset()
    internal_target_index = _build_canonical_target_index(internal_targets)
    additional_target_index = _build_canonical_target_index(additional_foreground_targets)
    catalog_target_index = _build_canonical_target_index(catalog_targets)

    for activity in inventory_data:
        for exchange in activity.get("exchanges", []):
            if exchange.get("type") != "technosphere":
                continue
            key = (
                str(exchange.get("name") or "").strip(),
                str(exchange.get("reference product") or "").strip(),
                str(exchange.get("location") or "").strip(),
                str(exchange.get("unit") or "").strip(),
            )
            if not all(key):
                continue
            if key in internal_targets or key in additional_foreground_targets or key in catalog_targets:
                continue

            matched_target = _find_unique_canonical_match(key, internal_target_index)
            if matched_target is None:
                matched_target = _find_unique_canonical_match(key, additional_target_index)
            if matched_target is None:
                matched_target = _find_unique_canonical_match(key, catalog_target_index)
            if matched_target is None:
                continue

            (
                exchange["name"],
                exchange["reference product"],
                exchange["location"],
                exchange["unit"],
            ) = matched_target


def _synchronize_production_exchanges_with_activity(
    inventory_data: list[dict],
) -> None:
    for activity in inventory_data:
        activity_name = str(activity.get("name") or "").strip()
        activity_reference_product = str(activity.get("reference product") or "").strip()
        activity_location = str(activity.get("location") or "").strip()
        activity_unit = str(activity.get("unit") or "").strip()
        for exchange in activity.get("exchanges", []):
            if exchange.get("type") != "production":
                continue
            if not str(exchange.get("name") or "").strip() and activity_name:
                exchange["name"] = activity_name
            if not str(exchange.get("reference product") or "").strip() and activity_reference_product:
                exchange["reference product"] = activity_reference_product
            if not str(exchange.get("location") or "").strip() and activity_location:
                exchange["location"] = activity_location
            if not str(exchange.get("unit") or "").strip() and activity_unit:
                exchange["unit"] = activity_unit


@lru_cache(maxsize=1)
def _biosphere_correspondence():
    return load_biosphere_correspondence()


@lru_cache(maxsize=1)
def _biosphere_flow_reference():
    return tuple(load_ei_biosphere_flows())


def _biosphere_reference_version(source_profile: BackgroundProfile) -> str:
    normalized = source_profile.normalized()
    if normalized.family == "ecoinvent" and normalized.version:
        return normalized.version
    if normalized.family == "uvek":
        return "3.10"
    return "3.10"


def _normalize_biosphere_exchanges(
    inventory_data: list[dict],
    source_profile: BackgroundProfile,
    catalog,
) -> None:
    biosphere_reference = _biosphere_flow_reference()
    correspondence = _biosphere_correspondence()
    reference_version = _biosphere_reference_version(source_profile)

    for activity in inventory_data:
        for exchange in activity.get("exchanges", []):
            if exchange.get("type") != "biosphere":
                continue
            current_key = (
                str(exchange.get("name") or ""),
                tuple(str(item) for item in exchange.get("categories", ())),
                str(exchange.get("unit") or ""),
            )
            if catalog is not None and current_key in catalog.biosphere:
                continue
            mapped_current = _map_biosphere_name_for_category(
                current_key[0],
                current_key[1][0] if current_key[1] else "",
                correspondence,
            )
            mapped_current_key = (
                mapped_current,
                current_key[1],
                current_key[2],
            )
            if catalog is not None and mapped_current != current_key[0] and mapped_current_key in catalog.biosphere:
                exchange["name"] = mapped_current
                continue

            candidate = deepcopy(exchange)
            try:
                format_biosphere_exchange(
                    candidate,
                    reference_version,
                    biosphere_reference,
                    correspondence,
                    copy=False,
                )
            except (KeyError, TypeError, ValueError):
                continue
            normalized_key = (
                str(candidate.get("name") or ""),
                tuple(str(item) for item in candidate.get("categories", ())),
                str(candidate.get("unit") or ""),
            )
            mapped_normalized = _map_biosphere_name_for_category(
                normalized_key[0],
                normalized_key[1][0] if normalized_key[1] else "",
                correspondence,
            )
            mapped_normalized_key = (
                mapped_normalized,
                normalized_key[1],
                normalized_key[2],
            )
            if catalog is None or normalized_key in catalog.biosphere:
                exchange.update(candidate)
            elif mapped_normalized != normalized_key[0] and mapped_normalized_key in catalog.biosphere:
                candidate["name"] = mapped_normalized
                exchange.update(candidate)


def _map_biosphere_name_for_category(
    name: str,
    main_category: str,
    correspondence,
) -> str:
    supplemental = _SUPPLEMENTAL_BIOSPHERE_NAME_ALIASES.get(main_category, {})
    if name in supplemental:
        return supplemental[name]
    mapping = correspondence.get(main_category, {})
    return str(mapping.get(name, name))


def _validate_exact_analysis_background_links(
    inventory_data: list[dict],
    source_context: InventoryContext,
    catalog_provider: CatalogProvider,
    *,
    additional_foreground_targets: frozenset[tuple[str, str, str, str]],
) -> tuple[list[Issue], list[Issue]]:
    report = validate_exact_background_links(
        inventory_data,
        source_context.background,
        catalog_provider,
        foreground_technosphere_targets=additional_foreground_targets,
    )
    candidate_issues = []
    file_issues = []
    code_aliases = {
        "background.technosphere_link_unresolved": "unknown_technosphere_target",
        "background.biosphere_link_unresolved": "unknown_biosphere_flow",
    }
    for issue in report.issues:
        converted = Issue(
            severity=issue.severity.value,
            code=code_aliases.get(issue.code, issue.code),
            message=issue.message,
            path=issue.path.replace("datasets[", "activity[", 1),
            suggested_fix=issue.suggested_fix,
        )
        if "_catalog_" in issue.code:
            file_issues.append(converted)
        else:
            candidate_issues.append(converted)
    return candidate_issues, file_issues


def _validate_background_links(
    inventory_data: list[dict],
    source_profile: BackgroundProfile,
    additional_foreground_targets: frozenset[tuple[str, str, str, str]],
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
    internal_targets.update(additional_foreground_targets)
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
    lines = ["Technosphere exchanges do not match an uploaded dataset or the selected background reference catalog."]
    for name, reference_product, location, unit in exchanges:
        lines.append(f"- {name or '?'} | {reference_product or '?'} | {location or '?'} | {unit or '?'}")
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
