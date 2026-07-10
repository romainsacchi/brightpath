from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


def _normalize_family(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    if normalized == "bafu":
        return "uvek"
    return normalized


def _normalize_system_model(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"cut-off", "cutoff"}:
        return "cutoff"
    return normalized


def _normalize_version(family: str, value: str | None) -> str:
    normalized = str(value or "").strip()
    if family == "uvek" and normalized.endswith(".0") and normalized[:-2].isdigit():
        return normalized[:-2]
    if family != "ecoinvent":
        return normalized

    parts = normalized.split(".")
    if len(parts) >= 3 and all(part.isdigit() for part in parts):
        return ".".join(parts[:2])
    return normalized


class InventoryFormat(str, Enum):
    """Known source and target format identifiers.

    Brightway Excel and SimaPro CSV have full facade support. Brightway CSV and
    TSV are currently accepted by the upload-analysis API. OpenLCA Excel and
    ecospold2 are reserved for future adapters.
    """

    BRIGHTWAY_EXCEL = "brightway_excel"
    BRIGHTWAY_CSV = "brightway_csv"
    BRIGHTWAY_TSV = "brightway_tsv"
    SIMAPRO_CSV = "simapro_csv"
    OPENLCA_EXCEL = "openlca_excel"
    ECOSPOLD2 = "ecospold2"


@dataclass(frozen=True)
class BackgroundProfile:
    """Identify a background database independently of the file format.

    :param family: Database family, such as ``"ecoinvent"`` or ``"uvek"``.
        The legacy value ``"bafu"`` is normalized to ``"uvek"``.
    :param version: Database version, such as ``"3.10"`` or ``"2025"``.
        Numeric ecoinvent patch versions are reduced to major/minor versions.
    :param system_model: System model, such as ``"cutoff"`` or
        ``"consequential"``. ``"cut-off"`` is normalized to ``"cutoff"``.
    """

    family: str = ""
    version: str = ""
    system_model: str = ""

    def normalized(self) -> "BackgroundProfile":
        """Return a profile with canonical family, version, and model names."""

        family = _normalize_family(self.family)
        return BackgroundProfile(
            family=family,
            version=_normalize_version(family, self.version),
            system_model=_normalize_system_model(self.system_model),
        )

    @property
    def is_complete(self) -> bool:
        """Whether family, version, and system model are all non-empty."""

        normalized = self.normalized()
        return bool(normalized.family and normalized.version and normalized.system_model)

    def label(self) -> str:
        """Return a compact human-readable profile label."""

        normalized = self.normalized()
        return " ".join(
            part
            for part in (
                normalized.family,
                normalized.version,
                normalized.system_model,
            )
            if part
        )


@dataclass
class Issue:
    """A structured validation, analysis, rendering, or migration issue.

    :param severity: Usually ``"info"``, ``"warning"``, or ``"error"``.
    :param code: Stable machine-readable issue identifier.
    :param message: Human-readable explanation.
    :param path: Optional dataset or exchange path associated with the issue.
    :param suggested_fix: Optional corrective action.
    """

    severity: str
    code: str
    message: str
    path: str = ""
    suggested_fix: str = ""


@dataclass
class CandidateSummary:
    """Summary of one dataset discovered by upload analysis."""

    index: int
    name: str = ""
    reference_product: str = ""
    location: str = ""
    unit: str = ""
    description_hint: str = ""
    source_hint: str = ""
    issues: list[Issue] = field(default_factory=list)


@dataclass
class AnalysisResult:
    """Structured result returned by :func:`brightpath.analysis.analyze_inventory`."""

    detected_software: str
    detected_format: str
    source_profile: BackgroundProfile = field(default_factory=BackgroundProfile)
    file_issues: list[Issue] = field(default_factory=list)
    candidates: list[CandidateSummary] = field(default_factory=list)
    inventory_data: list[dict] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        """Whether any file-level or candidate-level issue is an error."""

        if any(issue.severity == "error" for issue in self.file_issues):
            return True
        return any(issue.severity == "error" for candidate in self.candidates for issue in candidate.issues)


@dataclass
class ValidationReport:
    """Read-only validation result for one background profile."""

    profile: BackgroundProfile
    issues: list[Issue] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        """Whether the report contains at least one error."""

        return any(issue.severity == "error" for issue in self.issues)

    @property
    def is_valid(self) -> bool:
        """Whether the report contains no errors."""

        return not self.has_errors


class InventoryDocument:
    """Software-neutral inventory data with copy-on-read semantics.

    This is the internal boundary shared by the public format facades. Most
    users should construct :class:`~brightpath.BrightwayInventory` or
    :class:`~brightpath.SimaProInventory` instead.
    """

    def __init__(
        self,
        *,
        data: list[dict],
        background_profile: BackgroundProfile,
        inventory_format: InventoryFormat,
        database_name: str = "",
        metadata: dict | None = None,
        database_parameters: list[dict] | None = None,
        project_parameters: list[dict] | None = None,
        migration_reports: tuple[Any, ...] = (),
    ) -> None:
        if not isinstance(data, list):
            raise TypeError("Inventory data must be a list of dataset dictionaries.")

        self._data = deepcopy(data)
        self.background_profile = background_profile.normalized()
        self.inventory_format = inventory_format
        self.database_name = str(database_name or "")
        self._metadata = deepcopy(metadata or {})
        self._database_parameters = deepcopy(database_parameters)
        self._project_parameters = deepcopy(project_parameters)
        self.migration_reports = tuple(migration_reports)

    @property
    def data(self) -> list[dict]:
        """Return a deep copy of the canonical dataset dictionaries."""

        return deepcopy(self._data)

    @property
    def metadata(self) -> dict:
        """Return a deep copy of database-level metadata."""

        return deepcopy(self._metadata)

    @property
    def database_parameters(self) -> list[dict] | None:
        """Return a deep copy of database parameters, when present."""

        return deepcopy(self._database_parameters)

    @property
    def project_parameters(self) -> list[dict] | None:
        """Return a deep copy of project parameters, when present."""

        return deepcopy(self._project_parameters)

    def replace(
        self,
        *,
        data: list[dict] | None = None,
        background_profile: BackgroundProfile | None = None,
        inventory_format: InventoryFormat | None = None,
        migration_reports: tuple[Any, ...] | None = None,
    ) -> "InventoryDocument":
        """Return a copied document with selected fields replaced."""

        return InventoryDocument(
            data=self.data if data is None else data,
            background_profile=background_profile or self.background_profile,
            inventory_format=inventory_format or self.inventory_format,
            database_name=self.database_name,
            metadata=self.metadata,
            database_parameters=self.database_parameters,
            project_parameters=self.project_parameters,
            migration_reports=(self.migration_reports if migration_reports is None else migration_reports),
        )
