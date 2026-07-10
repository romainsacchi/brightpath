from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from .core.context import (
    BackgroundContext,
    BiosphereProfile,
    FormatProfile,
    InventoryContext,
    TechnosphereProfile,
)
from .core.schema import CanonicalInventory


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
    :param version: Exact database version, such as ``"3.10.1"`` or
        ``"2025"``. Migration-series resolution is a separate operation.
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

    def to_technosphere_profile(self) -> TechnosphereProfile:
        """Return the exact typed technosphere profile.

        :raises ValueError: If this legacy projection is incomplete.
        """

        normalized = self.normalized()
        return TechnosphereProfile(
            normalized.family,
            normalized.version,
            normalized.system_model,
        )

    @classmethod
    def from_technosphere_profile(cls, profile: TechnosphereProfile) -> "BackgroundProfile":
        """Create the legacy public projection of a typed profile."""

        return cls(profile.family, profile.version, profile.system_model)


def default_biosphere_profile(profile: BackgroundProfile | TechnosphereProfile) -> BiosphereProfile:
    """Return the documented legacy biosphere default for a technosphere.

    New code should pass an explicit :class:`BiosphereProfile`. This helper is
    limited to compatibility boundaries where the old API supplied only one
    combined background profile. UVEK 2025 inventories use the ecoinvent 3.10
    biosphere catalog in the currently supported data release.
    """

    technosphere = profile.to_technosphere_profile() if isinstance(profile, BackgroundProfile) else profile
    if technosphere.family == "uvek" and technosphere.version == "2025":
        return BiosphereProfile("ecoinvent", "3.10")
    return BiosphereProfile(technosphere.family, technosphere.version)


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
        background_profile: BackgroundProfile | None = None,
        inventory_format: InventoryFormat | None = None,
        context: InventoryContext | None = None,
        biosphere_profile: BiosphereProfile | None = None,
        database_name: str = "",
        metadata: dict | None = None,
        database_parameters: list[dict] | None = None,
        project_parameters: list[dict] | None = None,
        migration_reports: tuple[Any, ...] = (),
    ) -> None:
        if not isinstance(data, list):
            raise TypeError("Inventory data must be a list of dataset dictionaries.")
        if context is None:
            if background_profile is None:
                raise TypeError("background_profile or context must be provided.")
            if inventory_format is None:
                raise TypeError("inventory_format or context must be provided.")
            technosphere = background_profile.to_technosphere_profile()
            context = InventoryContext(
                format=FormatProfile(inventory_format.value),
                background=BackgroundContext(
                    technosphere=technosphere,
                    biosphere=biosphere_profile or default_biosphere_profile(technosphere),
                ),
            )
        elif not isinstance(context, InventoryContext):
            raise TypeError("context must be an InventoryContext.")
        else:
            _check_legacy_context_arguments(
                context,
                background_profile=background_profile,
                inventory_format=inventory_format,
                biosphere_profile=biosphere_profile,
            )

        self._inventory = CanonicalInventory.from_legacy_dicts(
            data,
            context=context,
            database_name=database_name,
            metadata=metadata,
            database_parameters=database_parameters,
            project_parameters=project_parameters,
            source_namespace=_source_namespace(context.format),
        )
        self.migration_reports = tuple(migration_reports)

    @property
    def context(self) -> InventoryContext:
        """Return the exact software, technosphere, and biosphere context."""

        return self._inventory.context

    @property
    def data(self) -> list[dict]:
        """Return a deep copy of the canonical dataset dictionaries."""

        return self._inventory.to_legacy_dicts()

    @property
    def background_profile(self) -> BackgroundProfile:
        """Return the legacy technosphere-only profile projection."""

        return BackgroundProfile.from_technosphere_profile(self.context.background.technosphere)

    @property
    def biosphere_profile(self) -> BiosphereProfile:
        """Return the exact biosphere profile."""

        return self.context.background.biosphere

    @property
    def inventory_format(self) -> InventoryFormat:
        """Return the legacy enum projection of the software format."""

        return InventoryFormat(self.context.format.format_id)

    @property
    def database_name(self) -> str:
        """Return the foreground database name."""

        return self._inventory.database_name

    @property
    def metadata(self) -> dict:
        """Return a deep copy of database-level metadata."""

        return self._inventory.metadata.to_dict()

    @property
    def database_parameters(self) -> list[dict] | None:
        """Return a deep copy of database parameters, when present."""

        return self._inventory.to_legacy_components()["database_parameters"]

    @property
    def project_parameters(self) -> list[dict] | None:
        """Return a deep copy of project parameters, when present."""

        return self._inventory.to_legacy_components()["project_parameters"]

    def replace(
        self,
        *,
        data: list[dict] | None = None,
        background_profile: BackgroundProfile | None = None,
        inventory_format: InventoryFormat | None = None,
        context: InventoryContext | None = None,
        biosphere_profile: BiosphereProfile | None = None,
        migration_reports: tuple[Any, ...] | None = None,
    ) -> "InventoryDocument":
        """Return a copied document with selected fields replaced."""

        if context is not None and any(
            value is not None for value in (background_profile, inventory_format, biosphere_profile)
        ):
            raise TypeError("context cannot be combined with legacy context replacements.")

        next_context = context or InventoryContext(
            format=(FormatProfile(inventory_format.value) if inventory_format is not None else self.context.format),
            background=BackgroundContext(
                technosphere=(
                    background_profile.to_technosphere_profile()
                    if background_profile is not None
                    else self.context.background.technosphere
                ),
                biosphere=(
                    biosphere_profile
                    if biosphere_profile is not None
                    else (
                        default_biosphere_profile(background_profile)
                        if background_profile is not None
                        else self.context.background.biosphere
                    )
                ),
            ),
        )
        return InventoryDocument(
            data=self.data if data is None else data,
            context=next_context,
            database_name=self.database_name,
            metadata=self.metadata,
            database_parameters=self.database_parameters,
            project_parameters=self.project_parameters,
            migration_reports=(self.migration_reports if migration_reports is None else migration_reports),
        )


def _source_namespace(profile: FormatProfile) -> str:
    if profile.format_id.startswith("brightway"):
        return "brightway"
    if profile.format_id.startswith("simapro"):
        return "simapro"
    if profile.format_id.startswith("openlca"):
        return "openlca"
    if profile.format_id.startswith("ecospold"):
        return "ecospold2"
    return profile.format_id


def _check_legacy_context_arguments(
    context: InventoryContext,
    *,
    background_profile: BackgroundProfile | None,
    inventory_format: InventoryFormat | None,
    biosphere_profile: BiosphereProfile | None,
) -> None:
    if (
        background_profile is not None
        and background_profile.to_technosphere_profile() != context.background.technosphere
    ):
        raise ValueError("background_profile conflicts with context.technosphere.")
    if inventory_format is not None and inventory_format.value != context.format.format_id:
        raise ValueError("inventory_format conflicts with context.format.")
    if biosphere_profile is not None and biosphere_profile != context.background.biosphere:
        raise ValueError("biosphere_profile conflicts with context.biosphere.")
