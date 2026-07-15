from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Iterable

from .background import (
    CatalogProvider,
    catalog_provider_from_environment,
    execute_background_migration,
)
from .core.context import (
    BackgroundContext,
    BiosphereProfile,
    InventoryContext,
    TechnosphereProfile,
)
from .core.policies import MigrationPolicy
from .core.reports import OperationReport
from .exceptions import InventoryValidationError, MigrationError
from .formats import load_simapro_csv, render_simapro_rows, write_simapro_csv
from .formats.simapro_csv import SimaProRenderResult
from .models import (
    BackgroundProfile,
    InventoryDocument,
    InventoryFormat,
    Issue,
    ValidationReport,
    default_biosphere_profile,
)
from .normalization import normalize_inventory
from .validation import validate_brightway_inventory

if TYPE_CHECKING:
    from .brightway import BrightwayInventory

_STRICT_MIGRATION_POLICY = MigrationPolicy.strict()
_FORMAT_ID = InventoryFormat.SIMAPRO_CSV.value


class SimaProInventory:
    """Load, inspect, validate, migrate, and write a SimaPro inventory.

    The facade stores canonical Brightway-style dictionaries internally while
    making SimaPro CSV parsing, rendering, and writing explicit. Transforming
    methods return new facades and never mutate caller-owned input.
    """

    def __init__(self, document: InventoryDocument) -> None:
        if not isinstance(document, InventoryDocument):
            raise TypeError("document must be an InventoryDocument.")
        if document.context.format.format_id != _FORMAT_ID:
            raise ValueError("SimaProInventory requires a simapro_csv document.")
        self._document = document

    @classmethod
    def from_csv(
        cls,
        path: str | Path,
        *,
        background_profile: BackgroundProfile | None = None,
        biosphere_profile: BiosphereProfile | None = None,
        context: InventoryContext | None = None,
        database_name: str | None = None,
        catalog_provider: CatalogProvider | None = None,
    ) -> "SimaProInventory":
        """Load and normalize a SimaPro CSV export.

        :param path: Path to a semicolon-delimited SimaPro ``.csv`` file.
        :param background_profile: Legacy technosphere-only profile used to
            interpret SimaPro process names and background links.
        :param biosphere_profile: Explicit biosphere profile for legacy calls.
        :param context: Complete exact context. Its format must be
            ``simapro_csv``.
        :param database_name: Optional foreground name; defaults to the file
            stem.
        :param catalog_provider: Exact biosphere catalog provider used while
            normalizing SimaPro flow names. Application defaults use the
            environment/package provider stack.
        :return: A SimaPro inventory facade.
        :raises FileNotFoundError: If *path* does not exist.
        :raises ValueError: If *path* does not have a ``.csv`` suffix.
        """

        return cls(
            load_simapro_csv(
                path,
                background_profile=background_profile,
                biosphere_profile=biosphere_profile,
                context=context,
                database_name=database_name,
                catalog_provider=catalog_provider or catalog_provider_from_environment(),
            )
        )

    @classmethod
    def from_data(
        cls,
        data: list[dict],
        *,
        background_profile: BackgroundProfile | None = None,
        context: InventoryContext | None = None,
        biosphere_profile: BiosphereProfile | None = None,
        database_name: str = "brightpath-inventory",
        metadata: dict | None = None,
        database_parameters: list[dict] | None = None,
        project_parameters: list[dict] | None = None,
    ) -> "SimaProInventory":
        """Construct a SimaPro view from canonical dictionaries.

        :param data: Dataset dictionaries in the canonical Brightway-style
            representation.
        :param background_profile: Legacy technosphere-only profile currently
            linked by the inventory. Prefer *context* in new code.
        :param context: Exact software, technosphere, and biosphere context.
        :param biosphere_profile: Explicit biosphere used with the legacy
            *background_profile* argument.
        :param database_name: Foreground database name.
        :param metadata: Optional database metadata.
        :param database_parameters: Optional database-scoped parameters.
        :param project_parameters: Optional project-scoped parameters.
        :return: A facade that owns a deep copy of all supplied values.
        """

        if context is not None and context.format.format_id != _FORMAT_ID:
            raise ValueError("SimaProInventory context format must be simapro_csv.")

        return cls(
            InventoryDocument(
                data=data,
                background_profile=background_profile,
                inventory_format=(InventoryFormat.SIMAPRO_CSV if context is None else None),
                context=context,
                biosphere_profile=biosphere_profile,
                database_name=database_name,
                metadata=metadata,
                database_parameters=database_parameters,
                project_parameters=project_parameters,
            )
        )

    @property
    def data(self) -> list[dict]:
        """Return a deep copy of the canonical dataset dictionaries."""

        return self._document.data

    @property
    def background_profile(self) -> BackgroundProfile:
        """The normalized profile currently linked by the inventory."""

        return self._document.background_profile

    @property
    def context(self) -> InventoryContext:
        """The exact software, technosphere, and biosphere context."""

        return self._document.context

    @property
    def biosphere_profile(self) -> BiosphereProfile:
        """The exact biosphere profile currently linked by the inventory."""

        return self._document.biosphere_profile

    @property
    def database_name(self) -> str:
        """The foreground database name."""

        return self._document.database_name

    @property
    def metadata(self) -> dict:
        """Return a deep copy of database-level metadata."""

        return self._document.metadata

    @property
    def inventory_format(self) -> InventoryFormat:
        """The current software-format view."""

        return self._document.inventory_format

    @property
    def database_parameters(self) -> list[dict] | None:
        """Return a deep copy of database-scoped parameters."""

        return self._document.database_parameters

    @property
    def project_parameters(self) -> list[dict] | None:
        """Return a deep copy of project-scoped parameters."""

        return self._document.project_parameters

    @property
    def migration_reports(self) -> tuple[OperationReport, ...]:
        """Return immutable operation reports for successful migrations."""

        return tuple(self._document.migration_reports)

    @property
    def last_migration_report(self) -> OperationReport | None:
        """Return the most recent immutable migration operation report."""

        reports = self.migration_reports
        return reports[-1] if reports else None

    def normalize(self) -> "SimaProInventory":
        """Return a normalized copy of the canonical inventory data."""

        return SimaProInventory(normalize_inventory(self._document))

    def validate(
        self,
        *,
        check_background_links: bool = True,
        check_simapro_rendering: bool = False,
        additional_foreground_targets: Iterable[tuple[str, str, str, str]] = (),
        catalog_provider: CatalogProvider | None = None,
    ) -> ValidationReport:
        """Validate structure, links, SimaPro rendering, and model markers.

        :param check_background_links: Check external technosphere and
            biosphere identities against the exact background catalog.
        :param check_simapro_rendering: Also check whether canonical data can
            be represented as SimaPro rows.
        :param additional_foreground_targets: Valid external foreground
            identities as ``(name, reference product, location, unit)`` tuples.
        :param catalog_provider: Explicit exact-catalog provider. The facade
            uses the application environment provider when omitted.
        :return: A structured report. Validation never mutates the inventory.
        """

        report = validate_brightway_inventory(
            self._document,
            check_background_links=check_background_links,
            additional_foreground_targets=additional_foreground_targets,
            catalog_provider=catalog_provider,
        )
        if check_simapro_rendering:
            report.issues.extend(self.render().issues)
        detected_system_models = set(self.metadata.get("simapro detected system models", ()))
        if len(detected_system_models) > 1:
            report.issues.append(
                Issue(
                    severity="error",
                    code="simapro_system_model_mixed",
                    message=("SimaPro inventory contains both cut-off and consequential ecoinvent name markers."),
                )
            )
        elif (
            detected_system_models
            and self.background_profile.family == "ecoinvent"
            and self.background_profile.system_model not in detected_system_models
        ):
            detected = next(iter(detected_system_models))
            report.issues.append(
                Issue(
                    severity="error",
                    code="simapro_system_model_mismatch",
                    message=(
                        f"SimaPro names indicate ecoinvent {detected}, but the selected background "
                        f"profile is {self.background_profile.system_model or 'unspecified'}."
                    ),
                )
            )
        return report

    def render(self) -> SimaProRenderResult:
        """Render rows in memory and return rows plus structured issues."""

        return render_simapro_rows(self._document)

    def migrate_background(
        self,
        target: BackgroundContext | BackgroundProfile | TechnosphereProfile,
        *,
        biosphere_profile: BiosphereProfile | None = None,
        policy: MigrationPolicy = _STRICT_MIGRATION_POLICY,
        catalog_provider: CatalogProvider | None = None,
        additional_foreground_targets: Iterable[tuple[str, str, str, str]] = (),
    ) -> "SimaProInventory":
        """Transactionally migrate background links and return a new facade.

        The preferred *target* is a complete
        :class:`~brightpath.core.context.BackgroundContext`. Legacy
        :class:`~brightpath.models.BackgroundProfile` and
        :class:`~brightpath.core.context.TechnosphereProfile` targets may be
        paired with an explicit *biosphere_profile*. When it is omitted, the
        inventory's exact existing biosphere profile is preserved, except that
        the legacy UVEK 2025 target uses its documented ecoinvent 3.10
        biosphere. Reverse migrations require an explicitly permissive policy.

        :param target: Exact destination background context or a legacy
            technosphere-only target.
        :param biosphere_profile: Exact destination biosphere for a legacy
            technosphere-only target. Omit it to preserve the current exact
            biosphere profile, or to use ecoinvent 3.10 for UVEK 2025.
        :param policy: Validation, loss, and reverse-route decisions. Strict by
            default.
        :param catalog_provider: Exact source and target catalogs. When omitted,
            the application environment and packaged catalogs are used.
        :param additional_foreground_targets: Valid external foreground
            identities used by source and target validation.
        :raises brightpath.MigrationError: If planning, validation, or execution
            fails under *policy*. The exception contains the immutable report.
        """

        target_context = _coerce_migration_target(self._document, target, biosphere_profile)
        provider = catalog_provider if catalog_provider is not None else catalog_provider_from_environment()
        result = execute_background_migration(
            self._document,
            target_context,
            provider,
            policy,
            foreground_technosphere_targets=additional_foreground_targets,
        )
        if result.error:
            raise MigrationError(_migration_failure_message(result.report), report=result.report)
        migrated_document = result.value.replace(
            migration_reports=(*self._document.migration_reports, result.report),
        )
        return SimaProInventory(migrated_document)

    def write_csv(
        self,
        path: str | Path,
        *,
        validate: bool = True,
        additional_foreground_targets: Iterable[tuple[str, str, str, str]] = (),
    ) -> Path:
        """Write a Latin-1 SimaPro CSV file and return its absolute path.

        :param path: Destination. ``.csv`` is added when no suffix is given.
        :param validate: Validate structure, background links, and rendering
            before writing.
        :param additional_foreground_targets: Valid external foreground
            identities used by validation.
        :raises brightpath.InventoryValidationError: If enabled validation has
            errors.
        :raises brightpath.SimaProSerializationError: If rendering or Latin-1
            encoding fails.
        """

        if validate:
            report = self.validate(
                check_simapro_rendering=True,
                additional_foreground_targets=additional_foreground_targets,
            )
            if report.has_errors:
                raise InventoryValidationError(report)
        destination, _result = write_simapro_csv(self._document, path)
        return destination

    def to_brightway(self) -> "BrightwayInventory":
        """Return a Brightway-format view without changing background links."""

        from .brightway import BrightwayInventory

        return BrightwayInventory(self._document.replace(inventory_format=InventoryFormat.BRIGHTWAY_EXCEL))


def _coerce_migration_target(
    document: InventoryDocument,
    target: BackgroundContext | BackgroundProfile | TechnosphereProfile,
    biosphere_profile: BiosphereProfile | None,
) -> BackgroundContext:
    if isinstance(target, BackgroundContext):
        if biosphere_profile is not None:
            raise TypeError("biosphere_profile cannot be combined with a complete BackgroundContext target.")
        return target
    if isinstance(target, BackgroundProfile):
        technosphere = target.to_technosphere_profile()
    elif isinstance(target, TechnosphereProfile):
        technosphere = target
    else:
        raise TypeError("target must be a BackgroundContext, BackgroundProfile, or TechnosphereProfile.")
    if biosphere_profile is not None and not isinstance(biosphere_profile, BiosphereProfile):
        raise TypeError("biosphere_profile must be a BiosphereProfile or None.")
    return BackgroundContext(
        technosphere=technosphere,
        biosphere=(
            biosphere_profile
            or (default_biosphere_profile(technosphere) if technosphere.family == "uvek" else None)
            or document.context.background.biosphere
        ),
    )


def _migration_failure_message(report: OperationReport) -> str:
    first_error = next((issue.message for issue in report.issues if issue.severity.value == "error"), None)
    if first_error:
        return f"Background migration failed: {first_error}"
    return "Background migration failed under the selected policy."
