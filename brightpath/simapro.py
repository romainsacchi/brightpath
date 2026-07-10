from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import TYPE_CHECKING, Iterable

from .exceptions import InventoryValidationError
from .formats import load_simapro_csv, render_simapro_rows, write_simapro_csv
from .formats.simapro_csv import SimaProRenderResult
from .migrations import MigrationReport, migrate_inventory
from .models import BackgroundProfile, InventoryDocument, InventoryFormat, Issue, ValidationReport
from .normalization import normalize_inventory
from .validation import validate_brightway_inventory

if TYPE_CHECKING:
    from .brightway import BrightwayInventory


class SimaProInventory:
    """Load, inspect, validate, migrate, and write a SimaPro inventory.

    The facade stores canonical Brightway-style dictionaries internally while
    making SimaPro CSV parsing, rendering, and writing explicit. Transforming
    methods return new facades and never mutate caller-owned input.
    """

    def __init__(self, document: InventoryDocument) -> None:
        self._document = document

    @classmethod
    def from_csv(
        cls,
        path: str | Path,
        *,
        background_profile: BackgroundProfile,
        database_name: str | None = None,
    ) -> "SimaProInventory":
        """Load and normalize a SimaPro CSV export.

        :param path: Path to a semicolon-delimited SimaPro ``.csv`` file.
        :param background_profile: Explicit profile used to interpret SimaPro
            process names and background links.
        :param database_name: Optional foreground name; defaults to the file
            stem.
        :return: A SimaPro inventory facade.
        :raises FileNotFoundError: If *path* does not exist.
        :raises ValueError: If *path* does not have a ``.csv`` suffix.
        """

        return cls(
            load_simapro_csv(
                path,
                background_profile=background_profile,
                database_name=database_name,
            )
        )

    @classmethod
    def from_data(
        cls,
        data: list[dict],
        *,
        background_profile: BackgroundProfile,
        database_name: str = "brightpath-inventory",
        metadata: dict | None = None,
        database_parameters: list[dict] | None = None,
        project_parameters: list[dict] | None = None,
    ) -> "SimaProInventory":
        """Construct a SimaPro view from canonical dictionaries.

        :param data: Dataset dictionaries in the canonical Brightway-style
            representation.
        :param background_profile: Profile currently linked by the inventory.
        :param database_name: Foreground database name.
        :param metadata: Optional database metadata.
        :param database_parameters: Optional database-scoped parameters.
        :param project_parameters: Optional project-scoped parameters.
        :return: A facade that owns a deep copy of all supplied values.
        """

        return cls(
            InventoryDocument(
                data=data,
                background_profile=background_profile,
                inventory_format=InventoryFormat.SIMAPRO_CSV,
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
    def migration_reports(self) -> tuple[MigrationReport, ...]:
        """Return copied audit reports for all migrations in this pipeline."""

        return deepcopy(self._document.migration_reports)

    @property
    def last_migration_report(self) -> MigrationReport | None:
        """The most recent migration report, or ``None`` before migration."""

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
    ) -> ValidationReport:
        """Validate structure, links, SimaPro rendering, and model markers.

        :param check_background_links: Check external technosphere and
            biosphere identities against the exact background catalog.
        :param check_simapro_rendering: Also check whether canonical data can
            be represented as SimaPro rows.
        :param additional_foreground_targets: Valid external foreground
            identities as ``(name, reference product, location, unit)`` tuples.
        :return: A structured report. Validation never mutates the inventory.
        """

        report = validate_brightway_inventory(
            self._document,
            check_background_links=check_background_links,
            additional_foreground_targets=additional_foreground_targets,
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
        target_profile: BackgroundProfile,
        *,
        validate_target: bool = True,
        additional_foreground_targets: Iterable[tuple[str, str, str, str]] = (),
    ) -> "SimaProInventory":
        """Return a SimaPro copy linked to *target_profile*.

        The software format remains SimaPro CSV. Only packaged ecoinvent
        cut-off migrations are currently available.

        :param target_profile: Explicit destination background profile.
        :param validate_target: Append target-catalog validation issues to the
            migration report after applying the route.
        :param additional_foreground_targets: Valid external foreground
            identities used by target validation.
        :raises brightpath.MigrationUnavailableError: If no supported route
            exists.
        """

        migrated_document, report = migrate_inventory(self._document, target_profile)
        migrated = SimaProInventory(migrated_document)
        if validate_target:
            validation = migrated.validate(
                additional_foreground_targets=additional_foreground_targets,
            )
            report.issues.extend(validation.issues)
        return migrated

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
