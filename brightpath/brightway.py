from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import TYPE_CHECKING, Iterable

from .core.context import BiosphereProfile, InventoryContext
from .exceptions import InventoryValidationError
from .formats import load_brightway_excel, write_brightway_excel
from .migrations import MigrationReport, migrate_inventory
from .models import (
    BackgroundProfile,
    InventoryDocument,
    InventoryFormat,
    ValidationReport,
)
from .normalization import normalize_inventory
from .validation import validate_brightway_inventory

if TYPE_CHECKING:
    from .simapro import SimaProInventory


class BrightwayInventory:
    """Load, inspect, validate, migrate, and write a Brightway inventory.

    Use :meth:`from_excel` for a workbook or :meth:`from_data` for canonical
    Brightway-style dictionaries. All transforming methods return a new facade;
    the source object and caller-owned input remain unchanged.
    """

    def __init__(self, document: InventoryDocument) -> None:
        self._document = document

    @classmethod
    def from_excel(
        cls,
        path: str | Path,
        *,
        background_profile: BackgroundProfile | None = None,
        biosphere_profile: BiosphereProfile | None = None,
        context: InventoryContext | None = None,
    ) -> "BrightwayInventory":
        """Load a ``bw2io``-compatible Brightway Excel workbook.

        :param path: Path to an ``.xlsx`` workbook.
        :param background_profile: Explicit source profile. If omitted,
            BrightPath uses profile metadata embedded by :meth:`write_excel`,
            when available.
        :param biosphere_profile: Explicit biosphere profile for legacy calls.
        :param context: Complete exact context. Its format must be
            ``brightway_excel``.
        :return: A Brightway inventory facade.
        :raises FileNotFoundError: If *path* does not exist.
        :raises ValueError: If *path* does not have an ``.xlsx`` suffix.
        """

        return cls(
            load_brightway_excel(
                path,
                background_profile=background_profile,
                biosphere_profile=biosphere_profile,
                context=context,
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
    ) -> "BrightwayInventory":
        """Construct a Brightway inventory from canonical dictionaries.

        :param data: Dataset dictionaries in the canonical Brightway-style
            representation.
        :param background_profile: Legacy technosphere-only profile currently
            linked by the inventory. Prefer *context* in new code.
        :param context: Exact software, technosphere, and biosphere context.
        :param biosphere_profile: Explicit biosphere used with the legacy
            *background_profile* argument.
        :param database_name: Name written into exported workbooks.
        :param metadata: Optional database metadata.
        :param database_parameters: Optional database-scoped parameters.
        :param project_parameters: Optional project-scoped parameters.
        :return: A facade that owns a deep copy of all supplied values.
        """

        return cls(
            InventoryDocument(
                data=data,
                background_profile=background_profile,
                inventory_format=(InventoryFormat.BRIGHTWAY_EXCEL if context is None else None),
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
    def migration_reports(self) -> tuple[MigrationReport, ...]:
        """Return copied audit reports for all migrations in this pipeline."""

        return deepcopy(self._document.migration_reports)

    @property
    def last_migration_report(self) -> MigrationReport | None:
        """The most recent migration report, or ``None`` before migration."""

        reports = self.migration_reports
        return reports[-1] if reports else None

    def normalize(self) -> "BrightwayInventory":
        """Return a normalized copy of the inventory.

        Normalization promotes legacy ``product`` fields, converts category
        sequences to tuples, and synchronizes production exchanges with their
        owning datasets.
        """

        return BrightwayInventory(normalize_inventory(self._document))

    def validate(
        self,
        *,
        check_background_links: bool = True,
        additional_foreground_targets: Iterable[tuple[str, str, str, str]] = (),
    ) -> ValidationReport:
        """Validate structure, plausibility, identities, and optional links.

        :param check_background_links: Check external technosphere and
            biosphere identities against the exact background catalog.
        :param additional_foreground_targets: Valid external foreground
            identities as ``(name, reference product, location, unit)`` tuples.
        :return: A structured report. Validation never mutates the inventory.
        """

        return validate_brightway_inventory(
            self._document,
            check_background_links=check_background_links,
            additional_foreground_targets=additional_foreground_targets,
        )

    def migrate_background(
        self,
        target_profile: BackgroundProfile,
        *,
        validate_target: bool = True,
        additional_foreground_targets: Iterable[tuple[str, str, str, str]] = (),
    ) -> "BrightwayInventory":
        """Return a copy linked to *target_profile*.

        The software format remains Brightway Excel. Only packaged ecoinvent
        cut-off migrations are currently available; reverse routes can be
        lossy and must be reviewed through :attr:`last_migration_report`.

        :param target_profile: Explicit destination background profile.
        :param validate_target: Append target-catalog validation issues to the
            migration report after applying the route.
        :param additional_foreground_targets: Valid external foreground
            identities used by target validation.
        :raises brightpath.MigrationUnavailableError: If no supported route
            exists.
        """

        migrated_document, report = migrate_inventory(self._document, target_profile)
        migrated = BrightwayInventory(migrated_document)
        if validate_target:
            validation = migrated.validate(
                additional_foreground_targets=additional_foreground_targets,
            )
            report.issues.extend(validation.issues)
        return migrated

    def write_excel(
        self,
        path: str | Path,
        *,
        validate: bool = True,
        additional_foreground_targets: Iterable[tuple[str, str, str, str]] = (),
    ) -> Path:
        """Write a Brightway Excel workbook and return its absolute path.

        :param path: Destination. ``.xlsx`` is added when no suffix is given.
        :param validate: Validate against the selected background catalog
            before writing.
        :param additional_foreground_targets: Valid external foreground
            identities used by validation.
        :raises brightpath.InventoryValidationError: If enabled validation has
            errors.
        :raises brightpath.ExcelSerializationError: If a value cannot be
            represented in the workbook.
        """

        if validate:
            report = self.validate(
                additional_foreground_targets=additional_foreground_targets,
            )
            if report.has_errors:
                raise InventoryValidationError(report)
        return write_brightway_excel(self._document, path)

    def to_simapro(self) -> "SimaProInventory":
        """Return a SimaPro-format view without changing background links."""

        from .simapro import SimaProInventory

        return SimaProInventory(self._document.replace(inventory_format=InventoryFormat.SIMAPRO_CSV))
