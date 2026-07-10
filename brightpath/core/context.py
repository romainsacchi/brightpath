"""Explicit software-format and background-database context models.

The context objects in this module deliberately keep file-format selection,
technosphere selection, and biosphere selection on separate axes.  Exact
database versions are retained on the profiles.  Migration code can request a
coarser migration series through :func:`resolve_migration_series` without
changing the source profile.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Union


def _string_value(value: object) -> str:
    """Return a stripped string, accepting string-valued enums at boundaries."""

    if isinstance(value, Enum):
        value = value.value
    return str(value or "").strip()


def _required(value: object, field_name: str) -> str:
    normalized = _string_value(value)
    if not normalized:
        raise ValueError("{} must not be empty.".format(field_name))
    return normalized


def _normalize_family(value: object) -> str:
    family = _required(value, "family").lower()
    if family == "bafu":
        return "uvek"
    return family


def _normalize_system_model(value: object) -> str:
    system_model = _required(value, "system_model").lower()
    if system_model in {"cut-off", "cut off"}:
        return "cutoff"
    return system_model


@dataclass(frozen=True)
class VersionResolution:
    """Describe how an exact database version maps to a migration series.

    :param family: Canonical background family.
    :param exact_version: Version retained by the inventory context.
    :param migration_series: Version key used to locate migration resources.
    :param strategy: Stable identifier for the resolution rule.

    Resolution is an audit value, not a normalized profile.  In particular,
    resolving ecoinvent ``3.10.1`` to series ``3.10`` never changes the exact
    version stored by a profile.
    """

    family: str
    exact_version: str
    migration_series: str
    strategy: str

    @property
    def changed(self) -> bool:
        """Whether the migration series differs from the exact version."""

        return self.exact_version != self.migration_series

    def label(self) -> str:
        """Return a compact, human-readable resolution description."""

        if self.changed:
            return "{} {} -> migration series {}".format(
                self.family,
                self.exact_version,
                self.migration_series,
            )
        return "{} {}".format(self.family, self.exact_version)


def resolve_migration_series(family: object, exact_version: object) -> VersionResolution:
    """Resolve an exact background version to its migration-resource series.

    Numeric ecoinvent patch releases use the corresponding major/minor series.
    Other version schemes are retained exactly.  The returned object records
    both values so callers can include the resolution in operation reports.

    :param family: Background family.  The legacy alias ``BAFU`` is accepted
        and normalized to ``uvek``.
    :param exact_version: Exact database version from the inventory context.
    :return: An auditable version resolution.
    """

    normalized_family = _normalize_family(family)
    version = _required(exact_version, "exact_version")
    if normalized_family == "ecoinvent":
        match = re.fullmatch(r"(?P<major>\d+)\.(?P<minor>\d+)(?:\.\d+)+", version)
        if match:
            series = "{}.{}".format(match.group("major"), match.group("minor"))
            return VersionResolution(normalized_family, version, series, "ecoinvent-major-minor")
    return VersionResolution(normalized_family, version, version, "exact")


@dataclass(frozen=True)
class FormatProfile:
    """Identify a software exchange format and its dialect.

    :param format_id: Stable format identifier, for example
        ``"brightway_excel"`` or ``"simapro_csv"``.
    :param format_version: Optional exact software-format version.
    :param dialect: Optional parser/writer dialect, such as ``"bw2io"``.
    :param encoding: Optional text encoding required by the format profile.
    """

    format_id: str
    format_version: str = ""
    dialect: str = ""
    encoding: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "format_id", _required(self.format_id, "format_id").lower())
        object.__setattr__(self, "format_version", _string_value(self.format_version))
        object.__setattr__(self, "dialect", _string_value(self.dialect).lower())
        object.__setattr__(self, "encoding", _string_value(self.encoding).lower())

    @property
    def id(self) -> str:
        """Alias used by adapter descriptors for the stable format identifier."""

        return self.format_id

    @property
    def identifier(self) -> str:
        """Return the stable format identifier."""

        return self.format_id

    def label(self) -> str:
        """Return a compact, human-readable format label."""

        parts = [self.format_id]
        if self.format_version:
            parts.append(self.format_version)
        if self.dialect:
            parts.append("({})".format(self.dialect))
        return " ".join(parts)


@dataclass(frozen=True)
class TechnosphereProfile:
    """Identify an exact technosphere background database.

    :param family: Background family, such as ``ecoinvent`` or ``uvek``.
        ``BAFU`` is accepted as a legacy input alias and immediately normalized
        to ``uvek``.
    :param version: Exact database version.  Patch releases are preserved.
    :param system_model: Explicit system model, for example ``cutoff`` or
        ``consequential``.
    """

    family: str
    version: str
    system_model: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "family", _normalize_family(self.family))
        object.__setattr__(self, "version", _required(self.version, "version"))
        object.__setattr__(self, "system_model", _normalize_system_model(self.system_model))

    def resolve_migration_series(self) -> VersionResolution:
        """Return the migration-series resolution without changing this profile."""

        return resolve_migration_series(self.family, self.version)

    def label(self) -> str:
        """Return a compact, human-readable profile label."""

        return "{} {} {}".format(self.family, self.version, self.system_model)


@dataclass(frozen=True)
class BiosphereProfile:
    """Identify an exact biosphere database independently of technosphere.

    :param family: Biosphere family.  ``BAFU`` remains accepted only as a
        legacy boundary alias and is normalized to ``uvek``.
    :param version: Exact biosphere version; patch releases are preserved.
    """

    family: str
    version: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "family", _normalize_family(self.family))
        object.__setattr__(self, "version", _required(self.version, "version"))

    def resolve_migration_series(self) -> VersionResolution:
        """Return the migration-series resolution without changing this profile."""

        return resolve_migration_series(self.family, self.version)

    def label(self) -> str:
        """Return a compact, human-readable profile label."""

        return "{} {}".format(self.family, self.version)


@dataclass(frozen=True)
class BackgroundContext:
    """Pair explicit technosphere and biosphere profiles."""

    technosphere: TechnosphereProfile
    biosphere: BiosphereProfile

    def __post_init__(self) -> None:
        if not isinstance(self.technosphere, TechnosphereProfile):
            raise TypeError("technosphere must be a TechnosphereProfile.")
        if not isinstance(self.biosphere, BiosphereProfile):
            raise TypeError("biosphere must be a BiosphereProfile.")


@dataclass(frozen=True)
class InventoryContext:
    """Complete source or target context for an inventory operation."""

    format: FormatProfile
    background: BackgroundContext

    def __post_init__(self) -> None:
        if not isinstance(self.format, FormatProfile):
            raise TypeError("format must be a FormatProfile.")
        if not isinstance(self.background, BackgroundContext):
            raise TypeError("background must be a BackgroundContext.")

    def as_hint(self) -> "ContextHint":
        """Return this complete context as a parser hint."""

        return ContextHint(format=self.format, background=self.background)


@dataclass(frozen=True)
class ContextHint:
    """Optional context supplied to parsing and detection operations.

    A hint may identify only the format, either background component, a
    complete background, all axes, or none.  It therefore never invents
    missing context.  Call
    :meth:`require_complete` only at an operation boundary that needs a full
    :class:`InventoryContext`.
    """

    format: Optional[FormatProfile] = None
    background: Optional[BackgroundContext] = None
    technosphere: Optional[TechnosphereProfile] = None
    biosphere: Optional[BiosphereProfile] = None

    def __post_init__(self) -> None:
        if self.format is not None and not isinstance(self.format, FormatProfile):
            raise TypeError("format must be a FormatProfile or None.")
        if self.background is not None and not isinstance(self.background, BackgroundContext):
            raise TypeError("background must be a BackgroundContext or None.")
        if self.technosphere is not None and not isinstance(self.technosphere, TechnosphereProfile):
            raise TypeError("technosphere must be a TechnosphereProfile or None.")
        if self.biosphere is not None and not isinstance(self.biosphere, BiosphereProfile):
            raise TypeError("biosphere must be a BiosphereProfile or None.")

        if self.background is not None:
            if self.technosphere is not None and self.technosphere != self.background.technosphere:
                raise ValueError("technosphere hint conflicts with the complete background hint.")
            if self.biosphere is not None and self.biosphere != self.background.biosphere:
                raise ValueError("biosphere hint conflicts with the complete background hint.")
            object.__setattr__(self, "technosphere", self.background.technosphere)
            object.__setattr__(self, "biosphere", self.background.biosphere)
        elif self.technosphere is not None and self.biosphere is not None:
            object.__setattr__(self, "background", BackgroundContext(self.technosphere, self.biosphere))

    @classmethod
    def from_context(cls, context: InventoryContext) -> "ContextHint":
        """Create a complete hint from an inventory context."""

        if not isinstance(context, InventoryContext):
            raise TypeError("context must be an InventoryContext.")
        return cls(format=context.format, background=context.background)

    @property
    def is_complete(self) -> bool:
        """Whether both the format and background axes are present."""

        return self.format is not None and self.background is not None

    def require_complete(self) -> InventoryContext:
        """Return a complete inventory context or identify missing axes."""

        missing = []
        if self.format is None:
            missing.append("format")
        if self.background is None:
            missing.append("background")
        if missing:
            raise ValueError("Context hint is incomplete; missing {}.".format(" and ".join(missing)))
        return InventoryContext(format=self.format, background=self.background)


ProfileWithVersion = Union[TechnosphereProfile, BiosphereProfile]


def resolve_profile_migration_series(profile: ProfileWithVersion) -> VersionResolution:
    """Resolve the migration series for a technosphere or biosphere profile."""

    if not isinstance(profile, (TechnosphereProfile, BiosphereProfile)):
        raise TypeError("profile must be a TechnosphereProfile or BiosphereProfile.")
    return resolve_migration_series(profile.family, profile.version)
