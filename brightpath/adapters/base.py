"""Format-adapter contracts independent of any concrete inventory schema."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol, runtime_checkable


class ArtifactKind(str, Enum):
    """Physical shape of an artifact accepted or produced by an adapter."""

    FILE = "file"
    DIRECTORY = "directory"
    STREAM = "stream"
    BYTES = "bytes"


def _clean_identifier(value: object, *, field_name: str) -> str:
    """Return a non-empty, case-normalized identifier."""

    normalized = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    if not normalized:
        raise ValueError(f"{field_name} must not be empty.")
    return normalized


@dataclass(frozen=True, order=True)
class FormatDescriptor:
    """Stable identity for one software exchange format or dialect.

    ``format_id`` identifies the format family, while ``version`` and
    ``dialect`` can select a more specific adapter. The ``id`` and
    ``identifier`` properties make the descriptor compatible with context
    objects and earlier registry prototypes without coupling this module to
    the core context model.

    :param format_id: Stable identifier such as ``"brightway_excel"``.
    :param version: Optional software-format version.
    :param dialect: Optional vendor or serialization dialect.
    """

    format_id: str
    version: str = ""
    dialect: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "format_id", _clean_identifier(self.format_id, field_name="format_id"))
        object.__setattr__(self, "version", str(self.version or "").strip())
        object.__setattr__(self, "dialect", str(self.dialect or "").strip().lower())

    @property
    def id(self) -> str:
        """Alias for :attr:`format_id` used by format profiles."""

        return self.format_id

    @property
    def identifier(self) -> str:
        """Alias for :attr:`format_id` used by generic registries."""

        return self.format_id

    def label(self) -> str:
        """Return a compact label suitable for reports and error messages."""

        qualifiers = "/".join(value for value in (self.version, self.dialect) if value)
        return f"{self.format_id} ({qualifiers})" if qualifiers else self.format_id


def coerce_format_descriptor(value: object) -> FormatDescriptor:
    """Coerce a string, enum, or profile-like object to a descriptor.

    Profile-like objects can expose ``format_id``, ``id``, or ``identifier``.
    ``format_version`` is accepted as an alias for ``version``.
    """

    if isinstance(value, FormatDescriptor):
        return value

    if isinstance(value, Enum):
        value = value.value

    if isinstance(value, str):
        return FormatDescriptor(value)

    format_id = None
    for attribute in ("format_id", "id", "identifier"):
        candidate = getattr(value, attribute, None)
        if candidate:
            format_id = candidate
            break
    if format_id is None:
        raise TypeError("Format identifiers must be strings, enums, or objects exposing format_id, id, or identifier.")

    version = getattr(value, "format_version", None)
    if version is None:
        version = getattr(value, "version", "")
    return FormatDescriptor(
        format_id=format_id,
        version=version,
        dialect=getattr(value, "dialect", ""),
    )


def _freeze_artifact_kinds(values: object, *, field_name: str) -> frozenset[ArtifactKind]:
    """Validate and freeze an artifact-kind collection."""

    try:
        return frozenset(value if isinstance(value, ArtifactKind) else ArtifactKind(value) for value in values)  # type: ignore[arg-type]
    except (TypeError, ValueError) as error:
        raise ValueError(f"{field_name} contains an unsupported artifact kind.") from error


@dataclass(frozen=True)
class AdapterCapabilities:
    """Read, write, and content-detection support declared by an adapter.

    An empty collection means that the operation is unsupported. Detection is
    declared independently from reading because some adapters require an
    explicit format even though they can parse the artifact.
    """

    read_artifact_kinds: frozenset[ArtifactKind] = field(default_factory=frozenset)
    write_artifact_kinds: frozenset[ArtifactKind] = field(default_factory=frozenset)
    detection_artifact_kinds: frozenset[ArtifactKind] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "read_artifact_kinds",
            _freeze_artifact_kinds(self.read_artifact_kinds, field_name="read_artifact_kinds"),
        )
        object.__setattr__(
            self,
            "write_artifact_kinds",
            _freeze_artifact_kinds(self.write_artifact_kinds, field_name="write_artifact_kinds"),
        )
        object.__setattr__(
            self,
            "detection_artifact_kinds",
            _freeze_artifact_kinds(self.detection_artifact_kinds, field_name="detection_artifact_kinds"),
        )

    def supports_read(self, artifact_kind: ArtifactKind | str) -> bool:
        """Whether the adapter can read ``artifact_kind``."""

        return ArtifactKind(artifact_kind) in self.read_artifact_kinds

    def supports_write(self, artifact_kind: ArtifactKind | str) -> bool:
        """Whether the adapter can write ``artifact_kind``."""

        return ArtifactKind(artifact_kind) in self.write_artifact_kinds

    def supports_detection(self, artifact_kind: ArtifactKind | str) -> bool:
        """Whether the adapter can probe ``artifact_kind`` for its format."""

        return ArtifactKind(artifact_kind) in self.detection_artifact_kinds


@dataclass(frozen=True)
class DetectionCandidate:
    """One adapter's evidence that an artifact uses its format.

    :param descriptor: Format claimed by the probe.
    :param confidence: Confidence from ``0.0`` through ``1.0``.
    :param evidence: Human-readable observations supporting the confidence.
    """

    descriptor: FormatDescriptor
    confidence: float
    evidence: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        descriptor = coerce_format_descriptor(self.descriptor)
        confidence = float(self.confidence)
        if not 0.0 <= confidence <= 1.0:
            raise ValueError("Detection confidence must be between 0.0 and 1.0.")
        object.__setattr__(self, "descriptor", descriptor)
        object.__setattr__(self, "confidence", confidence)
        object.__setattr__(self, "evidence", tuple(str(item) for item in self.evidence))


@runtime_checkable
class FormatAdapter(Protocol):
    """Structural contract implemented by format-specific adapters.

    Detection must inspect without consuming or modifying reusable caller-owned
    input. Read and write use ``Any`` until the canonical inventory schema is a
    stable dependency of the adapter layer.
    """

    descriptor: FormatDescriptor
    capabilities: AdapterCapabilities

    def detect(self, artifact: object, *, artifact_kind: ArtifactKind) -> DetectionCandidate | None:
        """Return detection evidence, or ``None`` when there is no match."""

    def read(self, artifact: object, **kwargs: Any) -> Any:
        """Parse an artifact into the canonical inventory representation."""

    def write(self, document: object, artifact: object, **kwargs: Any) -> Any:
        """Serialize a canonical inventory representation to an artifact."""
