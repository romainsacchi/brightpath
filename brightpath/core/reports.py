"""Immutable, serializable reports for BrightPath operations.

The report types in this module are independent of file-format and background
database implementations.  They provide a stable boundary for composing the
read, validation, migration, conversion, and write stages of a pipeline.
"""

from __future__ import annotations

import json
import math
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, Generic, Optional, Tuple, TypeVar

REPORT_SCHEMA_VERSION = 1


class Severity(str, Enum):
    """Severity assigned to a structured report issue."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class OperationKind(str, Enum):
    """Top-level operation represented by an :class:`OperationReport`."""

    ANALYZE = "analyze"
    READ = "read"
    NORMALIZE = "normalize"
    VALIDATE = "validate"
    MIGRATE = "migrate"
    CONVERT = "convert"
    WRITE = "write"


class StageKind(str, Enum):
    """Known stages that can contribute findings to an operation report."""

    FORMAT_DETECTION = "format_detection"
    PARSE = "parse"
    NORMALIZE = "normalize"
    RECONCILE = "reconcile"
    STRUCTURAL_VALIDATION = "structural_validation"
    FORMAT_VALIDATION = "format_validation"
    PROFILE_INFERENCE = "profile_inference"
    BACKGROUND_VALIDATION = "background_validation"
    MIGRATION_PLANNING = "migration_planning"
    BACKGROUND_MIGRATION = "background_migration"
    CONVERSION_PREFLIGHT = "conversion_preflight"
    FORMAT_CONVERSION = "format_conversion"
    SERIALIZATION = "serialization"
    WRITE = "write"


class _FrozenMapping(Mapping[str, Any]):
    """Small hashable mapping used to expose immutable JSON objects."""

    __slots__ = ("_items",)

    def __init__(self, items: Tuple[Tuple[str, Any], ...]) -> None:
        self._items = items

    def __getitem__(self, key: str) -> Any:
        for candidate, value in self._items:
            if candidate == key:
                return value
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        return (key for key, _ in self._items)

    def __len__(self) -> int:
        return len(self._items)

    def __hash__(self) -> int:
        return hash(self._items)

    def __repr__(self) -> str:
        return repr(dict(self._items))


def _freeze_json(value: Any, *, location: str = "value") -> Any:
    """Copy and recursively freeze a JSON-compatible value."""

    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{location} must not contain non-finite floats.")
        return value
    if isinstance(value, Mapping):
        keys = tuple(value)
        if any(not isinstance(key, str) for key in keys):
            raise TypeError(f"{location} must contain only string object keys.")
        items = []
        for key in sorted(keys):
            items.append((key, _freeze_json(value[key], location=f"{location}.{key}")))
        return _FrozenMapping(tuple(items))
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_json(item, location=f"{location}[{index}]") for index, item in enumerate(value))
    raise TypeError(f"{location} contains unsupported value of type {type(value).__name__!r}.")


def _freeze_object(value: Mapping[str, Any], *, location: str) -> Mapping[str, Any]:
    frozen = _freeze_json(value, location=location)
    if not isinstance(frozen, Mapping):  # pragma: no cover - guarded by the type signature
        raise TypeError(f"{location} must be a mapping.")
    return frozen


def _thaw_json(value: Any) -> Any:
    """Return ordinary JSON containers for a recursively frozen value."""

    if isinstance(value, Mapping):
        return {key: _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    return value


def _canonical_json(value: Any) -> str:
    return json.dumps(_thaw_json(value), ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True)


def _enum_value(enum_type: Any, value: Any, *, field_name: str) -> Any:
    try:
        return enum_type(value)
    except (TypeError, ValueError) as error:
        allowed = ", ".join(member.value for member in enum_type)
        raise ValueError(f"{field_name} must be one of: {allowed}.") from error


_SEVERITY_ORDER = {Severity.ERROR: 0, Severity.WARNING: 1, Severity.INFO: 2}
_STAGE_ORDER = {stage: index for index, stage in enumerate(StageKind)}


@dataclass(frozen=True)
class Issue:
    """A structured issue emitted by one pipeline stage.

    ``details`` must contain JSON-compatible data. It is copied and recursively
    frozen, so later changes to caller-owned dictionaries cannot alter a report.
    """

    severity: Severity
    code: str
    message: str
    stage: StageKind
    path: str = ""
    details: Mapping[str, Any] = field(default_factory=dict)
    suggested_fix: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "severity", _enum_value(Severity, self.severity, field_name="severity"))
        object.__setattr__(self, "stage", _enum_value(StageKind, self.stage, field_name="stage"))
        if not self.code:
            raise ValueError("Issue code must not be empty.")
        if not self.message:
            raise ValueError("Issue message must not be empty.")
        object.__setattr__(self, "details", _freeze_object(self.details, location="issue details"))

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-compatible representation."""

        return {
            "severity": self.severity.value,
            "code": self.code,
            "message": self.message,
            "stage": self.stage.value,
            "path": self.path,
            "details": _thaw_json(self.details),
            "suggested_fix": self.suggested_fix,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Issue":
        """Construct an issue from :meth:`to_dict` output."""

        return cls(
            severity=Severity(data["severity"]),
            code=str(data["code"]),
            message=str(data["message"]),
            stage=StageKind(data["stage"]),
            path=str(data.get("path", "")),
            details=data.get("details", {}),
            suggested_fix=str(data.get("suggested_fix", "")),
        )


def _issue_sort_key(issue: Issue) -> Tuple[Any, ...]:
    return (
        _STAGE_ORDER[issue.stage],
        _SEVERITY_ORDER[issue.severity],
        issue.path,
        issue.code,
        issue.message,
        issue.suggested_fix,
        _canonical_json(issue.details),
    )


@dataclass(frozen=True)
class Change:
    """A non-lossy transformation made by a pipeline stage."""

    code: str
    message: str
    stage: StageKind
    path: str = ""
    before: Any = None
    after: Any = None
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "stage", _enum_value(StageKind, self.stage, field_name="stage"))
        if not self.code:
            raise ValueError("Change code must not be empty.")
        if not self.message:
            raise ValueError("Change message must not be empty.")
        object.__setattr__(self, "before", _freeze_json(self.before, location="change before"))
        object.__setattr__(self, "after", _freeze_json(self.after, location="change after"))
        object.__setattr__(self, "details", _freeze_object(self.details, location="change details"))

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-compatible representation."""

        return {
            "code": self.code,
            "message": self.message,
            "stage": self.stage.value,
            "path": self.path,
            "before": _thaw_json(self.before),
            "after": _thaw_json(self.after),
            "details": _thaw_json(self.details),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Change":
        """Construct a change from :meth:`to_dict` output."""

        return cls(
            code=str(data["code"]),
            message=str(data["message"]),
            stage=StageKind(data["stage"]),
            path=str(data.get("path", "")),
            before=data.get("before"),
            after=data.get("after"),
            details=data.get("details", {}),
        )


def _change_sort_key(change: Change) -> Tuple[Any, ...]:
    return (
        _STAGE_ORDER[change.stage],
        change.path,
        change.code,
        change.message,
        _canonical_json(change.before),
        _canonical_json(change.after),
        _canonical_json(change.details),
    )


@dataclass(frozen=True)
class Loss:
    """An explicit information loss caused by a transformation."""

    code: str
    message: str
    stage: StageKind
    path: str = ""
    recoverable: bool = False
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "stage", _enum_value(StageKind, self.stage, field_name="stage"))
        if not self.code:
            raise ValueError("Loss code must not be empty.")
        if not self.message:
            raise ValueError("Loss message must not be empty.")
        if not isinstance(self.recoverable, bool):
            raise TypeError("Loss recoverable must be a boolean.")
        object.__setattr__(self, "details", _freeze_object(self.details, location="loss details"))

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-compatible representation."""

        return {
            "code": self.code,
            "message": self.message,
            "stage": self.stage.value,
            "path": self.path,
            "recoverable": self.recoverable,
            "details": _thaw_json(self.details),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Loss":
        """Construct a loss from :meth:`to_dict` output."""

        return cls(
            code=str(data["code"]),
            message=str(data["message"]),
            stage=StageKind(data["stage"]),
            path=str(data.get("path", "")),
            recoverable=bool(data.get("recoverable", False)),
            details=data.get("details", {}),
        )


def _loss_sort_key(loss: Loss) -> Tuple[Any, ...]:
    return (
        _STAGE_ORDER[loss.stage],
        loss.path,
        loss.code,
        loss.message,
        loss.recoverable,
        _canonical_json(loss.details),
    )


@dataclass(frozen=True)
class StageReport:
    """Immutable findings and metrics for one pipeline stage."""

    stage: StageKind
    label: str = ""
    issues: Tuple[Issue, ...] = ()
    changes: Tuple[Change, ...] = ()
    losses: Tuple[Loss, ...] = ()
    metrics: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        stage = _enum_value(StageKind, self.stage, field_name="stage")
        object.__setattr__(self, "stage", stage)
        issues = tuple(self.issues)
        changes = tuple(self.changes)
        losses = tuple(self.losses)
        expected_types = ((issues, Issue), (changes, Change), (losses, Loss))
        for findings, expected_type in expected_types:
            if any(not isinstance(finding, expected_type) for finding in findings):
                raise TypeError(
                    f"Stage report {expected_type.__name__.lower()}s must be {expected_type.__name__} objects."
                )
        for finding in issues + changes + losses:
            if finding.stage != stage:
                raise ValueError(
                    f"{type(finding).__name__} stage {finding.stage.value!r} does not match "
                    f"stage report {stage.value!r}."
                )
        object.__setattr__(self, "issues", tuple(sorted(issues, key=_issue_sort_key)))
        object.__setattr__(self, "changes", tuple(sorted(changes, key=_change_sort_key)))
        object.__setattr__(self, "losses", tuple(sorted(losses, key=_loss_sort_key)))
        object.__setattr__(self, "metrics", _freeze_object(self.metrics, location="stage metrics"))

    @property
    def changed(self) -> bool:
        """Whether this stage records any transformation."""

        return bool(self.changes or self.losses)

    @property
    def lossy(self) -> bool:
        """Whether this stage records any information loss."""

        return bool(self.losses)

    @property
    def has_errors(self) -> bool:
        """Whether this stage contains an error issue."""

        return any(issue.severity is Severity.ERROR for issue in self.issues)

    @property
    def error(self) -> bool:
        """Alias for :attr:`has_errors`, suitable for pipeline predicates."""

        return self.has_errors

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-compatible representation."""

        return {
            "stage": self.stage.value,
            "label": self.label,
            "issues": [issue.to_dict() for issue in self.issues],
            "changes": [change.to_dict() for change in self.changes],
            "losses": [loss.to_dict() for loss in self.losses],
            "metrics": _thaw_json(self.metrics),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "StageReport":
        """Construct a stage report from :meth:`to_dict` output."""

        return cls(
            stage=StageKind(data["stage"]),
            label=str(data.get("label", "")),
            issues=tuple(Issue.from_dict(item) for item in data.get("issues", [])),
            changes=tuple(Change.from_dict(item) for item in data.get("changes", [])),
            losses=tuple(Loss.from_dict(item) for item in data.get("losses", [])),
            metrics=data.get("metrics", {}),
        )


@dataclass(frozen=True)
class OperationReport:
    """Complete immutable report for one BrightPath operation."""

    operation: OperationKind
    stages: Tuple[StageReport, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)
    schema_version: int = REPORT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "operation", _enum_value(OperationKind, self.operation, field_name="operation"))
        stages = tuple(self.stages)
        if any(not isinstance(stage, StageReport) for stage in stages):
            raise TypeError("Operation report stages must be StageReport objects.")
        object.__setattr__(self, "stages", stages)
        if self.schema_version != REPORT_SCHEMA_VERSION:
            raise ValueError(
                f"Unsupported report schema version {self.schema_version!r}; expected {REPORT_SCHEMA_VERSION}."
            )
        object.__setattr__(self, "metadata", _freeze_object(self.metadata, location="operation metadata"))

    @property
    def issues(self) -> Tuple[Issue, ...]:
        """Return all issues in stable canonical order."""

        return tuple(sorted((issue for stage in self.stages for issue in stage.issues), key=_issue_sort_key))

    @property
    def changes(self) -> Tuple[Change, ...]:
        """Return all changes in pipeline stage order."""

        return tuple(change for stage in self.stages for change in stage.changes)

    @property
    def losses(self) -> Tuple[Loss, ...]:
        """Return all losses in pipeline stage order."""

        return tuple(loss for stage in self.stages for loss in stage.losses)

    @property
    def changed(self) -> bool:
        """Whether the operation records any transformation."""

        return any(stage.changed for stage in self.stages)

    @property
    def lossy(self) -> bool:
        """Whether the operation records any information loss."""

        return any(stage.lossy for stage in self.stages)

    @property
    def has_errors(self) -> bool:
        """Whether the operation contains an error issue."""

        return any(stage.has_errors for stage in self.stages)

    @property
    def error(self) -> bool:
        """Alias for :attr:`has_errors`, suitable for pipeline predicates."""

        return self.has_errors

    @property
    def succeeded(self) -> bool:
        """Whether the operation completed without reported errors."""

        return not self.has_errors

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-compatible representation with a schema version."""

        return {
            "schema_version": self.schema_version,
            "operation": self.operation.value,
            "stages": [stage.to_dict() for stage in self.stages],
            "metadata": _thaw_json(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "OperationReport":
        """Construct an operation report from :meth:`to_dict` output."""

        return cls(
            operation=OperationKind(data["operation"]),
            stages=tuple(StageReport.from_dict(item) for item in data.get("stages", [])),
            metadata=data.get("metadata", {}),
            schema_version=int(data.get("schema_version", REPORT_SCHEMA_VERSION)),
        )

    def to_json(self, *, indent: Optional[int] = None) -> str:
        """Serialize this report to deterministic UTF-8 JSON text."""

        separators = None if indent is not None else (",", ":")
        return json.dumps(
            self.to_dict(),
            ensure_ascii=False,
            allow_nan=False,
            indent=indent,
            separators=separators,
            sort_keys=True,
        )

    @classmethod
    def from_json(cls, payload: str) -> "OperationReport":
        """Deserialize a report produced by :meth:`to_json`."""

        data = json.loads(payload)
        if not isinstance(data, dict):
            raise TypeError("An operation report JSON document must contain an object.")
        return cls.from_dict(data)


T = TypeVar("T")


@dataclass(frozen=True)
class OperationResult(Generic[T]):
    """Pair a pipeline value with the immutable report that produced it."""

    value: T
    report: OperationReport

    @property
    def changed(self) -> bool:
        """Whether the operation records any transformation."""

        return self.report.changed

    @property
    def lossy(self) -> bool:
        """Whether the operation records any information loss."""

        return self.report.lossy

    @property
    def error(self) -> bool:
        """Whether the operation contains any error issue."""

        return self.report.error

    @property
    def succeeded(self) -> bool:
        """Whether the operation completed without reported errors."""

        return self.report.succeeded

    def to_dict(self, value_encoder: Optional[Callable[[T], Any]] = None) -> Dict[str, Any]:
        """Return a JSON-compatible result representation.

        :param value_encoder: Optional function for values that are not already
            JSON-compatible. The encoded value is validated and copied.
        """

        encoded = value_encoder(self.value) if value_encoder is not None else self.value
        return {
            "value": _thaw_json(_freeze_json(encoded, location="operation result value")),
            "report": self.report.to_dict(),
        }

    def to_json(
        self,
        value_encoder: Optional[Callable[[T], Any]] = None,
        *,
        indent: Optional[int] = None,
    ) -> str:
        """Serialize this result to deterministic UTF-8 JSON text."""

        separators = None if indent is not None else (",", ":")
        return json.dumps(
            self.to_dict(value_encoder),
            ensure_ascii=False,
            allow_nan=False,
            indent=indent,
            separators=separators,
            sort_keys=True,
        )

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        value_decoder: Optional[Callable[[Any], T]] = None,
    ) -> "OperationResult[T]":
        """Construct a result from :meth:`to_dict` output."""

        raw_value = data["value"]
        value = value_decoder(raw_value) if value_decoder is not None else raw_value
        return cls(value=value, report=OperationReport.from_dict(data["report"]))

    @classmethod
    def from_json(
        cls,
        payload: str,
        value_decoder: Optional[Callable[[Any], T]] = None,
    ) -> "OperationResult[T]":
        """Deserialize a result produced by :meth:`to_json`."""

        data = json.loads(payload)
        if not isinstance(data, dict):
            raise TypeError("An operation result JSON document must contain an object.")
        return cls.from_dict(data, value_decoder)
