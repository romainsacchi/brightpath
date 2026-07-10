"""BrightPath exception hierarchy with structured operation reports."""

from __future__ import annotations

from typing import Any, Iterable

from .core.reports import (
    Issue,
    OperationKind,
    OperationReport,
    Severity,
    StageKind,
    StageReport,
)


class BrightPathError(Exception):
    """Base exception for BrightPath domain errors.

    :param message: Human-readable failure summary.
    :param report: Optional immutable machine-readable operation report.
    """

    def __init__(self, message: str, *, report: OperationReport | None = None) -> None:
        self.report = report
        super().__init__(message)


class OperationError(BrightPathError):
    """Base class for failures produced by a reported pipeline operation."""


class FormatDetectionError(OperationError, ValueError):
    """Raised when source format detection is absent or ambiguous."""


class InventoryValidationError(OperationError, ValueError):
    """Raised when inventory validation reports one or more errors.

    Legacy writer validation objects remain available through
    :attr:`legacy_report`. Upload-analysis failures retain their
    :attr:`result`. :attr:`report` is always the immutable shared report type.
    """

    def __init__(
        self,
        validation: Any = None,
        *,
        result: Any = None,
        message: str | None = None,
        report: OperationReport | None = None,
    ) -> None:
        if result is None and _looks_like_analysis_result(validation):
            result = validation
            validation = None
        self.result = result
        self.legacy_report = validation
        operation_report = report or _validation_operation_report(validation, result)
        detail = message or _validation_message(operation_report)
        super().__init__(detail, report=operation_report)


class MigrationError(OperationError, ValueError):
    """Base exception for background migration failures."""

    def __init__(self, message: str, *, report: OperationReport | None = None) -> None:
        super().__init__(
            message,
            report=report or _single_error_report(OperationKind.MIGRATE, StageKind.BACKGROUND_MIGRATION, message),
        )


class MigrationUnavailableError(MigrationError):
    """Raised when no supported migration route exists."""


class ConversionError(OperationError, ValueError):
    """Raised when a format conversion cannot satisfy its policy."""

    def __init__(self, message: str, *, report: OperationReport | None = None) -> None:
        super().__init__(
            message,
            report=report or _single_error_report(OperationKind.CONVERT, StageKind.FORMAT_CONVERSION, message),
        )


class SerializationError(OperationError, ValueError):
    """Base class for format serialization failures."""

    format_code = "serialization.failed"

    def __init__(self, message: str, *, report: OperationReport | None = None) -> None:
        super().__init__(
            message,
            report=report
            or _single_error_report(
                OperationKind.WRITE,
                StageKind.SERIALIZATION,
                message,
                code=self.format_code,
            ),
        )


class ExcelSerializationError(SerializationError):
    """Raised when inventory values cannot be represented in Brightway Excel."""

    format_code = "brightway_excel.serialization_failed"


class SimaProSerializationError(SerializationError):
    """Raised when inventory values cannot be represented in SimaPro CSV."""

    format_code = "simapro_csv.serialization_failed"


def _validation_operation_report(validation: Any, result: Any) -> OperationReport:
    stages = []
    if validation is not None:
        stages.append(
            StageReport(
                StageKind.STRUCTURAL_VALIDATION,
                label="inventory validation",
                issues=tuple(_coerce_issues(getattr(validation, "issues", ()), StageKind.STRUCTURAL_VALIDATION)),
            )
        )
    if result is not None:
        file_issues = tuple(_coerce_issues(getattr(result, "file_issues", ()), StageKind.PARSE))
        candidate_issues = []
        for index, candidate in enumerate(getattr(result, "candidates", ())):
            for issue in _coerce_issues(getattr(candidate, "issues", ()), StageKind.STRUCTURAL_VALIDATION):
                if issue.path:
                    candidate_issues.append(issue)
                else:
                    candidate_issues.append(
                        Issue(
                            severity=issue.severity,
                            code=issue.code,
                            message=issue.message,
                            stage=issue.stage,
                            path=f"datasets[{index}]",
                            details=issue.details,
                            suggested_fix=issue.suggested_fix,
                        )
                    )
        if file_issues:
            stages.append(StageReport(StageKind.PARSE, label="input analysis", issues=file_issues))
        if candidate_issues:
            stages.append(
                StageReport(
                    StageKind.STRUCTURAL_VALIDATION,
                    label="candidate validation",
                    issues=tuple(candidate_issues),
                )
            )
    if not stages:
        stages.append(
            StageReport(
                StageKind.STRUCTURAL_VALIDATION,
                issues=(
                    Issue(
                        Severity.ERROR,
                        "validation.failed",
                        "Inventory validation failed.",
                        StageKind.STRUCTURAL_VALIDATION,
                    ),
                ),
            )
        )
    return OperationReport(OperationKind.VALIDATE, stages=tuple(stages))


def _coerce_issues(values: Iterable[Any], stage: StageKind) -> Iterable[Issue]:
    for value in values:
        if isinstance(value, Issue):
            if value.stage == stage:
                yield value
            else:
                yield Issue(
                    value.severity,
                    value.code,
                    value.message,
                    stage,
                    path=value.path,
                    details=value.details,
                    suggested_fix=value.suggested_fix,
                )
            continue
        yield Issue(
            severity=Severity(str(getattr(value, "severity", "error"))),
            code=str(getattr(value, "code", "validation.failed")),
            message=str(getattr(value, "message", value)),
            stage=stage,
            path=str(getattr(value, "path", "")),
            suggested_fix=str(getattr(value, "suggested_fix", "")),
        )


def _validation_message(report: OperationReport) -> str:
    messages = [issue.message for issue in report.issues if issue.severity is Severity.ERROR]
    return "Inventory validation failed:\n{}".format("\n".join(messages) or "Unknown inventory validation error.")


def _single_error_report(
    operation: OperationKind,
    stage: StageKind,
    message: str,
    *,
    code: str = "operation.failed",
) -> OperationReport:
    return OperationReport(
        operation,
        stages=(
            StageReport(
                stage,
                issues=(Issue(Severity.ERROR, code, message, stage),),
            ),
        ),
    )


def _looks_like_analysis_result(value: Any) -> bool:
    return value is not None and hasattr(value, "file_issues") and hasattr(value, "candidates")
