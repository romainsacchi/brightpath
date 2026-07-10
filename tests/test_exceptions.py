import pytest

from brightpath.core import OperationKind, Severity, StageKind
from brightpath.exceptions import (
    BrightPathError,
    ExcelSerializationError,
    InventoryValidationError,
    MigrationUnavailableError,
)
from brightpath.models import AnalysisResult, BackgroundProfile, Issue, ValidationReport


def test_writer_validation_exception_projects_legacy_issues_to_immutable_report():
    legacy = ValidationReport(
        BackgroundProfile("ecoinvent", "3.10.1", "cutoff"),
        issues=[Issue("error", "invalid", "Invalid exchange.", path="activity[0]")],
    )

    error = InventoryValidationError(legacy)

    assert isinstance(error, BrightPathError)
    assert error.legacy_report is legacy
    assert error.report.operation is OperationKind.VALIDATE
    assert error.report.issues[0].severity is Severity.ERROR
    assert error.report.issues[0].path == "activity[0]"
    assert "Invalid exchange" in str(error)


def test_analysis_validation_exception_retains_result_and_shared_report():
    result = AnalysisResult(
        detected_software="brightway",
        detected_format="brightway_excel",
        file_issues=[Issue("error", "parse", "Could not parse file.")],
    )

    error = InventoryValidationError(result=result)

    assert error.result is result
    assert error.report.stages[0].stage is StageKind.PARSE
    assert error.report.error


@pytest.mark.parametrize(
    "error",
    [
        ExcelSerializationError("Cannot serialize value."),
        MigrationUnavailableError("No route."),
    ],
)
def test_operation_failures_include_a_structured_error_report(error):
    assert error.report.error
    assert len(error.report.issues) == 1
