from dataclasses import FrozenInstanceError

import pytest

from brightpath.core.policies import ConversionPolicy, MigrationPolicy, PolicyAction
from brightpath.core.reports import (
    Change,
    Issue,
    Loss,
    OperationKind,
    OperationReport,
    OperationResult,
    Severity,
    StageKind,
    StageReport,
)


def test_issue_copies_and_deeply_freezes_details():
    details = {"z": [{"flow": "Carbon dioxide"}], "count": 1}
    issue = Issue(
        severity="warning",
        code="background.unresolved",
        message="A background link could not be resolved.",
        stage="background_validation",
        path="datasets[0].exchanges[1]",
        details=details,
    )

    details["z"][0]["flow"] = "changed by caller"

    assert issue.severity is Severity.WARNING
    assert issue.stage is StageKind.BACKGROUND_VALIDATION
    assert issue.details["z"][0]["flow"] == "Carbon dioxide"
    with pytest.raises(TypeError):
        issue.details["new"] = "value"
    with pytest.raises(TypeError):
        issue.details["z"][0]["flow"] = "value"


def test_stage_report_sorts_findings_without_mutating_input_lists():
    issues = [
        Issue(Severity.INFO, "z", "last", StageKind.PARSE, path="datasets[1]"),
        Issue(Severity.ERROR, "b", "second", StageKind.PARSE, path="datasets[0]"),
        Issue(Severity.ERROR, "a", "first", StageKind.PARSE, path="datasets[0]"),
    ]
    original = list(issues)
    report = StageReport(stage=StageKind.PARSE, issues=issues)

    assert issues == original
    assert [issue.code for issue in report.issues] == ["a", "b", "z"]
    with pytest.raises(FrozenInstanceError):
        report.stage = StageKind.WRITE


def test_stage_report_rejects_findings_from_another_stage():
    issue = Issue(Severity.ERROR, "parse.invalid", "Invalid input.", StageKind.PARSE)

    with pytest.raises(ValueError, match="does not match"):
        StageReport(stage=StageKind.WRITE, issues=(issue,))


def test_operation_report_aggregates_status_and_round_trips_deterministically():
    parse = StageReport(
        stage=StageKind.PARSE,
        label="source",
        issues=(Issue(Severity.WARNING, "parse.encoding", "Converted Zürich text.", StageKind.PARSE),),
        metrics={"datasets": 2, "nested": {"exchanges": [1, 3]}},
    )
    conversion = StageReport(
        stage=StageKind.FORMAT_CONVERSION,
        changes=(
            Change(
                "format.name",
                "Rendered a target name.",
                StageKind.FORMAT_CONVERSION,
                path="datasets[0].name",
                before="market for electricity",
                after="Electricity, medium voltage {CH}",
            ),
        ),
        losses=(
            Loss(
                "format.metadata",
                "Target format cannot represent one extension.",
                StageKind.FORMAT_CONVERSION,
                details={"field": "custom"},
            ),
        ),
    )
    report = OperationReport(
        operation=OperationKind.CONVERT,
        stages=(parse, conversion),
        metadata={"target": "simapro_csv", "source": "brightway_excel"},
    )

    first = report.to_json()
    second = report.to_json()
    restored = OperationReport.from_json(first)

    assert first == second
    assert restored == report
    assert restored.to_json() == first
    assert report.changed
    assert report.lossy
    assert not report.error
    assert report.succeeded
    assert len(report.issues) == 1
    assert len(report.changes) == 1
    assert len(report.losses) == 1


def test_loss_alone_marks_a_stage_and_operation_as_changed():
    stage = StageReport(
        StageKind.FORMAT_CONVERSION,
        losses=(Loss("field.dropped", "A field was dropped.", StageKind.FORMAT_CONVERSION),),
    )
    report = OperationReport(OperationKind.CONVERT, stages=(stage,))

    assert stage.changed
    assert report.changed
    assert report.lossy


def test_operation_report_orders_issues_across_stage_input_order():
    write = StageReport(
        StageKind.WRITE,
        issues=(Issue(Severity.ERROR, "write.failed", "Write failed.", StageKind.WRITE),),
    )
    parse = StageReport(
        StageKind.PARSE,
        issues=(Issue(Severity.WARNING, "parse.warning", "Parse warning.", StageKind.PARSE),),
    )
    report = OperationReport(OperationKind.WRITE, stages=(write, parse))

    assert [issue.code for issue in report.issues] == ["parse.warning", "write.failed"]
    assert report.error
    assert not report.succeeded


def test_operation_result_serializes_generic_json_values():
    report = OperationReport(OperationKind.READ, stages=(StageReport(StageKind.PARSE),))
    result = OperationResult(value={"datasets": [1, 2]}, report=report)

    restored = OperationResult.from_json(result.to_json())

    assert restored.value == result.value
    assert restored.report == report
    assert restored.succeeded
    assert not restored.changed
    assert not restored.lossy


def test_operation_result_supports_value_codec():
    report = OperationReport(OperationKind.READ)
    result = OperationResult(value=("inventory", 3), report=report)

    payload = result.to_json(lambda value: {"name": value[0], "count": value[1]})
    restored = OperationResult.from_json(payload, lambda value: (value["name"], value["count"]))

    assert restored == result


@pytest.mark.parametrize("bad_value", [float("nan"), float("inf"), {"not-json"}])
def test_reports_reject_non_deterministic_or_non_json_values(bad_value):
    with pytest.raises((TypeError, ValueError)):
        OperationReport(OperationKind.READ, metadata={"bad": bad_value})


def test_reports_reject_non_string_metadata_keys_consistently():
    with pytest.raises(TypeError, match="string object keys"):
        OperationReport(OperationKind.READ, metadata={"valid": 1, 2: "invalid"})


def test_reports_reject_unknown_schema_version():
    with pytest.raises(ValueError, match="schema version"):
        OperationReport(OperationKind.READ, schema_version=2)


def test_policy_factories_are_explicit_and_immutable():
    conversion_strict = ConversionPolicy.strict()
    conversion_permissive = ConversionPolicy.permissive()
    migration_strict = MigrationPolicy.strict()
    migration_permissive = MigrationPolicy.permissive()

    assert conversion_strict.on_information_loss is PolicyAction.ERROR
    assert conversion_permissive.on_information_loss is PolicyAction.WARN
    assert migration_strict.on_unresolved_link is PolicyAction.ERROR
    assert migration_strict.minimum_coverage == 1.0
    assert migration_permissive.on_unresolved_link is PolicyAction.WARN
    assert migration_permissive.minimum_coverage == 0.0
    with pytest.raises(FrozenInstanceError):
        conversion_strict.validate_target = False


def test_policies_coerce_actions_and_round_trip_snapshots():
    conversion = ConversionPolicy(on_information_loss="allow", validate_target=False)
    migration = MigrationPolicy(on_deletion="warn", minimum_coverage=0.75)

    assert conversion.on_information_loss is PolicyAction.ALLOW
    assert ConversionPolicy.from_dict(conversion.to_dict()) == conversion
    assert MigrationPolicy.from_dict(migration.to_dict()) == migration
    assert ConversionPolicy.from_json(conversion.to_json()) == conversion
    assert MigrationPolicy.from_json(migration.to_json()) == migration


@pytest.mark.parametrize("coverage", [-0.1, 1.1])
def test_migration_policy_rejects_out_of_range_coverage(coverage):
    with pytest.raises(ValueError, match="between 0 and 1"):
        MigrationPolicy(minimum_coverage=coverage)


def test_policy_from_dict_rejects_unknown_fields():
    with pytest.raises(ValueError, match="Unknown ConversionPolicy fields"):
        ConversionPolicy.from_dict({"future_option": "allow"})


def test_policies_reject_non_boolean_validation_flags():
    with pytest.raises(TypeError, match="validate_target must be a boolean"):
        ConversionPolicy(validate_target="yes")
