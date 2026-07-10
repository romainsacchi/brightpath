"""Command-line interface for explicit BrightPath inventory operations."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Callable, Iterable, Sequence

from brightpath.adapters import default_adapter_registry
from brightpath.background.catalogs import CatalogProvider, catalog_provider_from_environment
from brightpath.background.execution import execute_background_migration
from brightpath.capabilities import capability_snapshot
from brightpath.core.audit import ArtifactDigest, digest_artifact, write_report_sidecar
from brightpath.core.context import (
    BackgroundContext,
    BiosphereProfile,
    ContextHint,
    FormatProfile,
    TechnosphereProfile,
)
from brightpath.core.pipeline import InventoryPipeline
from brightpath.core.policies import ConversionPolicy, MigrationPolicy
from brightpath.core.reports import (
    OperationKind,
    OperationReport,
    Severity,
    StageKind,
    StageReport,
)

EXIT_SUCCESS = 0
EXIT_USAGE = 2
EXIT_READ = 3
EXIT_VALIDATION = 4
EXIT_MIGRATION = 5
EXIT_CONVERSION = 6


class _UsageError(ValueError):
    """Invalid combination of otherwise syntactically valid CLI options."""


def build_parser() -> argparse.ArgumentParser:
    """Build the BrightPath argument parser."""

    parser = argparse.ArgumentParser(
        prog="brightpath",
        description="Inspect, validate, migrate, and convert foreground LCA inventories.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    formats = subparsers.add_parser("formats", help="List supported formats, migrations, and catalogs.")
    _add_common_options(formats)

    inspect = subparsers.add_parser("inspect", help="Read an inventory and show its resolved context.")
    inspect.add_argument("source", type=Path)
    _add_source_context_options(inspect)
    _add_common_options(inspect)

    validate = subparsers.add_parser("validate", help="Validate inventory structure and background links.")
    validate.add_argument("source", type=Path)
    _add_source_context_options(validate)
    _add_common_options(validate)

    convert = subparsers.add_parser("convert-format", help="Convert an inventory to an explicit software format.")
    convert.add_argument("source", type=Path)
    convert.add_argument("destination", type=Path)
    convert.add_argument("--target-format", required=True, help="Registered target format identifier.")
    convert.add_argument("--dry-run", action="store_true", help="Run conversion preflight without writing.")
    _add_source_context_options(convert)
    _add_common_options(convert)

    migrate = subparsers.add_parser(
        "migrate-background",
        help="Migrate background links while preserving the software format.",
    )
    migrate.add_argument("source", type=Path)
    migrate.add_argument("destination", type=Path)
    migrate.add_argument("--target-technosphere-family", required=True)
    migrate.add_argument("--target-technosphere-version", required=True)
    migrate.add_argument("--target-technosphere-system-model", required=True)
    migrate.add_argument("--target-biosphere-family", required=True)
    migrate.add_argument("--target-biosphere-version", required=True)
    migrate.add_argument("--dry-run", action="store_true", help="Run migration in memory without writing.")
    _add_source_context_options(migrate)
    _add_common_options(migrate)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the BrightPath CLI and return a stable process exit code."""

    parser = build_parser()
    try:
        arguments = parser.parse_args(argv)
    except SystemExit as error:
        return int(error.code or 0)

    try:
        if arguments.command == "formats":
            return _run_formats(arguments)
        if arguments.command == "inspect":
            return _run_inspect(arguments)
        if arguments.command == "validate":
            return _run_validate(arguments)
        if arguments.command == "convert-format":
            return _run_convert(arguments)
        if arguments.command == "migrate-background":
            return _run_migrate(arguments)
    except _UsageError as error:
        _emit_runtime_error(arguments, str(error), EXIT_USAGE)
        return EXIT_USAGE
    except Exception as error:  # pragma: no cover - defensive application boundary
        _emit_runtime_error(arguments, str(error) or type(error).__name__, EXIT_CONVERSION)
        return EXIT_CONVERSION
    raise AssertionError(f"Unhandled command: {arguments.command}")


def _add_common_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--json", action="store_true", dest="json_output", help="Write machine-readable JSON.")
    parser.add_argument("--report", type=Path, help="Write an atomic JSON audit sidecar.")
    parser.add_argument(
        "--policy",
        choices=("strict", "permissive"),
        default="strict",
        help="Policy for unsafe conversion or migration conditions.",
    )


def _add_source_context_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--source-format", help="Explicit registered source format identifier.")
    parser.add_argument("--source-technosphere-family")
    parser.add_argument("--source-technosphere-version")
    parser.add_argument("--source-technosphere-system-model")
    parser.add_argument("--source-biosphere-family")
    parser.add_argument("--source-biosphere-version")


def _run_formats(arguments: argparse.Namespace) -> int:
    snapshot = capability_snapshot()
    report = OperationReport(
        OperationKind.ANALYZE,
        stages=(
            StageReport(
                StageKind.FORMAT_DETECTION,
                label="capability discovery",
                metrics={
                    "formats": len(snapshot["formats"]),
                    "migrations": len(snapshot["migrations"]),
                    "technosphere_catalogs": len(snapshot["catalogs"]["technosphere"]),
                    "biosphere_catalogs": len(snapshot["catalogs"]["biosphere"]),
                },
            ),
        ),
        metadata={"command": "formats"},
    )
    payload = {"command": "formats", "success": True, "capabilities": snapshot, "report": report.to_dict()}
    return _finish(arguments, payload, report, EXIT_SUCCESS, human_renderer=lambda: _render_capabilities(snapshot))


def _run_inspect(arguments: argparse.Namespace) -> int:
    hint = _source_hint(arguments)
    pipeline, _provider = _application_services()
    read_result = pipeline.read(arguments.source, hint=hint)
    reports = (("read", read_result.report),)
    report = _combine_reports(OperationKind.READ, reports, arguments)
    document = read_result.value
    payload = _operation_payload("inspect", reports, _document_summary(document) if document is not None else None)
    exit_code = EXIT_SUCCESS if document is not None and read_result.succeeded else EXIT_READ
    return _finish(arguments, payload, report, exit_code, artifact_paths=(("source", arguments.source),))


def _run_validate(arguments: argparse.Namespace) -> int:
    hint = _source_hint(arguments)
    pipeline, _provider = _application_services()
    read_result = pipeline.read(arguments.source, hint=hint)
    if read_result.value is None or not read_result.succeeded:
        reports = (("read", read_result.report),)
        report = _combine_reports(OperationKind.VALIDATE, reports, arguments)
        payload = _operation_payload("validate", reports, None)
        return _finish(arguments, payload, report, EXIT_READ, artifact_paths=(("source", arguments.source),))

    validation_result = pipeline.validate(read_result.value)
    reports = (("read", read_result.report), ("validation", validation_result.report))
    report = _combine_reports(OperationKind.VALIDATE, reports, arguments)
    payload = _operation_payload("validate", reports, _document_summary(validation_result.value))
    exit_code = EXIT_VALIDATION if validation_result.error else EXIT_SUCCESS
    return _finish(arguments, payload, report, exit_code, artifact_paths=(("source", arguments.source),))


def _run_convert(arguments: argparse.Namespace) -> int:
    hint = _source_hint(arguments)
    conversion_policy = _conversion_policy(arguments.policy)
    pipeline, _provider = _application_services()
    read_result = pipeline.read(arguments.source, hint=hint)
    if read_result.value is None or not read_result.succeeded:
        reports = (("read", read_result.report),)
        report = _combine_reports(OperationKind.CONVERT, reports, arguments)
        payload = _operation_payload("convert-format", reports, None, dry_run=arguments.dry_run)
        return _finish(arguments, payload, report, EXIT_READ, artifact_paths=(("source", arguments.source),))

    conversion_result = pipeline.convert(
        read_result.value,
        arguments.target_format,
        policy=conversion_policy,
    )
    reports: tuple[tuple[str, OperationReport], ...] = (
        ("read", read_result.report),
        ("conversion", conversion_result.report),
    )
    if conversion_result.value is None or not conversion_result.succeeded:
        report = _combine_reports(OperationKind.CONVERT, reports, arguments)
        payload = _operation_payload("convert-format", reports, None, dry_run=arguments.dry_run)
        return _finish(arguments, payload, report, EXIT_CONVERSION, artifact_paths=(("source", arguments.source),))

    if arguments.dry_run:
        report = _combine_reports(OperationKind.CONVERT, reports, arguments)
        payload = _operation_payload(
            "convert-format",
            reports,
            _document_summary(conversion_result.value),
            dry_run=True,
        )
        return _finish(arguments, payload, report, EXIT_SUCCESS, artifact_paths=(("source", arguments.source),))

    write_result = pipeline.write(
        conversion_result.value,
        arguments.destination,
        target_format=arguments.target_format,
        policy=conversion_policy,
    )
    reports += (("write", write_result.report),)
    report = _combine_reports(OperationKind.CONVERT, reports, arguments)
    payload = _operation_payload(
        "convert-format",
        reports,
        _document_summary(conversion_result.value),
        output=str(write_result.value) if write_result.value is not None else None,
        dry_run=False,
    )
    exit_code = EXIT_SUCCESS if write_result.value is not None and write_result.succeeded else EXIT_CONVERSION
    output_artifact = Path(write_result.value) if write_result.value is not None else arguments.destination
    artifacts = (("source", arguments.source), ("output", output_artifact))
    return _finish(arguments, payload, report, exit_code, artifact_paths=artifacts)


def _run_migrate(arguments: argparse.Namespace) -> int:
    hint = _source_hint(arguments)
    target = _target_background(arguments)
    migration_policy = _migration_policy(arguments.policy)
    pipeline, provider = _application_services()
    read_result = pipeline.read(arguments.source, hint=hint)
    if read_result.value is None or not read_result.succeeded:
        reports = (("read", read_result.report),)
        report = _combine_reports(OperationKind.MIGRATE, reports, arguments)
        payload = _operation_payload("migrate-background", reports, None, dry_run=arguments.dry_run)
        return _finish(arguments, payload, report, EXIT_READ, artifact_paths=(("source", arguments.source),))

    migration_result = execute_background_migration(
        read_result.value,
        target,
        provider,
        policy=migration_policy,
    )
    reports: tuple[tuple[str, OperationReport], ...] = (
        ("read", read_result.report),
        ("migration", migration_result.report),
    )
    if not migration_result.succeeded:
        report = _combine_reports(OperationKind.MIGRATE, reports, arguments)
        payload = _operation_payload(
            "migrate-background",
            reports,
            _document_summary(migration_result.value),
            dry_run=arguments.dry_run,
        )
        return _finish(arguments, payload, report, EXIT_MIGRATION, artifact_paths=(("source", arguments.source),))

    if arguments.dry_run:
        report = _combine_reports(OperationKind.MIGRATE, reports, arguments)
        payload = _operation_payload(
            "migrate-background",
            reports,
            _document_summary(migration_result.value),
            dry_run=True,
        )
        return _finish(arguments, payload, report, EXIT_SUCCESS, artifact_paths=(("source", arguments.source),))

    write_result = pipeline.write(
        migration_result.value,
        arguments.destination,
        policy=_conversion_policy(arguments.policy),
    )
    reports += (("write", write_result.report),)
    report = _combine_reports(OperationKind.MIGRATE, reports, arguments)
    payload = _operation_payload(
        "migrate-background",
        reports,
        _document_summary(migration_result.value),
        output=str(write_result.value) if write_result.value is not None else None,
        dry_run=False,
    )
    exit_code = EXIT_SUCCESS if write_result.value is not None and write_result.succeeded else EXIT_CONVERSION
    output_artifact = Path(write_result.value) if write_result.value is not None else arguments.destination
    artifacts = (("source", arguments.source), ("output", output_artifact))
    return _finish(arguments, payload, report, exit_code, artifact_paths=artifacts)


def _application_services() -> tuple[InventoryPipeline, CatalogProvider]:
    provider = catalog_provider_from_environment()
    return InventoryPipeline(default_adapter_registry(), provider), provider


def _source_hint(arguments: argparse.Namespace) -> ContextHint:
    values = {
        "--source-technosphere-family": arguments.source_technosphere_family,
        "--source-technosphere-version": arguments.source_technosphere_version,
        "--source-technosphere-system-model": arguments.source_technosphere_system_model,
        "--source-biosphere-family": arguments.source_biosphere_family,
        "--source-biosphere-version": arguments.source_biosphere_version,
    }
    supplied = [name for name, value in values.items() if value]
    missing = [name for name, value in values.items() if not value]
    if supplied and missing:
        raise _UsageError(
            "An explicit source background requires every technosphere and biosphere axis; missing {}.".format(
                ", ".join(missing)
            )
        )
    if not supplied:
        try:
            return ContextHint(format=FormatProfile(arguments.source_format) if arguments.source_format else None)
        except (TypeError, ValueError) as error:
            raise _UsageError(str(error)) from error
    try:
        return ContextHint(
            format=FormatProfile(arguments.source_format) if arguments.source_format else None,
            background=BackgroundContext(
                technosphere=TechnosphereProfile(
                    family=values["--source-technosphere-family"],
                    version=values["--source-technosphere-version"],
                    system_model=values["--source-technosphere-system-model"],
                ),
                biosphere=BiosphereProfile(
                    family=values["--source-biosphere-family"],
                    version=values["--source-biosphere-version"],
                ),
            ),
        )
    except (TypeError, ValueError) as error:
        raise _UsageError(str(error)) from error


def _target_background(arguments: argparse.Namespace) -> BackgroundContext:
    try:
        return BackgroundContext(
            technosphere=TechnosphereProfile(
                family=arguments.target_technosphere_family,
                version=arguments.target_technosphere_version,
                system_model=arguments.target_technosphere_system_model,
            ),
            biosphere=BiosphereProfile(
                family=arguments.target_biosphere_family,
                version=arguments.target_biosphere_version,
            ),
        )
    except (TypeError, ValueError) as error:
        raise _UsageError(str(error)) from error


def _conversion_policy(name: str) -> ConversionPolicy:
    return ConversionPolicy.strict() if name == "strict" else ConversionPolicy.permissive()


def _migration_policy(name: str) -> MigrationPolicy:
    return MigrationPolicy.strict() if name == "strict" else MigrationPolicy.permissive()


def _combine_reports(
    operation: OperationKind,
    reports: Iterable[tuple[str, OperationReport]],
    arguments: argparse.Namespace,
) -> OperationReport:
    values = tuple(reports)
    return OperationReport(
        operation,
        stages=tuple(stage for _name, report in values for stage in report.stages),
        metadata={
            "command": arguments.command,
            "policy": arguments.policy,
            "dry_run": bool(getattr(arguments, "dry_run", False)),
        },
    )


def _operation_payload(
    command: str,
    reports: Iterable[tuple[str, OperationReport]],
    inventory: dict | None,
    *,
    output: str | None = None,
    dry_run: bool = False,
) -> dict:
    values = tuple(reports)
    payload = {
        "command": command,
        "success": all(report.succeeded for _name, report in values),
        "dry_run": dry_run,
        "inventory": inventory,
        "reports": {name: report.to_dict() for name, report in values},
    }
    if output is not None:
        payload["output"] = output
    return payload


def _document_summary(document: object) -> dict:
    data = document.data
    context = document.context
    return {
        "database_name": document.database_name,
        "datasets": len(data),
        "exchanges": sum(len(dataset.get("exchanges", ())) for dataset in data),
        "context": {
            "format": {
                "format_id": context.format.format_id,
                "format_version": context.format.format_version,
                "dialect": context.format.dialect,
                "encoding": context.format.encoding,
            },
            "technosphere": {
                "family": context.background.technosphere.family,
                "version": context.background.technosphere.version,
                "system_model": context.background.technosphere.system_model,
            },
            "biosphere": {
                "family": context.background.biosphere.family,
                "version": context.background.biosphere.version,
            },
        },
    }


def _finish(
    arguments: argparse.Namespace,
    payload: dict,
    report: OperationReport,
    exit_code: int,
    *,
    artifact_paths: Iterable[tuple[str, Path]] = (),
    human_renderer: Callable[[], None] | None = None,
) -> int:
    if exit_code != EXIT_SUCCESS:
        payload["success"] = False
    if arguments.report is not None:
        try:
            artifacts = _artifact_digests(artifact_paths)
            write_report_sidecar(report, arguments.report, artifacts=artifacts)
            payload["report_path"] = str(arguments.report.expanduser().resolve())
        except Exception as error:
            payload["success"] = False
            payload["report_error"] = str(error) or type(error).__name__
            exit_code = EXIT_CONVERSION

    if arguments.json_output:
        print(json.dumps(payload, ensure_ascii=False, allow_nan=False, indent=2, sort_keys=True))
    elif human_renderer is not None:
        human_renderer()
        if payload.get("report_path"):
            print(f"Report: {payload['report_path']}")
        if payload.get("report_error"):
            print(f"Report error: {payload['report_error']}", file=sys.stderr)
    else:
        _render_operation(payload)
    return exit_code


def _artifact_digests(paths: Iterable[tuple[str, Path]]) -> tuple[ArtifactDigest, ...]:
    digests = []
    for role, path in paths:
        candidate = path.expanduser()
        if candidate.is_file():
            digests.append(digest_artifact(candidate, role=role))
    return tuple(digests)


def _render_operation(payload: dict) -> None:
    status = "success" if payload["success"] else "failed"
    print(f"{payload['command']}: {status}")
    if payload.get("dry_run"):
        print("Mode: dry-run")
    inventory = payload.get("inventory")
    if inventory:
        context = inventory["context"]
        print(f"Datasets: {inventory['datasets']} ({inventory['exchanges']} exchanges)")
        print(f"Format: {context['format']['format_id']}")
        technosphere = context["technosphere"]
        print(
            "Technosphere: {family} {version} {system_model}".format(
                family=technosphere["family"],
                version=technosphere["version"],
                system_model=technosphere["system_model"],
            )
        )
        biosphere = context["biosphere"]
        print(f"Biosphere: {biosphere['family']} {biosphere['version']}")
    if payload.get("output"):
        print(f"Output: {payload['output']}")
    for name, report in payload.get("reports", {}).items():
        issues = [issue for stage in report["stages"] for issue in stage["issues"]]
        errors = sum(issue["severity"] == Severity.ERROR.value for issue in issues)
        warnings = sum(issue["severity"] == Severity.WARNING.value for issue in issues)
        print(f"{name}: {errors} error(s), {warnings} warning(s)")
        for issue in issues:
            location = f" ({issue['path']})" if issue["path"] else ""
            print(f"  [{issue['severity']}] {issue['code']}{location}: {issue['message']}")
    if payload.get("report_path"):
        print(f"Report: {payload['report_path']}")
    if payload.get("report_error"):
        print(f"Report error: {payload['report_error']}", file=sys.stderr)


def _render_capabilities(snapshot: dict) -> None:
    print("Formats")
    rows = [
        (
            item["format_id"],
            ",".join(item["read"]) or "-",
            ",".join(item["write"]) or "-",
            ",".join(item["detect"]) or "-",
        )
        for item in snapshot["formats"]
    ]
    _render_table(("FORMAT", "READ", "WRITE", "DETECT"), rows)

    print("\nMigrations")
    rows = [
        (
            item["axis"],
            item["family"],
            item["system_model"] or "-",
            f"{item['source_series']} -> {item['target_series']}",
        )
        for item in snapshot["migrations"]
    ]
    _render_table(("AXIS", "FAMILY", "MODEL", "SERIES"), rows)

    print("\nCatalogs")
    technosphere = snapshot["catalogs"]["technosphere"]
    biosphere = snapshot["catalogs"]["biosphere"]
    print(f"Technosphere: {len(technosphere)}")
    print(f"Biosphere: {len(biosphere)}")


def _render_table(headers: tuple[str, ...], rows: Iterable[tuple[str, ...]]) -> None:
    values = tuple(rows)
    widths = [len(header) for header in headers]
    for row in values:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], len(value))
    template = "  ".join(f"{{:{width}}}" for width in widths)
    print(template.format(*headers))
    print(template.format(*(width * "-" for width in widths)))
    for row in values:
        print(template.format(*row))


def _emit_runtime_error(arguments: argparse.Namespace, message: str, exit_code: int) -> None:
    payload = {
        "command": getattr(arguments, "command", ""),
        "success": False,
        "exit_code": exit_code,
        "error": message,
    }
    if getattr(arguments, "json_output", False):
        print(json.dumps(payload, ensure_ascii=False, allow_nan=False, indent=2, sort_keys=True))
    else:
        print(f"brightpath: error: {message}", file=sys.stderr)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
