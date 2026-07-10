import json
from pathlib import Path

import pytest

from brightpath import cli
from brightpath.core.context import (
    BackgroundContext,
    BiosphereProfile,
    FormatProfile,
    InventoryContext,
    TechnosphereProfile,
)
from brightpath.core.reports import (
    Issue,
    OperationKind,
    OperationReport,
    OperationResult,
    Severity,
    StageKind,
    StageReport,
)
from brightpath.models import InventoryDocument


def _document(format_id="brightway_excel", version="3.10"):
    context = InventoryContext(
        format=FormatProfile(format_id),
        background=BackgroundContext(
            TechnosphereProfile("ecoinvent", version, "cutoff"),
            BiosphereProfile("ecoinvent", version),
        ),
    )
    return InventoryDocument(
        data=[
            {
                "name": "foreground process",
                "reference product": "foreground product",
                "location": "GLO",
                "unit": "kilogram",
                "code": "foreground",
                "exchanges": [],
            }
        ],
        context=context,
        database_name="foreground",
    )


def _report(operation, stage=StageKind.PARSE, *, error=False):
    issues = ()
    if error:
        issues = (Issue(Severity.ERROR, "test.failed", "Test operation failed.", stage),)
    return OperationReport(operation, stages=(StageReport(stage, issues=issues),))


class FakePipeline:
    def __init__(self, document=None):
        self.document = document or _document()
        self.calls = []
        self.read_result = OperationResult(self.document, _report(OperationKind.READ))
        self.validation_result = OperationResult(
            self.document,
            _report(OperationKind.VALIDATE, StageKind.STRUCTURAL_VALIDATION),
        )
        self.conversion_result = OperationResult(
            self.document,
            _report(OperationKind.CONVERT, StageKind.FORMAT_CONVERSION),
        )
        self.write_result = OperationResult(
            Path("output.xlsx"),
            _report(OperationKind.WRITE, StageKind.WRITE),
        )

    def read(self, source, **kwargs):
        self.calls.append(("read", source, kwargs))
        return self.read_result

    def validate(self, document, **kwargs):
        self.calls.append(("validate", document, kwargs))
        return self.validation_result

    def convert(self, document, target_format, **kwargs):
        self.calls.append(("convert", document, target_format, kwargs))
        return self.conversion_result

    def write(self, document, destination, **kwargs):
        self.calls.append(("write", document, destination, kwargs))
        return self.write_result


def _install_services(monkeypatch, pipeline):
    provider = object()
    monkeypatch.setattr(cli, "_application_services", lambda: (pipeline, provider))
    return provider


def test_formats_json_comes_from_capability_snapshot(capsys):
    exit_code = cli.main(["formats", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == cli.EXIT_SUCCESS
    assert payload["success"] is True
    assert {item["format_id"] for item in payload["capabilities"]["formats"]} == {
        "brightway_excel",
        "brightway_csv",
        "brightway_tsv",
        "simapro_csv",
    }


def test_inspect_builds_an_explicit_context_hint(monkeypatch, capsys, tmp_path):
    pipeline = FakePipeline()
    _install_services(monkeypatch, pipeline)
    source = tmp_path / "inventory.xlsx"

    exit_code = cli.main(
        [
            "inspect",
            str(source),
            "--source-format",
            "brightway_excel",
            "--source-technosphere-family",
            "ecoinvent",
            "--source-technosphere-version",
            "3.10.1",
            "--source-technosphere-system-model",
            "cutoff",
            "--source-biosphere-family",
            "ecoinvent",
            "--source-biosphere-version",
            "3.10.1",
            "--json",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    hint = pipeline.calls[0][2]["hint"]
    assert exit_code == cli.EXIT_SUCCESS
    assert hint.format.format_id == "brightway_excel"
    assert hint.background.technosphere.version == "3.10.1"
    assert hint.background.biosphere.version == "3.10.1"
    assert payload["inventory"]["datasets"] == 1


def test_convert_dry_run_preflights_without_writing(monkeypatch, capsys, tmp_path):
    converted = _document("simapro_csv")
    pipeline = FakePipeline()
    pipeline.conversion_result = OperationResult(
        converted,
        _report(OperationKind.CONVERT, StageKind.FORMAT_CONVERSION),
    )
    _install_services(monkeypatch, pipeline)
    destination = tmp_path / "destination.csv"

    exit_code = cli.main(
        [
            "convert-format",
            str(tmp_path / "source.xlsx"),
            str(destination),
            "--target-format",
            "simapro_csv",
            "--dry-run",
            "--json",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == cli.EXIT_SUCCESS
    assert [call[0] for call in pipeline.calls] == ["read", "convert"]
    assert payload["dry_run"] is True
    assert payload["inventory"]["context"]["format"]["format_id"] == "simapro_csv"
    assert not destination.exists()


def test_migration_dry_run_uses_every_explicit_target_axis(monkeypatch, capsys, tmp_path):
    pipeline = FakePipeline()
    provider = _install_services(monkeypatch, pipeline)
    migrated = _document(version="2025")
    captured = {}

    def fake_migrate(document, target, selected_provider, *, policy):
        captured.update(document=document, target=target, provider=selected_provider, policy=policy)
        return OperationResult(migrated, _report(OperationKind.MIGRATE, StageKind.BACKGROUND_MIGRATION))

    monkeypatch.setattr(cli, "execute_background_migration", fake_migrate)
    destination = tmp_path / "destination.xlsx"
    exit_code = cli.main(
        [
            "migrate-background",
            str(tmp_path / "source.xlsx"),
            str(destination),
            "--target-technosphere-family",
            "BAFU",
            "--target-technosphere-version",
            "2025",
            "--target-technosphere-system-model",
            "cutoff",
            "--target-biosphere-family",
            "BAFU",
            "--target-biosphere-version",
            "2025",
            "--policy",
            "permissive",
            "--dry-run",
            "--json",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == cli.EXIT_SUCCESS
    assert captured["provider"] is provider
    assert captured["target"].technosphere.family == "uvek"
    assert captured["target"].biosphere.family == "uvek"
    assert captured["policy"].minimum_coverage == 0.0
    assert [call[0] for call in pipeline.calls] == ["read"]
    assert payload["dry_run"] is True
    assert not destination.exists()


def test_validate_returns_validation_exit_code(monkeypatch, capsys, tmp_path):
    pipeline = FakePipeline()
    pipeline.validation_result = OperationResult(
        pipeline.document,
        _report(OperationKind.VALIDATE, StageKind.STRUCTURAL_VALIDATION, error=True),
    )
    _install_services(monkeypatch, pipeline)

    exit_code = cli.main(["validate", str(tmp_path / "source.xlsx"), "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == cli.EXIT_VALIDATION
    assert payload["success"] is False
    assert payload["reports"]["validation"]["stages"][0]["issues"][0]["code"] == "test.failed"


@pytest.mark.parametrize(
    ("command", "configured_result", "expected"),
    [
        ("inspect", "read", cli.EXIT_READ),
        ("convert-format", "conversion", cli.EXIT_CONVERSION),
    ],
)
def test_read_and_conversion_failures_have_stable_exit_codes(
    monkeypatch,
    capsys,
    tmp_path,
    command,
    configured_result,
    expected,
):
    pipeline = FakePipeline()
    if configured_result == "read":
        pipeline.read_result = OperationResult(None, _report(OperationKind.READ, error=True))
        arguments = [command, str(tmp_path / "source.xlsx"), "--json"]
    else:
        pipeline.conversion_result = OperationResult(
            None,
            _report(OperationKind.CONVERT, StageKind.FORMAT_CONVERSION, error=True),
        )
        arguments = [
            command,
            str(tmp_path / "source.xlsx"),
            str(tmp_path / "output.csv"),
            "--target-format",
            "simapro_csv",
            "--json",
        ]
    _install_services(monkeypatch, pipeline)

    exit_code = cli.main(arguments)

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == expected
    assert payload["success"] is False


def test_failed_migration_returns_stable_exit_code(monkeypatch, capsys, tmp_path):
    pipeline = FakePipeline()
    _install_services(monkeypatch, pipeline)

    def fake_migrate(document, target, provider, *, policy):
        return OperationResult(
            document,
            _report(OperationKind.MIGRATE, StageKind.MIGRATION_PLANNING, error=True),
        )

    monkeypatch.setattr(cli, "execute_background_migration", fake_migrate)
    exit_code = cli.main(
        [
            "migrate-background",
            str(tmp_path / "source.xlsx"),
            str(tmp_path / "output.xlsx"),
            "--target-technosphere-family",
            "ecoinvent",
            "--target-technosphere-version",
            "3.11",
            "--target-technosphere-system-model",
            "cutoff",
            "--target-biosphere-family",
            "ecoinvent",
            "--target-biosphere-version",
            "3.11",
            "--json",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == cli.EXIT_MIGRATION
    assert payload["success"] is False
    assert [call[0] for call in pipeline.calls] == ["read"]


def test_partial_source_background_is_a_usage_error(monkeypatch, capsys, tmp_path):
    monkeypatch.setattr(
        cli,
        "_application_services",
        lambda: pytest.fail("Services must not be constructed for invalid context."),
    )

    exit_code = cli.main(
        [
            "inspect",
            str(tmp_path / "source.xlsx"),
            "--source-technosphere-family",
            "ecoinvent",
            "--json",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == cli.EXIT_USAGE
    assert payload["success"] is False
    assert "requires every technosphere and biosphere axis" in payload["error"]


def test_missing_target_context_is_an_argparse_error(capsys, tmp_path):
    exit_code = cli.main(
        [
            "migrate-background",
            str(tmp_path / "source.xlsx"),
            str(tmp_path / "output.xlsx"),
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == cli.EXIT_USAGE
    assert "--target-technosphere-family" in captured.err


def test_inspect_writes_an_audit_sidecar_with_source_digest(monkeypatch, capsys, tmp_path):
    pipeline = FakePipeline()
    _install_services(monkeypatch, pipeline)
    source = tmp_path / "source.xlsx"
    source.write_bytes(b"inventory")
    sidecar = tmp_path / "inspection.json"

    exit_code = cli.main(["inspect", str(source), "--report", str(sidecar), "--json"])

    output = json.loads(capsys.readouterr().out)
    audit = json.loads(sidecar.read_text(encoding="utf-8"))
    assert exit_code == cli.EXIT_SUCCESS
    assert output["report_path"] == str(sidecar.resolve())
    assert audit["report"]["operation"] == "read"
    assert audit["artifacts"][0]["role"] == "source"
    assert audit["artifacts"][0]["size"] == len(b"inventory")
