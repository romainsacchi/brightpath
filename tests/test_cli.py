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


def _simapro_conversion_fixture(monkeypatch, tmp_path, *, stock_version="10.2", stock_name="Carbon dioxide, fossil"):
    from openpyxl import Workbook

    from brightpath.background.catalogs import PackageCatalogProvider

    document = _document("simapro_csv")
    activity = document.data[0]
    activity["comment"] = "Synthetic CLI substance-list validation fixture."
    activity["exchanges"] = [
        {
            "name": activity["name"],
            "reference product": activity["reference product"],
            "location": "GLO",
            "unit": "kilogram",
            "amount": 1,
            "type": "production",
            "simapro category": "material/Test",
        },
        {
            "name": "Carbon dioxide, fossil",
            "categories": ("air",),
            "unit": "kilogram",
            "amount": 2,
            "type": "biosphere",
        },
    ]
    document = document.replace(data=[activity])
    pipeline = FakePipeline(document)
    pipeline.conversion_result = OperationResult(document, _report(OperationKind.CONVERT, StageKind.FORMAT_CONVERSION))
    monkeypatch.setattr(cli, "_application_services", lambda: (pipeline, PackageCatalogProvider()))
    book = Workbook()
    book.active.append(["Flowable", "Context", "Unit", "FlowUUID", "SupportedNomenclatures"])
    book.active.append(
        [stock_name, "Airborne emissions", "kg", "synthetic-stock-carbon", f"Professional {stock_version}"]
    )
    reference = tmp_path / "substances.xlsx"
    book.save(reference)
    book.close()
    arguments = [
        "convert-format",
        str(tmp_path / "source.xlsx"),
        str(tmp_path / "output.csv"),
        "--target-format",
        "simapro_csv",
        "--simapro-version",
        stock_version,
        "--simapro-substance-list",
        str(reference),
        "--json",
    ]
    return pipeline, arguments


@pytest.mark.parametrize("extra", [[], ["--simapro-substance-list", "does-not-exist.xlsx"]])
def test_simapro_cli_without_target_version_exports_without_stock_validation(monkeypatch, capsys, tmp_path, extra):
    pipeline = FakePipeline()
    _install_services(monkeypatch, pipeline)

    def unexpected(*args, **kwargs):
        pytest.fail("Stock validation must not run without a target SimaPro version.")

    monkeypatch.setattr(cli, "_simapro_stock_report", unexpected)
    result = cli.main(
        ["convert-format", "source.xlsx", str(tmp_path / "output.csv"), "--target-format", "simapro_csv", "--json"]
        + extra
    )
    payload = json.loads(capsys.readouterr().out)
    assert result == cli.EXIT_SUCCESS, payload
    assert "simapro_substances" not in payload["reports"]
    assert [call[0] for call in pipeline.calls] == ["read", "convert", "write"]
    assert pipeline.calls[-1][3]["adapter_kwargs"] is None


def test_simapro_cli_given_version_requires_reference(monkeypatch, capsys, tmp_path):
    pipeline = FakePipeline()
    _install_services(monkeypatch, pipeline)
    result = cli.main(
        [
            "convert-format",
            "source.xlsx",
            str(tmp_path / "output.csv"),
            "--target-format",
            "simapro_csv",
            "--simapro-version",
            "10.2",
            "--json",
        ]
    )
    assert result == cli.EXIT_USAGE
    assert "--simapro-substance-list" in capsys.readouterr().out
    assert pipeline.calls == []


@pytest.mark.parametrize("stock_version", ["10.2", "10.4"])
def test_simapro_cli_uses_explicit_software_reference_separately_from_ecoinvent(
    monkeypatch, capsys, tmp_path, stock_version
):
    pipeline, arguments = _simapro_conversion_fixture(monkeypatch, tmp_path, stock_version=stock_version)
    before = pipeline.document.data
    result = cli.main(arguments)
    payload = json.loads(capsys.readouterr().out)
    assert result == cli.EXIT_SUCCESS, payload
    stock = payload["reports"]["simapro_substances"]["stages"][0]["metrics"]
    assert stock["stock_version"] == stock_version
    assert stock["matched"] == 1
    assert stock["stock_membership_validated"] is True
    assert stock["subcompartments_checked"] is False
    write = next(call for call in pipeline.calls if call[0] == "write")
    assert write[3]["adapter_kwargs"]["stock_version"] == stock_version
    assert pipeline.document.context.background.technosphere.version == "3.10"
    assert pipeline.document.context.background.biosphere.version == "3.10"
    assert pipeline.document.data == before


@pytest.mark.parametrize("extra", [[], ["--dry-run"], ["--policy", "permissive"]])
def test_simapro_cli_unlisted_substance_blocks_writing_and_dry_run(monkeypatch, capsys, tmp_path, extra):
    pipeline, arguments = _simapro_conversion_fixture(monkeypatch, tmp_path, stock_name="Different substance")
    result = cli.main(arguments + extra)
    payload = json.loads(capsys.readouterr().out)
    assert result == cli.EXIT_CONVERSION, payload
    stock = payload["reports"]["simapro_substances"]["stages"][0]
    assert stock["metrics"]["stock_membership_validated"] is False
    assert stock["metrics"]["unknown"] == 1
    assert stock["issues"][0]["details"]["name"] == "Carbon dioxide, fossil"
    assert all(call[0] != "write" for call in pipeline.calls)
    assert not (tmp_path / "output.csv").exists()


def test_simapro_cli_missing_version_catalogue_never_falls_back(monkeypatch, capsys, tmp_path):
    pipeline, arguments = _simapro_conversion_fixture(monkeypatch, tmp_path)
    arguments[arguments.index("--simapro-version") + 1] = "10.4"
    result = cli.main(arguments)
    payload = json.loads(capsys.readouterr().out)
    assert result == cli.EXIT_USAGE, payload
    assert "10.4" in str(payload)
    assert pipeline.calls == []


def test_simapro_reference_options_are_rejected_for_other_formats(monkeypatch, capsys, tmp_path):
    pipeline, arguments = _simapro_conversion_fixture(monkeypatch, tmp_path)
    arguments[arguments.index("--target-format") + 1] = "brightway_csv"
    assert cli.main(arguments) == cli.EXIT_USAGE
    assert "only be used" in capsys.readouterr().out
    assert pipeline.calls == []


@pytest.mark.parametrize(
    "stock_name, expected", [("Carbon dioxide, fossil", cli.EXIT_SUCCESS), ("Other", cli.EXIT_CONVERSION)]
)
def test_preserved_simapro_migration_checks_the_receiving_version(monkeypatch, capsys, tmp_path, stock_name, expected):
    pipeline, conversion_arguments = _simapro_conversion_fixture(monkeypatch, tmp_path, stock_name=stock_name)
    monkeypatch.setattr(
        cli,
        "execute_background_migration",
        lambda document, *args, **kwargs: OperationResult(
            document, _report(OperationKind.MIGRATE, StageKind.BACKGROUND_MIGRATION)
        ),
    )
    arguments = [
        "migrate-background",
        "source.csv",
        str(tmp_path / "output.csv"),
        "--target-technosphere-family",
        "ecoinvent",
        "--target-technosphere-version",
        "3.10",
        "--target-technosphere-system-model",
        "cutoff",
        "--target-biosphere-family",
        "ecoinvent",
        "--target-biosphere-version",
        "3.10",
        "--simapro-version",
        "10.2",
        "--simapro-substance-list",
        conversion_arguments[conversion_arguments.index("--simapro-substance-list") + 1],
        "--dry-run",
        "--json",
    ]
    result = cli.main(arguments)
    payload = json.loads(capsys.readouterr().out)
    assert result == expected, payload
    stock = payload["reports"]["simapro_substances"]["stages"][0]["metrics"]
    assert stock["stock_membership_validated"] is (expected == cli.EXIT_SUCCESS)
    assert all(call[0] != "write" for call in pipeline.calls)


@pytest.mark.parametrize("dry_run", [False, True])
def test_preserved_simapro_migration_without_software_version_skips_stock_validation(
    monkeypatch, capsys, tmp_path, dry_run
):
    pipeline, _ = _simapro_conversion_fixture(monkeypatch, tmp_path)

    def unexpected(*args, **kwargs):
        pytest.fail("Stock validation must not run without a target SimaPro version.")

    monkeypatch.setattr(cli, "_simapro_stock_report", unexpected)
    monkeypatch.setattr(
        cli,
        "execute_background_migration",
        lambda document, *args, **kwargs: OperationResult(
            document, _report(OperationKind.MIGRATE, StageKind.BACKGROUND_MIGRATION)
        ),
    )
    result = cli.main(
        [
            "migrate-background",
            "source.csv",
            str(tmp_path / "output.csv"),
            "--target-technosphere-family",
            "ecoinvent",
            "--target-technosphere-version",
            "3.10",
            "--target-technosphere-system-model",
            "cutoff",
            "--target-biosphere-family",
            "ecoinvent",
            "--target-biosphere-version",
            "3.10",
            "--json",
        ]
        + (["--dry-run"] if dry_run else [])
    )
    payload = json.loads(capsys.readouterr().out)
    assert result == cli.EXIT_SUCCESS, payload
    assert "simapro_substances" not in payload["reports"]
    assert [call[0] for call in pipeline.calls] == (["read"] if dry_run else ["read", "write"])


def test_formats_json_comes_from_capability_snapshot(capsys):
    exit_code = cli.main(["formats", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == cli.EXIT_SUCCESS
    assert payload["success"] is True
    assert {item["format_id"] for item in payload["capabilities"]["formats"]} == {
        "brightway_excel",
        "brightway_csv",
        "brightway_tsv",
        "ecospold1",
        "ecospold2",
        "ilcd",
        "openlca_jsonld",
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
    converted = _document("brightway_csv")
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
            "brightway_csv",
            "--dry-run",
            "--json",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == cli.EXIT_SUCCESS
    assert [call[0] for call in pipeline.calls] == ["read", "convert"]
    assert payload["dry_run"] is True
    assert payload["inventory"]["context"]["format"]["format_id"] == "brightway_csv"
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
            "brightway_csv",
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
