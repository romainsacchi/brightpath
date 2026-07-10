from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest
from bw2data import projects

import brightpath.adapters.builtins as builtin_adapters
from brightpath.adapters.base import ArtifactKind, FormatAdapter, FormatDescriptor
from brightpath.adapters.builtins import (
    BrightwayDelimitedAdapter,
    BrightwayExcelAdapter,
    SimaProCSVAdapter,
    default_adapter_registry,
)
from brightpath.background import BiosphereCatalog, InMemoryCatalogProvider
from brightpath.core import BackgroundContext, BiosphereProfile, FormatProfile, InventoryContext, TechnosphereProfile
from brightpath.core.policies import ConversionPolicy
from brightpath.core.reports import StageKind
from brightpath.models import BackgroundProfile, InventoryDocument


def _context(format_id: str) -> InventoryContext:
    return InventoryContext(
        format=FormatProfile(format_id),
        background=BackgroundContext(
            technosphere=TechnosphereProfile("ecoinvent", "3.9.1", "cutoff"),
            biosphere=BiosphereProfile("ecoinvent", "3.9.1"),
        ),
    )


def _document(format_id: str) -> InventoryDocument:
    return InventoryDocument(
        data=[
            {
                "name": "test process",
                "reference product": "test product",
                "location": "GLO",
                "unit": "kilogram",
                "exchanges": [
                    {
                        "type": "production",
                        "name": "test process",
                        "reference product": "test product",
                        "product": "test product",
                        "location": "GLO",
                        "unit": "kilogram",
                        "amount": 1.0,
                        "simapro category": "Materials/Test",
                    }
                ],
            }
        ],
        context=_context(format_id),
        database_name="adapter-test",
    )


def test_builtin_adapters_are_immutable_format_adapters():
    adapter = BrightwayExcelAdapter()

    assert isinstance(adapter, FormatAdapter)
    assert isinstance(SimaProCSVAdapter(), FormatAdapter)
    with pytest.raises(FrozenInstanceError):
        adapter.descriptor = FormatDescriptor("other")


@pytest.mark.parametrize(
    ("adapter", "format_id"),
    [
        (BrightwayExcelAdapter(), "brightway_excel"),
        (BrightwayDelimitedAdapter(FormatDescriptor("brightway_csv"), ","), "brightway_csv"),
        (SimaProCSVAdapter(), "simapro_csv"),
    ],
)
def test_builtin_adapters_expose_validation_and_conversion_preflight_hooks(adapter, format_id):
    document = _document(format_id)

    validation = adapter.validate_format(document)
    conversion = adapter.preflight_conversion(document, policy=ConversionPolicy.strict())

    assert validation.stage is StageKind.FORMAT_VALIDATION
    assert conversion.stage is StageKind.CONVERSION_PREFLIGHT
    assert not validation.has_errors
    assert not conversion.has_errors


def test_read_methods_forward_exact_context_arguments(monkeypatch):
    background = BackgroundProfile("ecoinvent", "3.9.1", "cutoff")
    biosphere = BiosphereProfile("ecoinvent", "3.9.1")
    brightway_context = _context("brightway_excel")
    simapro_context = _context("simapro_csv")
    calls = {}
    provider = InMemoryCatalogProvider(biosphere=[BiosphereCatalog(biosphere, set())])

    def fake_brightway_loader(path, **kwargs):
        calls["brightway"] = (path, kwargs)
        return "brightway-document"

    def fake_simapro_loader(path, **kwargs):
        calls["simapro"] = (path, kwargs)
        return "simapro-document"

    monkeypatch.setattr(builtin_adapters, "load_brightway_excel", fake_brightway_loader)
    monkeypatch.setattr(builtin_adapters, "load_simapro_csv", fake_simapro_loader)

    assert (
        BrightwayExcelAdapter().read(
            "source.xlsx",
            background_profile=background,
            biosphere_profile=biosphere,
            context=brightway_context,
        )
        == "brightway-document"
    )
    assert (
        SimaProCSVAdapter().read(
            "source.csv",
            background_profile=background,
            biosphere_profile=biosphere,
            context=simapro_context,
            database_name="foreground",
            catalog_provider=provider,
        )
        == "simapro-document"
    )

    assert calls["brightway"] == (
        "source.xlsx",
        {
            "background_profile": background,
            "biosphere_profile": biosphere,
            "context": brightway_context,
        },
    )
    assert calls["simapro"] == (
        "source.csv",
        {
            "background_profile": background,
            "biosphere_profile": biosphere,
            "context": simapro_context,
            "database_name": "foreground",
            "catalog_provider": provider,
        },
    )


def test_brightway_excel_adapter_writes_detects_and_reads_exact_context(tmp_path):
    adapter = BrightwayExcelAdapter()
    source = _document("brightway_excel")
    destination = adapter.write(source, tmp_path / "inventory.xlsx")
    contents_before = destination.read_bytes()

    candidate = adapter.detect(destination, artifact_kind=ArtifactKind.FILE)
    loaded = adapter.read(destination, context=source.context)

    assert candidate is not None
    assert candidate.descriptor == FormatDescriptor("brightway_excel")
    assert candidate.confidence == pytest.approx(0.98)
    assert "Activity" in candidate.evidence[1]
    assert destination.read_bytes() == contents_before
    assert loaded.context == source.context
    assert loaded.data[0]["name"] == "test process"


def test_brightway_probe_uses_xlsx_content_instead_of_the_suffix(tmp_path):
    adapter = BrightwayExcelAdapter()
    workbook = adapter.write(_document("brightway_excel"), tmp_path / "inventory.xlsx")
    disguised_workbook = workbook.rename(tmp_path / "inventory.csv")
    fake_workbook = tmp_path / "inventory.xlsx"
    fake_workbook.write_text("Activity\nExchanges\nDatabase\n", encoding="utf-8")

    assert adapter.detect(disguised_workbook, artifact_kind=ArtifactKind.FILE) is not None
    assert adapter.detect(fake_workbook, artifact_kind=ArtifactKind.FILE) is None


def test_simapro_adapter_writes_detects_and_reads_exact_context(tmp_path, monkeypatch):
    adapter = SimaProCSVAdapter()
    source = _document("simapro_csv")
    destination, render_result = adapter.write(source, tmp_path / "inventory.csv")
    contents_before = destination.read_bytes()
    monkeypatch.setattr(projects, "_base_logs_dir", str(tmp_path))
    Path(projects.logs_dir).mkdir(parents=True, exist_ok=True)

    candidate = adapter.detect(destination, artifact_kind=ArtifactKind.FILE)
    provider = InMemoryCatalogProvider(biosphere=[BiosphereCatalog(source.context.background.biosphere, set())])
    loaded = adapter.read(
        destination,
        context=source.context,
        database_name="loaded-database",
        catalog_provider=provider,
    )

    assert not render_result.has_errors
    assert candidate is not None
    assert candidate.descriptor == FormatDescriptor("simapro_csv")
    assert candidate.confidence == pytest.approx(0.99)
    assert destination.read_bytes() == contents_before
    assert loaded.context == source.context
    assert loaded.database_name == "loaded-database"
    assert loaded.data[0]["name"] == "test process"


def test_simapro_probe_uses_content_and_rejects_brightway_block_csv(tmp_path):
    adapter = SimaProCSVAdapter()
    destination, _result = adapter.write(_document("simapro_csv"), tmp_path / "inventory.csv")
    disguised_export = destination.rename(tmp_path / "inventory.data")
    brightway_block = tmp_path / "brightway.csv"
    brightway_block.write_text(
        "Activity\nname;reference product;location;unit\n"
        "foreground;service;GLO;unit\nExchanges\nname;amount;type\nforeground;1;production\n",
        encoding="utf-8",
    )

    assert adapter.detect(disguised_export, artifact_kind=ArtifactKind.FILE) is not None
    assert adapter.detect(brightway_block, artifact_kind=ArtifactKind.FILE) is None


def test_default_registry_advertises_and_detects_only_implemented_file_adapters(tmp_path):
    registry = default_adapter_registry()
    simapro_path, _result = registry.get("simapro_csv").write(
        _document("simapro_csv"),
        tmp_path / "inventory.csv",
    )

    report = registry.detect(simapro_path)

    assert registry.descriptors == (
        FormatDescriptor("brightway_excel"),
        FormatDescriptor("brightway_csv"),
        FormatDescriptor("brightway_tsv"),
        FormatDescriptor("simapro_csv"),
    )
    assert registry.supports_read("brightway_excel", ArtifactKind.FILE)
    assert registry.supports_write("simapro_csv", ArtifactKind.FILE)
    assert not registry.supports_read("openlca_excel", ArtifactKind.FILE)
    assert report.detected_format == FormatDescriptor("simapro_csv")
    assert not report.has_errors


@pytest.mark.parametrize(
    ("format_id", "delimiter", "suffix"),
    [
        ("brightway_csv", ",", ".csv"),
        ("brightway_tsv", "\t", ".tsv"),
    ],
)
def test_brightway_delimited_adapters_round_trip_and_detect_content(
    tmp_path,
    format_id,
    delimiter,
    suffix,
):
    adapter = BrightwayDelimitedAdapter(FormatDescriptor(format_id), delimiter)
    source = _document(format_id)

    destination = adapter.write(source, tmp_path / f"inventory{suffix}")
    candidate = adapter.detect(destination, artifact_kind=ArtifactKind.FILE)
    loaded = adapter.read(destination, context=source.context)

    assert candidate is not None
    assert candidate.descriptor == FormatDescriptor(format_id)
    assert loaded.context == source.context
    assert loaded.data[0]["name"] == source.data[0]["name"]
    assert loaded.data[0]["reference product"] == source.data[0]["reference product"]
    assert loaded.data[0]["exchanges"][0]["simapro category"] == "Materials/Test"


def test_brightway_csv_and_simapro_probes_are_content_disjoint(tmp_path):
    registry = default_adapter_registry()
    source = _document("brightway_csv")
    path = registry.get("brightway_csv").write(source, tmp_path / "inventory.csv")

    report = registry.detect(path)

    assert report.detected_format == FormatDescriptor("brightway_csv")
    assert [candidate.descriptor.format_id for candidate in report.candidates] == ["brightway_csv"]
