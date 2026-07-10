from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path

from brightpath.adapters import (
    AdapterCapabilities,
    AdapterRegistry,
    ArtifactKind,
    DetectionCandidate,
    FormatDescriptor,
    default_adapter_registry,
)
from brightpath.background import (
    BiosphereCatalog,
    InMemoryCatalogProvider,
    TechnosphereCatalog,
)
from brightpath.core.context import (
    BackgroundContext,
    BiosphereProfile,
    ContextHint,
    FormatProfile,
    InventoryContext,
    TechnosphereProfile,
)
from brightpath.core.pipeline import InventoryPipeline
from brightpath.core.policies import ConversionPolicy
from brightpath.core.reports import OperationKind, StageKind
from brightpath.models import InventoryDocument

FILE_ADAPTER = AdapterCapabilities(
    read_artifact_kinds={ArtifactKind.FILE},
    write_artifact_kinds={ArtifactKind.FILE},
    detection_artifact_kinds={ArtifactKind.FILE},
)


@dataclass
class FakeAdapter:
    descriptor: FormatDescriptor
    confidence: float | None = 0.9
    document: InventoryDocument | None = None
    read_error: Exception | None = None
    write_error: Exception | None = None
    capabilities: AdapterCapabilities = FILE_ADAPTER

    def detect(self, artifact, *, artifact_kind):
        if self.confidence is None:
            return None
        return DetectionCandidate(self.descriptor, self.confidence, ("fake evidence",))

    def read(self, artifact, **kwargs):
        if self.read_error is not None:
            raise self.read_error
        return self.document

    def write(self, document, artifact, **kwargs):
        if self.write_error is not None:
            raise self.write_error
        output = Path(artifact)
        output.write_text("written", encoding="utf-8")
        return output


def context(format_id="brightway_csv"):
    return InventoryContext(
        format=FormatProfile(format_id),
        background=BackgroundContext(
            technosphere=TechnosphereProfile("ecoinvent", "3.10.1", "cutoff"),
            biosphere=BiosphereProfile("ecoinvent", "3.10.1"),
        ),
    )


def activity(*, extra_exchanges=()):
    return {
        "name": "foreground process",
        "reference product": "foreground product",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [
            {
                "type": "production",
                "name": "foreground process",
                "reference product": "foreground product",
                "product": "foreground product",
                "location": "GLO",
                "unit": "kilogram",
                "amount": 1.0,
                "simapro category": "Materials/Test",
            },
            *extra_exchanges,
        ],
    }


def document(format_id="brightway_csv", *, data=None):
    return InventoryDocument(
        data=data if data is not None else [activity()],
        context=context(format_id),
        database_name="pipeline-test",
    )


def provider_for(source: InventoryDocument | None = None):
    source = source or document()
    return InMemoryCatalogProvider(
        technosphere=[TechnosphereCatalog(source.context.background.technosphere, set())],
        biosphere=[BiosphereCatalog(source.context.background.biosphere, set())],
    )


def test_detect_reports_ambiguous_adapter_evidence_without_guessing():
    registry = AdapterRegistry(
        (
            FakeAdapter(FormatDescriptor("brightway_csv"), confidence=0.8),
            FakeAdapter(FormatDescriptor("simapro_csv"), confidence=0.8),
        )
    )
    result = InventoryPipeline(registry, InMemoryCatalogProvider()).detect("ambiguous.csv")

    assert result.value is None
    assert result.error
    assert result.report.operation is OperationKind.ANALYZE
    assert [issue.code for issue in result.report.issues] == ["format_detection_ambiguous"]
    assert len(result.report.stages[0].metrics["candidates"]) == 2


def test_read_combines_detection_and_parse_reports_and_forwards_context():
    source = document()
    adapter = FakeAdapter(FormatDescriptor("brightway_csv"), document=source)
    result = InventoryPipeline(AdapterRegistry((adapter,)), provider_for(source)).read(
        "inventory.csv",
        hint=ContextHint(background=source.context.background),
    )

    assert result.value is source
    assert result.succeeded
    assert [stage.stage for stage in result.report.stages] == [StageKind.FORMAT_DETECTION, StageKind.PARSE]
    assert result.report.stages[-1].metrics["datasets"] == 1


def test_normalize_is_non_mutating_and_reports_a_change():
    data = [
        {
            "name": "legacy process",
            "product": "legacy product",
            "location": "GLO",
            "unit": "kilogram",
            "exchanges": [
                {
                    "type": "production",
                    "name": "wrong name",
                    "product": "legacy product",
                    "location": "GLO",
                    "unit": "kilogram",
                    "amount": 1,
                }
            ],
        }
    ]
    original = deepcopy(data)
    source = document(data=data)
    before = source.data

    result = InventoryPipeline(AdapterRegistry(), provider_for(source)).normalize(source)

    assert data == original
    assert source.data == before
    assert result.value is not None
    assert result.value.data[0]["reference product"] == "legacy product"
    assert result.value.data[0]["exchanges"][0]["name"] == "legacy process"
    assert result.changed
    assert result.report.changes[0].code == "inventory.normalized"


def test_structural_and_background_validation_run_as_independent_stages():
    background_exchange = {
        "type": "technosphere",
        "name": "missing background",
        "reference product": "missing product",
        "location": "GLO",
        "unit": "kilogram",
        "amount": 1,
    }
    duplicate = activity(extra_exchanges=(background_exchange,))
    source = document(data=[duplicate, deepcopy(duplicate)])
    original = source.data

    result = InventoryPipeline(AdapterRegistry(), provider_for(source)).validate(source)

    assert source.data == original
    assert [stage.stage for stage in result.report.stages] == [
        StageKind.STRUCTURAL_VALIDATION,
        StageKind.BACKGROUND_VALIDATION,
    ]
    assert "duplicate_dataset_identity" in {issue.code for issue in result.report.stages[0].issues}
    assert "background.technosphere_link_unresolved" in {issue.code for issue in result.report.stages[1].issues}


def test_convert_changes_only_the_format_context():
    source = document()
    target = FakeAdapter(FormatDescriptor("brightway_tsv"), confidence=None)

    result = InventoryPipeline(AdapterRegistry((target,)), provider_for(source)).convert(
        source,
        "brightway_tsv",
    )

    assert result.value is not None
    assert result.value.context.format.format_id == "brightway_tsv"
    assert result.value.context.background == source.context.background
    assert result.value.data == source.data
    assert source.context.format.format_id == "brightway_csv"
    assert result.report.changes[0].path == "context.format"


def test_strict_simapro_preflight_rejects_an_unused_exchange_as_explicit_loss():
    unused = {
        "type": "biosphere",
        "name": "Oxygen",
        "categories": ("air", "urban air close to ground"),
        "unit": "kilogram",
        "amount": 1.0,
    }
    source = document(data=[activity(extra_exchanges=(unused,))])
    pipeline = InventoryPipeline(default_adapter_registry(), provider_for(source))

    strict = pipeline.convert(source, "simapro_csv")
    permissive = pipeline.convert(source, "simapro_csv", policy=ConversionPolicy.permissive())

    assert strict.value is None
    assert strict.error
    assert [loss.code for loss in strict.report.losses] == ["simapro_exchange_unused"]
    assert permissive.value is not None
    assert not permissive.error
    assert permissive.lossy


def test_real_builtin_write_and_read_with_atomic_audit_sidecar(tmp_path):
    source = document()
    pipeline = InventoryPipeline(default_adapter_registry(), provider_for(source))

    written = pipeline.write(source, tmp_path / "inventory.csv", sidecar=True)

    assert written.succeeded
    assert written.value == (tmp_path / "inventory.csv").resolve()
    sidecar = tmp_path / "inventory.csv.brightpath.json"
    payload = json.loads(sidecar.read_text(encoding="utf-8"))
    assert payload["report"] == written.report.to_dict()
    assert payload["artifacts"][0]["role"] == "output"
    assert len(payload["artifacts"][0]["sha256"]) == 64

    loaded = pipeline.read(
        written.value,
        hint=ContextHint(background=source.context.background),
    )
    assert loaded.succeeded
    assert loaded.value is not None
    assert loaded.value.context == source.context
    assert loaded.value.data[0]["name"] == "foreground process"


def test_parse_and_write_failures_are_structured_operation_results(tmp_path):
    source = document()
    parse_adapter = FakeAdapter(
        FormatDescriptor("brightway_csv"),
        read_error=ValueError("bad block layout"),
    )
    parse_result = InventoryPipeline(AdapterRegistry((parse_adapter,)), provider_for(source)).read(
        tmp_path / "bad.csv",
        hint=ContextHint(background=source.context.background),
    )

    write_adapter = FakeAdapter(
        FormatDescriptor("brightway_csv"),
        document=source,
        write_error=OSError("disk full"),
    )
    write_result = InventoryPipeline(AdapterRegistry((write_adapter,)), provider_for(source)).write(
        source,
        tmp_path / "output.csv",
    )

    assert parse_result.value is None
    assert [issue.code for issue in parse_result.report.issues] == ["parse.failed"]
    assert write_result.value is None
    assert [issue.code for issue in write_result.report.issues] == ["write.failed"]
    assert write_result.report.issues[0].stage is StageKind.WRITE
