from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

import pytest

from brightpath.adapters import (
    AdapterCapabilities,
    AdapterRegistry,
    ArtifactKind,
    DetectionCandidate,
    FormatDescriptor,
    coerce_format_descriptor,
    default_adapter_registry,
)
from brightpath.core import FormatProfile

FILE_READ_WRITE = AdapterCapabilities(
    read_artifact_kinds={ArtifactKind.FILE},
    write_artifact_kinds={ArtifactKind.FILE},
    detection_artifact_kinds={ArtifactKind.FILE},
)


@dataclass
class FakeAdapter:
    descriptor: FormatDescriptor
    confidence: float | None
    evidence: tuple[str, ...] = ()
    capabilities: AdapterCapabilities = FILE_READ_WRITE
    error: Exception | None = None
    probes: int = 0

    def detect(self, artifact, *, artifact_kind):
        self.probes += 1
        if self.error:
            raise self.error
        if self.confidence is None:
            return None
        return DetectionCandidate(self.descriptor, self.confidence, self.evidence)

    def read(self, artifact, **kwargs):
        return artifact

    def write(self, document, artifact, **kwargs):
        return artifact


def adapter(format_id, confidence, *, evidence=(), capabilities=FILE_READ_WRITE, version="", dialect=""):
    return FakeAdapter(
        descriptor=FormatDescriptor(format_id, version=version, dialect=dialect),
        confidence=confidence,
        evidence=evidence,
        capabilities=capabilities,
    )


def test_format_descriptor_accepts_profile_aliases_and_enum_values():
    class FormatEnum(str, Enum):
        BRIGHTWAY = "brightway_excel"

    class Profile:
        format_id = "SimaPro-CSV"
        format_version = "9.5"
        dialect = "CSV"

    enum_descriptor = coerce_format_descriptor(FormatEnum.BRIGHTWAY)
    profile_descriptor = coerce_format_descriptor(Profile())

    assert enum_descriptor.format_id == "brightway_excel"
    assert profile_descriptor == FormatDescriptor("simapro_csv", version="9.5", dialect="csv")
    assert profile_descriptor.id == profile_descriptor.identifier == "simapro_csv"


def test_capability_collections_are_immutable_and_operation_specific():
    kinds = [ArtifactKind.FILE]
    capabilities = AdapterCapabilities(read_artifact_kinds=kinds)
    kinds.append(ArtifactKind.DIRECTORY)

    assert capabilities.supports_read(ArtifactKind.FILE)
    assert not capabilities.supports_read(ArtifactKind.DIRECTORY)
    assert not capabilities.supports_write(ArtifactKind.FILE)
    assert not capabilities.supports_detection(ArtifactKind.FILE)
    with pytest.raises(AttributeError):
        capabilities.read_artifact_kinds = frozenset()


def test_registry_capabilities_come_from_registered_adapters_not_known_identifiers():
    brightway = adapter("brightway_excel", 0.9)
    registry = AdapterRegistry([brightway])

    assert registry.adapters == (brightway,)
    assert registry.supports_read("brightway_excel", ArtifactKind.FILE)
    assert not registry.supports_read("openlca_excel", ArtifactKind.FILE)
    assert registry.readable_formats(ArtifactKind.FILE) == (FormatDescriptor("brightway_excel"),)
    assert registry.writable_formats(ArtifactKind.FILE) == (FormatDescriptor("brightway_excel"),)
    with pytest.raises(AttributeError):
        registry.adapters = ()


def test_registry_rejects_duplicate_descriptors_and_requires_specific_dialect():
    generic = adapter("brightway_excel", 0.9, dialect="bw2io")
    alternate = adapter("brightway_excel", 0.8, dialect="custom")

    with pytest.raises(ValueError, match="Duplicate adapter"):
        AdapterRegistry([generic, generic])

    registry = AdapterRegistry([generic, alternate])
    with pytest.raises(LookupError, match="Multiple adapters"):
        registry.get("brightway_excel")
    assert registry.get(FormatDescriptor("brightway_excel", dialect="custom")) is alternate


def test_qualified_profile_falls_back_to_generic_builtin_descriptor():
    registry = default_adapter_registry()
    profile = FormatProfile("brightway_excel", format_version="1.0", dialect="bw2io")

    selected = registry.get(profile)

    assert selected.descriptor == FormatDescriptor("brightway_excel")
    assert registry.matching(profile) == (selected,)
    assert registry.supports_write(profile, ArtifactKind.FILE)


def test_exact_then_generic_fallback_avoids_qualified_adapter_ambiguity():
    generic = adapter("brightway_excel", 0.7)
    bw2io = adapter("brightway_excel", 0.8, dialect="bw2io")
    custom = adapter("brightway_excel", 0.9, dialect="custom")
    registry = AdapterRegistry((generic, bw2io, custom))

    assert registry.get(FormatDescriptor("brightway_excel", dialect="bw2io")) is bw2io
    assert registry.get(FormatDescriptor("brightway_excel", dialect="future")) is generic
    assert registry.get("brightway_excel") is generic


def test_unique_high_confidence_candidate_is_selected_with_all_evidence_retained():
    brightway = adapter("brightway_excel", 0.92, evidence=("Workbook metadata sheet",))
    simapro = adapter("simapro_csv", 0.4, evidence=("Semicolon-delimited text",))

    report = AdapterRegistry([brightway, simapro]).detect("inventory", minimum_confidence=0.7)

    assert report.detected_format == FormatDescriptor("brightway_excel")
    assert [candidate.descriptor.format_id for candidate in report.candidates] == [
        "brightway_excel",
        "simapro_csv",
    ]
    assert report.candidates[0].evidence == ("Workbook metadata sheet",)
    assert not report.has_errors


def test_equal_confidence_is_reported_as_ambiguous_instead_of_guessed():
    report = AdapterRegistry([adapter("brightway_csv", 0.8), adapter("simapro_csv", 0.8)]).detect("ambiguous.csv")

    assert report.selected is None
    assert report.is_ambiguous
    assert report.has_errors
    assert {candidate.descriptor.format_id for candidate in report.candidates} == {
        "brightway_csv",
        "simapro_csv",
    }


def test_low_confidence_is_reported_instead_of_selected():
    report = AdapterRegistry([adapter("simapro_csv", 0.49)]).detect("unknown.csv", minimum_confidence=0.5)

    assert report.selected is None
    assert [issue.code for issue in report.issues] == ["format_detection_low_confidence"]


def test_explicit_format_wins_but_all_detector_evidence_is_still_collected():
    brightway = adapter("brightway_csv", 0.2, evidence=("Brightway headers partially matched",))
    simapro = adapter("simapro_csv", 0.99, evidence=("SimaPro section headers matched",))

    report = AdapterRegistry([brightway, simapro]).detect(
        "ambiguous.csv",
        explicit_format="brightway_csv",
    )

    assert brightway.probes == simapro.probes == 1
    assert report.detected_format == FormatDescriptor("brightway_csv")
    assert report.selected.confidence == 1.0
    assert report.selected.evidence == (
        "Format selected explicitly by the caller.",
        "Brightway headers partially matched",
    )
    assert {candidate.descriptor.format_id for candidate in report.candidates} == {
        "brightway_csv",
        "simapro_csv",
    }


def test_explicit_format_must_be_registered_and_readable():
    write_only = AdapterCapabilities(write_artifact_kinds={ArtifactKind.FILE})
    registry = AdapterRegistry([adapter("ecospold2", None, capabilities=write_only)])

    unavailable = registry.detect("inventory", explicit_format="openlca_excel")
    unsupported = registry.detect("inventory", explicit_format="ecospold2")

    assert [issue.code for issue in unavailable.issues] == ["explicit_format_unavailable"]
    assert [issue.code for issue in unsupported.issues] == ["explicit_format_unsupported_artifact"]
    assert unavailable.selected is unsupported.selected is None


def test_probe_failure_is_reported_without_discarding_other_candidates():
    broken = adapter("brightway_excel", 0.9)
    broken.error = RuntimeError("not a workbook")
    simapro = adapter("simapro_csv", 0.8)

    report = AdapterRegistry([broken, simapro]).detect("inventory.csv")

    assert report.detected_format == FormatDescriptor("simapro_csv")
    assert [issue.code for issue in report.issues] == ["format_detection_probe_failed"]
    assert not report.has_errors


def test_no_candidate_returns_structured_no_match_issue():
    report = AdapterRegistry([adapter("brightway_excel", None)]).detect("unknown")

    assert report.selected is None
    assert [issue.code for issue in report.issues] == ["format_detection_no_match"]
