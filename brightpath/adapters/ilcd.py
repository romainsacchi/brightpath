"""Registered ILCD process adapter and its representability checks."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from lxml import etree

from brightpath.core.reports import Issue, Severity, StageKind, StageReport
from brightpath.formats.ilcd import _reference_exchange, _unsupported, load_ilcd, render_ilcd_package, write_ilcd
from brightpath.formats.ilcd_package import NS, ILCDError, parse_xml, read_members, validate_xml

from .base import AdapterCapabilities, ArtifactKind, DetectionCandidate, FormatDescriptor


@dataclass(frozen=True)
class ILCDAdapter:
    """Read and write single-reference ILCD 1.1 process packages."""

    descriptor: FormatDescriptor = FormatDescriptor("ilcd")
    capabilities: AdapterCapabilities = AdapterCapabilities(
        read_artifact_kinds={ArtifactKind.FILE, ArtifactKind.DIRECTORY},
        write_artifact_kinds={ArtifactKind.FILE, ArtifactKind.DIRECTORY},
        detection_artifact_kinds={ArtifactKind.FILE, ArtifactKind.DIRECTORY},
        compatible_format_versions={"1.1"},
        compatible_dialects={"core", "openlca"},
        can_validate_format=True,
        can_preflight_conversion=True,
    )

    def detect(self, artifact, *, artifact_kind):
        if ArtifactKind(artifact_kind) not in {ArtifactKind.FILE, ArtifactKind.DIRECTORY}:
            return None
        if not isinstance(artifact, (str, Path)):
            return None
        try:
            for contents in read_members(artifact).values():
                root = parse_xml(contents)
                if root.tag == f"{{{NS['p']}}}processDataSet":
                    dialect = (
                        "openlca"
                        if any(str(k).startswith("{" + NS["olca"] + "}") for e in root.iter() for k in e.attrib)
                        else "core"
                    )
                    return DetectionCandidate(
                        self.descriptor,
                        0.98,
                        (f"ILCD processDataSet XML namespace found; {dialect} dialect evidence.",),
                    )
        except (OSError, ValueError, etree.Error):
            return None
        return None

    def read(self, artifact, **kwargs):
        return load_ilcd(artifact, **kwargs)

    def write(self, document, artifact, **kwargs):
        return write_ilcd(document, artifact, **kwargs)

    def validate_format(self, document):
        issues = []
        for index, dataset in enumerate(document.data):
            imported = dataset.get("ilcd", {})
            raw = imported.get("resources", {}).get(imported.get("process_path"))
            if raw is not None:
                try:
                    root = parse_xml(raw.encode())
                    _unsupported(root)
                    _reference_exchange(root)
                    validate_xml(root)
                except ILCDError as error:
                    issues.append(
                        Issue(
                            Severity.ERROR,
                            error.code,
                            str(error),
                            StageKind.FORMAT_VALIDATION,
                            path=f"datasets[{index}].ilcd",
                        )
                    )
            for finding in dataset.get("ilcd", {}).get("issues", []):
                issues.append(
                    Issue(
                        Severity(finding.get("severity", "error")),
                        finding["code"],
                        finding["message"],
                        StageKind.FORMAT_VALIDATION,
                        path=f"datasets[{index}].{finding.get('path', '')}",
                    )
                )
        return StageReport(StageKind.FORMAT_VALIDATION, label="ILCD process representation", issues=tuple(issues))

    def preflight_conversion(self, document, *, policy, **kwargs):
        try:
            render_ilcd_package(document, **kwargs)
            issues = tuple(
                Issue(
                    Severity.WARNING,
                    finding["code"],
                    finding["message"],
                    StageKind.CONVERSION_PREFLIGHT,
                    path=f"datasets[{index}].{finding.get('path', '')}",
                )
                for index, dataset in enumerate(document.data)
                for finding in dataset.get("ilcd", {}).get("issues", [])
                if finding.get("severity") == "warning"
            )
        except (ILCDError, ValueError, TypeError, etree.Error) as error:
            issues = (
                Issue(
                    Severity.ERROR,
                    getattr(error, "code", "ilcd.unrepresentable"),
                    str(error),
                    StageKind.CONVERSION_PREFLIGHT,
                    path=getattr(error, "path", ""),
                ),
            )
        return StageReport(
            StageKind.CONVERSION_PREFLIGHT,
            label="ILCD process export",
            issues=issues,
            metrics={"format": "ilcd", "version": "1.1", "policy": policy.to_dict()},
        )
