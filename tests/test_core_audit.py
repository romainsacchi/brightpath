import json

import pytest

from brightpath.core.audit import ArtifactDigest, digest_artifact, write_report_sidecar
from brightpath.core.reports import OperationKind, OperationReport, StageKind, StageReport


def test_digest_artifact_records_exact_content_identity(tmp_path):
    artifact = tmp_path / "inventory.csv"
    artifact.write_bytes(b"inventory\n")

    digest = digest_artifact(artifact, role="source", chunk_size=2)

    assert digest.role == "source"
    assert digest.path == str(artifact.resolve())
    assert digest.size == 10
    assert digest.sha256 == "01adaa40bf3775a40204c1e2cec2d366ac37b17b860c024d6cfccfebaf28370e"


def test_report_sidecar_is_deterministic_and_replaces_atomically(tmp_path):
    report = OperationReport(
        OperationKind.VALIDATE,
        stages=(StageReport(StageKind.STRUCTURAL_VALIDATION, metrics={"datasets": 1}),),
    )
    destination = tmp_path / "inventory.report.json"
    digest = ArtifactDigest("source", "/tmp/source", "a" * 64, 12)

    first = write_report_sidecar(report, destination, artifacts=(digest,)).read_bytes()
    second = write_report_sidecar(report, destination, artifacts=(digest,)).read_bytes()
    payload = json.loads(second)

    assert first == second
    assert payload["report"] == report.to_dict()
    assert payload["artifacts"][0]["sha256"] == "a" * 64
    assert not list(tmp_path.glob("*.tmp"))


def test_report_sidecar_requires_an_existing_parent(tmp_path):
    with pytest.raises(FileNotFoundError, match="Report directory"):
        write_report_sidecar(OperationReport(OperationKind.READ), tmp_path / "missing" / "report.json")
