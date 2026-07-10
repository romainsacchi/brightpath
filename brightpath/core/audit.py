"""Deterministic, atomic persistence for operation audit reports."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .reports import OperationReport


@dataclass(frozen=True)
class ArtifactDigest:
    """Content identity recorded alongside an operation report."""

    role: str
    path: str
    sha256: str
    size: int

    def to_dict(self) -> dict:
        """Return a JSON-compatible representation."""

        return {
            "role": self.role,
            "path": self.path,
            "sha256": self.sha256,
            "size": self.size,
        }


def digest_artifact(path: str | Path, *, role: str, chunk_size: int = 1024 * 1024) -> ArtifactDigest:
    """Hash one file without loading it wholly into memory."""

    artifact = Path(path).expanduser().resolve()
    if not artifact.is_file():
        raise FileNotFoundError(f"Artifact not found: {artifact}")
    if not role.strip():
        raise ValueError("Artifact role must not be empty.")
    if chunk_size < 1:
        raise ValueError("chunk_size must be positive.")

    digest = hashlib.sha256()
    with artifact.open("rb") as stream:
        for chunk in iter(lambda: stream.read(chunk_size), b""):
            digest.update(chunk)
    return ArtifactDigest(
        role=role.strip(),
        path=str(artifact),
        sha256=digest.hexdigest(),
        size=artifact.stat().st_size,
    )


def write_report_sidecar(
    report: OperationReport,
    destination: str | Path,
    *,
    artifacts: Iterable[ArtifactDigest] = (),
) -> Path:
    """Atomically write a deterministic operation-report sidecar.

    The caller supplies already-computed artifact digests so reading and
    writing remain explicit. The destination parent must exist; a report
    writer never creates an unexpected directory tree.
    """

    if not isinstance(report, OperationReport):
        raise TypeError("report must be an OperationReport.")
    target = Path(destination).expanduser().resolve()
    if not target.parent.is_dir():
        raise FileNotFoundError(f"Report directory does not exist: {target.parent}")
    artifact_values = tuple(artifacts)
    if any(not isinstance(value, ArtifactDigest) for value in artifact_values):
        raise TypeError("artifacts must contain only ArtifactDigest values.")

    payload = {
        "artifacts": [value.to_dict() for value in sorted(artifact_values, key=lambda item: (item.role, item.path))],
        "report": report.to_dict(),
    }
    serialized = json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        indent=2,
        sort_keys=True,
    )
    temporary_name = ""
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=target.parent,
            prefix=f".{target.name}.",
            suffix=".tmp",
            delete=False,
        ) as stream:
            temporary_name = stream.name
            stream.write(serialized)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_name, target)
    finally:
        if temporary_name and os.path.exists(temporary_name):
            os.unlink(temporary_name)
    return target
