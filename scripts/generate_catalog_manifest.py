"""Generate the integrity and provenance manifest for packaged catalogs."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


def build_manifest(directory: Path) -> dict:
    """Return a deterministic manifest for combined reference catalogs."""

    resources = []
    for path in sorted(directory.glob("*__*__*.json")):
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
        profile = payload["profile"]
        technosphere_rows = payload.get("technosphere", ())
        biosphere_rows = payload.get("biosphere", ())
        resources.append(
            {
                "file": path.name,
                "sha256": hashlib.sha256(raw).hexdigest(),
                "size": len(raw),
                "schema_version": int(payload.get("schema_version", 1)),
                "profile": {
                    "family": str(profile["family"]),
                    "version": str(profile["version"]),
                    "system_model": str(profile["system_model"]),
                },
                "technosphere_identities": len(technosphere_rows),
                "biosphere_identities": len(biosphere_rows),
                "technosphere_uuid_references": sum(
                    bool(row.get("activity_id") and row.get("flow_id")) for row in technosphere_rows
                ),
                "biosphere_uuid_references": sum(bool(row.get("flow_id")) for row in biosphere_rows),
            }
        )
    return {
        "schema_version": 1,
        "status": "legal_review_required",
        "description": (
            "Validation identities and ecoSpold2 UUID references generated from locally installed "
            "background releases."
        ),
        "generator": "scripts/generate_reference_catalogs.py",
        "resources": resources,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "directory",
        nargs="?",
        type=Path,
        default=Path("brightpath/data/export/reference_catalogs"),
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    output = args.output or args.directory / "RESOURCE_MANIFEST.json"
    output.write_text(
        json.dumps(build_manifest(args.directory), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
