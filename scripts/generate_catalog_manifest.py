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
                "technosphere_identities": len(payload.get("technosphere", ())),
                "biosphere_identities": len(payload.get("biosphere", ())),
            }
        )
    return {
        "schema_version": 1,
        "status": "legal_review_required",
        "description": "Identity-only validation catalogs generated from locally installed background databases.",
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
