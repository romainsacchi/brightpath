"""Generate integrity metadata for packaged migration resources."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


def build_manifest(directory: Path) -> dict:
    """Return a deterministic manifest for every migration JSON resource."""

    resources = []
    for path in sorted(directory.rglob("*.json")):
        if path.name == "RESOURCE_MANIFEST.json":
            continue
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
        resources.append(
            {
                "path": path.relative_to(directory).as_posix(),
                "sha256": hashlib.sha256(raw).hexdigest(),
                "size": len(raw),
                "schema_version": int(payload.get("schema_version", 1)),
                "name": str(payload.get("name") or ""),
                "status": str(payload.get("status") or "active"),
                "source_id": str(payload.get("source_id") or ""),
                "target_id": str(payload.get("target_id") or ""),
                "licenses": sorted(
                    str(item.get("name") or "")
                    for item in payload.get("licenses", ())
                    if isinstance(item, dict) and item.get("name")
                ),
            }
        )
    return {
        "schema_version": 1,
        "generator": "scripts/generate_migration_manifest.py",
        "resources": resources,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "directory",
        nargs="?",
        type=Path,
        default=Path("brightpath/data/migrations"),
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
