"""Generate bundled UUID, flow identity, and quantity references from local inputs."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
from pathlib import Path

from brightpath import DATA_DIR
from brightpath.formats.openlca_methods import OpenLCAMethodMapping, _read_source_rows


def select_fields(record, *fields):
    return {key: record[key] for key in fields if key in record}


def reference_entities(method_package):
    package = OpenLCAMethodMapping._read_entities(method_package)
    return {
        "flows": {
            code: {
                **select_fields(flow, "@id", "name", "category", "flowType"),
                "flowProperties": [
                    {
                        **select_fields(factor, "conversionFactor", "isRefFlowProperty", "referenceFlowProperty"),
                        "flowProperty": select_fields(factor.get("flowProperty", {}), "@id"),
                    }
                    for factor in flow.get("flowProperties", [])
                    if factor.get("isRefFlowProperty") or factor.get("referenceFlowProperty")
                ],
            }
            for code, flow in package["flows"].items()
        },
        "flow_properties": {
            code: {
                **select_fields(prop, "@id", "name"),
                "unitGroup": select_fields(prop.get("unitGroup", {}), "@id"),
            }
            for code, prop in package["flow_properties"].items()
        },
        "unit_groups": {
            code: {
                "units": [
                    select_fields(unit, "@id", "name", "conversionFactor", "isRefUnit", "referenceUnit")
                    for unit in group.get("units", [])
                ]
            }
            for code, group in package["unit_groups"].items()
        },
    }


def generate(version, biosphere_csv, method_package, output_dir):
    policy = "preserve" if version == "3.8" else "error"
    OpenLCAMethodMapping(method_package, biosphere_csv, biosphere_version=version, conflict_policy=policy)
    source = {"filename": method_package.name, "sha256": hashlib.sha256(method_package.read_bytes()).hexdigest()}
    payload = {
        "schema_version": 1,
        "biosphere_version": version,
        "biosphere_source_sha256": hashlib.sha256(biosphere_csv.read_bytes()).hexdigest(),
        "method_package": source,
        "source_rows": _read_source_rows(biosphere_csv),
        "entities": reference_entities(method_package),
    }
    raw = gzip.compress(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode(), mtime=0
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    filename = f"ecoinvent__{version}__methods.json.gz"
    manifest_path = output_dir / "ecoinvent_methods_manifest.json"
    manifest = (
        json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest_path.is_file()
        else {"schema_version": 1, "status": "legal_review_required", "versions": {}}
    )
    manifest["versions"][version] = {
        "file": filename,
        "sha256": hashlib.sha256(raw).hexdigest(),
        "method_package": source,
    }
    (output_dir / filename).write_bytes(raw)
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("version")
    parser.add_argument("biosphere_csv", type=Path)
    parser.add_argument("method_package", type=Path)
    parser.add_argument("--output-dir", type=Path, default=DATA_DIR / "export" / "openlca_references")
    args = parser.parse_args()
    generate(args.version, args.biosphere_csv, args.method_package, args.output_dir)
