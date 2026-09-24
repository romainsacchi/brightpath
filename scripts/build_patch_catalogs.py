"""Generate identity-only patch catalogs and a reproducible comparison report."""

import hashlib
import json
from pathlib import Path

import bw2data as bd


def main():
    root = Path(__file__).resolve().parents[1] / "brightpath/data"
    directory = root / "export/reference_catalogs"
    manifest_path = directory / "RESOURCE_MANIFEST.json"
    manifest = json.loads(manifest_path.read_text())
    comparisons = []
    for base, patch in [("3.9", "3.9.1"), ("3.10", "3.10.1")]:
        original = json.loads((directory / f"ecoinvent__{base}__cutoff.json").read_text())
        if patch == "3.9.1":
            bd.projects.set_current("ecoinvent-3.9.1-cutoff")
            technosphere = [
                {
                    "name": node["name"],
                    "reference_product": node["reference product"],
                    "location": node["location"],
                    "unit": node["unit"],
                }
                for node in bd.Database("ecoinvent-3.9.1-cutoff")
            ]
            biosphere_name = "ecoinvent-3.9.1-biosphere"
        else:
            bd.projects.set_current("brightpath-biosphere-enrichment-3.10.1")
            technosphere = original["technosphere"]
            biosphere_name = "ecoinvent-3.10.1-biosphere"
        biosphere = [
            {"name": node["name"], "categories": list(node["categories"]), "unit": node["unit"]}
            for node in bd.Database(biosphere_name)
        ]
        if not technosphere or not biosphere:
            raise ValueError("Missing exact-version reference database.")
        payload = {
            "profile": {"family": "ecoinvent", "version": patch, "system_model": "cutoff"},
            "technosphere": sorted(technosphere, key=lambda row: json.dumps(row, sort_keys=True)),
            "biosphere": sorted(biosphere, key=lambda row: json.dumps(row, sort_keys=True)),
        }
        path = directory / f"ecoinvent__{patch}__cutoff.json"
        raw = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
        path.write_bytes(raw)
        manifest["resources"] = [entry for entry in manifest["resources"] if entry["file"] != path.name]
        manifest["resources"].append(
            {
                "file": path.name,
                "profile": payload["profile"],
                "schema_version": 1,
                "sha256": hashlib.sha256(raw).hexdigest(),
                "size": len(raw),
                "technosphere_identities": len(technosphere),
                "biosphere_identities": len(biosphere),
            }
        )
        for axis in ["technosphere", "biosphere"]:
            before = {json.dumps(row, sort_keys=True) for row in original[axis]}
            after = {json.dumps(row, sort_keys=True) for row in payload[axis]}
            comparisons.append(
                {
                    "source": base,
                    "target": patch,
                    "axis": axis,
                    "shared": len(before & after),
                    "removed": len(before - after),
                    "added": len(after - before),
                    "target_sha256": hashlib.sha256(raw).hexdigest(),
                }
            )
    manifest["resources"].sort(key=lambda row: row["file"])
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    (root.parents[1] / "docs/patch-catalog-comparison.json").write_text(json.dumps(comparisons, indent=2) + "\n")
    print(json.dumps(comparisons, indent=2))


if __name__ == "__main__":
    main()
