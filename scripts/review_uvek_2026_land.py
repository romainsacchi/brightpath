"""Review seven exact modern land labels independently in each direction.

The source publisher uses both old and modern labels. This batch accepts only
the complete modern label in the precise land compartment, with matching units
and the published UUID. It does not infer land classes or reverse water splits.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import dumps_migration_resource, intern_review, rule_identifier
from brightpath.units import normalize_unit
from scripts.review_uvek_2026_biosphere import (
    ROOT,
    WORKBOOK_SHA256,
    WORKBOOK_URL,
    digest,
    identity,
    native_metadata,
    workbook_rows,
)
from scripts.review_uvek_2026_radioactivity import REPORT as PARENT_REVIEW

REPORT = ROOT / "docs/uvek_2026_land_review.json"
METHOD = "reviewed_exact_modern_land_identity"
# Explicit review scope, with publisher row and land classification. An exact
# name outside this list is not approval to change a chemical or a land class.
LAND = {
    "Occupation, industrial area": (1704, "CORINE 121"),
    "Occupation, traffic area, rail/road embankment": (1723, "CORINE 122b"),
    "Transformation, from pasture, man made": (1794, "CORINE 231"),
    "Transformation, from seabed, unspecified": (1804, "CORINE 523"),
    "Transformation, from unspecified": (1811, "CORINE x"),
    "Transformation, to industrial area": (1834, "CORINE 121"),
    "Transformation, to traffic area, rail/road embankment": (1853, "CORINE 122b"),
}


def resource_hash(resource):
    return hashlib.sha256(dumps_migration_resource(resource).encode()).hexdigest()


def validate_decision(decision, native_catalog, ecoinvent_catalog):
    """Require a unique exact destination for this direction, without scaling."""
    native, current, row = decision["native"], decision["ecoinvent"], decision["publisher_row"]
    name = native["name"]
    expected = LAND.get(name)
    native_matches = [item for item in native_catalog if identity(item) == identity(native)]
    current_matches = [item for item in ecoinvent_catalog if identity(item) == identity(current)]
    if (
        expected is None
        or (row["row"], row["Formula"]) != expected
        or row["Elementary Flow Name v3"] != name
        or row["Category v2.2"].lower() != "resource"
        or row["SubCategory v2.2"].lower() != "land"
        or row["Compartment v3"] != "natural resource"
        or row["Subcompartment v3"] != "land"
        or native["categories"] != ["resources", "land"]
        or current["categories"] != ["natural resource", "land"]
        or name != current["name"]
        or native["unit"] != current["unit"]
        or normalize_unit(row["Unit"]) != native["unit"]
        or native["unit"] != ("square meter-year" if name.startswith("Occupation,") else "square meter")
        or decision["source_cas"]
        or decision["source_formulas"]
        or row["CAS"]
        or len(native_matches) != 1
        or len(current_matches) != 1
        or current_matches[0]["flow_id"] != current.get("uuid")
        or row["UUID Elementary Flow v3"] != current.get("uuid")
        or "conversion_factor" in decision
        or "biosphere_information_loss" in decision
        or decision.get("direction") not in {"forward", "reverse"}
    ):
        raise ValueError("Land correspondence fails exact publisher, unit or compartment checks.")


def apply_land_review(resources, path, catalog_directory):
    """Add explicitly authored directions; leave every existing rule unchanged."""
    report = json.loads(path.read_text())
    sources = report["sources"]
    if report.get("schema_version") != 1 or sources["parent_review_sha256"] != digest(PARENT_REVIEW):
        raise ValueError("Land review parent or schema mismatch.")
    if sources["publisher_workbook"]["sha256"] != WORKBOOK_SHA256:
        raise ValueError("Land review publisher digest mismatch.")
    catalogs = {}
    for family, filename in (("native", "uvek__2026__cutoff.json"), ("ecoinvent", "ecoinvent__3.10__cutoff.json")):
        catalog_path = catalog_directory / filename
        if sources[f"{family}_catalog_sha256"] != digest(catalog_path):
            raise ValueError("Land review exact catalog digest mismatch.")
        catalogs[family] = json.loads(catalog_path.read_text())["biosphere"]
    result = deepcopy(resources)
    for direction, family in (("forward", "uvek"), ("reverse", "ecoinvent")):
        resource = next(
            row for row in result if row["axis"] == "biosphere" and row["source_profiles"][0]["family"] == family
        )
        if sources[f"baseline_{direction}_sha256"] != resource_hash(resource):
            raise ValueError("Land review directional baseline mismatch.")
        if sources["native_archive_sha256"] != resource["provenance"]["sha256"]:
            raise ValueError("Land review archive mismatch.")
        review_ref = intern_review(
            resource,
            {
                "rationale": "Exact modern publisher land label, precise land compartment, unchanged unit and published UUID, independently checked for this direction.",
                "evidence": [f"{path.name} SHA-256 {digest(path)}", WORKBOOK_URL],
                "limitations": [
                    "Only the seven explicitly reviewed modern land labels are eligible.",
                    "No land-class aggregation, unspecified-compartment allocation or inverse water mapping is inferred.",
                ],
            },
        )
        unresolved = {identity(row) for row in resource["unresolved"]}
        added = set()
        for decision in report["decisions"]:
            if decision["direction"] != direction:
                continue
            validate_decision(decision, catalogs["native"], catalogs["ecoinvent"])
            source, target = (
                (decision["native"], decision["ecoinvent"])
                if direction == "forward"
                else (decision["ecoinvent"], decision["native"])
            )
            key = identity(source)
            if key not in unresolved or key in added:
                raise ValueError("Land review duplicates an existing or reviewed source.")
            added.add(key)
            resource["replace"].append(
                {
                    "id": rule_identifier(resource["name"], source),
                    "source": source,
                    "target": target,
                    "classification": "correspondence",
                    "mapping_method": METHOD,
                    "review_ref": review_ref,
                    "report_correspondence": True,
                }
            )
        if len(added) != report["summary"][f"{direction}_additions"]:
            raise ValueError("Land review directional total mismatch.")
        resource["replace"].sort(key=lambda row: identity(row["source"]))
        resource["unresolved"] = [row for row in resource["unresolved"] if identity(row) not in added]
        resource["coverage"].update(
            mapped_identities=len(resource["replace"]),
            unresolved_identities=len(resource["unresolved"]),
            mapping_methods=dict(sorted(Counter(row["mapping_method"] for row in resource["replace"]).items())),
            directional_additions=resource["coverage"].get("directional_additions", 0) + len(added),
        )
        resource["provenance"]["land_review"] = {"file": path.name, "sha256": digest(path), "sources": sources}
        if not any(row["name"] == "Source publisher terms" for row in resource["licenses"]):
            resource["licenses"].append(
                {
                    "name": "Source publisher terms",
                    "scope": "ecoinvent correspondence reference metadata; no standalone reuse license is asserted",
                    "path": WORKBOOK_URL,
                }
            )
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("workbook", type=Path)
    parser.add_argument("--output", type=Path, default=REPORT)
    args = parser.parse_args()
    from scripts.generate_uvek_2026_resources import build_resources

    directory = ROOT / "brightpath/data/export/reference_catalogs"
    _catalog, resources = build_resources(args.archive, directory, land_review=None)
    resources = [row for row in resources if row["axis"] == "biosphere"]
    by_direction = {"forward" if row["source_profiles"][0]["family"] == "uvek" else "reverse": row for row in resources}
    catalog_paths = {
        "native": directory / "uvek__2026__cutoff.json",
        "ecoinvent": directory / "ecoinvent__3.10__cutoff.json",
    }
    catalogs = {key: json.loads(path.read_text())["biosphere"] for key, path in catalog_paths.items()}
    metadata, rows = native_metadata(args.archive), workbook_rows(args.workbook)
    decisions = []
    for name, (number, _classification) in LAND.items():
        native = next(
            row for row in catalogs["native"] if row["name"] == name and row["categories"] == ["resources", "land"]
        )
        publisher = next(row for row in rows if row["row"] == number)
        current = next(
            row for row in catalogs["ecoinvent"] if row.get("flow_id") == publisher["UUID Elementary Flow v3"]
        )
        current = {key: current[key] for key in ("name", "categories", "unit")} | {"uuid": current["flow_id"]}
        meta = metadata[identity(native)]
        for direction, resource in by_direction.items():
            source = native if direction == "forward" else current
            if identity(source) not in {identity(row) for row in resource["unresolved"]}:
                continue
            decision = {
                "direction": direction,
                "native": native,
                "ecoinvent": current,
                "source_cas": sorted(meta["cas"]),
                "source_formulas": sorted(meta["formulas"]),
                "native_exchange_uses": meta["uses"],
                "publisher_row": publisher,
            }
            validate_decision(decision, catalogs["native"], catalogs["ecoinvent"])
            decisions.append(decision)
    sources = {
        "native_archive_sha256": digest(args.archive),
        "parent_review_sha256": digest(PARENT_REVIEW),
        "publisher_workbook": {"url": WORKBOOK_URL, "sha256": digest(args.workbook)},
        **{f"{key}_catalog_sha256": digest(path) for key, path in catalog_paths.items()},
        **{f"baseline_{key}_sha256": resource_hash(resource) for key, resource in by_direction.items()},
        "baseline_coverage": {key: row["coverage"] for key, row in by_direction.items()},
        "baseline_licenses": {key: row["licenses"] for key, row in by_direction.items()},
    }
    summary = {
        f"{direction}_additions": sum(row["direction"] == direction for row in decisions) for direction in by_direction
    }
    report = {
        "schema_version": 1,
        "generator": "scripts/review_uvek_2026_land.py",
        "sources": sources,
        "summary": summary,
        "decisions": decisions,
    }
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False) + "\n")
    with args.output.with_suffix(".csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "direction",
                "name",
                "native_categories",
                "ecoinvent_categories",
                "unit",
                "uuid",
                "publisher_row",
                "native_exchange_uses",
            ]
        )
        for row in decisions:
            writer.writerow(
                [
                    row["direction"],
                    row["native"]["name"],
                    " / ".join(row["native"]["categories"]),
                    " / ".join(row["ecoinvent"]["categories"]),
                    row["native"]["unit"],
                    row["ecoinvent"]["uuid"],
                    row["publisher_row"]["row"],
                    row["native_exchange_uses"],
                ]
            )
    print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
