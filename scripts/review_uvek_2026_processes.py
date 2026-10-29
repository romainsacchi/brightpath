"""Review the seven remaining release identities against original publisher records.

Stable source record identifiers and unchanged reference quantities support
release correspondence. Geography revisions need explicit information-loss
acceptance; changed background coefficients do not establish LCIA equivalence.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path
from zipfile import ZipFile

from brightpath.migrations.schema import intern_review, rule_identifier
from scripts.generate_uvek_2026_resources import TECH_FIELDS, identities
from scripts.review_uvek_2026_biosphere import ROOT, digest
from scripts.review_uvek_2026_land import resource_hash
from scripts.review_uvek_2026_units import identity, read_workbook, review_record

REPORT = ROOT / "docs/uvek_2026_process_review.json"
METHOD = "reviewed_publisher_release_identity"
LABELS = {
    "Rapeseed cultivation {RER} | rapeseed production Europe | Alloc Rec, U": "Rapeseed cultivation",
    "Refined Waste Cooking Oil {RER} | Refining of waste cooking oil Europe | Alloc Rec, U": "Refined Waste Cooking Oil",
    "Reinforcing steel, import to CH, at plant": "Reinforcing steel, import to CH",
    **{
        f"Wind turbine 2.3MW, onshore, {part}": f"Wind turbine 2.3MW, onshore, {part}"
        for part in ("foundation", "moving parts", "tower, concrete", "tower, steel")
    },
}
REQUIRED_CHECKS = (
    "same_source_filename",
    "same_source_uuid",
    "same_process_number",
    "target_identity_matches_catalog",
    "reference_output_amount_unchanged",
)


def validate_decision(row, old_catalog, native_catalog):
    source, target = row["source"], row["target"]
    wind = source["name"].startswith("Wind turbine 2.3MW, onshore,")
    revised = source["name"] == "Reinforcing steel, import to CH, at plant"
    if (
        LABELS.get(source["name"]) != target["name"]
        or source["reference product"] != source["name"]
        or target["reference product"] != target["name"]
        or (source["location"], target["location"]) != (("CH", "RER") if wind else ("RER", "RER"))
        or source["unit"] != target["unit"]
        or source["unit"] != ("unit" if wind else "kilogram" if revised else "megajoule")
        or identity(source) not in old_catalog
        or identity(target) not in native_catalog
        or any(row["checks"].get(check) is not True for check in REQUIRED_CHECKS)
        or row["old_reference_amount"] != row["native_reference_amount"]
        or row["old_reference_amount"] <= 0
        or row["checks"]["technosphere_flow_ids_and_coefficients_unchanged"] != (not revised)
        or row["background_coefficients_revised"] != revised
        or row["geography_revised"] != wind
    ):
        raise ValueError(
            "Process correspondence lacks reviewed publisher identity, reference quantity or geography evidence."
        )


def apply_process_review(resources, path, catalog_directory):
    report = json.loads(path.read_text())
    sources = report["sources"]
    if report.get("schema_version") != 1:
        raise ValueError("Unsupported process review schema.")
    unit_review = json.loads((ROOT / "docs/uvek_2026_unit_review.json").read_text())
    if sources["workbook_sha256"] != unit_review["sources"]["workbook"]["sha256"]:
        raise ValueError("Process review workbook digest mismatch.")
    catalogs = []
    for family, filename in (("old", "uvek__2025__cutoff.json"), ("native", "uvek__2026__cutoff.json")):
        catalog_path = catalog_directory / filename
        if sources[f"{family}_catalog_sha256"] != digest(catalog_path):
            raise ValueError("Process review exact catalog digest mismatch.")
        catalogs.append(identities(json.loads(catalog_path.read_text()), "technosphere"))
    rows = report["decisions"]
    if len(rows) != len(LABELS) or {row["source"]["name"] for row in rows} != LABELS.keys():
        raise ValueError("Process review must cover exactly the seven authored identities.")
    for row in rows:
        validate_decision(row, *catalogs)
    result = deepcopy(resources)
    for direction, version in (("forward", "2025"), ("reverse", "2026")):
        resource = next(
            row for row in result if row["axis"] == "technosphere" and row["source_profiles"][0]["version"] == version
        )
        if sources[f"baseline_{direction}_sha256"] != resource_hash(resource):
            raise ValueError("Process review directional baseline mismatch.")
        if sources["native_archive_sha256"] != resource["provenance"]["sha256"]:
            raise ValueError("Process review archive digest mismatch.")
        review_ref = intern_review(
            resource,
            {
                "rationale": "Individually reviewed publisher record UUID/number, reference unit and positive output quantity across UVEK releases; same functional demand under the recorded label/geography revision.",
                "evidence": [
                    f"{path.name} SHA-256 {digest(path)}",
                    f"Source workbook SHA-256 {sources['workbook_sha256']}",
                    f"Native archive SHA-256 {sources['native_archive_sha256']}",
                ],
                "limitations": [
                    "A release correspondence does not assert unchanged background coefficients or LCIA results.",
                    "Four wind component records change geographic scope between CH and RER; each direction requires explicit information-loss acceptance.",
                    "No washing-machine unit conversion is inferred.",
                ],
            },
        )
        unresolved = {identity(row) for row in resource["unresolved"]}
        added = set()
        for row in rows:
            source, target = (
                (row["source"], row["target"]) if direction == "forward" else (row["target"], row["source"])
            )
            if identity(source) not in unresolved or identity(source) in added:
                raise ValueError("Process review source already has a release mapping.")
            added.add(identity(source))
            rule = {
                "id": rule_identifier(resource["name"], source),
                "source": source,
                "target": target,
                "classification": "correspondence",
                "mapping_method": METHOD,
                "review_ref": review_ref,
                "report_correspondence": True,
            }
            if row["geography_revised"]:
                rule["technosphere_information_loss"] = (
                    f"The publisher component record changes geographic scope from {source['location']} to {target['location']}; geographic representativeness is not equivalent."
                )
            resource["replace"].append(rule)
        resource["replace"].sort(key=lambda row: identity(row["source"]))
        resource["unresolved"] = [row for row in resource["unresolved"] if identity(row) not in added]
        resource["coverage"].update(
            mapped_identities=len(resource["replace"]),
            unresolved_identities=len(resource["unresolved"]),
            mapping_methods=dict(sorted(Counter(row["mapping_method"] for row in resource["replace"]).items())),
            information_loss_rules=sum("technosphere_information_loss" in row for row in resource["replace"]),
        )
        resource["provenance"]["process_review"] = {"file": path.name, "sha256": digest(path), "sources": sources}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("workbook", type=Path)
    parser.add_argument("archive", type=Path)
    parser.add_argument("--output", type=Path, default=REPORT)
    args = parser.parse_args()
    from scripts.generate_uvek_2026_resources import build_resources

    directory = ROOT / "brightpath/data/export/reference_catalogs"
    _catalog, resources = build_resources(args.archive, directory, process_review=None)
    by_direction = {
        "forward" if row["source_profiles"][0]["version"] == "2025" else "reverse": row
        for row in resources
        if row["axis"] == "technosphere"
    }
    baseline = by_direction["forward"]
    sources = [row for row in baseline["unresolved"] if row["name"] in LABELS]
    records = read_workbook(args.workbook, {identity(row) for row in sources})

    def catalog_records(filename):
        return [
            dict(zip(TECH_FIELDS, key, strict=True))
            for key in sorted(identities(json.loads((directory / filename).read_text()), "technosphere"))
        ]

    native_catalog = catalog_records("uvek__2026__cutoff.json")
    old_catalog = catalog_records("uvek__2025__cutoff.json")
    decisions = []
    with ZipFile(args.archive) as archive:
        members = {Path(name).name: name for name in archive.namelist() if name.lower().endswith(".xml")}
        for source in sources:
            target = next(
                row for row in native_catalog if row["name"] == LABELS[source["name"]] and row["location"] == "RER"
            )
            record = records[identity(source)]
            member = members[record["filename"]]
            comparison = review_record(
                {"id": "release-identity-review", "source": source, "target": target},
                record,
                archive.read(member),
                member,
                {},
            )
            # Unit-review support status is intentionally discarded: that review
            # requires identical labels and locations, which is not this scope.
            row = {
                key: comparison[key]
                for key in (
                    "source",
                    "target",
                    "source_filename",
                    "source_code",
                    "native_xml_sha256",
                    "checks",
                    "old_reference_amount",
                    "native_reference_amount",
                    "old_technosphere_vector_sha256",
                    "native_technosphere_vector_sha256",
                )
            }
            row.update(
                geography_revised=source["location"] != target["location"],
                background_coefficients_revised=not row["checks"]["technosphere_flow_ids_and_coefficients_unchanged"],
            )
            validate_decision(
                row, {identity(item) for item in old_catalog}, {identity(item) for item in native_catalog}
            )
            decisions.append(row)
    report = {
        "schema_version": 1,
        "generator": "scripts/review_uvek_2026_processes.py",
        "sources": {
            "native_archive_sha256": digest(args.archive),
            "workbook_sha256": digest(args.workbook),
            "old_catalog_sha256": digest(directory / "uvek__2025__cutoff.json"),
            "native_catalog_sha256": digest(directory / "uvek__2026__cutoff.json"),
            **{f"baseline_{key}_sha256": resource_hash(row) for key, row in by_direction.items()},
            "baseline_coverage": {key: row["coverage"] for key, row in by_direction.items()},
        },
        "summary": {
            "reviewed_processes": len(decisions),
            "geographic_revisions": sum(row["geography_revised"] for row in decisions),
            "background_revisions": sum(row["background_coefficients_revised"] for row in decisions),
        },
        "decisions": decisions,
    }
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False) + "\n")
    print(json.dumps(report["summary"], sort_keys=True))


if __name__ == "__main__":
    main()
