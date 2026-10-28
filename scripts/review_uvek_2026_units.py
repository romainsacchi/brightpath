"""Audit UVEK 2025/2026 unit labels against the source workbook and XML.

This read-only review writes reference metadata, counts, hashes and findings.
It does not export inventories, change source files, or approve runtime rules.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
from collections import Counter, defaultdict
from pathlib import Path
from zipfile import ZipFile

from lxml import etree
from openpyxl import load_workbook

from brightpath.units import normalize_unit

FIELDS = ("name", "reference product", "location", "unit")
REL_TOL = 1e-10
ABS_TOL = 1e-14


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def identity(record: dict) -> tuple:
    return tuple(record.get(field, "") for field in FIELDS)


def read_workbook(path: Path, wanted: set[tuple]) -> dict[tuple, dict]:
    """Read selected activity blocks directly, before any import strategies."""
    result = {}
    names = {key[0] for key in wanted}
    workbook = load_workbook(path, read_only=True, data_only=True)

    def retain(dataset):
        key = identity(dataset)
        if key in wanted:
            if key in result:
                raise ValueError(f"Duplicate workbook identity: {key!r}")
            result[key] = dataset

    try:
        for sheet in workbook:
            dataset, headers, exchanges = {}, (), False
            for row in sheet.iter_rows(values_only=True):
                first = row[0] if row else None
                if first == "Activity" and row[1] in names and not any(row[2:]):
                    retain(dataset)
                    dataset, headers, exchanges = {"name": row[1], "exchanges": []}, (), False
                elif first == "Activity" and not any(row[2:]):
                    retain(dataset)
                    dataset, headers, exchanges = {}, (), False
                elif not dataset or first is None:
                    continue
                elif first == "Exchanges":
                    exchanges = True
                elif exchanges and first == "name":
                    headers = tuple(row)
                elif exchanges and headers:
                    dataset["exchanges"].append(
                        {
                            key: value
                            for key, value in zip(headers, row, strict=True)
                            if key is not None and value is not None
                        }
                    )
                elif not exchanges and isinstance(first, str):
                    dataset[first] = row[1]
            retain(dataset)
    finally:
        workbook.close()
    if result.keys() != wanted:
        raise ValueError(f"Workbook is missing {len(wanted - result.keys())} requested identities.")
    return result


def read_xml(raw: bytes) -> etree._Element:
    parser = etree.XMLParser(resolve_entities=False, load_dtd=False, no_network=True)
    root = etree.fromstring(raw, parser=parser)
    if root.getroottree().docinfo.doctype:
        raise ValueError("DTD declarations are unsupported.")
    for element in root.iter():
        if isinstance(element.tag, str):
            element.tag = etree.QName(element).localname
    return root


def flow_number(value) -> str:
    number = float(value)
    if not math.isfinite(number) or not number.is_integer():
        raise ValueError(f"Invalid source flow number: {value!r}")
    return str(int(number))


def coefficient_vector(exchanges: list[dict], reference_amount: float) -> dict:
    vector = defaultdict(list)
    for exchange in exchanges:
        if exchange["type"] in {"production", "technosphere"}:
            vector[(exchange["type"], flow_number(exchange["flow"]))].append(
                float(exchange["amount"]) / reference_amount
            )
    return {key: sorted(values) for key, values in vector.items()}


def equal_vectors(left: dict, right: dict) -> bool:
    return left.keys() == right.keys() and all(
        len(values) == len(right[key])
        and all(math.isclose(a, b, rel_tol=REL_TOL, abs_tol=ABS_TOL) for a, b in zip(values, right[key], strict=True))
        for key, values in left.items()
    )


def _vector_digest(vector: dict) -> str:
    return digest(
        json.dumps([[*key, values] for key, values in sorted(vector.items())], separators=(",", ":")).encode()
    )


def review_record(rule: dict, old: dict, raw: bytes, archive_member: str, supplier_locations: dict) -> dict:
    """Compare stable process/flow identifiers and normalized reference amounts."""
    root = read_xml(raw)
    datasets = root.findall("dataset")
    if len(datasets) != 1:
        raise ValueError("Expected one process per source XML file.")
    dataset = datasets[0]
    reference = dataset.find("metaInformation/processInformation/referenceFunction")
    geography = dataset.find("metaInformation/processInformation/geography")
    if reference is None or geography is None:
        raise ValueError("Missing process reference metadata.")
    native = []
    filled_locations = 0
    for element in dataset.findall("flowData/exchange"):
        production = element.findtext("outputGroup") == "0"
        technosphere = element.findtext("inputGroup") == "5"
        if not (production or technosphere):
            continue
        location = element.get("location")
        if not location:
            location = supplier_locations.get(element.get("number"))
            filled_locations += bool(location)
        native.append(
            {
                "name": element.get("name", "").strip(),
                "location": location or "",
                "flow": element.get("number"),
                "unit": normalize_unit(element.get("unit", "")),
                "amount": float(element.get("meanValue")),
                "type": "production" if production else "technosphere",
            }
        )
    old_outputs = [exchange for exchange in old["exchanges"] if exchange.get("type") == "production"]
    new_outputs = [exchange for exchange in native if exchange["type"] == "production"]
    if len(old_outputs) != 1 or len(new_outputs) != 1:
        raise ValueError("Expected one reference output on each side.")
    old_reference, new_reference = float(old_outputs[0]["amount"]), new_outputs[0]["amount"]
    if not (old_reference > 0 and new_reference > 0):
        raise ValueError("Reference output amounts must be positive.")
    old_vector = coefficient_vector(old["exchanges"], old_reference)
    new_vector = coefficient_vector(native, new_reference)
    old_metadata = {
        (exchange["type"], flow_number(exchange["flow"])): (exchange["name"], exchange.get("location", ""))
        for exchange in old["exchanges"]
        if exchange["type"] in {"production", "technosphere"}
    }
    new_metadata = {
        (exchange["type"], flow_number(exchange["flow"])): (exchange["name"], exchange["location"])
        for exchange in native
    }
    code = flow_number(str(old["code"]).rsplit("-", 1)[-1])
    native_identity = (
        reference.get("name", "").strip(),
        new_outputs[0]["name"],
        geography.get("location", ""),
        normalize_unit(reference.get("unit", "")),
    )
    coefficients_match = equal_vectors(old_vector, new_vector)
    checks = {
        "same_source_filename": Path(archive_member).name == old["filename"],
        "same_source_uuid": Path(archive_member).stem.removeprefix("process_")
        == Path(old["filename"]).stem.removeprefix("process_"),
        "same_process_number": dataset.get("number") == code,
        "same_name_product_location": identity(old)[:3] == native_identity[:3],
        "target_identity_matches_catalog": identity(rule["target"]) == native_identity,
        "reference_output_amount_unchanged": math.isclose(
            old_reference, new_reference, rel_tol=REL_TOL, abs_tol=ABS_TOL
        ),
        "technosphere_flow_ids_and_coefficients_unchanged": coefficients_match,
        "technosphere_names_and_resolved_locations_unchanged": old_metadata == new_metadata,
    }
    nonproduction = sum(exchange["type"] == "technosphere" for exchange in native)
    suspicious = rule["target"]["unit"] == "hour" and nonproduction == 0
    supported = all(checks.values()) and not suspicious
    group = "gas_volume" if rule["source"]["unit"] == "cubic meter" else "time_label"
    comment = str(old.get("comment", ""))
    return {
        "rule_id": rule["id"],
        "source": rule["source"],
        "target": rule["target"],
        "group": group,
        "source_filename": old["filename"],
        "source_code": old["code"],
        "native_xml_sha256": digest(raw),
        "native_reference_unit": reference.get("unit"),
        "checks": checks,
        "old_reference_amount": old_reference,
        "native_reference_amount": new_reference,
        "technosphere_input_exchanges": nonproduction,
        "supplier_locations_resolved_by_number": filled_locations,
        "old_technosphere_vector_sha256": _vector_digest(old_vector),
        "native_technosphere_vector_sha256": _vector_digest(new_vector),
        "old_comment_mentions_normal_volume": bool(re.search(r"Nm[3³]|normal cubic", comment, re.IGNORECASE)),
        "native_documentation_mentions_hours": bool(
            re.search(r"\bhours?\b|\bper h\b", " ".join(reference.attrib.values()), re.IGNORECASE)
        ),
        "status": "supported_label_correspondence" if supported else "manual_review_required",
        "recommended_forward_factor": new_reference / old_reference if supported else None,
        "recommended_reverse_factor": old_reference / new_reference if supported else None,
        "rationale": (
            "Same source process UUID and number, same reference output quantity, same supplier identities and per-reference technosphere coefficients. Factor 1 is a release-specific label correspondence, not a general physical unit conversion."
            if supported
            else (
                "The hour-based record has no technosphere inputs to anchor its operating-time basis. Identical record identifiers do not establish its intended functional unit; publisher/source confirmation is needed."
                if suspicious
                else "At least one source comparison check failed; no factor is recommended."
            )
        ),
    }


def build_review(workbook: Path, archive: Path, resource_path: Path) -> dict:
    """Return a source-bound review of every currently unfactored process rule."""
    resource_bytes = resource_path.read_bytes()
    resource = json.loads(resource_bytes)
    if resource.get("provenance", {}).get("unit_review"):
        raise ValueError(
            "Review requires the historical baseline; generate resources with --unreviewed in a separate directory."
        )
    rules = [
        rule
        for rule in resource["replace"]
        if rule["source"]["unit"] != rule["target"]["unit"] and "conversion_factor" not in rule
    ]
    old = read_workbook(workbook, {identity(rule["source"]) for rule in rules})
    rows = []
    with ZipFile(archive) as zipped:
        members = {Path(name).name: name for name in zipped.namelist() if name.lower().endswith(".xml")}
        if len(members) != sum(name.lower().endswith(".xml") for name in zipped.namelist()):
            raise ValueError("Ambiguous archive basenames.")
        supplier_locations = {}
        for member in sorted(members.values()):
            root = read_xml(zipped.read(member))
            for dataset in root.findall("dataset"):
                geography = dataset.find("metaInformation/processInformation/geography")
                if geography is not None:
                    number, location = dataset.get("number"), geography.get("location")
                    if number in supplier_locations:
                        raise ValueError(f"Duplicate native process number: {number}")
                    supplier_locations[number] = location
        for rule in rules:
            record = old[identity(rule["source"])]
            member = members[record["filename"]]
            rows.append(review_record(rule, record, zipped.read(member), member, supplier_locations))
    rows.sort(key=lambda row: identity(row["source"]))
    summary = {
        "reviewed": len(rows),
        "groups": dict(Counter(row["group"] for row in rows)),
        "statuses": dict(Counter(row["status"] for row in rows)),
        "all_checks_passed": sum(all(row["checks"].values()) for row in rows),
        "normal_volume_comments": sum(row["old_comment_mentions_normal_volume"] for row in rows),
        "native_documentation_mentions_hours": sum(row["native_documentation_mentions_hours"] for row in rows),
        "missing_supplier_locations_resolved": sum(row["supplier_locations_resolved_by_number"] for row in rows),
        "recommended_factors": dict(Counter(str(row["recommended_forward_factor"]) for row in rows)),
    }
    return {
        "schema_version": 1,
        "generator": "scripts/review_uvek_2026_units.py",
        "sources": {
            "workbook": {"file": workbook.name, "sha256": digest(workbook.read_bytes())},
            "native_archive": {"file": archive.name, "sha256": digest(archive.read_bytes())},
            "migration_resource": {"file": resource_path.name, "sha256": digest(resource_bytes)},
        },
        "comparison": {"relative_tolerance": REL_TOL, "absolute_tolerance": ABS_TOL},
        "scope": "Reference-unit review only. Biosphere remapping, LCIA equivalence and physical quality of the source inventories are not established by unchanged technosphere coefficients.",
        "interpretation": "The compared records support release-specific unit-label correspondences with factor 1; this is an inference from matched source identifiers and unchanged per-reference technosphere coefficients, not a publisher-confirmed correction or a general physical unit conversion.",
        "runtime_rules_changed": False,
        "summary": summary,
        "processes": rows,
    }


def write_review(report: dict, output: Path) -> None:
    output.with_suffix(".json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8"
    )
    fields = (
        "name",
        "location",
        "source_unit",
        "target_unit",
        "group",
        "status",
        "recommended_forward_factor",
        "recommended_reverse_factor",
        "source_filename",
        "all_checks_passed",
        "rationale",
    )
    with output.with_suffix(".csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for row in report["processes"]:
            writer.writerow(
                {
                    "name": row["source"]["name"],
                    "location": row["source"]["location"],
                    "source_unit": row["source"]["unit"],
                    "target_unit": row["target"]["unit"],
                    **{
                        field: row[field]
                        for field in (
                            "group",
                            "status",
                            "recommended_forward_factor",
                            "recommended_reverse_factor",
                            "source_filename",
                            "rationale",
                        )
                    },
                    "all_checks_passed": all(row["checks"].values()),
                }
            )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("workbook", type=Path)
    parser.add_argument("archive", type=Path)
    parser.add_argument(
        "--resource",
        type=Path,
        default=Path("brightpath/data/migrations/uvek/uvek-2025-cutoff-to-uvek-2026-cutoff.json"),
    )
    parser.add_argument("--output", type=Path, default=Path("docs/uvek_2026_unit_review"))
    args = parser.parse_args()
    report = build_review(args.workbook, args.archive, args.resource)
    write_review(report, args.output)
    print(json.dumps(report["summary"], indent=2))


if __name__ == "__main__":
    main()
