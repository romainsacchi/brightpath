"""Audit native SimaPro flow representation without copying inventory contents.

Usage: python scripts/audit_simapro_flow_representation.py reference.csv --output audit.json
Add --excluded-report to replay a Premise export's reported exclusions as small,
synthetic Brightpath rendering probes. This is not a full database re-export.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter
from pathlib import Path

from brightpath import BackgroundProfile, SimaProInventory
from brightpath.profiles.simapro_biosphere import FINAL_WASTE_NAMES, REVIEWED_ECOINVENT_FLOW_UNITS, TURBINE_WATER

SECTIONS = {"Resources", "Emissions to air", "Emissions to water", "Emissions to soil", "Final waste flows"}


def audit_reference(path: Path) -> dict:
    """Count actual process exchange rows, excluding comments and metadata."""
    flows = Counter()
    nonzero = Counter()
    categories = Counter()
    process_count = 0
    active = False
    section = None
    names = (
        set(REVIEWED_ECOINVENT_FLOW_UNITS)
        | FINAL_WASTE_NAMES
        | {"Hazardous waste disposed", "Non-hazardous waste disposed"}
    )
    with path.open(encoding="latin-1", newline="") as stream:
        for row in csv.reader(stream, delimiter=";"):
            if not row or not any(row):
                section = None
                continue
            if section is None:
                section = row[0]
                if section == "Process":
                    process_count += 1
                    active = True
                elif section == "End":
                    active = False
                continue
            if not active:
                continue
            if section == "Category type":
                categories[row[0]] += 1
            if section not in SECTIONS or len(row) < 4:
                continue
            name = TURBINE_WATER if row[0].startswith(TURBINE_WATER + ", ") else row[0]
            if name not in names:
                continue
            key = (name, section, row[1], row[2])
            flows[key] += 1
            try:
                if float(row[3]) != 0:
                    nonzero[key] += 1
            except ValueError:
                pass  # Do not evaluate inventory formulas.
    with path.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    return {
        "source_sha256": digest,
        "process_count": process_count,
        "category_types": dict(categories),
        "flows": [
            dict(name=k[0], section=k[1], subcompartment=k[2], unit=k[3], rows=n, nonzero_numeric_rows=nonzero[k])
            for k, n in sorted(flows.items())
        ],
    }


def replay_exclusions(report_path: Path) -> dict:
    """Probe each excluded flow identity using synthetic amounts and processes."""
    report = json.loads(report_path.read_text())
    counts = Counter((e["name"], tuple(e["categories"]), e["unit"]) for e in report["excluded_exchanges"])
    results = []
    for (name, categories, unit), occurrences in sorted(counts.items()):
        inventory = SimaProInventory.from_data(
            [
                {
                    "name": "flow representation probe",
                    "reference product": "test product",
                    "location": "GLO",
                    "unit": "kilogram",
                    "comment": "Synthetic audit; no proprietary inventory amounts.",
                    "exchanges": [
                        {
                            "type": "production",
                            "name": "flow representation probe",
                            "reference product": "test product",
                            "location": "GLO",
                            "unit": "kilogram",
                            "amount": 1,
                            "simapro category": "material/Test",
                        },
                        {"type": "biosphere", "name": name, "categories": categories, "unit": unit, "amount": 1},
                    ],
                }
            ],
            background_profile=BackgroundProfile("ecoinvent", report["source_version"], report["system_model"]),
        )
        rendered = inventory.render()
        represented = not rendered.has_errors and not any(i.code == "simapro_exchange_unused" for i in rendered.issues)
        results.append(
            {
                "name": name,
                "categories": categories,
                "unit": unit,
                "occurrences": occurrences,
                "represented": represented,
                "issue_codes": sorted({i.code for i in rendered.issues}),
            }
        )
    return {
        "mode": "synthetic flow-identity replay; not a complete inventory re-export",
        "results": results,
        "represented_occurrences": sum(x["occurrences"] for x in results if x["represented"]),
        "unresolved_occurrences": sum(x["occurrences"] for x in results if not x["represented"]),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reference", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--excluded-report", type=Path)
    args = parser.parse_args()
    result = audit_reference(args.reference)
    if args.excluded_report:
        result["exclusion_replay"] = replay_exclusions(args.excluded_report)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(f"Audited {result['process_count']} native processes; report: {args.output}")
    if "exclusion_replay" in result:
        print({key: value for key, value in result["exclusion_replay"].items() if key != "results"})


if __name__ == "__main__":
    main()
