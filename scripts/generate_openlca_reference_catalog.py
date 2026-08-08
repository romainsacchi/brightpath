"""Generate exact openLCA background-link references from Brightway metadata.

The raw reference input is a JSON object with ``processes`` and ``flows``
arrays exported from the target openLCA database. Process rows are keyed by
``process_ref_id`` and elementary-flow rows by ``flow_ref_id``. Brightway
provides the software-neutral identities: UVEK activities retain their source
openLCA process UUID in ``filename``, and ecoinvent biosphere flow codes retain
the openLCA flow UUID.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import bw2data as bd

_PROCESS_FILENAME = re.compile(r"^process_([0-9a-fA-F-]{36})\.xml$")


def _required(row: dict, field: str, *, label: str) -> str:
    value = str(row.get(field) or "").strip()
    if not value:
        raise ValueError(f"{label} is missing {field!r}.")
    return value


def _process_id(activity) -> str:
    filename = str(activity.get("filename") or "").strip()
    match = _PROCESS_FILENAME.fullmatch(filename)
    if match is None:
        raise ValueError(f"UVEK activity {activity.key!r} has no openLCA process UUID in filename={filename!r}.")
    return match.group(1).lower()


def generate_catalog(
    *,
    raw_references: dict,
    technosphere_database: str,
    biosphere_database: str,
    technosphere_profile: dict[str, str],
    biosphere_profile: dict[str, str],
    source: str,
    allow_missing_biosphere: bool = False,
) -> dict:
    """Join exact openLCA references to Brightway background identities."""

    process_rows = {
        _required(row, "process_ref_id", label="openLCA process row").lower(): row
        for row in raw_references.get("processes", [])
    }
    flow_rows = {
        _required(row, "flow_ref_id", label="openLCA flow row").lower(): row for row in raw_references.get("flows", [])
    }

    technosphere = []
    for activity in bd.Database(technosphere_database):
        process_id = _process_id(activity)
        try:
            row = process_rows[process_id]
        except KeyError as error:
            raise ValueError(f"No openLCA process reference was found for {activity.key!r} ({process_id}).") from error
        technosphere.append(
            {
                "name": str(activity.get("name") or ""),
                "reference_product": str(activity.get("reference product") or ""),
                "location": str(activity.get("location") or ""),
                "unit": str(activity.get("unit") or ""),
                "process_id": process_id,
                "process_name": _required(row, "process_name", label=f"openLCA process {process_id}"),
                "flow_id": _required(row, "flow_ref_id", label=f"openLCA process {process_id}"),
                "flow_name": _required(row, "flow_name", label=f"openLCA process {process_id}"),
                "flow_property_id": _required(row, "flow_property_ref_id", label=f"openLCA process {process_id}"),
                "flow_property_name": _required(row, "flow_property_name", label=f"openLCA process {process_id}"),
                "unit_id": _required(row, "unit_ref_id", label=f"openLCA process {process_id}"),
                "unit_name": _required(row, "unit", label=f"openLCA process {process_id}"),
                "location_id": _required(row, "location_ref_id", label=f"openLCA process {process_id}"),
            }
        )

    biosphere = []
    missing_biosphere = []
    for flow in bd.Database(biosphere_database):
        flow_id = str(flow.get("code") or "").strip().lower()
        row = flow_rows.get(flow_id)
        if row is None:
            missing_biosphere.append(flow.key)
            continue
        biosphere.append(
            {
                "name": str(flow.get("name") or ""),
                "categories": [str(value) for value in flow.get("categories", ())],
                "unit": str(flow.get("unit") or ""),
                "flow_id": flow_id,
                "flow_name": _required(row, "flow_name", label=f"openLCA flow {flow_id}"),
                "flow_property_id": _required(row, "flow_property_ref_id", label=f"openLCA flow {flow_id}"),
                "flow_property_name": _required(row, "flow_property_name", label=f"openLCA flow {flow_id}"),
                "unit_id": _required(row, "unit_ref_id", label=f"openLCA flow {flow_id}"),
                "unit_name": _required(row, "unit", label=f"openLCA flow {flow_id}"),
            }
        )

    if missing_biosphere and not allow_missing_biosphere:
        sample = ", ".join(repr(key) for key in missing_biosphere[:5])
        raise ValueError(
            f"The openLCA target is missing {len(missing_biosphere)} Brightway biosphere flows; "
            f"examples: {sample}. Pass --allow-missing-biosphere to record partial exact coverage."
        )

    technosphere.sort(key=lambda row: (row["name"], row["reference_product"], row["location"], row["unit"]))
    biosphere.sort(key=lambda row: (row["name"], row["categories"], row["unit"]))
    return {
        "schema_version": 1,
        "format": "openlca_jsonld",
        "profile": {
            "technosphere": technosphere_profile,
            "biosphere": biosphere_profile,
        },
        "source": source,
        "coverage": {
            "technosphere_references": len(technosphere),
            "biosphere_references": len(biosphere),
            "missing_biosphere_references": len(missing_biosphere),
        },
        "technosphere": technosphere,
        "biosphere": biosphere,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-references", required=True, type=Path)
    parser.add_argument("--project", required=True)
    parser.add_argument("--technosphere-database", required=True)
    parser.add_argument("--biosphere-database", required=True)
    parser.add_argument("--technosphere-family", required=True)
    parser.add_argument("--technosphere-version", required=True)
    parser.add_argument("--system-model", required=True)
    parser.add_argument("--biosphere-family", required=True)
    parser.add_argument("--biosphere-version", required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--allow-missing-biosphere", action="store_true")
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    bd.projects.set_current(args.project)
    raw_references = json.loads(args.raw_references.read_text(encoding="utf-8"))
    payload = generate_catalog(
        raw_references=raw_references,
        technosphere_database=args.technosphere_database,
        biosphere_database=args.biosphere_database,
        technosphere_profile={
            "family": args.technosphere_family,
            "version": args.technosphere_version,
            "system_model": args.system_model,
        },
        biosphere_profile={"family": args.biosphere_family, "version": args.biosphere_version},
        source=args.source,
        allow_missing_biosphere=args.allow_missing_biosphere,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    print(
        f"Wrote {args.output} with {len(payload['technosphere'])} technosphere and "
        f"{len(payload['biosphere'])} biosphere references."
    )


if __name__ == "__main__":
    main()
