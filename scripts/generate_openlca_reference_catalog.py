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
import hashlib
import json
import re
import uuid
from pathlib import Path

import bw2data as bd

_PROCESS_FILENAME = re.compile(r"^process_([0-9a-fA-F-]{36})\.xml$")
_CORRESPONDENCE_PATH = (
    Path(__file__).resolve().parents[1] / "brightpath/data/export/openlca_references/biosphere_correspondences.json"
)
_NATIVE_CORRESPONDENCE_FIELDS = (
    "flow_ref_id",
    "flow_name",
    "category_path",
    "flow_type",
    "flow_property_ref_id",
    "flow_property_name",
    "unit_ref_id",
    "unit",
    "unit_group_ref_id",
    "reference_unit_factor",
)


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


def _correspondence_index(profile: dict, supplied: dict | None) -> tuple[dict, dict]:
    """Load only integrity-pinned, exact-profile, explicitly reviewed UUID correspondences."""
    if supplied is None:
        raw = _CORRESPONDENCE_PATH.read_bytes()
        manifest = json.loads((_CORRESPONDENCE_PATH.parent / "RESOURCE_MANIFEST.json").read_text())
        entry = manifest["biosphere_correspondences"]
        if (
            entry["file"] != _CORRESPONDENCE_PATH.name
            or entry["size"] != len(raw)
            or entry["sha256"] != hashlib.sha256(raw).hexdigest()
        ):
            raise ValueError("Biosphere correspondence metadata does not match its integrity manifest.")
        bundle = json.loads(raw)
        if bundle.get("profile") != profile:
            return {}, {}
        if entry["profile"] != profile or entry["rows"] != len(bundle.get("mappings", [])):
            raise ValueError("Biosphere correspondence profile/count conflicts with its manifest.")
        from brightpath.formats.openlca_quantity_projection import reviewed_mapping

        quantity_bundle = reviewed_mapping()
        if quantity_bundle["profile"] == profile:
            if quantity_bundle["native_source"] != bundle["native_source"]:
                raise ValueError("Quantity correspondence native source build mismatch.")
            bundle["mappings"] = [*bundle["mappings"], *quantity_bundle["mappings"]]
    else:
        bundle = supplied
        if bundle.get("profile") != profile:
            raise ValueError("Biosphere correspondence profile does not match the requested exact profile.")
    if bundle.get("schema_version") != 1:
        raise ValueError("Unsupported biosphere correspondence schema.")
    index = {}
    native_ids = set()
    for mapping in bundle["mappings"]:
        source_id = str(uuid.UUID(mapping["source"]["flow_id"]))
        native_id = str(uuid.UUID(mapping["native"]["flow_ref_id"]))
        if source_id in index or native_id in native_ids:
            raise ValueError("Ambiguous source or native biosphere correspondence UUID.")
        index[source_id] = mapping
        native_ids.add(native_id)
    return index, bundle["native_source"]


def _corresponding_native_row(flow, mapping: dict, rows: list[dict], raw_source: dict, expected_source: dict) -> dict:
    """Check source identity and all recorded native quantity references before insertion."""
    if raw_source != expected_source:
        raise ValueError("Native source build does not match the verified biosphere correspondence build.")
    expected_identity = {key: mapping["source"][key] for key in ("name", "categories", "unit")}
    actual_identity = {
        "name": flow.get("name"),
        "categories": list(flow.get("categories", ())),
        "unit": flow.get("unit"),
    }
    if actual_identity != expected_identity:
        raise ValueError("Source biosphere identity does not match its verified UUID correspondence.")
    if len(rows) != 1:
        raise ValueError("Expected exactly one native flow for the verified correspondence UUID.")
    row = rows[0]
    expected = mapping["native"]
    for field in _NATIVE_CORRESPONDENCE_FIELDS:
        if field not in row or row[field] != expected[field]:
            raise ValueError(f"Native biosphere correspondence {field!r} conflicts with the recorded native metadata.")
    if (
        row["flow_type"] != "ELEMENTARY_FLOW"
        or row["reference_unit_factor"] != 1.0
        or row["unit"] != "kg"
        or row["flow_property_name"] != "Mass"
        or flow.get("unit") != "kilogram"
    ) and not mapping.get("amount_factor"):
        raise ValueError("Biosphere correspondence requires the verified mass reference without conversion.")
    if mapping.get("amount_factor"):
        from brightpath.formats.openlca_quantity_projection import reviewed_mapping

        if mapping != reviewed_mapping()["mappings"][0]:
            raise ValueError("Unreviewed native quantity correspondence.")
    return row


def generate_catalog(
    *,
    raw_references: dict,
    technosphere_database: str,
    biosphere_database: str,
    technosphere_profile: dict[str, str],
    biosphere_profile: dict[str, str],
    source: str,
    allow_missing_biosphere: bool = False,
    biosphere_correspondences: dict | None = None,
) -> dict:
    """Join exact openLCA references to Brightway background identities."""

    process_rows = {
        _required(row, "process_ref_id", label="openLCA process row").lower(): row
        for row in raw_references.get("processes", [])
    }
    flow_rows = {
        _required(row, "flow_ref_id", label="openLCA flow row").lower(): row for row in raw_references.get("flows", [])
    }
    indexed_native_rows = {}
    for row in raw_references.get("flows", []):
        indexed_native_rows.setdefault(_required(row, "flow_ref_id", label="openLCA flow row").lower(), []).append(row)
    correspondences, native_source = _correspondence_index(
        {"technosphere": technosphere_profile, "biosphere": biosphere_profile}, biosphere_correspondences
    )

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
                "category": _required(row, "category", label=f"openLCA process {process_id}"),
            }
        )

    biosphere = []
    missing_biosphere = []
    for flow in bd.Database(biosphere_database):
        flow_id = str(flow.get("code") or "").strip().lower()
        row = flow_rows.get(flow_id)
        mapping = None
        if flow_id in correspondences:
            if row is not None:
                raise ValueError("Source UUID unexpectedly also exists in the pinned native correspondence build.")
            mapping = correspondences[flow_id]
            row = _corresponding_native_row(
                flow,
                mapping,
                indexed_native_rows.get(mapping["native"]["flow_ref_id"], []),
                raw_references.get("native_source", {}),
                native_source,
            )
        if row is None:
            missing_biosphere.append(flow.key)
            continue
        biosphere.append(
            {
                "name": str(flow.get("name") or ""),
                "categories": [str(value) for value in flow.get("categories", ())],
                "unit": str(flow.get("unit") or ""),
                "flow_id": _required(row, "flow_ref_id", label=f"openLCA flow {flow_id}"),
                "flow_name": _required(row, "flow_name", label=f"openLCA flow {flow_id}"),
                "flow_property_id": _required(row, "flow_property_ref_id", label=f"openLCA flow {flow_id}"),
                "flow_property_name": _required(row, "flow_property_name", label=f"openLCA flow {flow_id}"),
                "unit_id": _required(row, "unit_ref_id", label=f"openLCA flow {flow_id}"),
                "unit_name": _required(row, "unit", label=f"openLCA flow {flow_id}"),
                **(
                    {
                        "amount_factor": mapping["amount_factor"],
                        "source_flow_id": flow_id,
                        "native_categories": mapping["native"]["category_path"],
                        "quantity_projection_id": mapping["id"],
                    }
                    if mapping is not None and mapping.get("amount_factor")
                    else {}
                ),
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
        "schema_version": 2,
        "format": "openlca_jsonld",
        "profile": {
            "technosphere": technosphere_profile,
            "biosphere": biosphere_profile,
        },
        "source": source,
        "coverage": {
            "technosphere_references": len(technosphere),
            "process_category_references": len(technosphere),
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
