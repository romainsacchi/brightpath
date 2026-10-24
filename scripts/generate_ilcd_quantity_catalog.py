"""Build ILCD quantity definitions from an openLCA reference-template extract.

The input is a JSON list of property_id, property_name, group_id, group_name,
reference_unit_id, reference_unit and units (id, name, factor) records. It
contains reference identities and conversion factors, never inventory data.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

# Explicit mappings to the units in openLCA 2.6.2's public units template.
UNIT_SELECTIONS = {
    "kilogram": ("Mass", "kg"),
    "gram": ("Mass", "g"),
    "ton": ("Mass", "t"),
    "megajoule": ("Energy", "MJ"),
    "kilowatt hour": ("Energy", "kWh"),
    "cubic meter": ("Volume", "m3"),
    "litre": ("Volume", "l"),
    "meter": ("Length", "m"),
    "kilometer": ("Length", "km"),
    "square meter": ("Area", "m2"),
    "hectare": ("Area", "ha"),
    "hour": ("Time", "h"),
    "year": ("Time", "a"),
    "second": ("Time", "s"),
    "unit": ("Number", "Item(s)"),
    "kilo Becquerel": ("Radioactivity", "kBq"),
    "Becquerel": ("Radioactivity", "Bq"),
    "person kilometer": ("Person transport", "p*km"),
    "person-kilometer": ("Person transport", "p*km"),
    "ton kilometer": ("Mass transport", "t*km"),
    "kilogram day": ("Mass*Time", "kg*d"),
    "meter-year": ("Length*Time", "m*a"),
    "kilometer-year": ("Length*Time", "km*a"),
    "square meter-year": ("Area*Time", "m2*a"),
    "cubic meter-year": ("Volume*Time", "m3*a"),
    "guest night": ("Guest night", "guest night"),
}


def generate(records: list[dict], template_sha256: str) -> dict:
    """Select exact property/unit identities with their reference conversions."""
    properties = {row["property_name"]: row for row in records}
    units = {}
    for canonical, (property_name, unit_name) in UNIT_SELECTIONS.items():
        prop = properties[property_name]
        unit = next(unit for unit in prop["units"] if unit["name"] == unit_name)
        units[canonical] = {
            "property_id": prop["property_id"],
            "property_name": prop["property_name"],
            "group_id": prop["group_id"],
            "group_name": prop["group_name"],
            "reference_unit_id": prop["reference_unit_id"],
            "reference_unit": prop["reference_unit"],
            "unit_id": unit["id"],
            "factor": unit["factor"],
            "units": sorted(prop["units"], key=lambda row: row["id"]),
        }
    return {
        "schema_version": 1,
        "source": "openLCA 2.6.2 units.zolca reference template",
        "source_sha256": template_sha256,
        "generator": "scripts/generate_ilcd_quantity_catalog.py",
        "units": units,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--template", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = generate(json.loads(args.input.read_text()), hashlib.sha256(args.template.read_bytes()).hexdigest())
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
