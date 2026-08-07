"""Generate an aggregate SimaPro category catalog from a licensed export."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter
from pathlib import Path

from brightpath.models import BackgroundProfile
from brightpath.profiles.simapro import parse_simapro_technosphere_name
from brightpath.profiles.simapro_categories import normalize_simapro_category
from brightpath.units import normalize_unit


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate identity/category observations without copying exchanges or amounts."
    )
    parser.add_argument("input", type=Path, help="Licensed full SimaPro CSV export")
    parser.add_argument("output", type=Path, help="Aggregate UTF-8 CSV resource")
    parser.add_argument("--family", default="ecoinvent")
    parser.add_argument("--version", required=True, help="Exact source database version")
    parser.add_argument("--system-model", default="cutoff")
    parser.add_argument("--simapro-version", default="", help="SimaPro version used for the source export")
    parser.add_argument("--manifest", type=Path, help="Manifest destination; defaults beside the output")
    return parser.parse_args()


def collect_category_observations(path: Path, profile: BackgroundProfile) -> Counter[tuple[str, ...]]:
    """Return aggregate product/unit/role/category counts from *path*."""

    counts: Counter[tuple[str, ...]] = Counter()
    category_type = ""
    current_section = ""
    expect_category_type = False
    with path.open(newline="", encoding="latin-1") as handle:
        for raw_row in csv.reader(handle, delimiter=";"):
            row = [cell.strip() for cell in raw_row]
            if not any(row):
                continue
            if len(row) == 1:
                marker = row[0]
                if expect_category_type:
                    category_type = marker
                    expect_category_type = False
                    continue
                if marker == "Category type":
                    expect_category_type = True
                current_section = marker if marker in {"Products", "Waste treatment", "Waste scenario"} else ""
                continue
            if current_section not in {"Products", "Waste treatment", "Waste scenario"}:
                continue

            category_index = 5 if current_section == "Products" else 4
            if len(row) <= category_index or not row[category_index]:
                current_section = ""
                continue
            try:
                name, reference_product, _location = parse_simapro_technosphere_name(
                    row[0],
                    unit=row[1],
                    profile=profile,
                )
                category = normalize_simapro_category(f"{category_type}/{row[category_index]}")
            except ValueError:
                current_section = ""
                continue
            role = "market" if name.casefold().startswith(("market for ", "market group for ")) else "transformation"
            counts[(reference_product, str(normalize_unit(row[1])), role, category)] += 1
            current_section = ""
    return counts


def write_resource(counts: Counter[tuple[str, ...]], output: Path) -> None:
    """Write deterministic aggregate observations."""

    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(("reference_product", "unit", "process_role", "category", "dataset_count"))
        for key in sorted(counts, key=lambda item: tuple(value.casefold() for value in item)):
            writer.writerow((*key, counts[key]))


def write_manifest(
    output: Path,
    manifest: Path,
    *,
    profile: BackgroundProfile,
    simapro_version: str,
    dataset_count: int,
) -> None:
    """Write deterministic integrity and provenance metadata."""

    payload = output.read_bytes()
    with output.open(newline="", encoding="utf-8") as handle:
        rows = tuple(csv.DictReader(handle))
    categories = {row["category"] for row in rows}
    document = {
        "description": "Aggregate product/category observations from a licensed SimaPro reference export.",
        "generator": "scripts/generate_simapro_category_catalog.py",
        "resources": [
            {
                "categories": len(categories),
                "datasets_observed": dataset_count,
                "file": output.name,
                "reference_rows": len(rows),
                "schema_version": 1,
                "sha256": hashlib.sha256(payload).hexdigest(),
                "simapro_version": simapro_version,
                "size": len(payload),
                "source_profile": {
                    "family": profile.family,
                    "system_model": profile.system_model,
                    "version": profile.version,
                },
            }
        ],
        "schema_version": 1,
        "status": "legal_review_required",
    }
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    profile = BackgroundProfile(args.family, args.version, args.system_model).normalized()
    counts = collect_category_observations(args.input, profile)
    if not counts:
        raise ValueError(f"No SimaPro category observations found in {args.input}.")
    write_resource(counts, args.output)
    write_manifest(
        args.output,
        args.manifest or args.output.with_name("RESOURCE_MANIFEST.json"),
        profile=profile,
        simapro_version=args.simapro_version,
        dataset_count=sum(counts.values()),
    )


if __name__ == "__main__":
    main()
