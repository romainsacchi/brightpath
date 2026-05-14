"""Import the SimaPro ecoinvent 3.10 Si wafer export into Brightway.

The source CSV is expected to be a SimaPro export compatible with
``brightpath.SimaproConverter``. The Brightway project must already contain
the ecoinvent 3.10 cutoff technosphere database and a matching biosphere
database; this script does not import or redistribute ecoinvent data.

Examples
--------
Dry-run conversion and matching:

    python dev/import_ei310_si_wafer_to_brightway.py

Write the converted foreground database:

    python dev/import_ei310_si_wafer_to_brightway.py --write
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Iterable

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from brightpath import SimaproConverter  # noqa: E402


DEFAULT_INPUT = REPO_ROOT / "dev" / "ei310_Si_wafer_RER_.csv"
DEFAULT_PROJECT = "ecoinvent-3.10-cutoff"
DEFAULT_ECOINVENT_DB = "ecoinvent-3.10.1-cutoff"
DEFAULT_BIOSPHERE_CANDIDATES = ("ecoinvent-3.10-biosphere", "biosphere3")
DEFAULT_UNLINKED_REPORT = REPO_ROOT / "dev" / (
    f"{DEFAULT_INPUT.stem}_unlinked_exchanges.csv"
)
UNLINKED_REPORT_FIELDS = [
    "dataset",
    "dataset reference product",
    "dataset location",
    "type",
    "name",
    "product",
    "reference product",
    "location",
    "categories",
    "unit",
    "amount",
]


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Convert a SimaPro ecoinvent 3.10 cutoff CSV to a Brightway "
            "foreground database."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--db-name", default=DEFAULT_INPUT.stem)
    parser.add_argument("--ecoinvent-db", default=DEFAULT_ECOINVENT_DB)
    parser.add_argument(
        "--biosphere-db",
        help=(
            "Biosphere database to match. If omitted, the script tries "
            f"{', '.join(DEFAULT_BIOSPHERE_CANDIDATES)}."
        ),
    )
    parser.add_argument("--ecoinvent-version", default="3.10")
    parser.add_argument(
        "--write",
        action="store_true",
        help="Write the converted database. Without this flag, run a dry run.",
    )
    parser.add_argument(
        "--replace-existing",
        action="store_true",
        help="Allow --write to replace an existing database with --db-name.",
    )
    parser.add_argument(
        "--allow-unlinked",
        action="store_true",
        help="Allow --write even when unlinked exchanges remain.",
    )
    parser.add_argument(
        "--convert-only",
        action="store_true",
        help="Only parse and convert the CSV; skip Brightway project matching.",
    )
    parser.add_argument(
        "--unlinked-report",
        type=Path,
        default=DEFAULT_UNLINKED_REPORT,
        help="CSV path for unlinked exchange diagnostics.",
    )
    parser.add_argument(
        "--no-unlinked-report",
        action="store_true",
        help="Do not write the unlinked exchange diagnostics CSV.",
    )
    parser.add_argument(
        "--unlinked-limit",
        type=int,
        default=20,
        help="Number of unique unlinked exchanges to print; 0 prints all.",
    )
    args = parser.parse_args(argv)

    if args.convert_only and args.write:
        parser.error("--convert-only cannot be combined with --write")
    if args.replace_existing and not args.write:
        parser.error("--replace-existing only applies together with --write")
    if args.unlinked_limit < 0:
        parser.error("--unlinked-limit must be zero or positive")
    if args.no_unlinked_report:
        args.unlinked_report = None
    return args


def require_existing_input(path: Path) -> Path:
    path = path.expanduser().resolve()
    if not path.exists():
        raise SystemExit(f"Input CSV not found: {path}")
    if path.suffix.lower() != ".csv":
        raise SystemExit(f"Expected a .csv SimaPro export, got: {path}")
    return path


def warn_about_source(path: Path) -> None:
    sample = path.read_text(encoding="utf-8", errors="replace")[:10000]
    if "SimaPro" not in sample:
        print("Warning: input does not look like a SimaPro export.")
    if "ecoquery.ecoinvent.org/3.10/cutoff" not in sample:
        print("Warning: input does not advertise ecoinvent 3.10 cutoff URLs.")


def require_project(project: str):
    import bw2data as bd

    if project not in bd.projects:
        available = ", ".join(sorted(str(name) for name in bd.projects)) or "none"
        raise SystemExit(
            f"Brightway project not found: {project}\n"
            f"Available projects: {available}"
        )
    bd.projects.set_current(project)
    return bd


def require_database(bd, name: str, role: str) -> str:
    if name not in bd.databases:
        available = ", ".join(sorted(str(db) for db in bd.databases)) or "none"
        raise SystemExit(
            f"{role} database not found in project {bd.projects.current}: {name}\n"
            f"Available databases: {available}"
        )
    return name


def select_biosphere_database(bd, requested: str | None) -> str:
    if requested:
        return require_database(bd, requested, "Biosphere")

    for candidate in DEFAULT_BIOSPHERE_CANDIDATES:
        if candidate in bd.databases:
            return candidate

    available = ", ".join(sorted(str(db) for db in bd.databases)) or "none"
    raise SystemExit(
        "Could not find a biosphere database. Pass --biosphere-db explicitly.\n"
        f"Tried: {', '.join(DEFAULT_BIOSPHERE_CANDIDATES)}\n"
        f"Available databases: {available}"
    )


def print_linking_plan(args: argparse.Namespace, biosphere_db: str | None = None) -> None:
    biosphere_target = (
        biosphere_db
        if biosphere_db
        else f"auto-select from {', '.join(DEFAULT_BIOSPHERE_CANDIDATES)}"
    )
    print("Linking plan:", flush=True)
    print(f"  Brightway project: {args.project}", flush=True)
    print(f"  Foreground database: {args.db_name}", flush=True)
    print(f"  ecoinvent technosphere database: {args.ecoinvent_db}", flush=True)
    print(f"  Biosphere database: {biosphere_target}", flush=True)


def print_inventory_summary(importer) -> None:
    datasets = len(importer.data)
    exchanges = sum(len(ds.get("exchanges", [])) for ds in importer.data)
    print(f"Converted {datasets} datasets with {exchanges} exchanges.")
    if importer.data:
        first = importer.data[0]
        print(
            "First dataset: "
            f"{first.get('name')} | {first.get('reference product')} | "
            f"{first.get('location')}"
        )


def format_categories(categories) -> str:
    return " / ".join(categories or ())


def collect_unlinked_rows(importer) -> list[dict]:
    rows = []
    for dataset in importer.data:
        for exc in dataset.get("exchanges", []):
            if exc.get("input"):
                continue
            rows.append(
                {
                    "dataset": dataset.get("name", ""),
                    "dataset reference product": dataset.get(
                        "reference product", ""
                    ),
                    "dataset location": dataset.get("location", ""),
                    "type": exc.get("type", ""),
                    "name": exc.get("name", ""),
                    "product": exc.get("product", ""),
                    "reference product": exc.get("reference product", ""),
                    "location": exc.get("location", ""),
                    "categories": format_categories(exc.get("categories", ())),
                    "unit": exc.get("unit", ""),
                    "amount": exc.get("amount", ""),
                }
            )
    return rows


def unique_unlinked_rows(rows: list[dict]) -> list[dict]:
    seen = set()
    unique = []
    for row in rows:
        key = tuple(
            row[field]
            for field in (
                "type",
                "name",
                "product",
                "reference product",
                "location",
                "categories",
                "unit",
            )
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(row)
    return unique


def print_unlinked_summary(rows: list[dict], limit: int) -> None:
    if not rows:
        print("No unique unlinked exchanges remain.")
        return

    unique = unique_unlinked_rows(rows)
    print(
        f"{len(rows)} unlinked exchange occurrences remain "
        f"({len(unique)} unique)."
    )
    displayed = unique if limit == 0 else unique[:limit]
    for index, row in enumerate(displayed, start=1):
        details = [
            row["type"],
            row["name"],
            row["product"] or row["reference product"],
            row["location"],
            row["categories"],
            row["unit"],
            f"used by {row['dataset']} [{row['dataset location']}]",
        ]
        print(f"  {index}. " + " | ".join(str(item) for item in details if item))

    if limit and len(unique) > limit:
        print(f"  ... {len(unique) - limit} more unique exchanges not shown")


def write_unlinked_report(path: Path, rows: list[dict]) -> None:
    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=UNLINKED_REPORT_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    print(f"Wrote unlinked exchange report: {path}")


def convert_source(source: Path, db_name: str, ecoinvent_version: str):
    print(f"Converting source: {source}")
    converter = SimaproConverter(
        source,
        ecoinvent_version=ecoinvent_version,
        db_name=db_name,
    )
    converter.convert_to_brightway()
    print_inventory_summary(converter.i)
    return converter


def match_importer(importer, ecoinvent_db: str, biosphere_db: str) -> None:
    print(f"Matching internal foreground links for {importer.db_name}.")
    importer.match_database(fields=["name", "reference product", "location"])
    print(f"Matching technosphere links against {ecoinvent_db}.")
    importer.match_database(
        ecoinvent_db,
        fields=["name", "reference product", "location"],
    )
    print(f"Matching biosphere links against {biosphere_db}.")
    importer.match_database(biosphere_db, fields=["name", "categories"])
    importer.statistics()


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    source = require_existing_input(args.input)
    warn_about_source(source)

    bd = None
    biosphere_db = None
    if not args.convert_only:
        print_linking_plan(args)
        bd = require_project(args.project)
        require_database(bd, args.ecoinvent_db, "ecoinvent cutoff")
        biosphere_db = select_biosphere_database(bd, args.biosphere_db)
        print_linking_plan(args, biosphere_db)

    converter = convert_source(source, args.db_name, args.ecoinvent_version)
    importer = converter.i

    if args.convert_only:
        print("Conversion-only check completed; no Brightway project was touched.")
        return 0

    match_importer(importer, args.ecoinvent_db, biosphere_db)

    unlinked = collect_unlinked_rows(importer)
    print_unlinked_summary(unlinked, args.unlinked_limit)
    if args.unlinked_report:
        write_unlinked_report(args.unlinked_report, unlinked)

    if not args.write:
        print("Dry run completed; re-run with --write to import the database.")
        return 0

    if args.db_name in bd.databases and not args.replace_existing:
        raise SystemExit(
            f"Database already exists: {args.db_name}\n"
            "Pass --replace-existing with --write to overwrite it."
        )

    if unlinked and not args.allow_unlinked:
        raise SystemExit(
            "Refusing to write with unlinked exchanges. Fix the links, pass "
            "--unlinked-report for diagnostics, or add --allow-unlinked if this "
            "is intentional."
        )

    importer.write_database(delete_existing=args.replace_existing)
    print(f"Imported Brightway database: {args.db_name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
