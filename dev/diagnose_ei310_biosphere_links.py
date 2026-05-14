"""Diagnose unlinked exchanges from the ecoinvent 3.10 Si wafer import."""

from __future__ import annotations

import argparse
import collections
import csv
import difflib
import re
from pathlib import Path
from typing import Iterable

import bw2data as bd


DEFAULT_REPORT = Path("dev/ei310_Si_wafer_RER__unlinked_exchanges.csv")


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default="ecoinvent-3.10-cutoff")
    parser.add_argument("--ecoinvent-db", default="ecoinvent-3.10.1-cutoff")
    parser.add_argument("--biosphere-db", default="ecoinvent-3.10-biosphere")
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--limit", type=int, default=35)
    return parser.parse_args(argv)


def unique_rows(rows: list[dict], fields: tuple[str, ...]) -> list[dict]:
    seen = set()
    unique = []
    for row in rows:
        key = tuple(row[field] for field in fields)
        if key in seen:
            continue
        seen.add(key)
        unique.append(row)
    return unique


def load_biosphere_index(database_name: str) -> dict[str, list[dict]]:
    index = collections.defaultdict(list)
    for flow in bd.Database(database_name):
        index[flow.get("name")].append(
            {
                "name": flow.get("name"),
                "categories": " / ".join(flow.get("categories") or ()),
                "unit": flow.get("unit"),
                "code": flow.get("code"),
            }
        )
    return index


def proposed_biosphere_names(name: str, categories: str) -> list[str]:
    candidates = []
    replacements = {
        "BOD5 (Biological Oxygen Demand)": "BOD5, Biological Oxygen Demand",
        "COD (Chemical Oxygen Demand)": "COD, Chemical Oxygen Demand",
        "Nitrogen, atmospheric": "Nitrogen",
        "Argon-40/kg": "Argon",
        "Transformation, to rivers, artificial": "Transformation, to river, artificial",
        "Carbon monoxide biogenic": "Carbon monoxide, non-fossil",
        "Xylene": "Xylenes, unspecified",
        "AOX, Adsorbable Organic Halogen": "AOX, Adsorbable Organic Halides",
        "Benzene, hexachloro-": "Hexachlorobenzene",
    }
    if name in replacements:
        candidates.append(replacements[name])

    if re.search(r"/m3, .+$", name):
        candidates.append(name.split("/m3", 1)[0])
    if name.endswith("/kg"):
        candidates.append(name.removesuffix("/kg"))
    if re.search(r"\([IVX]+\)$", name):
        candidates.append(re.sub(r"\s+\(([IVX]+)\)$", r" \1", name))
    if name.endswith(", ion"):
        candidates.append(name.removesuffix(", ion") + " ion")
    if name == "Strontium" and not categories.startswith("natural resource"):
        candidates.append("Strontium II")

    unique = []
    for candidate in candidates:
        if candidate not in unique:
            unique.append(candidate)
    return unique


def print_proposed_biosphere_coverage(unique: list[dict], by_name: dict[str, list[dict]]):
    direct_keys = {
        (name, match["categories"], match["unit"])
        for name, matches in by_name.items()
        for match in matches
    }
    resolved = []
    unresolved = []
    for row in unique:
        candidates = proposed_biosphere_names(row["name"], row["categories"])
        match = next(
            (
                candidate
                for candidate in candidates
                if (candidate, row["categories"], row["unit"]) in direct_keys
            ),
            None,
        )
        if match:
            resolved.append((row, match))
        else:
            unresolved.append(row)

    print("\nCoverage from simple proposed biosphere normalizers:")
    print(f"- Resolved unique candidates: {len(resolved)} of {len(unique)}")
    print(f"- Still unresolved unique candidates: {len(unresolved)}")
    for row, match in resolved[:15]:
        print(f"  {row['name']} -> {match} | {row['categories']} | {row['unit']}")
    if len(resolved) > 15:
        print(f"  ... {len(resolved) - 15} more resolved by these rules")
    if unresolved:
        print("  unresolved samples:")
        for row in unresolved[:15]:
            print(f"  {row['name']} | {row['categories']} | {row['unit']}")


def load_technosphere_index(database_name: str) -> dict[str, list[dict]]:
    index = collections.defaultdict(list)
    for activity in bd.Database(database_name):
        index[activity.get("name")].append(
            {
                "name": activity.get("name"),
                "reference product": activity.get("reference product"),
                "location": activity.get("location"),
                "unit": activity.get("unit"),
                "code": activity.get("code"),
            }
        )
    return index


def print_technosphere_diagnostics(rows: list[dict], database_name: str, limit: int):
    technosphere_rows = [row for row in rows if row["type"] == "technosphere"]
    unique = unique_rows(
        technosphere_rows,
        ("name", "reference product", "location", "unit"),
    )
    by_name = load_technosphere_index(database_name)
    print(f"\necoinvent database: {database_name}")
    print(
        "Unlinked technosphere exchanges: "
        f"{len(technosphere_rows)} occurrences, {len(unique)} unique"
    )
    print(
        "Unique unlinked names present exactly in ecoinvent DB: "
        f"{sum(1 for row in unique if row['name'] in by_name)}"
    )
    all_names = list(by_name)
    print("\nTechnosphere missing links:")
    for row in unique[:limit]:
        matches = by_name.get(row["name"], [])
        if not matches:
            matches = [
                candidate
                for name in difflib.get_close_matches(
                    row["name"], all_names, n=3, cutoff=0.6
                )
                for candidate in by_name[name][:3]
            ]
        print(
            f"- {row['name']} | {row['reference product']} | "
            f"{row['location']} | {row['unit']}"
        )
        for match in matches[:8]:
            print(
                "    candidate "
                f"{match['name']} | {match['reference product']} | "
                f"{match['location']} | {match['unit']}"
            )


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    bd.projects.set_current(args.project)

    rows = list(csv.DictReader(args.report.open(encoding="utf-8")))
    biosphere_rows = [row for row in rows if row["type"] == "biosphere"]
    unique = unique_rows(biosphere_rows, ("name", "categories", "unit"))
    by_name = load_biosphere_index(args.biosphere_db)

    print(f"Project: {bd.projects.current}")
    print(f"Biosphere database: {args.biosphere_db}")
    print(f"Biosphere database flows: {sum(len(v) for v in by_name.values())}")
    print(
        "Unlinked biosphere exchanges: "
        f"{len(biosphere_rows)} occurrences, {len(unique)} unique"
    )
    print(
        "Unique unlinked names present exactly in biosphere DB: "
        f"{sum(1 for row in unique if row['name'] in by_name)}"
    )

    print("\nExact name matches with category/unit mismatch:")
    count = 0
    for row in unique:
        matches = by_name.get(row["name"], [])
        if not matches:
            continue
        if any(
            match["categories"] == row["categories"]
            and match["unit"] == row["unit"]
            for match in matches
        ):
            continue
        db_variants = "; ".join(
            f"{match['categories']} | {match['unit']}" for match in matches[:4]
        )
        print(
            f"- {row['name']} | CSV: {row['categories']} | "
            f"{row['unit']} | DB: {db_variants}"
        )
        count += 1
        if count >= args.limit:
            break

    all_names = list(by_name)
    print("\nMissing names with closest biosphere DB names:")
    count = 0
    for row in unique:
        if row["name"] in by_name:
            continue
        close = difflib.get_close_matches(row["name"], all_names, n=4, cutoff=0.62)
        print(
            f"- {row['name']} | {row['categories']} | "
            f"{row['unit']} -> {close}"
        )
        for candidate in close[:2]:
            variants = "; ".join(
                f"{match['categories']} | {match['unit']}"
                for match in by_name[candidate][:6]
            )
            print(f"    candidate {candidate}: {variants}")
        count += 1
        if count >= args.limit:
            break

    patterns = collections.Counter()
    for row in unique:
        name = row["name"]
        if any(suffix in name for suffix in ("/m3", "/kg", "/MJ")):
            patterns["unit suffix in name"] += 1
        if any(location in name for location in (", RER", ", RoW", ", GLO")):
            patterns["location suffix in biosphere name"] += 1
        if "(" in name and ")" in name:
            patterns["parenthetical/oxidation state"] += 1
        if row["categories"] in {"water", "air", "soil"}:
            patterns["single top-level category"] += 1
        if row["categories"].startswith("natural resource"):
            patterns["natural resource"] += 1

    print("\nRepeated patterns:")
    for pattern, value in patterns.most_common():
        print(f"- {pattern}: {value}")

    print_proposed_biosphere_coverage(unique, by_name)
    print_technosphere_diagnostics(rows, args.ecoinvent_db, args.limit)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
