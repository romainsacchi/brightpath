"""Audit native UVEK 2026 routes separately from the historical export baseline."""

import argparse
import json
from pathlib import Path

from brightpath.background.audit import compare_baseline
from brightpath.background.catalogs import PackageCatalogProvider
from brightpath.background.uvek_audit import run_uvek_2026_audit, uvek_2026_tasks


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument(
        "--route", action="append", help="Select an exact task name; output is a partial discovery report."
    )
    parser.add_argument("--list-routes", action="store_true")
    args = parser.parse_args(argv)
    if args.workers < 1:
        parser.error("--workers must be positive")
    if args.list_routes:
        tasks, scope = uvek_2026_tasks(PackageCatalogProvider())
        print(
            json.dumps(
                {"scope": scope, "routes": {name: len(task[0]) for name, task in sorted(tasks.items())}}, indent=2
            )
        )
        return 0
    if args.output is None:
        parser.error("--output is required unless listing routes")
    report = run_uvek_2026_audit(
        progress=lambda name, summary: print(f"{name}: {summary['statuses']}", flush=True),
        workers=args.workers,
        route_names=args.route,
    )
    differences = (
        compare_baseline(report, json.loads(args.baseline.read_text(encoding="utf-8")))
        if args.baseline
        else ["No native-route baseline supplied; review coverage and blockers."]
    )
    if not report["baseline"]["complete"]:
        differences.append("Partial native-route audit cannot pass the release gate.")
    report["gate"] = {"passed": not differences, "differences": differences}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
    for difference in differences:
        print(difference, flush=True)
    return int(bool(differences))


if __name__ == "__main__":
    raise SystemExit(main())
