"""Audit packaged migration coverage; optionally enforce a reviewed regression baseline."""

import argparse
import json
from pathlib import Path

from brightpath.background.audit import compare_baseline, run_migration_audit


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--workers", type=int, default=1)
    args = parser.parse_args(argv)
    if args.workers < 1:
        parser.error("--workers must be positive")
    report = run_migration_audit(
        progress=lambda name, summary: print(f"{name}: {summary['statuses']}", flush=True), workers=args.workers
    )
    if args.baseline:
        differences = compare_baseline(report, json.loads(args.baseline.read_text(encoding="utf-8")))
    else:
        differences = ["No regression baseline supplied; review blocked cases and collision candidates."]
    report["gate"] = {"passed": not differences, "differences": differences}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
    for difference in differences:
        print(difference, flush=True)
    print(f"Full audit: {args.output}", flush=True)
    return int(bool(differences))


if __name__ == "__main__":
    raise SystemExit(main())
