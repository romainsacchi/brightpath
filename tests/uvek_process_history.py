"""Restore the original release bridge for independent unit-review regressions."""

import json
from copy import deepcopy

from brightpath.migrations.resources import load_uvek_release_resources
from scripts.review_uvek_2026_processes import METHOD, REPORT
from scripts.review_uvek_2026_units import identity


def before_process_review():
    resources = deepcopy(list(load_uvek_release_resources("technosphere")))
    if not any("process_review" in row["provenance"] for row in resources):
        return resources
    report = json.loads(REPORT.read_text())
    for resource in resources:
        direction = "forward" if resource["source_profiles"][0]["version"] == "2025" else "reverse"
        added = [row for row in resource["replace"] if row["mapping_method"] == METHOD]
        resource["replace"] = [row for row in resource["replace"] if row["mapping_method"] != METHOD]
        resource["unresolved"] = sorted([*resource["unresolved"], *(row["source"] for row in added)], key=identity)
        used = {row["review_ref"] for row in resource["replace"]}
        resource["reviews"] = {key: value for key, value in resource["reviews"].items() if key in used}
        resource["coverage"] = report["sources"]["baseline_coverage"][direction]
        resource["provenance"].pop("process_review")
    return resources
