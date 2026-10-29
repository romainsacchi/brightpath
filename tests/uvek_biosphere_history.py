"""Reconstruct earlier authored batches without weakening their digest checks."""

import json
from copy import deepcopy

from brightpath.migrations.resources import load_uvek_release_resources
from scripts.review_uvek_2026_biosphere import identity
from scripts.review_uvek_2026_land import METHOD, REPORT


def before_land_review():
    resources = deepcopy(list(load_uvek_release_resources("biosphere")))
    if not any("land_review" in resource["provenance"] for resource in resources):
        return resources
    report = json.loads(REPORT.read_text())
    for resource in resources:
        direction = "forward" if resource["source_profiles"][0]["family"] == "uvek" else "reverse"
        added = [row for row in resource["replace"] if row["mapping_method"] == METHOD]
        resource["replace"] = [row for row in resource["replace"] if row["mapping_method"] != METHOD]
        resource["unresolved"] = sorted(
            [
                *resource["unresolved"],
                *({key: row["source"][key] for key in ("name", "categories", "unit")} for row in added),
            ],
            key=identity,
        )
        used = {row["review_ref"] for row in resource["replace"]}
        resource["reviews"] = {key: value for key, value in resource["reviews"].items() if key in used}
        resource["provenance"].pop("land_review")
        resource["coverage"] = report["sources"]["baseline_coverage"][direction]
        resource["licenses"] = report["sources"]["baseline_licenses"][direction]
    return resources
