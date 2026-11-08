"""Reconstruct earlier authored batches without weakening their digest checks."""

import json

from brightpath.migrations.resources import load_uvek_release_resources
from scripts.review_uvek_2026_ash_water import before_ash_water_review
from scripts.review_uvek_2026_biosphere import identity
from scripts.review_uvek_2026_land import METHOD, REPORT
from scripts.review_uvek_2026_water import METHOD as WATER_METHOD
from scripts.review_uvek_2026_water import REPORT as WATER_REPORT


def before_water_review():
    """Keep the independently reviewed native-to-ecoinvent direction unchanged."""
    resources = before_ash_water_review(list(load_uvek_release_resources("biosphere")))
    report = None
    for resource in resources:
        if "water_review" not in resource["provenance"]:
            continue
        report = report or json.loads(WATER_REPORT.read_text())
        added = [row for row in resource["replace"] if row["mapping_method"] == WATER_METHOD]
        resource["replace"] = [row for row in resource["replace"] if row["mapping_method"] != WATER_METHOD]
        resource["unresolved"] = sorted([*resource["unresolved"], *(row["source"] for row in added)], key=identity)
        used = {row["review_ref"] for row in resource["replace"]}
        resource["reviews"] = {key: value for key, value in resource["reviews"].items() if key in used}
        resource["provenance"].pop("water_review")
        resource["coverage"] = report["sources"]["baseline_coverage"]
    return resources


def before_land_review():
    resources = before_water_review()
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
