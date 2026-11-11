"""Replay the inspected ecoinvent 3.6 geothermal electricity correspondences."""

import hashlib
import json
from copy import deepcopy
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_geothermal_36_grid_reference_review.json"
REVIEW_SHA256 = "9f14014f072292500d116832d5c66defc5be5f48ec99eb81fb6c439abbea6348"


def compiled_grid_rule(row):
    """Preserve the rule except for the independently witnessed sign profile."""
    rule = deepcopy(row["previous_rule"])
    if row["resource_action"] == "append_exact_profile":
        profiles = rule["reference_flow_sign_review"]["profiles"]
        if any(p["source_profile"] == row["source_profile"] for p in profiles):
            raise ValueError("Geothermal grid profile already has a decision.")
        profiles.append(
            {
                "source_profile": row["source_profile"],
                "conversion_factor": 1,
                "review": {
                    "status": "verified",
                    "source_sign": 1,
                    "target_sign": 1,
                    "reason": " ".join(row["limitations"]),
                    "evidence": [f"{REVIEW_PATH}#{row['rule_id']}"],
                },
            }
        )
    elif row["resource_action"] != "unchanged_rule":
        raise ValueError("Unknown geothermal grid resource action.")
    return rule


def apply_geothermal_36_grid_review(resource, root=ROOT):
    """Validate the complete cohort before changing the exact-profile decision."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Geothermal grid evidence changed; repeat its review.")
    review = json.loads(raw)
    profile = {"family": "ecoinvent", "version": "3.6", "system_model": "cutoff"}
    if (
        review["source_profile"] != profile
        or profile not in resource["source_profiles"]
        or len(review["records"]) != 24
    ):
        raise ValueError("Geothermal grid source scope changed.")
    indexed = {r["id"]: r for r in resource["replace"]}
    pending = []
    for row in review["records"]:
        compiled = compiled_grid_rule(row)
        current = indexed[row["rule_id"]]
        if (
            current not in (row["previous_rule"], compiled)
            or compiled != row["compiled_rule"]
            or row["source_profile"] != profile
            or row["source_reference_amount"] != 1
            or row["conversion_factor"] != 1
            or row["source"]["location"] != row["target"]["location"]
            or not row["source"]["unit"] == row["target"]["unit"] == "kilowatt hour"
            or set(row["native_references"]) != {"2025", "2026"}
            or any(w["reference_amount"] != 1 for w in row["native_references"].values())
        ):
            raise ValueError("Geothermal grid rule, scope or reference changed.")
        pending.append((current, compiled))
    # Earlier replaced rules left these review records unreachable. Their
    # historical evidence remains in Git and the independent review files.
    for key in review["retired_unused_review_ids"]:
        if key in json.dumps({k: v for k, v in resource.items() if k != "reviews"}):
            raise ValueError("Cannot retire a referenced migration review.")
    for current, compiled in pending:
        current.clear()
        current.update(compiled)
    for key in review["retired_unused_review_ids"]:
        resource["reviews"].pop(key, None)
