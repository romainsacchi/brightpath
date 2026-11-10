"""Extend inspected utility/material profiles without rewriting earlier decisions."""

import hashlib
import json
from copy import deepcopy
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_heat_310_utilities_materials_review.json"
REVIEW_SHA256 = "aae40d3793f73cc92bca293ed7e3782863ac0882c60f247a850c064b05bd37ac"


def digest(value):
    """Hash the complete portable rule, including all original profile decisions."""
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


def compiled_utility_rule(row):
    """Add only missing exact-profile evidence; preserve every older field."""
    rule = deepcopy(row["previous_rule"])
    if row["resource_action"] == "unchanged_rule":
        return rule
    if row["resource_action"] != "append_exact_profile":
        raise ValueError("Unknown heat utility/material qualification action.")
    profiles = rule["reference_flow_sign_review"]["profiles"]
    if any(p["source_profile"] == row["source_profile"] for p in profiles):
        raise ValueError("Heat utility/material profile already has a decision.")
    profiles.append(
        {
            "source_profile": deepcopy(row["source_profile"]),
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
    return rule


def apply_heat_310_utilities_review(resource, root=ROOT):
    """Validate the entire reviewed batch before changing any raw profile list."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Heat utility/material evidence changed; repeat its scientific review.")
    review = json.loads(raw)
    indexed = {r["id"]: r for r in resource["replace"]}
    pending = []
    for row in review["records"]:
        compiled = compiled_utility_rule(row)
        current = indexed.get(row["rule_id"])
        if (
            digest(row["previous_rule"]) != row["previous_rule_sha256"]
            or current not in (row["previous_rule"], compiled)
            or row["previous_rule"]["source"] != row["source"]
            or row["previous_rule"]["target"] != row["target"]
            or row["source_profile"] not in resource["source_profiles"]
            or row["source_profile"]["system_model"] != "cutoff"
            or row["source_reference_amount"] != 1
            or row["target_reference_amount"] != 1
            or row["conversion_factor"] != 1
            or row["source"]["unit"] != row["target"]["unit"]
            or set(row["native_references"]) != {"2025", "2026"}
            or any(n["reference_amount"] != 1 for n in row["native_references"].values())
        ):
            raise ValueError("Heat utility/material identity, reference or earlier rule changed.")
        pending.append((current, compiled))
    for current, compiled in pending:
        current.clear()
        current.update(compiled)
