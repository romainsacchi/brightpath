"""Extend eight inspected 3.8 profiles without rewriting earlier decisions."""

import hashlib
import json
from copy import deepcopy
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_heat_38_profiles_review.json"
REVIEW_SHA256 = "9a5829fb520f662169f6fd2cbd18ff980e88a030581f47e843809e4ef9b8357e"


def digest(value):
    """Hash the complete portable rule, including all original profile decisions."""
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


def compiled_profile_rule(row):
    """Add only missing exact-profile evidence; preserve every older field."""
    rule = deepcopy(row["previous_rule"])
    if row["resource_action"] == "unchanged_rule":
        return rule
    if row["resource_action"] != "append_exact_profile":
        raise ValueError("Unknown heat3.8profile qualification action.")
    profiles = rule["reference_flow_sign_review"]["profiles"]
    if any(p["source_profile"] == row["source_profile"] for p in profiles):
        raise ValueError("Heat3.8profile profile already has a decision.")
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


def apply_heat_38_profiles_review(resource, root=ROOT):
    """Validate the entire reviewed batch before changing any raw profile list."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Heat3.8profile evidence changed; repeat its scientific review.")
    review = json.loads(raw)
    indexed = {r["id"]: r for r in resource["replace"]}
    pending = []
    for row in review["records"]:
        compiled = compiled_profile_rule(row)
        current = indexed.get(row["rule_id"])
        if (
            digest(row["previous_rule"]) != row["previous_rule_sha256"]
            or current not in (row["previous_rule"], compiled)
            or row["previous_rule"]["source"] != row["source"]
            or row["previous_rule"]["target"] != row["target"]
            or row["source_profile"] not in resource["source_profiles"]
            or row["source_profile"] != {"family": "ecoinvent", "version": "3.8", "system_model": "cutoff"}
            or row["source_reference_amount"] != row["conversion_factor"]
            or row["target_reference_amount"] != 1
            or row["conversion_factor"] not in (-1, 1)
            or row["source"]["unit"] != row["target"]["unit"]
            or set(row["native_references"]) != {"2025", "2026"}
            or any(n["reference_amount"] != 1 for n in row["native_references"].values())
        ):
            raise ValueError("Heat3.8profile identity, reference or earlier rule changed.")
        pending.append((current, compiled))
    for current, compiled in pending:
        current.clear()
        current.update(compiled)


def qualified_extended_profile_rule(actual, base, root=ROOT):
    """Permit only the frozen concrete profile extension during earlier-stage replay."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Heat3.8profile evidence changed; repeat its scientific review.")
    row = next((r for r in json.loads(raw)["records"] if r["rule_id"] == base["id"]), None)
    if (
        row is None
        or row["resource_action"] != "append_exact_profile"
        or row["previous_rule"] != base
        or actual != compiled_profile_rule(row)
    ):
        raise ValueError("Unreviewed concrete profile extension.")
    return deepcopy(actual)
