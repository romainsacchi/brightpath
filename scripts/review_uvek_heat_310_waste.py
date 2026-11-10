"""Qualify exact waste profiles with fresh quantities, uncertainty and omission audits."""

import hashlib
import json
from copy import deepcopy
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_heat_310_waste_review.json"
REVIEW_SHA256 = "1ddf1663f66fa48cb1cd10364899d9fc927c718d7e9957e9609f60a22c6a87b3"


def digest(value):
    """Bind the complete portable source rule to its independently reviewed state."""
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


def read_review(root=ROOT):
    """Refuse changed scientific evidence instead of broadening prior approvals."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Heat waste evidence changed; repeat its scientific review.")
    return json.loads(raw)


def compiled_waste_rule(row):
    """Preserve earlier targets and reference signs; add only the exact new profile."""
    rule = deepcopy(row["previous_rule"])
    if row["resource_action"] == "unchanged_rule":
        return rule
    if row["resource_action"] != "append_profile_with_recipe":
        raise ValueError("Unknown heat waste qualification action.")
    profiles = rule["reference_flow_sign_review"]["profiles"]
    if any(p["source_profile"] == row["source_profile"] for p in profiles):
        raise ValueError("Heat waste profile already has a decision.")
    profiles.append(
        {
            "source_profile": deepcopy(row["source_profile"]),
            "target": deepcopy(row["target"]),
            "conversion_factor": row["conversion_factor"],
            "review": {
                "status": "verified",
                "source_sign": row["source_reference_amount"],
                "target_sign": 1,
                "reason": " ".join(row["limitations"]),
                "evidence": [REVIEW_PATH],
            },
        }
    )
    return rule


def qualified_extended_waste_rule(resource, actual, base, root=ROOT):
    """Let an older authoring stage replay without erasing this exact reviewed extension.

    This is deliberately content-bound. A new arbitrary profile, altered old rule
    or changed helper is not an authorised extension of the older decision.
    """
    row = next((r for r in read_review(root)["records"] if r["rule_id"] == base["id"]), None)
    if (
        row is None
        or row["resource_action"] != "append_profile_with_recipe"
        or base != row["previous_rule"]
        or actual != compiled_waste_rule(row)
        or any(resource["recipes"].get(k) != v for k, v in row["new_recipes"].items())
    ):
        raise ValueError("Unreviewed waste-rule extension or changed helper.")
    return deepcopy(actual)


def apply_heat_310_waste_review(resource, root=ROOT):
    """Validate the entire batch and all existing helpers before any mutation."""
    review = read_review(root)
    indexed = {r["id"]: r for r in resource["replace"]}
    pending = []
    for row in review["records"]:
        previous = row["previous_rule"]
        current = indexed.get(row["rule_id"])
        compiled = compiled_waste_rule(row)
        if (
            digest(previous) != row["previous_rule_sha256"]
            or current not in (previous, compiled)
            or previous["source"] != row["source"]
            or previous.get("conversion_factor", 1) != row["conversion_factor"]
            or row["conversion_factor"] != row["source_reference_amount"]
            or row["target_reference_amount"] != 1
            or row["source_profile"] not in resource["source_profiles"]
            or row["source_profile"]["system_model"] != "cutoff"
        ):
            raise ValueError("Heat waste identity, reference sign or earlier rule changed.")
        for key, recipe in row["new_recipes"].items():
            if resource["recipes"].get(key) not in (None, recipe):
                raise ValueError("Heat waste helper or omission audit changed.")
            if recipe["unit"] != row["source"]["unit"]:
                raise ValueError("Heat waste reference unit changed.")
            if recipe.get("source_input_omissions", []) != row.get("approved_recipe_source_omissions", {}).get(key, []):
                raise ValueError("Heat waste literal omission approval changed.")
        pending.append((current, compiled, row))
    for current, compiled, row in pending:
        resource["recipes"].update(deepcopy(row["new_recipes"]))
        current.clear()
        current.update(compiled)
