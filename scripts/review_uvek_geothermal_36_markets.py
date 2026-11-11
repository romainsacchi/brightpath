"""Replay exact-profile geothermal dependency compositions without runtime changes."""

import hashlib
import json
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_geothermal_36_market_recipe_review.json"
REVIEW_SHA256 = "61198830017a6e18285fbc54d319c70e4be990804941c9565d91c29f5659bd41"


def compiled_market_rule(row):
    """Append one independently inspected profile without changing earlier entries."""
    rule = deepcopy(row["previous_rule"])
    review = deepcopy(rule.get("reference_flow_sign_review", {}))
    profiles = review.get("profiles", [])
    if any(p["source_profile"] == row["source_profile"] for p in profiles):
        raise ValueError("Geothermal market source already has a decision.")
    if "profiles" not in review:
        review = {
            "status": "unresolved",
            "reason": "Only enumerated exact source profiles have reviewed composition.",
            "evidence": [REVIEW_PATH],
            "profiles": profiles,
        }
    profiles.append(
        {
            "source_profile": row["source_profile"],
            "target": {"recipe_ref": row["recipe_id"]},
            "conversion_factor": row["conversion_factor"],
            "review": {
                "status": "verified",
                "source_sign": row["source_reference_amount"],
                "target_sign": 1,
                "reason": " ".join(row["limitations"]),
                "evidence": [REVIEW_PATH + "#" + row["rule_id"]],
            },
        }
    )
    review["profiles"] = profiles
    rule["reference_flow_sign_review"] = review
    rule["review_ref"] = intern_review({}, row["rule_review"])
    return rule


def apply_geothermal_36_market_review(resource, root=ROOT):
    """Validate every parent and dependency before applying the reviewed cohort."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Geothermal composition evidence changed; repeat review.")
    review = json.loads(raw)
    profile = {"family": "ecoinvent", "version": "3.6", "system_model": "cutoff"}
    if review["source_profile"] != profile or len(review["records"]) != 3 or len(review["recipes"]) != 3:
        raise ValueError("Geothermal composition scope changed.")
    indexed = {r["id"]: r for r in resource["replace"]}
    pending = []
    for row in review["records"]:
        compiled = compiled_market_rule(row)
        parent = indexed[row["rule_id"]]
        recipe = review["recipes"][row["recipe_id"]]
        if (
            parent not in (row["previous_rule"], compiled)
            or compiled != row["compiled_rule"]
            or row["source_profile"] != profile
            or row["conversion_factor"] != row["source_reference_amount"]
            or recipe["unit"] != row["source"]["unit"]
            or not recipe["inputs"]
        ):
            raise ValueError("Geothermal composition parent or reference changed.")
        pending.append((parent, compiled))
    for key, value in review["existing_recipe_dependencies"].items():
        if resource["recipes"].get(key) != value:
            raise ValueError("Geothermal composition dependency changed.")
    for key, value in review["recipes"].items():
        if key in resource["recipes"] and resource["recipes"][key] != value:
            raise ValueError("Geothermal composed helper changed.")
    if len(review["native_witnesses"]) != 28 or any(w["reference_amount"] != 1 for w in review["native_witnesses"]):
        raise ValueError("Geothermal native reference evidence changed.")
    for key, value in review["previous_review_records"].items():
        if resource["reviews"].get(key) != value:
            raise ValueError("Prior geothermal review changed.")
    resource["recipes"].update(deepcopy(review["recipes"]))
    for row in review["records"]:
        intern_review(resource, row["rule_review"])
    for parent, compiled in pending:
        parent.clear()
        parent.update(compiled)
