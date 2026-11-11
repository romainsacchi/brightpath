"""Extend HDPE delivery with exact 3.8/3.11 profiles, preserving older selections."""

import hashlib
import json
from copy import deepcopy
from pathlib import Path

from brightpath import TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_packaging_hdpe_review.json"
REVIEW_SHA256 = "dfc0dda86956a7b7c65dcc374a22a1417ab2f232181c1a9385f5976e75dc5b3a"


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()


def apply_packaging_hdpe_review(resource, root=ROOT):
    """Validate full frozen evidence and old helper closures before mutation."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("HDPE evidence changed; repeat the scientific review.")
    review = json.loads(raw)
    parent = next(row for row in resource["replace"] if row["id"] == review["rule_id"])
    if parent not in (review["original_rule"], review["compiled_rule"]):
        raise ValueError("HDPE parent changed before extension.")
    for rid, expected in review["recipe_dependencies"].items():
        if digest(resource["recipes"].get(rid)) != expected:
            raise ValueError("Previously qualified HDPE helper closure changed.")
    for witness in review["native_2026_witnesses"]:
        if witness["reference_amount"] != 1 or witness["nonproduction_count"] <= 0:
            raise ValueError("Native HDPE/freight reference changed.")
    profiles = [row["source_profile"] for row in review["records"]]
    if profiles != [
        {"family": "ecoinvent", "version": version, "system_model": "cutoff"} for version in ("3.8", "3.11")
    ]:
        raise ValueError("New HDPE source scope changed.")
    candidate = {**resource, "replace": [review["compiled_rule"]]}
    for row in review["records"]:
        selected = select_profile_sign_rules(candidate, TechnosphereProfile(**row["source_profile"]))["replace"][0]
        recipe = review["recipes"][row["new_recipe_id"]]
        omissions = recipe.get("source_input_omissions", [])
        expected_omissions = 4 if row["source_profile"]["version"] == "3.8" else 0
        if (
            row["source_witness"]["reference_amounts"] != [1]
            or selected["target"] != {"recipe_ref": row["new_recipe_id"]}
            or selected["conversion_factor"] != 1
            or len(recipe["inputs"]) != 5
            or len(omissions) != expected_omissions
            or omissions != row["source_omissions"]
            or any(o["source_profile"] != row["source_profile"] for o in omissions)
            or any(o["source_exchange"]["amount"] != 2.93285310007683e-14 for o in omissions)
        ):
            raise ValueError("HDPE functional reference, delivery or exact omission scope changed.")
    for proof in review["preserved_profiles"]:
        selected = select_profile_sign_rules(candidate, TechnosphereProfile(**proof["source_profile"]))["replace"][0]
        if (
            digest(selected) != proof["selected_rule_sha256"]
            or digest(resource["reviews"][selected["review_ref"]]) != proof["shared_review_sha256"]
        ):
            raise ValueError("Previously qualified HDPE selected behavior changed.")
    for rid, recipe in review["recipes"].items():
        if rid in resource["recipes"] and resource["recipes"][rid] != recipe:
            raise ValueError("Derived HDPE delivery helper changed.")
    resource["recipes"].update(deepcopy(review["recipes"]))
    parent.clear()
    parent.update(deepcopy(review["compiled_rule"]))
