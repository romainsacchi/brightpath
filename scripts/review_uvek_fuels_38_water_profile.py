"""Add only the independently inspected 3.8 ion-exchange water sign profile."""

import hashlib
import json
from copy import deepcopy
from pathlib import Path

from brightpath import TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_fuels_38_water_profile_review.json"
REVIEW_SHA256 = "50ffbb6e3c544a2c6d893b95a8d48045ee2cdc9453ff750cbd32cd25f468b0cd"


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def apply_fuels_38_water_profile_review(resource, root=ROOT):
    """Preserve old profile decisions and require positive one-kg references."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Water reference-profile evidence changed.")
    review = json.loads(raw)
    parent = next(r for r in resource["replace"] if r["id"] == review["rule_id"])
    if parent not in (review["original_rule"], review["compiled_rule"]):
        raise ValueError("Water parent changed before scoped extension.")
    evidence = review["prior_inspection"]
    if hashlib.sha256((root / "brightpath/data" / evidence["path"]).read_bytes()).hexdigest() != evidence["sha256"]:
        raise ValueError("Water physical-basis inspection changed.")
    if (
        review["source_profile"] != {"family": "ecoinvent", "version": "3.8", "system_model": "cutoff"}
        or review["source_witness"]["reference_amounts"] != [1]
        or any(w["reference_amounts"] != [1] for w in review["source_producer_witnesses"])
        or review["native_2026_witness"]["reference_amount"] != 1
        or review["compiled_rule"]["source"]["unit"] != "kilogram"
        or review["compiled_rule"]["target"]["unit"] != "kilogram"
    ):
        raise ValueError("Water source scope or kg reference changed.")
    compiled = review["compiled_rule"]
    original = review["original_rule"]
    profiles = compiled["reference_flow_sign_review"]["profiles"]
    if profiles != original["reference_flow_sign_review"]["profiles"] + [review["new_profile"]]:
        raise ValueError("Water extension changed existing reference profiles.")
    entry = review["new_profile"]
    if entry["conversion_factor"] != 1 or entry["review"]["source_sign"] != 1 or entry["review"]["target_sign"] != 1:
        raise ValueError("Water sign or scale changed.")
    for proof in review["preserved_profiles"]:
        selected = select_profile_sign_rules({"replace": [compiled]}, TechnosphereProfile(**proof["source_profile"]))[
            "replace"
        ][0]
        if digest(selected) != proof["selected_rule_sha256"]:
            raise ValueError("Another water profile changed.")
    parent.clear()
    parent.update(deepcopy(compiled))
