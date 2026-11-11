"""Add scoped market profiles and the inspected patch cast-steel route split."""

import hashlib
import json
from copy import deepcopy
from pathlib import Path

from brightpath import TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_fuels_37_market_profiles_review.json"
REVIEW_SHA256 = "623453b9169bdb035d3a448507e73c5de6c1aa7e3436c0a7755859a9a6c3410b"


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def apply_fuels_37_market_profiles_review(resource, root=ROOT):
    """Preserve all older selected rules and recipe contents."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Market-profile evidence changed.")
    review = json.loads(raw)
    for row in review["records"]:
        old, new = row["original_rule"], row["compiled_rule"]
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        if parent not in (old, new):
            raise ValueError("Market parent changed before extension.")
        factor = row["conversion_factor"]
        if (
            factor not in (-1, 1)
            or row["source_profile"] != {"family": "ecoinvent", "version": "3.7", "system_model": "cutoff"}
            or row["source_witness"]["reference_amounts"] != [factor]
            or row["source_witness"]["inspected_profile"]["version"] != "3.7.1"
            or any(w["reference_amount"] != 1 for w in row["native_2026_witnesses"])
        ):
            raise ValueError("Market source reference or scope changed.")
        expected = deepcopy(old)
        expected["reference_flow_sign_review"]["profiles"].append(row["new_profile"])
        if expected != new or row["new_profile"]["review"]["source_sign"] != factor:
            raise ValueError("Market extension changed another mapping field.")
        for witness in row["existing_recipe_witnesses"]:
            if digest(resource["recipes"][witness["recipe_id"]]) != witness["sha256"]:
                raise ValueError("An existing market helper changed.")
        for proof in row["preserved_profiles"]:
            selected = select_profile_sign_rules({"replace": [new]}, TechnosphereProfile(**proof["source_profile"]))[
                "replace"
            ][0]
            if digest(selected) != proof["selected_rule_sha256"]:
                raise ValueError("An older selected market profile changed.")
        parent.clear()
        parent.update(deepcopy(new))
    for key, recipe in review["recipes"].items():
        if key in resource["recipes"] and resource["recipes"][key] != recipe:
            raise ValueError("New cast-route recipe conflicts with existing content.")
        resource["recipes"][key] = deepcopy(recipe)
