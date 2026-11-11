"""Extend only inspected 3.11 material and hydrogen sign profiles."""

import hashlib
import json
from copy import deepcopy
from pathlib import Path

from brightpath import TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_fuels_311_remaining_review.json"
REVIEW_SHA256 = "eb77660a7a48e8df51953d488a8b8cb19ec5ca67ea23ea24afd3adef263aa22e"


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def apply_fuels_311_remaining_review(resource, root=ROOT):
    """Keep all previous selected profiles, helpers and physical reference bases."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Material/hydrogen reference evidence changed.")
    for row in json.loads(raw)["records"]:
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        old, new = row["original_rule"], row["compiled_rule"]
        if parent not in (old, new):
            raise ValueError("Material/hydrogen parent changed before scoped extension.")
        if (
            row["source_profile"] != {"family": "ecoinvent", "version": "3.11", "system_model": "cutoff"}
            or row["source_witness"]["reference_amounts"] != [1]
            or any(p["reference_amounts"] != [1] for p in row["source_producer_witnesses"])
            or row["native_2026_witness"]["reference_amount"] != 1
            or old["source"]["unit"] != old["target"]["unit"]
        ):
            raise ValueError("Material/hydrogen source profile or functional reference changed.")
        expected = deepcopy(old)
        if row["new_profile"] is not None:
            expected["reference_flow_sign_review"]["profiles"].append(row["new_profile"])
        if expected != new or row["conversion_factor"] != 1:
            raise ValueError("Extension changed another mapping field.")
        for proof in row["preserved_profiles"]:
            selected = select_profile_sign_rules({"replace": [new]}, TechnosphereProfile(**proof["source_profile"]))[
                "replace"
            ][0]
            if digest(selected) != proof["selected_rule_sha256"]:
                raise ValueError("An older selected profile changed.")
        parent.clear()
        parent.update(deepcopy(new))
