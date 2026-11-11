"""Qualify exact 3.8 cast-stock profiles while preserving older selections."""

import hashlib
import json
from copy import deepcopy
from pathlib import Path

from brightpath import TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_fuels_38_cast_profiles_review.json"
REVIEW_SHA256 = "3ce75513d30db79a2a31346f5feb3acdacca8e95f66f84f15f5b7908e0731b78"


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()


def apply_fuels_38_cast_profiles_review(resource, root=ROOT):
    """Bind positive kg references and unchanged cast-stock helper closure."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Cast-stock reference evidence changed.")
    review = json.loads(raw)
    if len(review["records"]) != 2:
        raise ValueError("Cast-stock profile scope changed.")
    for key, expected in review["recipe_dependencies"].items():
        if digest(resource["recipes"].get(key)) != expected:
            raise ValueError("Existing cast-stock helper changed.")
    pending = []
    for row in review["records"]:
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        original, compiled = row["original_rule"], row["compiled_rule"]
        if parent not in (original, compiled):
            raise ValueError("Cast-stock parent changed.")
        if (
            row["source_profile"] != {"family": "ecoinvent", "version": "3.8", "system_model": "cutoff"}
            or row["source_witness"]["reference_amounts"] != [1]
            or any(w["reference_amounts"] != [1] for w in row["source_producer_witnesses"])
            or compiled["target"] != original["target"]
            or compiled["reference_flow_sign_review"]["profiles"]
            != original["reference_flow_sign_review"]["profiles"] + [row["new_profile"]]
        ):
            raise ValueError("Cast-stock reference or older profile changed.")
        entry = row["new_profile"]
        if (
            entry["conversion_factor"] != 1
            or entry["review"]["source_sign"] != 1
            or entry["review"]["target_sign"] != 1
        ):
            raise ValueError("Cast-stock sign or scale changed.")
        for proof in row["preserved_profiles"]:
            selected = select_profile_sign_rules(
                {"replace": [compiled]}, TechnosphereProfile(**proof["source_profile"])
            )["replace"][0]
            if digest(selected) != proof["selected_rule_sha256"]:
                raise ValueError("Another cast-stock selected profile changed.")
        pending.append((parent, compiled))
    for witness in review["native_2026_witnesses"]:
        if witness["reference_amount"] != 1 or witness["nonproduction_count"] <= 0:
            raise ValueError("Native cast-stock or delivery reference changed.")
        if witness["identity"]["unit"] == "kilogram" and witness["direct_rolling_services"]:
            raise ValueError("Cast-stock proxy includes direct rolling.")
    proof = review["low_alloyed_conservation_proof"]
    if proof["source_producer_entries"] != 9 or len(proof["retained_freight_rows"]) != 4:
        raise ValueError("Cast-stock route or delivery conservation scope changed.")
    for parent, compiled in pending:
        parent.clear()
        parent.update(deepcopy(compiled))
