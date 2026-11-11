"""Qualify exact 3.8 gas/wood profiles while preserving older selections."""

import hashlib
import json
from copy import deepcopy
from pathlib import Path

from brightpath import TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_fuels_38_gas_wood_review.json"
REVIEW_SHA256 = "03916903d0d6b26384820e09713218bb8aa226eac6e4d92f1c503b543963ac1d"


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()


def apply_fuels_38_gas_wood_review(resource, root=ROOT):
    """Bind positive kg references and unchanged gas/wood helper closure."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Gas/wood reference evidence changed.")
    review = json.loads(raw)
    if len(review["records"]) != 2:
        raise ValueError("Gas/wood profile scope changed.")
    pending = []
    for row in review["records"]:
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        original, compiled = row["original_rule"], row["compiled_rule"]
        if parent not in (original, compiled):
            raise ValueError("Gas/wood parent changed.")
        if (
            row["source_profile"] != {"family": "ecoinvent", "version": "3.8", "system_model": "cutoff"}
            or row["source_witness"]["reference_amounts"] != [1]
            or any(w["reference_amounts"] != [1] for w in row["source_producer_witnesses"])
            or compiled["target"] != original["target"]
            or compiled["reference_flow_sign_review"]["profiles"]
            != original["reference_flow_sign_review"]["profiles"] + [row["new_profile"]]
        ):
            raise ValueError("Gas/wood reference or older profile changed.")
        entry = row["new_profile"]
        if (
            entry["conversion_factor"] != 1
            or entry["review"]["source_sign"] != 1
            or entry["review"]["target_sign"] != 1
        ):
            raise ValueError("Gas/wood sign or scale changed.")
        for proof in row["preserved_profiles"]:
            selected = select_profile_sign_rules(
                {"replace": [compiled]}, TechnosphereProfile(**proof["source_profile"])
            )["replace"][0]
            if digest(selected) != proof["selected_rule_sha256"]:
                raise ValueError("Another gas/wood selected profile changed.")
        pending.append((parent, compiled))
    for witness in review["native_2026_witnesses"]:
        if witness["reference_amount"] != 1 or witness["nonproduction_count"] <= 0:
            raise ValueError("Native gas/wood or delivery reference changed.")
    proof = review["wood_delivery_proof"]
    recipe = review["recipes"]["fuels-38-wet-wood-dry-mass-delivery"]
    if (
        len(review["recipes"]) != 1
        or len(recipe["inputs"]) != 2
        or recipe["inputs"][0]["amount"] != 1
        or recipe["inputs"][0]["unit"] != "kilogram"
        or proof["statistics"]["amount"] != 0.105
        or proof["source_formula_transplanted"]
        or any(proof["native_direct_delivery_amounts"])
        or recipe["inputs"][1] != {**proof["statistics"], "type": "technosphere", **proof["target"]}
    ):
        raise ValueError("Wet-wood dry-mass or final-delivery basis changed.")
    for key, recipe in review["recipes"].items():
        if key in resource["recipes"] and resource["recipes"][key] != recipe:
            raise ValueError("Gas/wood delivery helper changed.")
    resource["recipes"].update(deepcopy(review["recipes"]))
    for parent, compiled in pending:
        parent.clear()
        parent.update(deepcopy(compiled))
