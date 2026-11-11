"""Refine only the inspected 3.8 RoW chemical-heat water uncertainty."""

import hashlib
import json
from copy import deepcopy
from pathlib import Path

from brightpath import TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_fuels_38_chemical_heat_review.json"
REVIEW_SHA256 = "ddaa829f9342dcdedd4b4e18cd1a0be7bcae8f800209f55d53104b4025c79694"


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()


def apply_fuels_38_chemical_heat_review(resource, root=ROOT):
    """Bind old helper preservation and all nine physical-water distributions."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Chemical-heat uncertainty evidence changed; repeat review.")
    review = json.loads(raw)
    parent = next(r for r in resource["replace"] if r["id"] == review["rule_id"])
    if parent not in (review["original_rule"], review["compiled_rule"]):
        raise ValueError("Chemical-heat parent changed before scoped refinement.")
    for rid, expected in review["recipe_dependencies"].items():
        if digest(resource["recipes"].get(rid)) != expected:
            raise ValueError("Previously qualified chemical-heat helper changed.")
    evidence = review["original_heat_boundary_evidence"]
    if hashlib.sha256((root / "brightpath/data" / evidence["path"]).read_bytes()).hexdigest() != evidence["sha256"]:
        raise ValueError("Native MJ/closed-steam water boundary evidence changed.")
    if review["source_market"]["reference_amounts"] != [1] or review["source_producer"]["reference_amounts"] != [1]:
        raise ValueError("Chemical-steam MJ reference changed.")
    candidate = {**resource, "replace": [review["compiled_rule"]]}
    selected = select_profile_sign_rules(candidate, TechnosphereProfile(**review["source_profile"]))["replace"][0]
    recipe = review["recipes"][review["new_recipe_id"]]
    if (
        review["source_profile"] != {"family": "ecoinvent", "version": "3.8", "system_model": "cutoff"}
        or selected["target"] != {"recipe_ref": review["new_recipe_id"]}
        or selected["conversion_factor"] != 1
        or len(recipe["inputs"]) != 10
        or recipe["inputs"][0]["amount"] != 1
        or recipe["inputs"][0]["unit"] != "megajoule"
        or len(review["retained_water_rows"]) != 9
    ):
        raise ValueError("Chemical-heat reference, source scope or water count changed.")
    for actual, row in zip(recipe["inputs"][1:], review["retained_water_rows"], strict=True):
        if row["statistics"]["uncertainty type"] != 2 or any(actual.get(k) != v for k, v in row["statistics"].items()):
            raise ValueError("Retained water quantity or marginal uncertainty changed.")
    for proof in review["preserved_profiles"]:
        selected = select_profile_sign_rules(candidate, TechnosphereProfile(**proof["source_profile"]))["replace"][0]
        if digest(selected) != proof["selected_rule_sha256"]:
            raise ValueError("Another chemical-heat profile changed.")
    for rid, q in review["recipes"].items():
        if rid in resource["recipes"] and resource["recipes"][rid] != q:
            raise ValueError("Refined chemical-heat helper changed.")
    resource["recipes"].update(deepcopy(review["recipes"]))
    parent.clear()
    parent.update(deepcopy(review["compiled_rule"]))
