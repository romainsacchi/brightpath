"""Replay the source-scoped small-scale non-gas heat mix."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_steel_39_small_heat_review.json"
REVIEW_SHA256 = "8f55c8165ad078cf09de31490bd183a0ac13d3df847623cc8da5be04d3c2a4db"


def apply_steel_39_small_heat_review(resource, root=ROOT):
    """Refuse evidence/parent drift before applying the exact reviewed rule."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Small-scale heat evidence changed; repeat review.")
    review = json.loads(raw)
    if len(review["records"]) != 1 or len(review["recipes"]) != 9:
        raise ValueError("Small-scale heat reference boundary changed.")
    row = review["records"][0]
    parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
    if parent not in (row["original_rule"], row["compiled_rule"]):
        raise ValueError("Small-scale heat parent changed; repeat review.")
    for rid, recipe in review["recipes"].items():
        if rid in resource.get("recipes", {}) and resource["recipes"][rid] != recipe:
            raise ValueError("Small-scale heat recipe changed; repeat review.")
    staged = {"reviews": deepcopy(resource.get("reviews", {}))}
    for card in review["reviews"]:
        intern_review(staged, card)
    compiled = row["compiled_rule"]
    if compiled["review_ref"] not in staged["reviews"]:
        raise ValueError("Small-scale heat review reference missing.")
    resource.setdefault("recipes", {}).update(deepcopy(review["recipes"]))
    resource["reviews"] = staged["reviews"]
    parent.clear()
    parent.update(deepcopy(compiled))
    resource["coverage"]["mapping_methods"] = dict(
        sorted(Counter(r["mapping_method"] for r in resource["replace"]).items())
    )
    resource["coverage"]["mean_confidence"] = round(
        sum(r["confidence"] for r in resource["replace"]) / len(resource["replace"]), 6
    )
    dispositions = Counter(
        "recipe" if "recipe_ref" in r["target"] else r.get("classification", "unclassified")
        for r in resource["replace"]
    )
    dispositions["omitted"] = sum(r.get("approved_omission", False) for r in resource["delete"])
    if resource["coverage"].get("unresolved_identities"):
        dispositions["unresolved"] = resource["coverage"]["unresolved_identities"]
    resource["coverage"]["dispositions"] = dict(dispositions)
