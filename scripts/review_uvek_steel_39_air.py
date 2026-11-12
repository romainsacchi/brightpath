"""Replay the source-scoped delivered compressed-air boundary correction."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_steel_39_air_review.json"
REVIEW_SHA256 = "007378f77d3801d4e9e402b226fa4bb77f7a7d86ec88cf7c6fe619a2cf7df9f4"


def apply_steel_39_air_review(resource, root=ROOT):
    """Refuse evidence/parent drift before applying the exact reviewed rule."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Compressed-air evidence changed; repeat review.")
    review = json.loads(raw)
    if len(review["records"]) != 1 or review["native_compressor_input"] != 1.3:
        raise ValueError("Compressed-air reference boundary changed.")
    row = review["records"][0]
    parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
    if parent not in (row["original_rule"], row["compiled_rule"]):
        raise ValueError("Compressed-air parent changed; repeat review.")
    staged = {"reviews": deepcopy(resource.get("reviews", {}))}
    for card in review["reviews"]:
        intern_review(staged, card)
    compiled = row["compiled_rule"]
    if compiled["review_ref"] not in staged["reviews"]:
        raise ValueError("Compressed-air review reference missing.")
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
