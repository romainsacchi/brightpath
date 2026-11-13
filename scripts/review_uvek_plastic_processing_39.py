"""Replay the exact source-3.9 plastic-processing services review."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_plastic_processing_39_review.json"
REVIEW_SHA256 = "bec6d3cfe95501fd43a5189a78833c2c86b3aeedae23968a3fc5776ad8e11e96"


def apply_plastic_processing_39_review(resource, root=ROOT):
    """Validate all frozen parents and reviews before mutating any rule."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Plastic-processing evidence changed; repeat review.")
    review = json.loads(raw)
    if len(review["records"]) != 3:
        raise ValueError("Plastic-processing scope changed.")
    pending = []
    staged = {"reviews": deepcopy(resource.get("reviews", {}))}
    for row in review["records"]:
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        if parent not in (row["original_rule"], row["compiled_rule"]):
            raise ValueError("Plastic-processing parent changed; repeat review.")
        pending.append((parent, row["compiled_rule"]))
    for key, expected in review["source_rule_dependencies"].items():
        if next(r for r in resource["replace"] if r["id"] == key) != expected:
            raise ValueError("Plastic-processing stock approval changed; repeat review.")
    for key, expected in review["recipe_dependencies"].items():
        if resource.get("recipes", {}).get(key) != expected:
            raise ValueError("Plastic-processing dependency changed; repeat review.")
    for key, expected in review["recipes"].items():
        if key in resource.get("recipes", {}) and resource["recipes"][key] != expected:
            raise ValueError("Plastic-processing recipe changed; repeat review.")
    for card in review["reviews"]:
        intern_review(staged, card)
    if any(rule.get("review_ref") and rule["review_ref"] not in staged["reviews"] for _, rule in pending):
        raise ValueError("Plastic-processing review reference missing.")
    resource.setdefault("recipes", {}).update(deepcopy(review["recipes"]))
    resource["reviews"] = staged["reviews"]
    for parent, compiled in pending:
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
