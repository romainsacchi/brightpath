"""Replay exact exact3.9auxiliary chemical markets."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_pulp_heat_wood_markets_review.json"
REVIEW_SHA256 = "e0ee81a930e2c5f29efcf306564648738f0bdae12da7c68d303eacfacd504c8c"


def apply_pulp_heat_wood_markets_review(resource, root=ROOT):
    """Validate all frozen parents and reviews before mutating any rule."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Auxiliary chemical market evidence changed; repeat review.")
    review = json.loads(raw)
    if len(review["records"]) != 5:
        raise ValueError("Auxiliary chemical market scope changed.")
    pending = []
    staged = {"reviews": deepcopy(resource.get("reviews", {}))}
    for row in review["records"]:
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        if parent not in (row["original_rule"], row["compiled_rule"]):
            raise ValueError("Auxiliary chemical market parent changed; repeat review.")
        pending.append((parent, row["compiled_rule"]))
    for key, expected in review["recipe_dependencies"].items():
        if resource.get("recipes", {}).get(key) != expected:
            raise ValueError("Residue dependency changed; repeat review.")
    for key, expected in review["recipes"].items():
        if key in resource.get("recipes", {}) and resource["recipes"][key] != expected:
            raise ValueError("Residue recipe changed; repeat review.")
    for card in review["reviews"]:
        intern_review(staged, card)
    if any(rule.get("review_ref") and rule["review_ref"] not in staged["reviews"] for _, rule in pending):
        raise ValueError("Auxiliary chemical market review reference missing.")
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
