"""Replay the exact source-3.9 ammonium sulfate review."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_ammonium_sulfate_39_review.json"
REVIEW_SHA256 = "49e70490e0bb37476aa40325863237c2af209253181cd5d84f13cd4086ccdc1e"


def apply_ammonium_sulfate_39_review(resource, root=ROOT):
    """Validate all frozen parents and reviews before mutating any rule."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Ammonium-sulfate evidence changed; repeat review.")
    review = json.loads(raw)
    if len(review["records"]) != 2:
        raise ValueError("Ammonium-sulfate scope changed.")
    pending = []
    staged = {"reviews": deepcopy(resource.get("reviews", {}))}
    for row in review["records"]:
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        if parent not in (row["original_rule"], row["compiled_rule"]):
            raise ValueError("Ammonium-sulfate parent changed; repeat review.")
        pending.append((parent, row["compiled_rule"]))
    for key, expected in review["source_rule_dependencies"].items():
        if next(r for r in resource["replace"] if r["id"] == key) != expected:
            raise ValueError("Ammonium-sulfate stock approval changed; repeat review.")
    for key, expected in review["recipe_dependencies"].items():
        if resource.get("recipes", {}).get(key) != expected:
            raise ValueError("Ammonium-sulfate dependency changed; repeat review.")
    for key, expected in review["recipes"].items():
        if key in resource.get("recipes", {}) and resource["recipes"][key] != expected:
            raise ValueError("Ammonium-sulfate recipe changed; repeat review.")
    for card in review["reviews"]:
        intern_review(staged, card)
    if any(rule.get("review_ref") and rule["review_ref"] not in staged["reviews"] for _, rule in pending):
        raise ValueError("Ammonium-sulfate review reference missing.")
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
