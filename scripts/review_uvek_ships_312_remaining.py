"""Replay the remaining source3.12 shipping boundaries for source 3.12."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_ships_312_remaining_review.json"
REVIEW_SHA256 = "81659b690957809862004c2ad78bb8159a4c22be697de4dd4ec662b86580544a"


def apply_ships_312_remaining_review(resource, root=ROOT):
    """Validate all frozen parents and reviews before mutating any rule."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Shipping3.12 evidence changed; repeat review.")
    review = json.loads(raw)
    if len(review["records"]) != 7:
        raise ValueError("Shipping3.12 scope changed.")
    pending = []
    staged = {"reviews": deepcopy(resource.get("reviews", {}))}
    for row in review["records"]:
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        if parent not in (row["original_rule"], row["compiled_rule"]):
            raise ValueError("Shipping3.12 parent changed; repeat review.")
        pending.append((parent, row["compiled_rule"]))
    for key, expected in review["recipe_dependencies"].items():
        if resource.get("recipes", {}).get(key) != expected:
            raise ValueError("Shipping3.12 dependency changed; repeat review.")
    for key, expected in review["recipes"].items():
        if key in resource.get("recipes", {}) and resource["recipes"][key] != expected:
            raise ValueError("Shipping3.12 recipe changed; repeat review.")
    for key, expected in review["retired_reviews"].items():
        if staged["reviews"].get(key) != expected:
            raise ValueError("Retired shipping review changed; repeat review.")
    for card in review["reviews"]:
        intern_review(staged, card)
    if any(rule.get("review_ref") and rule["review_ref"] not in staged["reviews"] for _, rule in pending):
        raise ValueError("Shipping3.12 review reference missing.")
    resource.setdefault("recipes", {}).update(deepcopy(review["recipes"]))
    resource["reviews"] = staged["reviews"]
    for parent, compiled in pending:
        parent.clear()
        parent.update(deepcopy(compiled))
    referenced = {
        entry["review_ref"]
        for operation in ("replace", "disaggregate", "delete")
        for rule in resource.get(operation, ())
        for entry in [rule, *rule.get("reference_flow_sign_review", {}).get("profiles", ())]
        if "review_ref" in entry
    }
    if set(review["retired_reviews"]) & referenced:
        raise ValueError("Retired shipping review is still referenced.")
    for key in review["retired_reviews"]:
        resource["reviews"].pop(key)
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
