"""Replay the exact source-3.9 retained acetoacetic-acid chemistry and feedstock market review."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath import TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_acetoacetic_acid_39_review.json"
REVIEW_SHA256 = "515c120c4f860eb6d4ecf27408aadc83d0a0359f8642ffb059dfae7ab796e0e6"


def apply_acetoacetic_acid_39_review(resource, root=ROOT):
    """Validate all frozen parents and reviews before mutating any rule."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Acetoacetic-acid evidence changed; repeat review.")
    review = json.loads(raw)
    if len(review["records"]) != 7:
        raise ValueError("Acetoacetic-acid scope changed.")
    pending = []
    staged = {"reviews": deepcopy(resource.get("reviews", {}))}
    for row in review["records"]:
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        if parent not in (row["original_rule"], row["compiled_rule"]):
            raise ValueError("Acetoacetic-acid parent changed; repeat review.")
        pending.append((parent, row["compiled_rule"]))
    source = TechnosphereProfile(**review["source_profile"])
    target = TechnosphereProfile(**resource["target_profile"])
    for dependency in review["source_rule_dependency_scopes"]:
        rule = next(r for r in resource["replace"] if r["id"] == dependency["rule_id"])
        if compatibility_rule_fingerprint(resource, "replace", rule, source, target) != dependency["fingerprint"]:
            raise ValueError("Acetoacetic-acid source dependency changed; repeat review.")
    for key, expected in review["recipe_dependencies"].items():
        if resource.get("recipes", {}).get(key) != expected:
            raise ValueError("Acetoacetic-acid dependency changed; repeat review.")
    for key, expected in review["recipes"].items():
        if key in resource.get("recipes", {}) and resource["recipes"][key] != expected:
            raise ValueError("Acetoacetic-acid recipe changed; repeat review.")
    for card in review["reviews"]:
        intern_review(staged, card)
    if any(rule.get("review_ref") and rule["review_ref"] not in staged["reviews"] for _, rule in pending):
        raise ValueError("Acetoacetic-acid review reference missing.")
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
