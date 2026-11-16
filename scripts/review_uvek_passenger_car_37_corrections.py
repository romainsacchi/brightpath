"""Replay the exact source3.7 alloy, fossil-petrol and CNG corrections."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_passenger_car_37_corrections_review.json"
REVIEW_SHA256 = "cf39a0b9f7bf2063e1eb1a3eaaacb05973b52931c7e64fb71651bafe24b5ed6b"


def apply_passenger_car_37_corrections_review(resource, root=ROOT):
    """Reject changed evidence or dependencies before applying exact profiles."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Source3.7 corrections passenger-car evidence changed; repeat review.")
    review = json.loads(raw)
    if len(review["records"]) != 3 or len(review["recipes"]) != 3:
        raise ValueError("Source3.7 corrections passenger-car scope changed.")
    pending = []
    for row in review["records"]:
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        if parent not in (row["original_rule"], row["compiled_rule"]):
            raise ValueError("Source3.7 corrections passenger-car parent changed; repeat review.")
        pending.append((parent, row["compiled_rule"]))
    for key, expected in review["recipe_dependency_sha256"].items():
        raw_recipe = json.dumps(resource["recipes"][key], sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        if hashlib.sha256(raw_recipe.encode()).hexdigest() != expected:
            raise ValueError("Source3.7 corrections passenger-car model dependency changed; repeat review.")
    for key, expected in review["recipes"].items():
        if key in resource["recipes"] and resource["recipes"][key] != expected:
            raise ValueError("Source3.7 corrections passenger-car recipe changed; repeat review.")
    staged = {"reviews": deepcopy(resource["reviews"])}
    for card in review["reviews"]:
        intern_review(staged, card)
    resource["recipes"].update(deepcopy(review["recipes"]))
    resource["reviews"] = staged["reviews"]
    for parent, compiled in pending:
        parent.clear()
        parent.update(deepcopy(compiled))
    rules = resource["replace"]
    resource["coverage"]["mapping_methods"] = dict(Counter(r["mapping_method"] for r in rules))
    resource["coverage"]["mean_confidence"] = round(sum(r["confidence"] for r in rules) / len(rules), 6)
    dispositions = Counter(
        "recipe" if "recipe_ref" in r["target"] else r.get("classification", "unclassified") for r in rules
    )
    omitted = sum(r.get("approved_omission", False) for r in resource["delete"])
    if omitted:
        dispositions["omitted"] = omitted
    if resource["coverage"].get("unresolved_identities"):
        dispositions["unresolved"] = resource["coverage"]["unresolved_identities"]
    resource["coverage"]["dispositions"] = dict(dispositions)
