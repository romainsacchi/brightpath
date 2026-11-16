"""Replay eight exact-profile car supplies without substituting unrelated oxides."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_passenger_car_39_review.json"
REVIEW_SHA256 = "073094ffa72451482e3e72586108ff33329b87adad46c9c5ff52be515104078f"


def apply_passenger_car_39_review(resource, root=ROOT):
    """Validate frozen parents and recipes before changing any resource data."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Passenger-car source3.9 evidence changed; repeat review.")
    review = json.loads(raw)
    if len(review["records"]) != 8 or len(review["recipes"]) != 2:
        raise ValueError("Passenger-car source3.9 scope changed.")
    pending = []
    for row in review["records"]:
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        if parent not in (row["original_rule"], row["compiled_rule"]):
            raise ValueError("Passenger-car source3.9 parent changed; repeat review.")
        pending.append((parent, row["compiled_rule"]))
    for key, expected in review["recipes"].items():
        if key in resource["recipes"] and resource["recipes"][key] != expected:
            raise ValueError("Passenger-car source3.9 recipe changed; repeat review.")
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
