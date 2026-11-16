"""Replay bounded car service approvals and two freight corrections."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_passenger_car_services_312_review.json"
REVIEW_SHA256 = "1479f238b58dd4d5e9a5bce20b1cd639676bbd86836b2ec2ba595c8a4e56602a"


def apply_passenger_car_services_312_review(resource, root=ROOT):
    """Validate all parents before applying only the inspected decisions."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Passenger-car service evidence changed; repeat review.")
    review = json.loads(raw)
    if len(review["records"]) != 11 or len(review["reviews"]) != 2:
        raise ValueError("Passenger-car service scope changed.")
    pending = []
    for row in review["records"]:
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        if parent not in (row["original_rule"], row["compiled_rule"]):
            raise ValueError("Passenger-car service parent changed; repeat review.")
        pending.append((parent, row["compiled_rule"]))
    staged = {"reviews": deepcopy(resource.get("reviews", {}))}
    for card in review["reviews"]:
        intern_review(staged, card)
    resource["reviews"] = staged["reviews"]
    for parent, compiled in pending:
        parent.clear()
        parent.update(deepcopy(compiled))
    resource["coverage"]["mapping_methods"] = dict(Counter(r["mapping_method"] for r in resource["replace"]))
    dispositions = Counter(
        "recipe" if "recipe_ref" in r["target"] else r.get("classification", "unclassified")
        for r in resource["replace"]
    )
    omitted = sum(r.get("approved_omission", False) for r in resource["delete"])
    if omitted:
        dispositions["omitted"] = omitted
    if resource["coverage"].get("unresolved_identities"):
        dispositions["unresolved"] = resource["coverage"]["unresolved_identities"]
    resource["coverage"]["dispositions"] = dict(dispositions)
