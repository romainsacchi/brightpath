"""Replay exact inspected complete stored-fuel proxies."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_ferrosilicon_stored_fuels_review.json"
REVIEW_SHA256 = "49d04d53874ead3b453cea1fe580a06760dba9ba46a064f23cea92cd0370d47c"


def apply_ferrosilicon_stored_fuels_review(resource, root=ROOT):
    """Validate all frozen parents and reviews before mutating any rule."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Stored-fuel supply evidence changed; repeat review.")
    review = json.loads(raw)
    if len(review["records"]) != 15:
        raise ValueError("Stored-fuel supply scope changed.")
    pending = []
    staged = {"reviews": deepcopy(resource.get("reviews", {}))}
    for row in review["records"]:
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        if parent not in (row["original_rule"], row["compiled_rule"]):
            raise ValueError("Stored-fuel supply parent changed; repeat review.")
        pending.append((parent, row["compiled_rule"]))
    for card in review["reviews"]:
        intern_review(staged, card)
    if any(rule["review_ref"] not in staged["reviews"] for _, rule in pending):
        raise ValueError("Stored-fuel supply review reference missing.")
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
