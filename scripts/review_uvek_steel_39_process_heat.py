"""Replay exact allocated3.9process-heat burdens."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_steel_39_process_heat_review.json"
REVIEW_SHA256 = "5d6d16df4371f2e0b4797e10f5db8d209cdcf2fc7adccef93e7e42e13d3c376b"


def apply_steel_39_process_heat_review(resource, root=ROOT):
    """Validate all frozen parents and reviews before mutating any rule."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Process-heat allocation evidence changed; repeat review.")
    review = json.loads(raw)
    if len(review["records"]) != 3:
        raise ValueError("Process-heat allocation scope changed.")
    pending = []
    staged = {"reviews": deepcopy(resource.get("reviews", {}))}
    for row in review["records"]:
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        if parent not in (row["original_rule"], row["compiled_rule"]):
            raise ValueError("Process-heat allocation parent changed; repeat review.")
        pending.append((parent, row["compiled_rule"]))
    for card in review["reviews"]:
        intern_review(staged, card)
    if any(rule["review_ref"] not in staged["reviews"] for _, rule in pending):
        raise ValueError("Process-heat allocation review reference missing.")
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
