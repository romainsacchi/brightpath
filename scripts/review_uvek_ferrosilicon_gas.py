"""Replay exact inspected delivered low-pressure gas proxies."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_ferrosilicon_gas_review.json"
REVIEW_SHA256 = "f2918c3bd4da3ab9a6011103f9da081464e6a63affe897853399d5c8b5482f7c"


def apply_ferrosilicon_gas_review(resource, root=ROOT):
    """Validate all frozen parents and reviews before mutating any rule."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Delivered-gas supply evidence changed; repeat review.")
    review = json.loads(raw)
    if len(review["records"]) != 13:
        raise ValueError("Delivered-gas supply scope changed.")
    pending = []
    staged = {"reviews": deepcopy(resource.get("reviews", {}))}
    for row in review["records"]:
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        if parent not in (row["original_rule"], row["compiled_rule"]):
            raise ValueError("Delivered-gas supply parent changed; repeat review.")
        pending.append((parent, row["compiled_rule"]))
    for card in review["reviews"]:
        intern_review(staged, card)
    if any(rule["review_ref"] not in staged["reviews"] for _, rule in pending):
        raise ValueError("Delivered-gas supply review reference missing.")
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
