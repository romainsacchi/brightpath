"""Replay the bounded source-3.9 train supplier review."""

import hashlib
import json
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_hydrogen_train_direct_39_review.json"
REVIEW_SHA256 = "31d8e3b769437c7a15e270796b720f929ab352b135495920f344198b89f2cbe3"


def apply_hydrogen_train_direct_39_review(resource, root=ROOT):
    """Freeze all twelve correspondences and change only the reviewed diesel rule."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Train source-3.9 supplier evidence changed; repeat review.")
    review = json.loads(raw)
    if len(review["rules"]) != 12 or len(review["profile_sign_extensions"]) != 1:
        raise ValueError("Train source-3.9 supplier scope changed.")
    pending = []
    for row in review["rules"]:
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        if parent not in (row["original_rule"], row["compiled_rule"]):
            raise ValueError("Train source-3.9 supplier parent changed; repeat review.")
        pending.append((parent, row["compiled_rule"]))
    staged = {"reviews": deepcopy(resource["reviews"])}
    for key, expected in review["retired_reviews"].items():
        if key in staged["reviews"] and staged["reviews"][key] != expected:
            raise ValueError("Retired train source-3.9 review changed; repeat review.")
    for card in review["reviews"]:
        intern_review(staged, card)
    compiled = {r["rule_id"]: r["compiled_rule"] for r in review["rules"]}
    references = {
        entry["review_ref"]
        for op in ("replace", "disaggregate", "delete")
        for parent in resource.get(op, ())
        for selected in [compiled.get(parent["id"], parent)]
        for entry in [selected, *selected.get("reference_flow_sign_review", {}).get("profiles", ())]
        if "review_ref" in entry
    }
    if set(review["retired_reviews"]) & references:
        raise ValueError("Retired train source-3.9 review is still referenced.")
    for key in review["retired_reviews"]:
        staged["reviews"].pop(key, None)
    resource["reviews"] = staged["reviews"]
    for parent, expected in pending:
        parent.clear()
        parent.update(deepcopy(expected))
