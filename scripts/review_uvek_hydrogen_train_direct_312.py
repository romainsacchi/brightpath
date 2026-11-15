"""Replay two exact-profile sign extensions backed by the train supply review."""

import hashlib
import json
from copy import deepcopy
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_hydrogen_train_direct_312_review.json"
REVIEW_SHA256 = "5ca880f0e916160f37f627a788870932607479345514cd70d29bef4d4ea2cf95"


def apply_hydrogen_train_direct_312_review(resource, root=ROOT):
    """Preserve existing profile decisions and reject changed parent rules."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Hydrogen-train direct supply evidence changed; repeat review.")
    review = json.loads(raw)
    if len(review["rules"]) != 13 or len(review["profile_sign_extensions"]) != 2:
        raise ValueError("Hydrogen-train direct supply scope changed.")
    pending = []
    for entry in review["profile_sign_extensions"]:
        parent = next(r for r in resource["replace"] if r["id"] == entry["rule_id"])
        if parent not in (entry["original_rule"], entry["compiled_rule"]):
            raise ValueError("Hydrogen-train direct supply parent changed; repeat review.")
        pending.append((parent, entry["compiled_rule"]))
    for parent, compiled in pending:
        parent.clear()
        parent.update(deepcopy(compiled))
