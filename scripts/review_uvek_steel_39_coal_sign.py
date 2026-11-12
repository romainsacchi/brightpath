"""Add the inspected 3.9 positive-mass coal reference without changing 3.8."""

import hashlib
import json
from copy import deepcopy
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_steel_39_remaining_supplies_review.json"
REVIEW_SHA256 = "bc3f80a505bef6a960aef939194151a52be1ebe4508114abdb3faf27e5423cbe"


def apply_steel_39_coal_sign_review(resource, root=ROOT):
    """Apply only the frozen profile addition after validating its parent."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Coal reference evidence changed; repeat review.")
    review = json.loads(raw)
    row = next(r for r in review["records"] if r["rule_id"] == "bp-5ede2be860ad337d")
    parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
    if parent not in (row["original_rule"], row["compiled_rule"]):
        raise ValueError("Coal reference parent changed; repeat review.")
    parent.clear()
    parent.update(deepcopy(row["compiled_rule"]))
