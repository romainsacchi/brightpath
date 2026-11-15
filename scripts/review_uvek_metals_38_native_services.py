"""Replay the inspected source-3.8 native-service reference metadata."""

import hashlib
import json
from copy import deepcopy
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_metals_38_native_services_review.json"
REVIEW_SHA256 = "75af2c313241dc22d4d99e7ff12abd30ff2bb49046c8b9ee9cca3126d5b91ed9"


def apply_metals_38_native_services_review(resource, root=ROOT):
    """Verify the complete frozen group before adding an exact sign profile."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Metals source-3.8 service evidence changed; repeat review.")
    review = json.loads(raw)
    if len(review["records"]) != 6:
        raise ValueError("Metals source-3.8 service scope changed.")
    pending = []
    indexed = {r["id"]: r for r in resource["replace"]}
    for row in review["records"]:
        parent = indexed[row["rule_id"]]
        if parent not in (row.get("original_rule", row["rule"]), row["rule"]):
            raise ValueError("Metals source-3.8 service parent changed; repeat review.")
        pending.append((parent, row["rule"]))
    for parent, compiled in pending:
        parent.clear()
        parent.update(deepcopy(compiled))
