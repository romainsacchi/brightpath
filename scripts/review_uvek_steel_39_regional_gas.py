"""Add the inspected 3.9 nominal-energy gas reference without changing other releases."""

import hashlib
import json
from copy import deepcopy
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_steel_39_regional_gas_review.json"
REVIEW_SHA256 = "f496fa6f29308681a12f8de6ec9609d9c72f3304a8e761a4ea0bbdc29264c07c"


def apply_steel_39_regional_gas_review(resource, root=ROOT):
    """Apply only the frozen profile addition after validating its parent."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Regional-gas reference evidence changed; repeat review.")
    review = json.loads(raw)
    row = next(r for r in review["records"] if r["rule_id"] == "bp-7fe8308839f735bb")
    parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
    if parent not in (row["original_rule"], row["compiled_rule"]):
        raise ValueError("Regional-gas reference parent changed; repeat review.")
    recipe = resource["recipes"][review["reused_recipe_id"]]
    digest = hashlib.sha256(json.dumps(recipe, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    if digest != review["reused_recipe_sha256"]:
        raise ValueError("Regional-gas recipe changed; repeat review.")
    parent.clear()
    parent.update(deepcopy(row["compiled_rule"]))
