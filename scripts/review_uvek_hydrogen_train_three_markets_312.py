"""Replay exact source-3.12 titanium, silica and hazardous-waste market reviews."""

import hashlib
import json
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_hydrogen_train_three_markets_312_review.json"
REVIEW_SHA256 = "92188700d50b7bb98b5d7ca3a4adc5a163e4f2f21b50fd73fa203f126d153a7e"


def apply_hydrogen_train_three_markets_312_review(resource, root=ROOT):
    """Check frozen parents and recipes before applying the reviewed extensions."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Hydrogen-train market evidence changed; repeat review.")
    review = json.loads(raw)
    if len(review["records"]) != 3 or len(review["recipes"]) != 3:
        raise ValueError("Hydrogen-train market scope changed.")
    staged = {"reviews": deepcopy(resource.get("reviews", {}))}
    pending = []
    for row in review["records"]:
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        if parent not in (row["original_rule"], row["compiled_rule"]):
            raise ValueError("Hydrogen-train market parent changed; repeat review.")
        pending.append((parent, row["compiled_rule"]))
    for key, expected in review["recipes"].items():
        if key in resource.get("recipes", {}) and resource["recipes"][key] != expected:
            raise ValueError("Hydrogen-train market recipe changed; repeat review.")
    for key, expected in review["retired_reviews"].items():
        if key in staged["reviews"] and staged["reviews"][key] != expected:
            raise ValueError("Retired hydrogen-train market review changed.")
    for card in review["reviews"]:
        intern_review(staged, card)
    replacements = {parent["id"]: compiled for parent, compiled in pending}
    referenced = {
        entry["review_ref"]
        for operation in ("replace", "disaggregate", "delete")
        for rule in resource.get(operation, ())
        for selected in [replacements.get(rule["id"], rule)]
        for entry in [selected, *selected.get("reference_flow_sign_review", {}).get("profiles", ())]
        if "review_ref" in entry
    }
    if set(review["retired_reviews"]) & referenced:
        raise ValueError("Retired hydrogen-train market review is still referenced.")
    for key in review["retired_reviews"]:
        staged["reviews"].pop(key, None)
    resource.setdefault("recipes", {}).update(deepcopy(review["recipes"]))
    resource["reviews"] = staged["reviews"]
    for parent, compiled in pending:
        parent.clear()
        parent.update(deepcopy(compiled))
