"""Replay the current FeSi market with an explicitly reviewed historical model."""

import hashlib
import json
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_hydrogen_train_ferrosilicon_312_review.json"
REVIEW_SHA256 = "4422bc1c643e1f8eaf05d7ecd9c147a3a4732105ec852cfdfe584a4c9488589d"


def apply_hydrogen_train_ferrosilicon_312_review(resource, root=ROOT):
    """Freeze the reviewed parent and complete reused model before mutation."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Historical FeSi proxy evidence changed; repeat review.")
    review = json.loads(raw)
    if len(review["records"]) != 1 or len(review["recipes"]) != 1:
        raise ValueError("Historical FeSi proxy scope changed.")
    row = review["records"][0]
    parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
    if parent not in (row["original_rule"], row["compiled_rule"]):
        raise ValueError("Historical FeSi proxy parent changed; repeat review.")
    for key, expected in review["recipe_dependency_sha256"].items():
        raw_recipe = json.dumps(resource["recipes"][key], sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        if hashlib.sha256(raw_recipe.encode()).hexdigest() != expected:
            raise ValueError("Historical FeSi model dependency changed; repeat review.")
    for key, expected in review["recipes"].items():
        if key in resource["recipes"] and resource["recipes"][key] != expected:
            raise ValueError("Historical FeSi market recipe changed; repeat review.")
    staged = {"reviews": deepcopy(resource["reviews"])}
    for key, expected in review["retired_reviews"].items():
        if key in staged["reviews"] and staged["reviews"][key] != expected:
            raise ValueError("Retired FeSi review changed; repeat review.")
    for card in review["reviews"]:
        intern_review(staged, card)
    referenced = {
        entry["review_ref"]
        for op in ("replace", "disaggregate", "delete")
        for rule in resource.get(op, ())
        for selected in [row["compiled_rule"] if rule["id"] == row["rule_id"] else rule]
        for entry in [selected, *selected.get("reference_flow_sign_review", {}).get("profiles", ())]
        if "review_ref" in entry
    }
    if set(review["retired_reviews"]) & referenced:
        raise ValueError("Retired FeSi review is still referenced.")
    for key in review["retired_reviews"]:
        staged["reviews"].pop(key, None)
    resource["recipes"].update(deepcopy(review["recipes"]))
    resource["reviews"] = staged["reviews"]
    parent.clear()
    parent.update(deepcopy(row["compiled_rule"]))
