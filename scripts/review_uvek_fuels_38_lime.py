"""Replay the inspected limestone delivery and allocated-mining correspondence."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_fuels_38_lime_review.json"
REVIEW_SHA256 = "47c608676be41fbf4cc2a34767e7b55ee7f1dd7c7414e2faf6b74287eb526760"


def apply_fuels_38_lime_review(resource, root=ROOT):
    """Require the frozen source, complete retained rows and native references."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Lime evidence changed; repeat its scientific review.")
    review = json.loads(raw)
    parent = next(r for r in resource["replace"] if r["id"] == review["rule_id"])
    compiled = review["compiled_rule"]
    profiles = compiled["reference_flow_sign_review"]["profiles"]
    delivery = review["recipes"][compiled["target"]["recipe_ref"]]
    if (
        parent not in (review["original_rule"], compiled)
        or review["source_witness"]["reference_amounts"] != [1]
        or compiled["source"] != review["source_witness"]["identity"]
        or len(profiles) != 1
        or profiles[0]["source_profile"] != {"family": "ecoinvent", "version": "3.8", "system_model": "cutoff"}
        or profiles[0]["conversion_factor"] != 1
        or profiles[0]["review"]["source_sign"] != 1
        or profiles[0]["review"]["target_sign"] != 1
        or len(delivery["inputs"]) != 6
        or delivery.get("source_input_omissions")
    ):
        raise ValueError("Lime source scope, reference or delivery boundary changed.")
    for actual, retained in zip(delivery["inputs"], review["retained_rows"], strict=True):
        if actual != {**retained["statistics"], **retained["target"]}:
            raise ValueError("Lime producer share, freight or uncertainty changed.")
    for witness in review["native_2026_witnesses"]:
        if witness["reference_amount"] != 1 or witness["nonproduction_count"] <= 0:
            raise ValueError("Native lime supplier reference changed.")
    for rid, recipe in review["recipes"].items():
        if rid in resource["recipes"] and resource["recipes"][rid] != recipe:
            raise ValueError("Derived lime helper changed.")
    for card in review["reviews"]:
        key = intern_review({}, card)
        if key in resource.get("reviews", {}) and resource["reviews"][key] != card:
            raise ValueError("Lime shared review changed.")
    resource["recipes"].update(deepcopy(review["recipes"]))
    for card in review["reviews"]:
        intern_review(resource, card)
    parent.clear()
    parent.update(deepcopy(compiled))
    resource["coverage"]["mapping_methods"] = dict(
        sorted(Counter(r["mapping_method"] for r in resource["replace"]).items())
    )
    dispositions = Counter(
        "recipe" if "recipe_ref" in r["target"] else r.get("classification", "unclassified")
        for r in resource["replace"]
    )
    dispositions["omitted"] = len(resource["delete"])
    if resource["coverage"].get("unresolved_identities"):
        dispositions["unresolved"] = resource["coverage"]["unresolved_identities"]
    resource["coverage"]["dispositions"] = dict(dispositions)
