"""Replay exact-profile native endpoint corrections for shared fuel preparation."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_fuels_39_endpoint_review.json"
REVIEW_SHA256 = "8982ef7bbb7082e0a29b4bf6d4e807b3090cb5c72ee50bc9c8e542378d276653"


def apply_fuels_39_endpoint_review(resource, root=ROOT):
    """Verify frozen parents and reference evidence before applying corrections."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Fuel endpoint evidence changed; repeat its scientific review.")
    review = json.loads(raw)
    profile = {"family": "ecoinvent", "version": "3.9", "system_model": "cutoff"}
    if review["source_profile"] != profile or len(review["records"]) != 3:
        raise ValueError("Fuel endpoint exact source scope changed.")
    pending = []
    for row in review["records"]:
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        compiled = row["compiled_rule"]
        entries = compiled["reference_flow_sign_review"]["profiles"]
        witness = row["native_2026_witness"]
        if (
            parent not in (row["original_rule"], compiled)
            or compiled["source"] != row["source_witness"]["identity"]
            or len(entries) != 1
            or entries[0]["source_profile"] != profile
            or row["source_witness"]["reference_amounts"] != [entries[0]["review"]["source_sign"]]
            or witness["identity"] != compiled["target"]
            or witness["reference_amounts"] != [1]
            or sum(witness["nonproduction_counts"].values()) <= 0
        ):
            raise ValueError("Fuel endpoint parent, scope or inspected reference changed.")
        if parent == compiled and resource.get("reviews", {}).get(parent["review_ref"]) != review["reviews"][0]:
            raise ValueError("Fuel endpoint shared review changed.")
        pending.append((parent, compiled))
    for card in review["reviews"]:
        intern_review(resource, card)
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
    dispositions["omitted"] = len(resource["delete"])
    if resource["coverage"].get("unresolved_identities"):
        dispositions["unresolved"] = resource["coverage"]["unresolved_identities"]
    resource["coverage"]["dispositions"] = dict(dispositions)
