"""Replay exact 3.9 steel prerequisite supply and inert-treatment decisions."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_steel_39_first_review.json"
REVIEW_SHA256 = "5964fb114e0e3c27a4c8dd9512aefead2a72e16d1bc3317d3221f9343dc799b9"


def apply_steel_39_first_review(resource, root=ROOT):
    """Validate frozen parents, references and complete recipes before mutation."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Steel prerequisite evidence changed; repeat its review.")
    review = json.loads(raw)
    profile = {"family": "ecoinvent", "version": "3.9", "system_model": "cutoff"}
    if review["source_profile"] != profile or len(review["records"]) != 4:
        raise ValueError("Steel prerequisite source scope changed.")
    recipes = review["recipes"]
    if (
        len(recipes) != 3
        or review["source_calorific_property_available"] is not False
        or review["zeolite_freight_rows_retained"] != 9
        or review["zeolite_treatment_components_retained"] != 2
    ):
        raise ValueError("Steel prerequisite quantity boundary changed.")
    if any(w["reference_amounts"] != [1] or w["nonproduction_count"] <= 0 for w in review["native_2026_witnesses"]):
        raise ValueError("Steel prerequisite native reference evidence changed.")
    pending = []
    for row in review["records"]:
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        compiled = row["compiled_rule"]
        factor = row["conversion_factor"]
        if (
            parent not in (row["original_rule"], compiled)
            or compiled["source"] != row["source_witness"]["identity"]
            or row["source_witness"]["reference_amounts"] != [factor]
            or factor not in (-1, 1)
        ):
            raise ValueError("Steel prerequisite parent or reference changed.")
        if row["resource_action"] != "unchanged_rule":
            scoped = compiled["reference_flow_sign_review"]["profiles"]
            if len(scoped) != 1 or scoped[0]["source_profile"] != profile or scoped[0]["conversion_factor"] != factor:
                raise ValueError("Steel prerequisite profile confinement changed.")
        pending.append((parent, compiled))
    for rid, recipe in recipes.items():
        if rid in resource.get("recipes", {}) and resource["recipes"][rid] != recipe:
            raise ValueError("Steel prerequisite recipe changed.")
    staged_reviews = {"reviews": deepcopy(resource.get("reviews", {}))}
    for card in review["reviews"]:
        intern_review(staged_reviews, card)
    for _, compiled in pending:
        if compiled.get("review_ref") and compiled["review_ref"] not in staged_reviews["reviews"]:
            raise ValueError("Steel prerequisite review reference missing.")
    resource.setdefault("recipes", {}).update(deepcopy(recipes))
    resource["reviews"] = staged_reviews["reviews"]
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
