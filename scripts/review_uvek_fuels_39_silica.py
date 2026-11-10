"""Replay a complete native activated-silica reconstruction and its ingredients."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath import TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_fuels_39_silica_review.json"
REVIEW_SHA256 = "b88de24792279ea5f7fca98fa3834b759e55d5eb6e4321aa05525081277b7d60"
FIELDS = ("name", "reference product", "location", "unit")


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()


def apply_fuels_39_silica_review(resource, root=ROOT):
    """Check exact parents, ingredient rules and helper closures before mutation."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Activated-silica evidence changed; repeat its scientific review.")
    review = json.loads(raw)
    profile = {"family": "ecoinvent", "version": "3.9", "system_model": "cutoff"}
    if review["source_profile"] != profile or len(review["records"]) != 4:
        raise ValueError("Activated-silica source scope changed.")
    for rid, expected in review["recipe_dependencies"].items():
        if digest(resource["recipes"].get(rid)) != expected:
            raise ValueError("Activated-silica existing steam/wastewater helper changed.")
    for witness in review["native_2026_witnesses"]:
        if witness["reference_amount"] != 1 or witness["nonproduction_count"] < 0:
            raise ValueError("Activated-silica native reference changed.")
        if witness["nonproduction_count"] == 0:
            dep = witness.get("frozen_zero_burden_dependency")
            identity = witness["identity"]
            # These two existing native co-products allocate 100% to disposal.
            # This is evidence about an unchanged helper, not a new zero recipe.
            if (
                dep != "pilot-wastewater-native-plant-only-13e44131b50b181f4131"
                or dep not in review["recipe_dependencies"]
                or identity["location"] != "CH"
                or (identity["name"], identity["unit"])
                not in {
                    ("Electricity from waste, at municipal waste incineration plant", "kilowatt hour"),
                    ("Heat from waste, at municipal waste incineration plant", "megajoule"),
                }
                or not any(all(e.get(k) == identity[k] for k in FIELDS) for e in resource["recipes"][dep]["inputs"])
            ):
                raise ValueError("Unreviewed zero-burden native ingredient.")
    compiled = {row["rule_id"]: row["compiled_rule"] for row in review["records"]}
    pending = []
    for row in review["records"]:
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        replacement = compiled[row["rule_id"]]
        entries = replacement["reference_flow_sign_review"]["profiles"]
        if (
            parent not in (row["original_rule"], replacement)
            or replacement["source"] != row["source_witness"]["identity"]
            or row["source_witness"]["reference_amounts"] != [1]
            or len(entries) != 1
            or entries[0]["source_profile"] != profile
            or entries[0]["conversion_factor"] != 1
            or entries[0]["review"]["source_sign"] != 1
            or entries[0]["review"]["target_sign"] != 1
        ):
            raise ValueError("Activated-silica parent, source or reference scope changed.")
        pending.append((parent, replacement))
    for card in review["reviews"]:
        ref = intern_review({}, card)
        if ref in resource.get("reviews", {}) and resource["reviews"][ref] != card:
            raise ValueError("Activated-silica shared review changed.")
    candidate = {**resource, "replace": [compiled.get(r["id"], r) for r in resource["replace"]]}
    selected = select_profile_sign_rules(candidate, TechnosphereProfile(**profile))
    indexed = {r["id"]: r for r in selected["replace"]}
    for binding in review["component_bindings"]:
        if digest(indexed.get(binding["rule_id"])) != binding["selected_rule_sha256"]:
            raise ValueError("Activated-silica exact ingredient correspondence changed.")
    for rid, recipe in review["recipes"].items():
        if rid in resource["recipes"] and resource["recipes"][rid] != recipe:
            raise ValueError("Activated-silica derived recipe changed.")
    resource["recipes"].update(deepcopy(review["recipes"]))
    for card in review["reviews"]:
        intern_review(resource, card)
    for parent, replacement in pending:
        parent.clear()
        parent.update(deepcopy(replacement))
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
