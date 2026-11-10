"""Replay complete molybdenum-oxide roasting and source supply portfolios."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath import TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_fuels_39_molybdenum_review.json"
REVIEW_SHA256 = "e4b99b5f90d4e0fda9f4ac81a542a760fa202763009fa43b1ecd87f02b9faf63"


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()


def apply_fuels_39_molybdenum_review(resource, root=ROOT):
    """Check exact parents, ingredient rules and helper closures before mutation."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Molybdenum evidence changed; repeat its scientific review.")
    review = json.loads(raw)
    profile = {"family": "ecoinvent", "version": "3.9", "system_model": "cutoff"}
    if review["source_profile"] != profile or len(review["records"]) != 3:
        raise ValueError("Molybdenum source scope changed.")
    for rid, expected in review["recipe_dependencies"].items():
        if digest(resource["recipes"].get(rid)) != expected:
            raise ValueError("Molybdenum existing dependency changed.")
    for witness in review["native_2026_witnesses"]:
        if witness["reference_amount"] != 1 or witness["nonproduction_count"] <= 0:
            raise ValueError("Molybdenum native reference or nonempty inventory changed.")
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
            raise ValueError("Molybdenum parent, source or reference scope changed.")
        pending.append((parent, replacement))
    for card in review["reviews"]:
        ref = intern_review({}, card)
        if ref in resource.get("reviews", {}) and resource["reviews"][ref] != card:
            raise ValueError("Molybdenum shared review changed.")
    candidate = {**resource, "replace": [compiled.get(r["id"], r) for r in resource["replace"]]}
    selected = select_profile_sign_rules(candidate, TechnosphereProfile(**profile))
    indexed = {r["id"]: r for r in selected["replace"]}
    for binding in review["component_bindings"]:
        if digest(indexed.get(binding["rule_id"])) != binding["selected_rule_sha256"]:
            raise ValueError("Molybdenum exact utility or freight correspondence changed.")
    for rid, recipe in review["recipes"].items():
        if rid in resource["recipes"] and resource["recipes"][rid] != recipe:
            raise ValueError("Molybdenum derived recipe changed.")
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
