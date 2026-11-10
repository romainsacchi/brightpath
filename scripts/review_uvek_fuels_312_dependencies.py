"""Replay inspected 3.12 fuel dependencies while preserving earlier profiles."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath import TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_fuels_312_dependencies_review.json"
REVIEW_SHA256 = "424c0ac8733f55d056b3b69f47460f2ee9f148578ed5a0cf606d1e2703987636"


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()


def apply_fuels_312_dependencies_review(resource, root=ROOT):
    """Refuse changed evidence, components or parents before any mutation."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Fuel dependency evidence changed; repeat its scientific review.")
    review = json.loads(raw)
    profile = {"family": "ecoinvent", "version": "3.12", "system_model": "cutoff"}
    if review["source_profile"] != profile or len(review["records"]) != 7:
        raise ValueError("Fuel dependency exact source scope changed.")
    for row in review["authorization_evidence"]:
        if hashlib.sha256((root / "brightpath/data" / row["path"]).read_bytes()).hexdigest() != row["sha256"]:
            raise ValueError("Fuel dependency authority changed; repeat its review.")
    for rid, expected in review["recipe_dependencies"].items():
        if digest(resource["recipes"].get(rid)) != expected:
            raise ValueError("Fuel dependency component helper changed.")
    selected = select_profile_sign_rules(resource, TechnosphereProfile(**profile))
    indexed = {r["id"]: r for r in selected["replace"]}
    for binding in review["component_bindings"]:
        if digest(indexed.get(binding["rule_id"])) != binding["selected_rule_sha256"]:
            raise ValueError("Fuel dependency exact freight binding changed.")
    if any(w["reference_amount"] != 1 or w["nonproduction_count"] <= 0 for w in review["native_2026_witnesses"]):
        raise ValueError("Fuel dependency native reference or complete service changed.")
    pending = []
    for row in review["records"]:
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        compiled = row["compiled_rule"]
        if "review_ref" in parent:
            expected_card = row["original_review"] if parent == row["original_rule"] else review["reviews"][0]
            if resource.get("reviews", {}).get(parent["review_ref"]) != expected_card:
                raise ValueError("Fuel dependency shared review changed.")
        profiles = compiled["reference_flow_sign_review"]["profiles"]
        if (
            parent not in (row["original_rule"], compiled)
            or compiled["source"] != row["source_witness"]["identity"]
            or profiles[:-1] != row["previous_profiles_preserved"]
            or profiles[-1]["source_profile"] != profile
            or row["source_witness"]["reference_amounts"] != [profiles[-1]["review"]["source_sign"]]
            or profiles[-1]["review"]["target_sign"] != 1
        ):
            raise ValueError("Fuel dependency parent, prior profile or reference changed.")
        pending.append((parent, compiled))
    for rid, recipe in review["recipes"].items():
        if rid in resource["recipes"] and resource["recipes"][rid] != recipe:
            raise ValueError("Fuel dependency derived helper changed.")
    resource["recipes"].update(deepcopy(review["recipes"]))
    for card in review["reviews"]:
        intern_review(resource, card)
    for parent, compiled in pending:
        parent.clear()
        parent.update(deepcopy(compiled))
    used_reviews = {
        rule["review_ref"]
        for operation in ("replace", "disaggregate", "delete")
        for rule in resource.get(operation, [])
        if "review_ref" in rule
    } | {
        entry["review_ref"]
        for rule in resource["replace"]
        for entry in rule.get("reference_flow_sign_review", {}).get("profiles", [])
        if "review_ref" in entry
    }
    # Superseded cards remain in the frozen evidence, not as orphan runtime data.
    prior_reviews = {row["original_rule"].get("review_ref") for row in review["records"]}
    for ref in prior_reviews - used_reviews - {None}:
        resource["reviews"].pop(ref, None)
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
