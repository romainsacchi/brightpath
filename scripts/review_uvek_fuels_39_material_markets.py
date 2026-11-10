"""Replay inspected material-market delivery and the kg-based factory correction."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath import TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_fuels_39_material_markets_review.json"
REVIEW_SHA256 = "39758c9adaca63c27ef0e6be1085b468782185b9bd20dcd83d33864beec22e1a"


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()


def apply_fuels_39_material_markets_review(resource, root=ROOT):
    """Check exact parents, ingredient rules and helper closures before mutation."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Material-market evidence changed; repeat its scientific review.")
    review = json.loads(raw)
    profile = {"family": "ecoinvent", "version": "3.9", "system_model": "cutoff"}
    if review["source_profile"] != profile or len(review["records"]) != 8:
        raise ValueError("Material-market source scope changed.")
    for rid, expected in review["recipe_dependencies"].items():
        if digest(resource["recipes"].get(rid)) != expected:
            raise ValueError("Material-market existing dependency changed.")
    for witness in review["native_2026_witnesses"]:
        if witness["reference_amount"] != 1 or witness["nonproduction_count"] <= 0:
            raise ValueError("Material-market native reference or nonempty inventory changed.")
    compiled = {row["rule_id"]: row["compiled_rule"] for row in review["records"]}
    pending = []
    for row in review["records"]:
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        replacement = compiled[row["rule_id"]]
        entries = replacement["reference_flow_sign_review"]["profiles"]
        scoped = [entry for entry in entries if entry["source_profile"] == profile]
        outside = [entry for entry in entries if entry["source_profile"] != profile]
        original_outside = row["original_rule"].get("reference_flow_sign_review", {}).get("profiles", [])
        if (
            parent not in (row["original_rule"], replacement)
            or replacement["source"] != row["source_witness"]["identity"]
            or row["source_witness"]["reference_amounts"] != [1]
            or len(scoped) != 1
            or outside != original_outside
            or scoped[0]["conversion_factor"] != 1
            or scoped[0]["review"]["source_sign"] != 1
            or scoped[0]["review"]["target_sign"] != 1
            or scoped[0].get("target", replacement["target"]) != {"recipe_ref": row["new_recipe_id"]}
            or any(witness["reference_amounts"] != [1] for witness in row["source_producer_witnesses"])
        ):
            raise ValueError("Material-market parent, source or reference scope changed.")
        if row["preserve_existing_global_rule"] and {
            key: value for key, value in replacement.items() if key != "reference_flow_sign_review"
        } != {key: value for key, value in row["original_rule"].items() if key != "reference_flow_sign_review"}:
            raise ValueError("Previously reviewed material-market global rule changed.")
        pending.append((parent, replacement))
    for card in review["reviews"]:
        ref = intern_review({}, card)
        if ref in resource.get("reviews", {}) and resource["reviews"][ref] != card:
            raise ValueError("Material-market shared review changed.")
    candidate = {**resource, "replace": [compiled.get(r["id"], r) for r in resource["replace"]]}
    selected = select_profile_sign_rules(candidate, TechnosphereProfile(**profile))
    indexed = {r["id"]: r for r in selected["replace"]}
    for row in review["records"]:
        for preserved in row["preserved_profiles"]:
            selected_profile = select_profile_sign_rules(candidate, TechnosphereProfile(**preserved["source_profile"]))
            selected_rule = next(rule for rule in selected_profile["replace"] if rule["id"] == row["rule_id"])
            if (
                digest(selected_rule) != preserved["selected_rule_sha256"]
                or digest(resource["reviews"][selected_rule["review_ref"]]) != preserved["shared_review_sha256"]
            ):
                raise ValueError("Previously reviewed material-market selected behavior changed.")
    for binding in review["component_bindings"]:
        if digest(indexed.get(binding["rule_id"])) != binding["selected_rule_sha256"]:
            raise ValueError("Material-market exact utility or freight correspondence changed.")
    for rid, recipe in review["recipes"].items():
        if rid in resource["recipes"] and resource["recipes"][rid] != recipe:
            raise ValueError("Material-market derived recipe changed.")
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
