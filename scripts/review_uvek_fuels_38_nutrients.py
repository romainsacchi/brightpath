"""Replay inspected native native-service proxies and one unchanged gas stock."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath import TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_fuels_38_nutrients_review.json"
REVIEW_SHA256 = "3de8d7721b56c0acf9fba8f0753323ae63eb4d5d6f271dad12b3bdca780c45c6"


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()


def apply_fuels_38_nutrients_review(resource, root=ROOT):
    """Validate exact source, allocation and existing-stock evidence before mutation."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Native-service evidence changed; repeat its scientific review.")
    review = json.loads(raw)
    profile = {"family": "ecoinvent", "version": "3.8", "system_model": "cutoff"}
    if review["source_profile"] != profile or len(review["records"]) != 12 or review["recipes"]:
        raise ValueError("Native-service source scope or recipe count changed.")
    for rid, expected in review["recipe_dependencies"].items():
        if digest(resource["recipes"].get(rid)) != expected:
            raise ValueError("Existing allocated sewage-gas helper changed.")
    for witness in review["native_2026_witnesses"]:
        if witness["reference_amount"] != 1 or witness["nonproduction_count"] < 0:
            raise ValueError("Native native-service reference changed.")
        if witness["nonproduction_count"] == 0:
            raise ValueError("Empty native service is not covered by this review.")
    if set(review["nutrient_reference_evidence"]) != {"N", "P2O5", "K2O"}:
        raise ValueError("Native nutrient-equivalent reference evidence changed.")
    for evidence in review["nutrient_reference_evidence"].values():
        if evidence["declared_output_amount"] != 1 or evidence["native_inventory_renormalized"]:
            raise ValueError("Native nutrient output or unmodified mix changed.")
    pending = []
    selected = select_profile_sign_rules(resource, TechnosphereProfile(**profile))
    indexed = {r["id"]: r for r in selected["replace"]}
    for row in review["records"]:
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        replacement = row["compiled_rule"]
        reference = row["source_witness"]["reference_amounts"]
        if parent not in (row["original_rule"], replacement) or reference not in ([1], [-1]):
            raise ValueError("Native-service parent or source reference changed.")
        if replacement["source"] != row["source_witness"]["identity"]:
            raise ValueError("Native-service source identity changed.")
        if row["resource_action"] == "unchanged_rule":
            if (
                replacement != row["original_rule"]
                or reference != [1]
                or indexed[row["rule_id"]]["target"] != row["new_target"]
                or indexed[row["rule_id"]].get("conversion_factor", 1) != 1
            ):
                raise ValueError("Previously selected native service changed.")
        else:
            entries = replacement["reference_flow_sign_review"]["profiles"]
            if (
                len(entries) != 1
                or entries[0]["source_profile"] != profile
                or entries[0]["conversion_factor"] != reference[0]
                or entries[0]["review"]["source_sign"] != reference[0]
                or entries[0]["review"]["target_sign"] != 1
                or replacement["target"] != row["new_target"]
            ):
                raise ValueError("Native-service exact profile or target changed.")
        pending.append((parent, replacement))
    for card in review["reviews"]:
        key = intern_review({}, card)
        if key in resource.get("reviews", {}) and resource["reviews"][key] != card:
            raise ValueError("Native-service shared review changed.")
    for rid, recipe in review["recipes"].items():
        if rid in resource["recipes"] and resource["recipes"][rid] != recipe:
            raise ValueError("Existing native petrol reconstruction changed.")
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
