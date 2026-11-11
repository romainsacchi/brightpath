"""Replay inspected native heat/recycling proxies and one unchanged gas stock."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath import TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_fuels_39_heat_recycling_review.json"
REVIEW_SHA256 = "a4d9155e3b015bdd4fd7c8ec5ccba8c6ccb1431257fb0011e5da8f3d4a569d2d"


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()


def apply_fuels_39_heat_recycling_review(resource, root=ROOT):
    """Validate exact source, allocation and existing-stock evidence before mutation."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Heat/recycling evidence changed; repeat its scientific review.")
    review = json.loads(raw)
    profile = {"family": "ecoinvent", "version": "3.9", "system_model": "cutoff"}
    if review["source_profile"] != profile or len(review["records"]) != 7 or review["recipes"]:
        raise ValueError("Heat/recycling source scope or recipe count changed.")
    for rid, expected in review["recipe_dependencies"].items():
        if digest(resource["recipes"].get(rid)) != expected:
            raise ValueError("Existing allocated sewage-gas helper changed.")
    for witness in review["native_2026_witnesses"]:
        if witness["reference_amount"] != 1 or witness["nonproduction_count"] < 0:
            raise ValueError("Native heat/recycling reference changed.")
        if witness["nonproduction_count"] == 0:
            basis = witness.get("allocation_basis", {})
            identity = witness["identity"]
            if (
                identity["name"] != "Heat from waste, at municipal waste incineration plant"
                or identity["location"] != "CH"
                or identity["unit"] != "megajoule"
                or basis.get("original_reference_amount") != 1
                or basis.get("original_reference_unit") != "MJ"
                or basis.get("original_total_exchange_count") != 1
                or basis.get("allocation_percent") != {"disposal": 100, "energy": 0}
                or review["waste_heat_source_market"]["source_lci"]["source_LCI_is_zero"]
            ):
                raise ValueError("Unreviewed empty native supplier or allocation claim.")
    pending = []
    selected = select_profile_sign_rules(resource, TechnosphereProfile(**profile))
    indexed = {r["id"]: r for r in selected["replace"]}
    for row in review["records"]:
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        replacement = row["compiled_rule"]
        reference = row["source_witness"]["reference_amounts"]
        if parent not in (row["original_rule"], replacement) or reference not in ([1], [-1]):
            raise ValueError("Heat/recycling parent or source reference changed.")
        if replacement["source"] != row["source_witness"]["identity"]:
            raise ValueError("Heat/recycling source identity changed.")
        if row["resource_action"] == "unchanged_rule":
            if (
                replacement != row["original_rule"]
                or reference != [1]
                or row["source_witness"]["nonproduction_counts"]
                or indexed[row["rule_id"]]["target"] != row["new_target"]
                or row["counterpart_witness"]["reference_amount"] != -1
                or row["counterpart_witness"]["nonproduction_counts"] != {"tech": 4, "bio": 2}
                or not resource["recipes"][row["existing_recipe_id"]].get("zero_burden")
            ):
                raise ValueError("Observed allocated sewage-gas source/helper changed.")
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
                raise ValueError("Heat/recycling exact profile or target changed.")
        pending.append((parent, replacement))
    for card in review["reviews"]:
        key = intern_review({}, card)
        if key in resource.get("reviews", {}) and resource["reviews"][key] != card:
            raise ValueError("Heat/recycling shared review changed.")
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
