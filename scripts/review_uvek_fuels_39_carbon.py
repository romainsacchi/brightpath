"""Replay granular-carbon production/reactivation and audited ash treatment."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath import TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_fuels_39_carbon_review.json"
REVIEW_SHA256 = "1d431e716ab7609f53db4cd514403a6b53dbd938e6242f344dd355d8f9bd0c34"


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()


def apply_fuels_39_carbon_review(resource, root=ROOT):
    """Check exact parents, ingredient rules and helper closures before mutation."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Activated-carbon evidence changed; repeat its scientific review.")
    review = json.loads(raw)
    profile = {"family": "ecoinvent", "version": "3.9", "system_model": "cutoff"}
    if review["source_profile"] != profile or len(review["records"]) != 7:
        raise ValueError("Activated-carbon source scope changed.")
    for rid, expected in review["recipe_dependencies"].items():
        if digest(resource["recipes"].get(rid)) != expected:
            raise ValueError("Activated-carbon existing dependency changed.")
    for witness in review["native_2026_witnesses"]:
        if witness["reference_amount"] != 1 or witness["nonproduction_count"] <= 0:
            raise ValueError("Activated-carbon native reference or nonempty inventory changed.")
    omissions = review["empty_receiver_omissions"]
    if len(omissions) != 51 or any(
        row["receiver"]["reference_amounts"] != [-1]
        or row["receiver"]["nonproduction_counts"]
        or row["nonproduction_count"] != 0
        or row["source_price"] != 0
        or row["signed_source_quantity"] >= 0
        for row in omissions
    ):
        raise ValueError("Activated-carbon zero-inventory receiving evidence changed.")
    if len(review["biosphere_identity_checks"]) != 288 or any(
        not row["same_publisher_uuid"] or not row["signed_quantity_retained"]
        for row in review["biosphere_identity_checks"]
    ):
        raise ValueError("Activated-carbon direct biosphere identity evidence changed.")
    compiled = {row["rule_id"]: row["compiled_rule"] for row in review["records"]}
    pending = []
    for row in review["records"]:
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        replacement = compiled[row["rule_id"]]
        entries = replacement["reference_flow_sign_review"]["profiles"]
        scoped = [entry for entry in entries if entry["source_profile"] == profile]
        previous_other = [
            entry
            for entry in row["original_rule"].get("reference_flow_sign_review", {}).get("profiles", [])
            if entry["source_profile"] != profile
        ]
        current_other = [entry for entry in entries if entry["source_profile"] != profile]
        references = row["source_witness"]["reference_amounts"]
        if (
            parent not in (row["original_rule"], replacement)
            or replacement["source"] != row["source_witness"]["identity"]
            or references not in ([1], [-1])
            or len(scoped) != 1
            or current_other != previous_other
            or scoped[0]["conversion_factor"] != references[0]
            or scoped[0]["review"]["source_sign"] != references[0]
            or scoped[0]["review"]["target_sign"] != 1
            or replacement["target"] != {"recipe_ref": row["new_recipe_id"]}
        ):
            raise ValueError("Activated-carbon parent, source or reference scope changed.")
        retention = row["retention"]
        if retention["source_nonproduction_rows"] != (
            retention["target_nonproduction_rows"] + retention.get("empty_receivers_omitted", 0)
        ):
            raise ValueError("Activated-carbon source inventory retention changed.")
        declared = review["recipes"][row["new_recipe_id"]].get("source_input_omissions", [])
        if len(declared) != retention.get("empty_receivers_omitted", 0) or any(
            omission["source_profile"] != profile
            or omission["source_activity"] != row["source_witness"]["identity"]
            or omission["source_inventory_sha256"] != row["source_witness"]["enriched_inventory_sha256"]
            for omission in declared
        ):
            raise ValueError("Activated-carbon declared source omission scope changed.")
        pending.append((parent, replacement))
    for card in review["reviews"]:
        ref = intern_review({}, card)
        if ref in resource.get("reviews", {}) and resource["reviews"][ref] != card:
            raise ValueError("Activated-carbon shared review changed.")
    candidate = {**resource, "replace": [compiled.get(r["id"], r) for r in resource["replace"]]}
    selected = select_profile_sign_rules(candidate, TechnosphereProfile(**profile))
    indexed = {r["id"]: r for r in selected["replace"]}
    for binding in review["component_bindings"]:
        if digest(indexed.get(binding["rule_id"])) != binding["selected_rule_sha256"]:
            raise ValueError("Activated-carbon exact utility or freight correspondence changed.")
    for rid, recipe in review["recipes"].items():
        if rid in resource["recipes"] and resource["recipes"][rid] != recipe:
            raise ValueError("Activated-carbon derived recipe changed.")
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
