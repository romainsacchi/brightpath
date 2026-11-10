"""Qualify inspected heat profiles while preserving earlier effective decisions."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_heat_profile_extension_review.json"
REVIEW_SHA256 = "ceb5b782aa15161a5680d1901cffa0851e0b384ead67dcdc4deae1f183231b6b"


def digest(value):
    """Hash the complete portable rule, including its exact-profile decisions."""
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


def compiled_profile_rule(row, authorization):
    """Add only the independently inspected correspondence or sign profile."""
    rule = deepcopy(row["previous_rule"])
    action = row["resource_action"]
    evidence = [f"{REVIEW_PATH}#{row['rule_id']}"]
    reason = " ".join(row["limitations"])
    entry = {
        "source_profile": row["source_profile"],
        "conversion_factor": 1,
        "review": {
            "status": "verified",
            "source_sign": 1,
            "target_sign": 1,
            "reason": reason,
            "evidence": evidence,
        },
    }
    shared = None
    if action == "compile_exact_profile_proxy":
        shared = {"rationale": authorization, "limitations": row["limitations"], "evidence": evidence}
        rule.update(
            mapping_method="reviewed_heat_vehicle_gas_proxy",
            confidence=1.0,
            conversion_factor=1,
            report_correspondence=True,
            technosphere_information_loss=reason,
            review_ref=intern_review({}, shared),
            reference_flow_sign_review={
                "status": "unresolved",
                "reason": "Only the inspected exact cut-off source profile is qualified.",
                "evidence": evidence,
                "profiles": [entry],
            },
        )
    elif action == "append_exact_sign_profile_preserving_existing_projection":
        profiles = rule["reference_flow_sign_review"]["profiles"]
        if any(p["source_profile"] == entry["source_profile"] for p in profiles):
            raise ValueError("The additional heat profile already has a decision.")
        profiles.append(entry)
    elif action != "qualify_existing_rule_without_change":
        raise ValueError("Unknown heat profile qualification action.")
    return rule, shared


def apply_heat_profile_review(resource, root=ROOT):
    """Validate the full batch before any mutation; replay is idempotent."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Heat profile evidence changed; repeat its scientific review.")
    review = json.loads(raw)
    indexed = {r["id"]: r for r in resource["replace"]}
    pending = []
    for row in review["records"]:
        previous = row["previous_rule"]
        compiled, shared = compiled_profile_rule(row, review["authorization"])
        current = indexed.get(row["rule_id"])
        if (
            digest(previous) != row["previous_rule_sha256"]
            or current not in (previous, compiled)
            or previous["source"] != row["source"]
            or previous["target"] != row["target"]
            or row["source_profile"] not in resource["source_profiles"]
            or row["source_profile"]["system_model"] != "cutoff"
            or row["source_reference_amount"] != 1
            or row["target_reference_amount"] != 1
            or row["conversion_factor"] != 1
            or any(n["reference_amount"] != 1 for n in row["native_references"].values())
        ):
            raise ValueError("Heat profile identity, reference or existing rule changed.")
        recipe = row.get("existing_exact_profile_recipe")
        if recipe and digest(resource["recipes"][recipe["recipe_ref"]]) != recipe["recipe_sha256"]:
            raise ValueError("Heat profile recipe changed; repeat its scientific review.")
        pending.append((current, compiled, shared))
    for current, compiled, shared in pending:
        current.clear()
        current.update(compiled)
        if shared is not None:
            intern_review(resource, shared)
    resource["coverage"]["mapping_methods"] = dict(
        sorted(Counter(r["mapping_method"] for r in resource["replace"]).items())
    )
    resource["coverage"]["mean_confidence"] = round(
        sum(r["confidence"] for r in resource["replace"]) / len(resource["replace"]), 6
    )
