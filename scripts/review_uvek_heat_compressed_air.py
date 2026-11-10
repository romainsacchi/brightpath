"""Replay the inspected delivered-air mix without altering native loss models."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_heat_compressed_air_review.json"
REVIEW_SHA256 = "7e2cec0f4d0d05085f06017b0ace05fb8d86de45b23273005fc9d57717ccbe1b"


def compiled_air_rule(review):
    """Keep signed m3 and the two original market shares through complete services."""
    rule = deepcopy(review["previous_rule"])
    shared = {
        "rationale": review["authorization"],
        "limitations": review["limitations"],
        "evidence": [REVIEW_PATH],
    }
    reason = " ".join(review["limitations"])
    rule.update(
        target={"recipe_ref": review["recipe_ref"]},
        classification="proxy",
        conversion_factor=1,
        confidence=1.0,
        mapping_method="reviewed_heat_compressed_air_network_mix",
        report_correspondence=True,
        technosphere_information_loss=reason,
        review_ref=intern_review({}, shared),
        reference_flow_sign_review={
            "status": "unresolved",
            "reason": "Only the independently inspected exact cut-off profile is qualified.",
            "evidence": [REVIEW_PATH],
            "profiles": [
                {
                    "source_profile": review["source_profile"],
                    "conversion_factor": 1,
                    "review": {
                        "status": "verified",
                        "source_sign": 1,
                        "target_sign": 1,
                        "reason": reason,
                        "evidence": [REVIEW_PATH],
                    },
                }
            ],
        },
    )
    return rule, shared


def apply_heat_compressed_air_review(resource, root=ROOT):
    """Validate before mutation and refuse drift in the source mix or helper."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Heat compressed-air evidence changed; repeat its scientific review.")
    review = json.loads(raw)
    previous = review["previous_rule"]
    digest = hashlib.sha256(
        json.dumps(previous, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()
    rule = next((r for r in resource["replace"] if r["id"] == review["rule_id"]), None)
    compiled, shared = compiled_air_rule(review)
    recipe = review["recipe"]
    existing = resource.get("recipes", {}).get(review["recipe_ref"])
    if (
        digest != review["previous_rule_sha256"]
        or rule not in (previous, compiled)
        or previous["source"] != review["source"]
        or review["source_profile"] not in resource["source_profiles"]
        or review["source_profile"]["system_model"] != "cutoff"
        or review["source_reference_amount"] != 1
        or review["target_reference_amount"] != 1
        or review["conversion_factor"] != 1
        or recipe["unit"] != review["source"]["unit"]
        or len(recipe["inputs"]) != 2
        or {r["role"] for r in review["roles"]} != {"average", "optimised"}
        or existing not in (None, recipe)
    ):
        raise ValueError("Heat compressed-air reference, scope, rule or helper changed.")
    for role, exchange in zip(review["roles"], recipe["inputs"], strict=True):
        if (
            role["source_market_share"] != 0.5
            or exchange != {"type": "technosphere", **role["target"], "amount": 0.5}
            or set(role["native_references"]) != {"2025", "2026"}
            or any(n["reference_amount"] != 1 for n in role["native_references"].values())
        ):
            raise ValueError("Heat compressed-air native service or original market shares changed.")
    resource.setdefault("recipes", {})[review["recipe_ref"]] = deepcopy(recipe)
    intern_review(resource, shared)
    rule.clear()
    rule.update(compiled)
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
    dispositions["omitted"] = len(resource.get("delete", []))
    if resource["coverage"].get("unresolved_identities"):
        dispositions["unresolved"] = resource["coverage"]["unresolved_identities"]
    resource["coverage"]["dispositions"] = dict(dispositions)
