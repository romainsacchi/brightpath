"""Replay exact-profile commodity and wastewater qualifications for native heat."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_heat_dependency_batch_review.json"
REVIEW_SHA256 = "4bfa4bc348a48d2e961b58f03b1b6c69fdfa7860f591fa0570a7723e212edac5"


def compiled_dependency_rule(row, authorization):
    """Compile one volume/mass-preserving, explicitly limited correspondence."""
    rule = deepcopy(row["previous_rule"])
    reason = " ".join(row["limitations"])
    evidence = [f"{REVIEW_PATH}#{row['rule_id']}"]
    shared = {"rationale": authorization, "limitations": row["limitations"], "evidence": evidence}
    rule.update(
        target=deepcopy(row["target"]),
        conversion_factor=row["conversion_factor"],
        mapping_method="reviewed_heat_dependency_proxy",
        confidence=1.0,
        report_correspondence=True,
        technosphere_information_loss=reason,
        review_ref=intern_review({}, shared),
        reference_flow_sign_review={
            "status": "unresolved",
            "reason": "Only the independently inspected exact cut-off profile is qualified.",
            "evidence": evidence,
            "profiles": [
                {
                    "source_profile": row["source_profile"],
                    "conversion_factor": row["conversion_factor"],
                    "review": {
                        "status": "verified",
                        "source_sign": row["source_reference_amount"],
                        "target_sign": 1,
                        "reason": reason,
                        "evidence": evidence,
                    },
                }
            ],
        },
    )
    return rule, shared


def apply_heat_dependency_review(resource, root=ROOT):
    """Validate the complete batch before modifying rules or shared reviews."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Heat dependency evidence changed; repeat its scientific review.")
    review = json.loads(raw)
    indexed = {r["id"]: r for r in resource["replace"]}
    pending = []
    for row in review["records"]:
        previous = row["previous_rule"]
        digest = hashlib.sha256(
            json.dumps(previous, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
        ).hexdigest()
        compiled, shared = compiled_dependency_rule(row, review["authorization"])
        current = indexed.get(row["rule_id"])
        sign = row["source_reference_amount"]
        if (
            digest != row["previous_rule_sha256"]
            or current not in (previous, compiled)
            or previous["source"] != row["source"]
            or previous["target"] != row["target"]
            or sign not in (-1, 1)
            or row["conversion_factor"] != sign
            or previous.get("conversion_factor", 1) != sign
            or row["source_profile"] not in resource["source_profiles"]
            or row["source_profile"]["system_model"] != "cutoff"
            or row["source"]["unit"] != row["target"]["unit"]
            or set(row["native_references"]) != {"2025", "2026"}
            or any(r["reference_amount"] != 1 for r in row["native_references"].values())
        ):
            raise ValueError("Heat dependency identity, reference coordinate or exact scope changed.")
        pending.append((current, compiled, shared))
    for current, compiled, shared in pending:
        current.clear()
        current.update(compiled)
        intern_review(resource, shared)
    resource["coverage"]["mapping_methods"] = dict(
        sorted(Counter(r["mapping_method"] for r in resource["replace"]).items())
    )
    resource["coverage"]["mean_confidence"] = round(
        sum(r["confidence"] for r in resource["replace"]) / len(resource["replace"]), 6
    )
