"""Correct three exact-profile native process boundaries without unit rescaling."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_heat_310_routes_review.json"
REVIEW_SHA256 = "11e0a2208e869ae67dabc994377a71572ed3fb27feb05709de67db32339c4f42"


def digest(value):
    """Bind the full earlier rule, not just its source and target labels."""
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


def shared_route_review(row):
    """Keep scientific qualifications in portable migration provenance."""
    return {"rationale": row["limitations"][0], "limitations": row["limitations"], "evidence": [REVIEW_PATH]}


def compiled_route_rule(row):
    """Make only the exact inspected source profile executable."""
    rule = deepcopy(row["previous_rule"])
    reason = " ".join(row["limitations"])
    rule.update(
        target=deepcopy(row["target"]),
        conversion_factor=1,
        classification="proxy",
        mapping_method="reviewed_heat_native_route",
        report_correspondence=True,
        review_ref=intern_review({}, shared_route_review(row)),
        technosphere_information_loss=reason,
        reference_flow_sign_review={
            "status": "unresolved",
            "reason": "Only the independently inspected exact source profile is qualified.",
            "evidence": [REVIEW_PATH],
            "profiles": [
                {
                    "source_profile": deepcopy(row["source_profile"]),
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
    return rule


def apply_heat_310_routes_review(resource, root=ROOT):
    """Validate the complete batch before mutating any rule; refuse arbitrary drift."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Heat route evidence changed; repeat its scientific review.")
    review = json.loads(raw)
    indexed = {rule["id"]: rule for rule in resource["replace"]}
    pending = []
    for row in review["records"]:
        previous = row["previous_rule"]
        compiled = compiled_route_rule(row)
        current = indexed.get(row["rule_id"])
        if (
            digest(previous) != row["previous_rule_sha256"]
            or current not in (previous, compiled)
            or previous["source"] != row["source"]
            or previous.get("reference_flow_sign_review")
            or previous.get("conversion_factor", 1) != 1
            or row["conversion_factor"] != 1
            or row["source_reference_amount"] != 1
            or row["target_reference_amount"] != 1
            or row["source"]["unit"] != row["target"]["unit"]
            or row["source_profile"] != {"family": "ecoinvent", "version": "3.10", "system_model": "cutoff"}
            or row["source_profile"] not in resource["source_profiles"]
            or set(row["native_references"]) != {"2025", "2026"}
            or any(
                n["reference_amount"] != 1 or n["identity"] != row["target"] for n in row["native_references"].values()
            )
        ):
            raise ValueError("Heat route identity, reference or earlier rule changed.")
        pending.append((current, compiled, row))
    for current, compiled, row in pending:
        intern_review(resource, shared_route_review(row))
        current.clear()
        current.update(compiled)

    resource["coverage"]["mapping_methods"] = dict(
        sorted(Counter(r["mapping_method"] for r in resource["replace"]).items())
    )
    dispositions = Counter(
        "recipe" if "recipe_ref" in r["target"] else r.get("classification", "unclassified")
        for r in resource["replace"]
    )
    dispositions["omitted"] = len(resource.get("delete", []))
    if resource["coverage"].get("unresolved_identities"):
        dispositions["unresolved"] = resource["coverage"]["unresolved_identities"]
    resource["coverage"]["dispositions"] = dict(dispositions)
