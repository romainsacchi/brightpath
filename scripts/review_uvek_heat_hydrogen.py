"""Replace an unreviewed electrolysis heuristic for the inspected fossil H2 market."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_heat_hydrogen_market_review.json"
REVIEW_SHA256 = "7952f80261da470630281a1b89e7dfc48f74058d2a0024242922c3529a97808a"


def apply_heat_hydrogen_review(resource, root=ROOT):
    """Retain positive kg references and refuse every uninspected source profile."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Heat hydrogen evidence changed; repeat its scientific review.")
    review = json.loads(raw)
    rule = next(r for r in resource["replace"] if r["id"] == review["rule_id"])
    if (
        rule["source"] != review["source"]
        or rule["target"] not in (review["previous_target"], review["target"])
        or rule.get("conversion_factor", 1) != 1
        or review["source_profile"] not in resource["source_profiles"]
        or review["source_reference_amount"] != 1
        or any(row["reference_amount"] != 1 for row in review["native_references"].values())
        or review["target"]["unit"] != rule["source"]["unit"]
    ):
        raise ValueError("Inspected heat hydrogen identity, reference or scope changed.")
    limitation = " ".join(review["limitations"])
    rule.update(
        target=deepcopy(review["target"]),
        conversion_factor=1,
        mapping_method="reviewed_heat_fossil_hydrogen_proxy",
        confidence=1.0,
        report_correspondence=True,
        technosphere_information_loss=limitation,
        reference_flow_sign_review={
            "status": "unresolved",
            "reason": "Only the exact inspected3.9cutoff hydrogen-market profile is reviewed.",
            "evidence": [REVIEW_PATH],
            "profiles": [
                {
                    "source_profile": review["source_profile"],
                    "conversion_factor": 1,
                    "review": {
                        "status": "verified",
                        "source_sign": 1,
                        "target_sign": 1,
                        "reason": limitation,
                        "evidence": [REVIEW_PATH],
                    },
                }
            ],
        },
    )
    rule["review_ref"] = intern_review(
        resource,
        {
            "rationale": review["authorization"],
            "limitations": review["limitations"],
            "evidence": [REVIEW_PATH],
        },
    )
    resource["coverage"]["mapping_methods"] = dict(
        sorted(Counter(r["mapping_method"] for r in resource["replace"]).items())
    )
    resource["coverage"]["mean_confidence"] = round(
        sum(r["confidence"] for r in resource["replace"]) / len(resource["replace"]), 6
    )
