"""Replay inspected pilot incinerator endpoints without construction-waste wrappers."""

from copy import deepcopy
import hashlib
import json
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_pilot_incinerator_endpoint_review.json"
REVIEW_SHA256 = "1c79c2adb0d30e5223040d6a4edd3f963f998b787451b18784fe8cde39739d4a"


def apply_pilot_incinerator_review(resource, root=ROOT):
    """Replace four reviewed endpoints; retain exact-profile sign and policy gates."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Pilot incinerator evidence changed; repeat the scientific review.")
    review = json.loads(raw)
    indexed = {rule["id"]: rule for rule in resource["replace"]}
    pending = []
    for decision in review["records"]:
        rule = indexed.get(decision["rule_id"])
        if (
            rule is None
            or rule["source"] != decision["source"]
            or rule["target"] not in (decision["previous_target"], decision["target"])
            or rule["conversion_factor"] != -1
            or decision["conversion_factor"] != -1
            or decision["source_profile"] not in resource["source_profiles"]
            or decision["source_reference_amount"] != -1
            or not decision["native_2026_wet_composition_matches_source"]
            or decision["matching_composition_coordinates"] != 44
            or rule["source"]["unit"] != decision["target"]["unit"]
            or rule["source"]["location"] != decision["target"]["location"]
            or set(decision["native_references"]) != {"2025", "2026"}
            or any(r["reference_amount"] != 1 for r in decision["native_references"].values())
        ):
            raise ValueError("Pilot incinerator source, target or reference coordinate changed.")
        pending.append((rule, decision))
    limitation = " ".join(review["limitations"])
    for rule, decision in pending:
        rule.update(
            target=deepcopy(decision["target"]),
            report_correspondence=True,
            technosphere_information_loss=limitation,
            reference_flow_sign_review={
                "status": "unresolved",
                "reason": "Only the exact inspected cut-off source profiles are reviewed.",
                "evidence": [REVIEW_PATH],
                "profiles": [
                    {
                        "source_profile": decision["source_profile"],
                        "conversion_factor": -1,
                        "review": {
                            "status": "verified",
                            "source_sign": -1,
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
            {"rationale": review["authorization"], "limitations": review["limitations"], "evidence": [REVIEW_PATH]},
        )
