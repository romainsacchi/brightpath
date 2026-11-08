"""Replay the inspected bare mineral-wool landfill endpoint correction."""

from copy import deepcopy
import hashlib
import json
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_pilot_bare_landfill_review.json"
REVIEW_SHA256 = "b50c3f9def3a445f55d8e4f066cad2d68f6002b3f97b52c910a0812d1eb37fd8"


def apply_pilot_landfill_review(resource, root=ROOT):
    """Remove a collection wrapper, retaining an exact-profile reference gate.

    The other inspected bare landfill already has the correct endpoint and
    remains unchanged. This correction does not approve other source versions.
    """
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Pilot landfill evidence changed; repeat the scientific review.")
    review = json.loads(raw)
    for authority in review["authorization_evidence"]:
        evidence = (root / "brightpath/data" / authority["path"]).read_bytes()
        if hashlib.sha256(evidence).hexdigest() != authority["sha256"]:
            raise ValueError("Prior landfill endpoint evidence changed; repeat its review.")
    (decision,) = [r for r in review["records"] if r["correct_endpoint"]]
    rule = next((r for r in resource["replace"] if r["id"] == decision["rule_id"]), None)
    if (
        rule is None
        or rule["source"] != decision["source"]
        or rule["target"] not in (decision["previous_target"], decision["target"])
        or rule["conversion_factor"] != -1
        or decision["conversion_factor"] != -1
        or decision["source_reference_amount"] != -1
        or decision["source_profile"] not in resource["source_profiles"]
        or rule["source"]["unit"] != decision["target"]["unit"]
        or rule["source"]["location"] != decision["target"]["location"]
        or set(decision["native_references"]) != {"2025", "2026"}
        or any(
            r["reference_amount"] != 1 or r["identity"] != decision["target"]
            for r in decision["native_references"].values()
        )
    ):
        raise ValueError("Pilot landfill source, target or reference coordinate changed.")
    limitations = [decision["boundary"], *review["limitations"]]
    reason = " ".join(limitations)
    rule.update(
        target=deepcopy(decision["target"]),
        report_correspondence=True,
        technosphere_information_loss=reason,
        reference_flow_sign_review={
            "status": "unresolved",
            "reason": "Only the exact inspected cut-off source profile is reviewed.",
            "evidence": [REVIEW_PATH],
            "profiles": [
                {
                    "source_profile": decision["source_profile"],
                    "conversion_factor": -1,
                    "review": {
                        "status": "verified",
                        "source_sign": -1,
                        "target_sign": 1,
                        "reason": reason,
                        "evidence": [REVIEW_PATH],
                    },
                }
            ],
        },
    )
    rule["review_ref"] = intern_review(
        resource,
        {"rationale": review["authorization"], "limitations": limitations, "evidence": [REVIEW_PATH]},
    )
