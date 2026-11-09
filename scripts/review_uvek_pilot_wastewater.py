"""Replay the inspected class-2 wastewater treatment endpoint correction."""

from copy import deepcopy
import hashlib
import json
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_pilot_wastewater_endpoint_review.json"
REVIEW_SHA256 = "d83932e5b421590f621b4b5d9f2532468fac7641846d70dc2f9a4c68dd0f1ada"


def apply_pilot_wastewater_review(resource, root=ROOT):
    """Select the same treatment class without an extra residential sewer stage.

    Keep the plant's internal sewer grid and all other native inputs/emissions.
    The opposite waste/service reference signs remain an exact-profile gate.
    """
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Pilot wastewater evidence changed; repeat the scientific review.")
    review = json.loads(raw)
    for authority in review["authorization_evidence"]:
        evidence = (root / "brightpath/data" / authority["path"]).read_bytes()
        if hashlib.sha256(evidence).hexdigest() != authority["sha256"]:
            raise ValueError("Prior wastewater endpoint evidence changed; repeat its review.")
    (decision,) = review["records"]
    rule = next((r for r in resource["replace"] if r["id"] == decision["rule_id"]), None)
    if (
        rule is None
        or rule["source"] != decision["source"]
        or rule["target"] not in (decision["previous_target"], decision["target"])
        or rule["conversion_factor"] != -1
        or decision["conversion_factor"] != -1
        or decision["source_reference_amount"] != -1
        or decision["capacity_class"] != 2
        or decision["declared_population_capacity"] != 71100
        or decision["declared_cod_kg_per_m3"] != 0.1556
        or not all(
            r["direct_biosphere_coordinates_unchanged"] for r in decision["previous_target_extra_stage"].values()
        )
        or decision["source_profile"] not in resource["source_profiles"]
        or rule["source"]["unit"] != decision["target"]["unit"]
        or rule["source"]["location"] != decision["target"]["location"]
        or set(decision["native_references"]) != {"2025", "2026"}
        or any(
            r["reference_amount"] != 1 or r["identity"] != decision["target"]
            for r in decision["native_references"].values()
        )
    ):
        raise ValueError("Pilot wastewater source, target or reference coordinate changed.")
    limitations = review["limitations"]
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
