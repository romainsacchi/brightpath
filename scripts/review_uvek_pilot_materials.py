"""Replay the inspected material corrections encountered by the Premise pilot."""

from collections import Counter
from copy import deepcopy
import hashlib
import json
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_pilot_material_review.json"
APPROVAL_SHA256 = "3dab2783feae4db513e39c9ec0cc6f65da73785619c2fda3270a244b47b13245"


def apply_pilot_material_review(resource, root=ROOT):
    """Change only approved identities, retaining exact-profile reference gates.

    The resource remains heuristic overall. These corrections neither approve
    unrelated rules nor define an inverse route or a cross-system-model route.
    """
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != APPROVAL_SHA256:
        raise ValueError("Pilot material approval changed; repeat its scientific review.")
    review = json.loads(raw)
    indexed = {rule["id"]: rule for rule in resource["replace"]}
    pending = []
    for decision in review["rules"]:
        rule = indexed.get(decision["rule_id"])
        if (
            rule is None
            or rule["source"] != decision["source"]
            or rule["target"] not in (decision["previous_target"], decision["target"])
            or rule["source"]["unit"] != decision["target"]["unit"]
        ):
            raise ValueError("Pilot material source, supplier or unit changed.")
        for row in decision["profiles"]:
            if row["source_profile"] not in resource["source_profiles"] or row["reference_amount"] != 1:
                raise ValueError("Pilot material reference profile changed.")
        pending.append((rule, decision))
    for rule, decision in pending:
        limitation = decision["limitations"]
        rule.update(
            target=deepcopy(decision["target"]),
            mapping_method="reviewed_pilot_material",
            confidence=1.0,
            conversion_factor=decision["conversion_factor"],
            report_correspondence=True,
            technosphere_information_loss=limitation,
            reference_flow_sign_review={
                "status": "unresolved",
                "reason": "Only the exact inspected cut-off source profiles are approved.",
                "evidence": [REVIEW_PATH],
                "profiles": [
                    {
                        "source_profile": row["source_profile"],
                        "conversion_factor": decision["conversion_factor"],
                        "review": {
                            "status": "verified",
                            "source_sign": 1,
                            "target_sign": 1,
                            "reason": limitation,
                            "evidence": [REVIEW_PATH],
                        },
                    }
                    for row in decision["profiles"]
                ],
            },
        )
        rule["review_ref"] = intern_review(
            resource,
            {"rationale": review["authorization"], "limitations": [limitation], "evidence": [REVIEW_PATH]},
        )
    resource["coverage"]["mapping_methods"] = dict(
        sorted(Counter(rule["mapping_method"] for rule in resource["replace"]).items())
    )
    resource["coverage"]["mean_confidence"] = round(
        sum(rule["confidence"] for rule in resource["replace"]) / len(resource["replace"]), 6
    )
    referenced = {
        rule["review_ref"]
        for operation in ("replace", "disaggregate", "delete")
        for rule in resource.get(operation, ())
        if "review_ref" in rule
    }
    referenced.update(
        entry["review_ref"]
        for rule in resource["replace"]
        for entry in rule.get("reference_flow_sign_review", {}).get("profiles", ())
        if "review_ref" in entry
    )
    resource["reviews"] = {key: value for key, value in resource["reviews"].items() if key in referenced}
