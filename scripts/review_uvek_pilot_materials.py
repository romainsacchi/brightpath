"""Replay the inspected material corrections encountered by the Premise pilot."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_pilot_material_review.json"
APPROVAL_SHA256 = "3dab2783feae4db513e39c9ec0cc6f65da73785619c2fda3270a244b47b13245"
PAINT_MARKET_PATH = "brightpath/data/export/uvek_pilot_paint_market_review.json"
PAINT_MARKET_SHA256 = "330d61db91b138f75ef8a7fd2af3ab459df2fffc0c602c248cc16fe9bd31893d"
SILVER_MARKET_PATH = "brightpath/data/export/uvek_pilot_silver_market_review.json"
SILVER_MARKET_SHA256 = "55cb4c66b7ebd0ec8d62cd41e697f653cfe259ccc61eba414979e44d66a74a07"


def apply_pilot_material_review(resource, root=ROOT):
    """Change only approved identities, retaining exact-profile reference gates.

    The resource remains heuristic overall. These corrections neither approve
    unrelated rules nor define an inverse route or a cross-system-model route.
    """
    _apply_material_review(resource, root, REVIEW_PATH, APPROVAL_SHA256)


def apply_pilot_paint_market_review(resource, root=ROOT):
    """Replay the same approved water-based producer behind its inspected market."""
    if hashlib.sha256((root / REVIEW_PATH).read_bytes()).hexdigest() != APPROVAL_SHA256:
        raise ValueError("Original paint approval changed; repeat its scientific review.")
    _apply_material_review(resource, root, PAINT_MARKET_PATH, PAINT_MARKET_SHA256)


def apply_pilot_silver_market_review(resource, root=ROOT):
    """Use the inspected complete silver supply mix for the exact pilot market."""
    _apply_material_review(resource, root, SILVER_MARKET_PATH, SILVER_MARKET_SHA256)


def _apply_material_review(resource, root, review_path, approval_sha256, *, mapping_method="reviewed_pilot_material"):
    raw = (root / review_path).read_bytes()
    if hashlib.sha256(raw).hexdigest() != approval_sha256:
        raise ValueError("Pilot material approval changed; repeat its scientific review.")
    review = json.loads(raw)
    indexed = {rule["id"]: rule for rule in resource["replace"]}
    pending = []
    for decision in review["rules"]:
        source_sign = decision.get("source_reference_amount", 1)
        rule = indexed.get(decision["rule_id"])
        if (
            source_sign not in (-1, 1)
            or rule is None
            or rule["source"] != decision["source"]
            or rule["target"] not in (decision["previous_target"], decision["target"])
            or rule["source"]["unit"] != decision["target"]["unit"]
        ):
            raise ValueError("Pilot material source, supplier or unit changed.")
        if (
            decision["rule_id"] == "bp-56229a990a6a39be"
            and rule.get("mapping_method") == "reviewed_inventory_boundary_correction"
        ):
            if __package__:
                from scripts.review_uvek_heat_38_corrections import preserve_superseding_lime_rule
            else:
                from review_uvek_heat_38_corrections import preserve_superseding_lime_rule

            preserve_superseding_lime_rule(rule, decision, root)
            pending.append((rule, None))
            continue
        for row in decision["profiles"]:
            if row["source_profile"] not in resource["source_profiles"] or row["reference_amount"] != source_sign:
                raise ValueError("Pilot material reference profile changed.")
        pending.append((rule, decision))
    for rule, decision in pending:
        if decision is None:
            continue
        source_sign = decision.get("source_reference_amount", 1)
        limitation = decision["limitations"]
        rule.update(
            target=deepcopy(decision["target"]),
            mapping_method=mapping_method,
            confidence=1.0,
            conversion_factor=decision["conversion_factor"],
            report_correspondence=True,
            technosphere_information_loss=limitation,
            reference_flow_sign_review={
                "status": "unresolved",
                "reason": "Only the exact inspected cut-off source profiles are approved.",
                "evidence": [review_path],
                "profiles": [
                    {
                        "source_profile": row["source_profile"],
                        "conversion_factor": decision["conversion_factor"],
                        "review": {
                            "status": "verified",
                            "source_sign": source_sign,
                            "target_sign": 1,
                            "reason": limitation,
                            "evidence": [review_path],
                        },
                    }
                    for row in decision["profiles"]
                ],
            },
        )
        rule["review_ref"] = intern_review(
            resource,
            {"rationale": review["authorization"], "limitations": [limitation], "evidence": [review_path]},
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
