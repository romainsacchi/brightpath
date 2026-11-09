"""Compose inspected waste markets with freight and exact allocated-handover audits."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review, rule_review

if __package__:
    from scripts.review_uvek_market_compositions import composition_decision, validate_composition_bindings
    from scripts.review_uvek_venting import compiled_venting_rule
else:
    from review_uvek_market_compositions import composition_decision, validate_composition_bindings
    from review_uvek_venting import compiled_venting_rule

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_deferred_waste_markets_review.json"
REVIEW_SHA256 = "19117fadd243bf92f8b637ff6f9f7233cbdd3a469ac4c125e63ac6ddced1e3e2"


def apply_deferred_waste_markets_review(resource, root=ROOT):
    """Preserve original market shares and freight; audit only verified empty terminals."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Deferred waste market evidence changed; repeat its scientific review.")
    review = json.loads(raw)
    for authority in review["extra_component_qualifications"]:
        if (
            hashlib.sha256((root / "brightpath/data" / authority["review_path"]).read_bytes()).hexdigest()
            != authority["review_sha256"]
        ):
            raise ValueError("Waste-market freight authority changed; repeat its review.")
    validate_composition_bindings(
        resource,
        approval={"rules": review["records"], "recipe_dependencies": review["recipe_dependencies"]},
        label="Deferred waste market",
    )
    for key, recipe in review["auxiliary_recipes"].items():
        if key in resource["recipes"] and resource["recipes"][key] != recipe:
            raise ValueError("Deferred waste market source helper changed; repeat its review.")
    pending = []
    for row in review["records"]:
        rule = next((r for r in resource["replace"] if r["id"] == row["rule_id"]), None)
        decision = composition_decision(
            row["previous_rule"],
            [(p["label"], p["source_sign"]) for p in row["profiles"]],
            [(p["label"], p["inventory_sha256"]) for p in row["profiles"]],
            row["previous_rule_sha256"],
            row["previous_review"],
            approval_row=row,
            rationale=row["limitations"],
            review_path=REVIEW_PATH,
            namespace="deferred-waste-market",
            mapping_method="reviewed_deferred_waste_markets",
        )
        compiled = compiled_venting_rule(decision)
        expected_review = row["previous_review"] if rule == row["previous_rule"] else decision["recipe_review"]
        if rule is None or rule_review(resource, rule) != expected_review:
            raise ValueError("Deferred waste-market shared review changed; repeat its review.")
        if rule not in (row["previous_rule"], compiled):
            raise ValueError("Deferred waste market parent changed; repeat its review.")
        for key, recipe in decision["recipes"].items():
            if key in resource["recipes"] and resource["recipes"][key] != recipe:
                raise ValueError("Deferred waste market composed helper changed; repeat its review.")
        pending.append((rule, compiled, decision))
    resource["recipes"].update(deepcopy(review["auxiliary_recipes"]))
    for rule, compiled, decision in pending:
        rule.clear()
        rule.update(compiled)
        resource["recipes"].update(deepcopy(decision["recipes"]))
        intern_review(resource, decision["recipe_review"])
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
    dispositions["omitted"] = len(resource["delete"])
    if resource["coverage"].get("unresolved_identities"):
        dispositions["unresolved"] = resource["coverage"]["unresolved_identities"]
    resource["coverage"]["dispositions"] = dict(dispositions)
