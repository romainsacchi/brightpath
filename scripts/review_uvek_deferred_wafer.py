"""Qualify the inspected fabricated-wafer area reference and complete native proxy."""

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
REVIEW_PATH = "brightpath/data/export/uvek_deferred_wafer_review.json"
REVIEW_SHA256 = "3b5049be3623274ede7921e8d798978f2d030a0122ed1f652d35493a1595fae6"


def apply_deferred_wafer_review(resource, root=ROOT):
    """Retain complete native fabrication and separate source market freight per square metre."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Deferred wafer evidence changed; repeat its scientific review.")
    review = json.loads(raw)
    for authority in review["extra_component_qualifications"]:
        if (
            hashlib.sha256((root / "brightpath/data" / authority["review_path"]).read_bytes()).hexdigest()
            != authority["review_sha256"]
        ):
            raise ValueError("wafer component authority changed; repeat its review.")
    if (
        hashlib.sha256(
            (root / "brightpath/data/export/reference_catalogs/ecoinvent__3.10__cutoff.json").read_bytes()
        ).hexdigest()
        != review["canonical_catalog_sha256"]
    ):
        raise ValueError("Canonical wafer emission definitions changed; repeat their review.")
    validate_composition_bindings(
        resource,
        approval={"rules": review["records"], "recipe_dependencies": review["recipe_dependencies"]},
        label="Deferred wafer",
    )
    for key, recipe in review["auxiliary_recipes"].items():
        if key in resource["recipes"] and resource["recipes"][key] != recipe:
            raise ValueError("Deferred wafer source helper changed; repeat its review.")
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
            namespace="deferred-wafer",
            mapping_method="reviewed_deferred_wafer",
        )
        compiled = compiled_venting_rule(decision)
        expected_review = row["previous_review"] if rule == row["previous_rule"] else decision["recipe_review"]
        if rule is None or rule_review(resource, rule) != expected_review:
            raise ValueError("Deferred wafer shared review changed; repeat its review.")
        if rule not in (row["previous_rule"], compiled):
            raise ValueError("Deferred wafer parent changed; repeat its review.")
        for key, recipe in decision["recipes"].items():
            if key in resource["recipes"] and resource["recipes"][key] != recipe:
                raise ValueError("Deferred wafer composed helper changed; repeat its review.")
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
