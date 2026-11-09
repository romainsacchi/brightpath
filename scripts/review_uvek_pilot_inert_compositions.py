"""Replay exact RoW inert-landfill compositions using reviewed components."""

from collections import Counter
from copy import deepcopy
import hashlib
import json
from pathlib import Path

from brightpath.migrations.schema import intern_review

if __package__:
    from scripts.review_uvek_market_compositions import composition_decision, validate_composition_bindings
    from scripts.review_uvek_venting import compiled_venting_rule
else:
    from review_uvek_market_compositions import composition_decision, validate_composition_bindings
    from review_uvek_venting import compiled_venting_rule

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_pilot_inert_composition_review.json"
REVIEW_SHA256 = "5a03c29a361ab5027a2134fa7904b186568738f39789846e88742fb89e0d5aa2"


def apply_pilot_inert_composition_review(resource, root=ROOT):
    """Preserve source operation/facility boundaries without inventing leachate."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Pilot inert-composition evidence changed; repeat the scientific review.")
    review = json.loads(raw)
    for authority in review["authorization_evidence"]:
        evidence = (root / "brightpath/data" / authority["path"]).read_bytes()
        if hashlib.sha256(evidence).hexdigest() != authority["sha256"]:
            raise ValueError("Prior inert-composition component evidence changed; repeat its review.")
    validate_composition_bindings(
        resource, approval={"rules": review["records"], "recipe_dependencies": {}}, label="Pilot inert-composition"
    )
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
            namespace="pilot-inert-composition",
            mapping_method="reviewed_pilot_inert_composition",
        )
        compiled = compiled_venting_rule(decision)
        if rule not in (row["previous_rule"], compiled):
            raise ValueError("Pilot inert-composition source or supplier changed; repeat the scientific review.")
        for recipe_id, recipe in decision["recipes"].items():
            if recipe_id in resource["recipes"] and resource["recipes"][recipe_id] != recipe:
                raise ValueError("Pilot inert-composition helper changed; repeat the scientific review.")
        pending.append((rule, compiled, decision))
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
