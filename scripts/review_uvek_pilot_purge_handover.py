"""Replay the sodium-nitrate-rich purge-stream cut-off handover."""

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
REVIEW_PATH = "brightpath/data/export/uvek_pilot_purge_handover_review.json"
REVIEW_SHA256 = "8ec148a7665f2214d56682f8b122b026921832420dbbd274687c0327d406699c"


def apply_pilot_purge_handover_review(resource, root=ROOT):
    """Preserve the exact positive market and its production-only cut-off supplier."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Pilot purge-stream evidence changed; repeat its scientific review.")
    review = json.loads(raw)
    for authority in review["authorization_evidence"]:
        if (
            hashlib.sha256((root / "brightpath/data" / authority["path"]).read_bytes()).hexdigest()
            != authority["sha256"]
        ):
            raise ValueError("Pilot purge-stream component authority changed; repeat its review.")
    stock = review["stock"]
    stock_rule = next((r for r in resource["replace"] if r["id"] == stock["rule_id"]), None)
    if stock_rule not in (stock["previous_rule"], stock["compiled_rule"]):
        raise ValueError("Pilot purge-stream recovered-product boundary changed; repeat its review.")
    if intern_review({}, stock["recipe_review"]) != stock["compiled_rule"]["review_ref"]:
        raise ValueError("Pilot purge-stream recovered-product provenance changed; repeat its review.")
    if stock["recipe_id"] in resource["recipes"] and resource["recipes"][stock["recipe_id"]] != stock["recipe"]:
        raise ValueError("Pilot purge-stream recovered-product helper changed; repeat its review.")

    # Validate the parent's bindings against the reviewed stock correction before
    # mutating either rule. The original child heuristic is never a zero fallback.
    candidate = {
        **resource,
        "replace": [stock["compiled_rule"] if r is stock_rule else r for r in resource["replace"]],
        "recipes": {**resource["recipes"], stock["recipe_id"]: stock["recipe"]},
    }
    validate_composition_bindings(
        candidate,
        approval={"rules": review["records"], "recipe_dependencies": review["recipe_dependencies"]},
        label="Pilot purge-stream",
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
            namespace="pilot-purge-handover",
            mapping_method="reviewed_pilot_purge_handover",
        )
        compiled = compiled_venting_rule(decision)
        if rule not in (row["previous_rule"], compiled):
            raise ValueError("Pilot purge-stream market changed; repeat its scientific review.")
        for key, recipe in decision["recipes"].items():
            if key in resource["recipes"] and resource["recipes"][key] != recipe:
                raise ValueError("Pilot purge-stream market helper changed; repeat its review.")
        pending.append((rule, compiled, decision))

    stock_rule.clear()
    stock_rule.update(deepcopy(stock["compiled_rule"]))
    resource["recipes"][stock["recipe_id"]] = deepcopy(stock["recipe"])
    intern_review(resource, stock["recipe_review"])
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
