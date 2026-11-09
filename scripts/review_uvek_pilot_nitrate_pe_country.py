"""Replay the exact nitrate PE country markets with their original fates and delivery."""

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
REVIEW_PATH = "brightpath/data/export/uvek_pilot_nitrate_pe_country_review.json"
REVIEW_SHA256 = "d9c0b1c3f2bb765114bf8ddfa529ca635ec165fa8eca35acbaac4f382ba2798b"


def apply_pilot_nitrate_pe_country_review(resource, root=ROOT):
    """Retain original country fates using complete independently reviewed treatments."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Pilot nitrate PE country evidence changed; repeat its scientific review.")
    review = json.loads(raw)
    for row in review["authorization_evidence"] + review["historical_binding_authorities"]:
        if hashlib.sha256((root / "brightpath/data" / row["path"]).read_bytes()).hexdigest() != row["sha256"]:
            raise ValueError("Pilot nitrate PE country authority changed; repeat its scientific review.")
    for path, expected in (
        ("export/reference_catalogs/ecoinvent__3.10__cutoff.json", review["canonical_catalog_sha256"]),
        ("migrations/uvek/ecoinvent-to-ecoinvent-3.10-biosphere.json", review["canonical_resource_sha256"]),
    ):
        if hashlib.sha256((root / "brightpath/data" / path).read_bytes()).hexdigest() != expected:
            raise ValueError("Pilot nitrate PE publisher identity changed; repeat its review.")
    validate_composition_bindings(
        resource,
        approval={"rules": review["records"], "recipe_dependencies": review["recipe_dependencies"]},
        label="Pilot nitrate PE country",
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
            namespace="pilot-nitrate-goods",
            mapping_method="reviewed_pilot_nitrate_pe_country",
        )
        compiled = compiled_venting_rule(decision)
        if rule not in (row["previous_rule"], compiled):
            raise ValueError("Pilot nitrate PE country parent changed; repeat its review.")
        for key, recipe in decision["recipes"].items():
            if key in resource["recipes"] and resource["recipes"][key] != recipe:
                raise ValueError("Pilot nitrate PE country composed helper changed; repeat its review.")
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
