"""Replay ordinary scrap markets while retaining collection uncertainty and older profiles."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

if __package__:
    from scripts.review_uvek_market_compositions import composition_decision, validate_composition_bindings
    from scripts.review_uvek_venting import compiled_venting_rule
else:
    from review_uvek_market_compositions import composition_decision, validate_composition_bindings
    from review_uvek_venting import compiled_venting_rule

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_pilot_nitrate_scrap_review.json"
REVIEW_SHA256 = "ec45074cda73e22561d106b1583fb7de617742911a2b4424a170a3060215db7f"


def apply_pilot_nitrate_scrap_review(resource, root=ROOT):
    """Retain ordinary treatment and every original collection coefficient."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Pilot ordinary scrap evidence changed; repeat its scientific review.")
    review = json.loads(raw)
    for row in review["authorization_evidence"]:
        if hashlib.sha256((root / "brightpath/data" / row["path"]).read_bytes()).hexdigest() != row["sha256"]:
            raise ValueError("Pilot ordinary scrap authority changed; repeat its scientific review.")
    validate_composition_bindings(
        resource,
        approval={"rules": review["records"], "recipe_dependencies": review["recipe_dependencies"]},
        label="Pilot ordinary scrap",
    )
    for key, recipe in review["auxiliary_recipes"].items():
        if key in resource["recipes"] and resource["recipes"][key] != recipe:
            raise ValueError("Pilot ordinary scrap source helper changed; repeat its review.")
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
            namespace="pilot-nitrate-scrap",
            mapping_method="reviewed_pilot_nitrate_scrap",
        )
        compiled = compiled_venting_rule(decision)
        if row["preserve_other_profiles"]:
            # Replace only the inspected 3.9 profile. The default target and
            # every other complete profile entry retain their earlier graph.
            compiled = deepcopy(row["previous_rule"])
            profiles = compiled["reference_flow_sign_review"]["profiles"]
            positions = [i for i, p in enumerate(profiles) if p["source_profile"] == row["source_profile"]]
            if len(positions) != 1:
                raise ValueError("Pilot ordinary scrap retained profiles changed; repeat its review.")
            profiles[positions[0]] = deepcopy(decision["review"]["profiles"][0])
            compiled["reference_flow_sign_review"]["evidence"].append(REVIEW_PATH)
            compiled["mapping_method"] = "reviewed_pilot_nitrate_scrap"
            compiled["review_ref"] = intern_review({}, decision["recipe_review"])
            compiled["technosphere_information_loss"] = row["limitations"]
        if rule not in (row["previous_rule"], compiled):
            raise ValueError("Pilot ordinary scrap parent changed; repeat its review.")
        for key, recipe in decision["recipes"].items():
            if key in resource["recipes"] and resource["recipes"][key] != recipe:
                raise ValueError("Pilot ordinary scrap composed helper changed; repeat its review.")
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
