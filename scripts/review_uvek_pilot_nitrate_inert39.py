"""Add the inspected 3.9 inert market while retaining its exact 3.12 recipe."""

from copy import deepcopy
import hashlib
import json
from pathlib import Path

from brightpath.migrations.schema import intern_review

if __package__:
    from scripts.review_uvek_market_compositions import composition_decision, validate_composition_bindings
else:
    from review_uvek_market_compositions import composition_decision, validate_composition_bindings

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_pilot_nitrate_inert39_review.json"
REVIEW_SHA256 = "6b6f31df4dd79f5ac3d8fbe035de5820e6b60956cd6f00bc27cf96d7271d29f2"


def apply_pilot_nitrate_inert39_review(resource, root=ROOT):
    """Extend exact-profile coverage without rewriting an admitted scientific graph."""
    evidence = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(evidence).hexdigest() != REVIEW_SHA256:
        raise ValueError("Pilot inert3.9 evidence changed; repeat its scientific review.")
    review = json.loads(evidence)
    for authority in review["authorization_evidence"]:
        if (
            hashlib.sha256((root / "brightpath/data" / authority["path"]).read_bytes()).hexdigest()
            != authority["sha256"]
        ):
            raise ValueError("Pilot inert3.9 authority changed; repeat its review.")
    validate_composition_bindings(
        resource,
        approval={"rules": review["records"], "recipe_dependencies": review["recipe_dependencies"]},
        label="Pilot inert3.9",
    )
    for key, recipe in review["auxiliary_recipes"].items():
        if key in resource["recipes"] and resource["recipes"][key] != recipe:
            raise ValueError("Pilot inert3.9 treatment helper changed; repeat its review.")
    (row,) = review["records"]
    current = next((r for r in resource["replace"] if r["id"] == row["rule_id"]), None)
    decision = composition_decision(
        row["previous_rule"],
        [(p["label"], p["source_sign"]) for p in row["profiles"]],
        [(p["label"], p["inventory_sha256"]) for p in row["profiles"]],
        row["previous_rule_sha256"],
        row["previous_review"],
        approval_row=row,
        rationale=review["combined_limitations"],
        review_path=REVIEW_PATH,
        namespace="pilot-nitrate-inert39",
        mapping_method=review["mapping_method_retained"],
    )
    # Default target and the complete older profile entry stay byte-for-byte
    # equal. Its raw-operation approval is explicitly reissued in Premise.
    compiled = deepcopy(row["previous_rule"])
    old_profiles = compiled["reference_flow_sign_review"]["profiles"]
    if old_profiles != [review["retained_312"]["previous_selected_profile_entry"]]:
        raise ValueError("Pilot inert3.9 retained profile changed; repeat its review.")
    old_profiles.append(deepcopy(decision["review"]["profiles"][0]))
    compiled["reference_flow_sign_review"]["evidence"].append(REVIEW_PATH)
    compiled["review_ref"] = intern_review({}, decision["recipe_review"])
    compiled["technosphere_information_loss"] = review["combined_limitations"]
    if current not in (row["previous_rule"], compiled):
        raise ValueError("Pilot inert3.9 parent changed; repeat its review.")
    for key, recipe in decision["recipes"].items():
        if key in resource["recipes"] and resource["recipes"][key] != recipe:
            raise ValueError("Pilot inert3.9 market helper changed; repeat its review.")
    resource["recipes"].update(deepcopy(review["auxiliary_recipes"]))
    resource["recipes"].update(deepcopy(decision["recipes"]))
    intern_review(resource, decision["recipe_review"])
    current.clear()
    current.update(compiled)
