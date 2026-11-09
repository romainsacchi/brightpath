"""Replay exact 3.6 CHP-component and helicopter phase correspondences."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review, rule_review

if __package__:
    from scripts.review_uvek_market_compositions import composition_decision
    from scripts.review_uvek_pilot_materials import _apply_material_review
    from scripts.review_uvek_venting import compiled_venting_rule
else:
    from review_uvek_market_compositions import composition_decision
    from review_uvek_pilot_materials import _apply_material_review
    from review_uvek_venting import compiled_venting_rule

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_pilot_legacy_components_review.json"
REVIEW_SHA256 = "d284a761fa98db3db6b10f2889939852fcba9a079c6d9a5eb1764d2d82b84b25"


def apply_pilot_legacy_components_review(resource, root=ROOT):
    """Validate the complete previous state before changing the three scoped rules."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Pilot legacy-component evidence changed; repeat its review.")
    review = json.loads(raw)
    for authority in review["authorization_evidence"]:
        if (
            hashlib.sha256((root / "brightpath/data" / authority["path"]).read_bytes()).hexdigest()
            != authority["sha256"]
        ):
            raise ValueError("Pilot legacy-component authority changed; repeat its review.")
    for path, expected in (
        ("export/reference_catalogs/uvek__2026__cutoff.json", review["native_catalog_sha256"]),
        ("export/reference_catalogs/ecoinvent__3.10__cutoff.json", review["canonical_catalog_sha256"]),
        ("migrations/uvek/ecoinvent-to-ecoinvent-3.10-biosphere.json", review["canonical_resource_sha256"]),
    ):
        if hashlib.sha256((root / "brightpath/data" / path).read_bytes()).hexdigest() != expected:
            raise ValueError("Pilot legacy-component catalog changed; repeat its review.")
    isolated = {
        "replace": [deepcopy(row["previous_rule"]) for row in review["rules"]],
        "source_profiles": resource["source_profiles"],
        "reviews": {},
        "coverage": {},
    }
    _apply_material_review(
        isolated, root, REVIEW_PATH, REVIEW_SHA256, mapping_method="reviewed_pilot_legacy_components"
    )
    for row in review["rules"]:
        if "generated_recipe_id" not in row:
            continue
        profiles = row["profiles"]
        decision = composition_decision(
            row["previous_rule"],
            [(p["label"], p["source_sign"]) for p in profiles],
            [(p["label"], p["inventory_sha256"]) for p in profiles],
            row["previous_rule_sha256"],
            row["previous_review"],
            approval_row=row,
            rationale=row["limitations"],
            review_path=REVIEW_PATH,
            namespace="pilot-legacy-hour",
            mapping_method="reviewed_pilot_legacy_components",
        )
        if decision["recipes"] != review["auxiliary_recipes"]:
            raise ValueError("Pilot legacy native-hour copy changed; repeat its review.")
        isolated["replace"] = [
            compiled_venting_rule(decision) if r["id"] == row["rule_id"] else r for r in isolated["replace"]
        ]
        intern_review(isolated, decision["recipe_review"])
    for rid, recipe in review["auxiliary_recipes"].items():
        if rid in resource["recipes"] and resource["recipes"][rid] != recipe:
            raise ValueError("Pilot legacy native-hour inventory changed; repeat its review.")
    indexed = {r["id"]: r for r in resource["replace"]}
    replacements = {r["id"]: r for r in isolated["replace"]}
    for row in review["rules"]:
        previous = row["previous_rule"]
        if hashlib.sha256(json.dumps(previous, sort_keys=True).encode()).hexdigest() != row["previous_rule_sha256"]:
            raise ValueError("Pilot legacy-component previous-rule witness changed.")
        actual = indexed.get(row["rule_id"])
        compiled = replacements[row["rule_id"]]
        if actual not in (previous, compiled):
            raise ValueError("Pilot legacy-component rule changed; repeat its review.")
        expected = row["previous_review"] if actual == previous else isolated["reviews"][compiled["review_ref"]]
        if rule_review(resource, actual) != expected:
            raise ValueError("Pilot legacy-component shared review changed; repeat its review.")
    future_rules = [replacements.get(r["id"], r) for r in resource["replace"]]
    references = {
        r["review_ref"]
        for r in [*future_rules, *resource.get("delete", []), *resource.get("disaggregate", [])]
        if "review_ref" in r
    }
    references.update(
        p["review_ref"]
        for r in future_rules
        for p in r.get("reference_flow_sign_review", {}).get("profiles", [])
        if "review_ref" in p
    )
    retired = set(resource["reviews"]) - references
    if not retired <= {r["previous_rule"].get("review_ref") for r in review["rules"]}:
        raise ValueError("Pilot legacy-component replay would remove an unrelated review.")
    for key, value in isolated["reviews"].items():
        if key in resource["reviews"] and resource["reviews"][key] != value:
            raise ValueError("Pilot legacy-component compiled review changed.")
    for rid, compiled in replacements.items():
        indexed[rid].clear()
        indexed[rid].update(deepcopy(compiled))
    resource["reviews"] = {k: v for k, v in resource["reviews"].items() if k in references}
    resource["reviews"].update({k: deepcopy(v) for k, v in isolated["reviews"].items() if k in references})
    resource["recipes"].update(deepcopy(review["auxiliary_recipes"]))
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
