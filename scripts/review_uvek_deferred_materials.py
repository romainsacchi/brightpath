"""Replay eight inspected deferred material suppliers without altering unrelated resources."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import rule_review

if __package__:
    from scripts.review_uvek_pilot_materials import _apply_material_review
else:
    from review_uvek_pilot_materials import _apply_material_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_deferred_materials_review.json"
REVIEW_SHA256 = "e2caa58488ec9f6b573300466849f6376a2082b33d5d68c391b7628abc571559"


def apply_deferred_materials_review(resource, root=ROOT):
    """Validate the complete previous state before changing the eight scoped rules."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Deferred material evidence changed; repeat its review.")
    review = json.loads(raw)
    isolated = {
        "replace": [deepcopy(row["previous_rule"]) for row in review["rules"]],
        "source_profiles": resource["source_profiles"],
        "reviews": {},
        "coverage": {},
    }
    _apply_material_review(isolated, root, REVIEW_PATH, REVIEW_SHA256, mapping_method="reviewed_deferred_materials")
    indexed = {r["id"]: r for r in resource["replace"]}
    replacements = {r["id"]: r for r in isolated["replace"]}
    for row in review["rules"]:
        previous = row["previous_rule"]
        if hashlib.sha256(json.dumps(previous, sort_keys=True).encode()).hexdigest() != row["previous_rule_sha256"]:
            raise ValueError("Deferred material previous-rule witness changed.")
        actual = indexed.get(row["rule_id"])
        compiled = replacements[row["rule_id"]]
        if actual not in (previous, compiled):
            if __package__:
                from scripts.review_uvek_heat_38_profiles import qualified_extended_profile_rule
            else:
                from review_uvek_heat_38_profiles import qualified_extended_profile_rule

            replacements[row["rule_id"]] = qualified_extended_profile_rule(actual, compiled, root)
        expected = row["previous_review"] if actual == previous else isolated["reviews"][compiled["review_ref"]]
        if rule_review(resource, actual) != expected:
            raise ValueError("Deferred material shared review changed; repeat its review.")
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
        raise ValueError("Deferred material replay would remove an unrelated review.")
    for key, value in isolated["reviews"].items():
        if key in resource["reviews"] and resource["reviews"][key] != value:
            raise ValueError("Deferred material compiled review changed.")
    for rid, compiled in replacements.items():
        indexed[rid].clear()
        indexed[rid].update(deepcopy(compiled))
    resource["reviews"] = {k: v for k, v in resource["reviews"].items() if k in references}
    resource["reviews"].update({k: deepcopy(v) for k, v in isolated["reviews"].items() if k in references})
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
