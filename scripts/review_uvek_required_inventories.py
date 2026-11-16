"""Replay fingerprinted inventory reviews after the shared component mappings."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath import TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.migrations.schema import intern_review, validate_migration_resource

ROOT = Path(__file__).resolve().parents[1]
INDEX_PATH = "brightpath/data/export/uvek_required_inventory_reviews.json"


def apply_required_inventory_reviews(resource, root=ROOT):
    """Validate every review and its dependencies before changing the resource.

    The index and individual reviews contain the scientific decisions. Keeping
    this final authoring stage generic lets subsequent reviews extend mapping
    data without adding another generator hook or changing runtime conversion.
    """
    root = Path(root)
    index = json.loads((root / INDEX_PATH).read_text())
    if index.get("schema_version") != 1:
        raise ValueError("Unsupported required-inventory review index.")
    if not index["reviews"]:
        return
    staged = deepcopy(resource)
    for entry in index["reviews"]:
        path = (root / entry["path"]).resolve()
        if not path.is_relative_to((root / "brightpath/data/export").resolve()):
            raise ValueError("Required-inventory review must be packaged export data.")
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != entry["sha256"]:
            raise ValueError("Required-inventory review changed; repeat review.")
        review = json.loads(raw)
        if review.get("schema_version") != 1 or review["target_profile"] != staged["target_profile"]:
            raise ValueError("Required-inventory review targets a different route.")
        profiles = review.get("source_profiles", [review.get("source_profile")])
        if not profiles or any(
            p not in staged["source_profiles"] or p["system_model"] != staged["target_profile"]["system_model"]
            for p in profiles
        ):
            raise ValueError("Required-inventory review cannot cross source system models.")
        indexed = {rule["id"]: rule for rule in staged["replace"]}
        for dependency in review["source_rule_dependency_scopes"]:
            rule = indexed[dependency["rule_id"]]
            actual = compatibility_rule_fingerprint(
                staged,
                "replace",
                rule,
                TechnosphereProfile(**dependency["source"]),
                TechnosphereProfile(**staged["target_profile"]),
            )
            if actual != dependency["fingerprint"]:
                raise ValueError("Required-inventory component mapping changed; repeat review.")
        for recipe_id, digest in review["recipe_dependency_sha256"].items():
            value = json.dumps(staged["recipes"][recipe_id], sort_keys=True, separators=(",", ":"), ensure_ascii=False)
            if hashlib.sha256(value.encode()).hexdigest() != digest:
                raise ValueError("Required-inventory component recipe changed; repeat review.")
        for row in review["records"]:
            original, compiled = row["original_rule"], row["compiled_rule"]
            if original["id"] != compiled["id"] or original["source"] != compiled["source"]:
                raise ValueError("Required-inventory review changes its source identity.")
            if indexed[row["rule_id"]] not in (original, compiled):
                raise ValueError("Required-inventory parent changed; repeat review.")
            guard = compiled.get("reference_flow_sign_review", {})
            if (
                guard.get("status") != "unresolved"
                or not guard.get("profiles")
                or any(
                    p["source_profile"] not in profiles or p["review"]["status"] != "verified"
                    for p in guard["profiles"]
                )
            ):
                raise ValueError("Required-inventory rules need exact reviewed source profiles.")
        for recipe_id, expected in review["recipes"].items():
            if recipe_id in staged["recipes"] and staged["recipes"][recipe_id] != expected:
                raise ValueError("Required-inventory recipe changed; repeat review.")
        for card in review["reviews"]:
            intern_review(staged, card)
        staged["recipes"].update(deepcopy(review["recipes"]))
        for row in review["records"]:
            if row["compiled_rule"]["review_ref"] not in staged["reviews"]:
                raise ValueError("Required-inventory rule has no packaged review.")
            indexed[row["rule_id"]].clear()
            indexed[row["rule_id"]].update(deepcopy(row["compiled_rule"]))
    rules = staged["replace"]
    staged["coverage"]["mapping_methods"] = dict(sorted(Counter(r["mapping_method"] for r in rules).items()))
    staged["coverage"]["mean_confidence"] = round(sum(r["confidence"] for r in rules) / len(rules), 6)
    dispositions = Counter(
        "recipe" if "recipe_ref" in r["target"] else r.get("classification", "unclassified") for r in rules
    )
    omitted = sum(r.get("approved_omission", False) for r in staged["delete"])
    if omitted:
        dispositions["omitted"] = omitted
    if staged["coverage"].get("unresolved_identities"):
        dispositions["unresolved"] = staged["coverage"]["unresolved_identities"]
    staged["coverage"]["dispositions"] = dict(dispositions)
    validate_migration_resource(staged)
    resource.clear()
    resource.update(staged)
