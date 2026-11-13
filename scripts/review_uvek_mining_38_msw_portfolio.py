"""Replay inspected municipal-waste disposal portfolio correspondence."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_mining_38_msw_portfolio_review.json"
REVIEW_SHA256 = "75463a6dcd949329ba3baf73ed0efa67d747d5121cb3881f8a334124377a73c7"


def apply_mining_38_msw_portfolio_review(resource, root=ROOT):
    """Validate all frozen parents and reviews before mutating any rule."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Municipal-waste portfolio evidence changed; repeat review.")
    review = json.loads(raw)
    if len(review["records"]) != 1:
        raise ValueError("Municipal-waste portfolio scope changed.")
    calculation = review["public_calculation"]
    if hashlib.sha256((root / calculation["path"]).read_bytes()).hexdigest() != calculation["sha256"]:
        raise ValueError("Public waste calculation changed; repeat review.")
    canonical = calculation["canonical_identity_dependency"]
    if hashlib.sha256((root / canonical["path"]).read_bytes()).hexdigest() != canonical["sha256"]:
        raise ValueError("Canonical biosphere dependency changed; repeat review.")
    pending = []
    staged = {"reviews": deepcopy(resource.get("reviews", {}))}
    for row in review["records"]:
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        if parent not in (row["original_rule"], row["compiled_rule"]):
            raise ValueError("Municipal-waste portfolio parent changed; repeat review.")
        pending.append((parent, row["compiled_rule"]))
    for key, expected in review.get("recipe_dependencies", {}).items():
        if resource.get("recipes", {}).get(key) != expected:
            raise ValueError("Inherited recipe dependency changed; repeat review.")
    for recipe in review["recipes"].values():
        for exchange in recipe["inputs"]:
            binding = exchange.get("zero_burden_supplier")
            if not binding:
                continue
            rule = next(r for r in resource["replace"] if r["id"] == binding["rule_id"])
            approval = next(
                p
                for p in rule["reference_flow_sign_review"]["profiles"]
                if p["source_profile"] == binding["source_profile"]
            )
            if (
                hashlib.sha256(json.dumps(approval, sort_keys=True).encode()).hexdigest()
                != binding["profile_review_sha256"]
            ):
                raise ValueError("Cut-off stock approval changed; repeat review.")
    for key, expected in review["recipes"].items():
        if key in resource.get("recipes", {}) and resource["recipes"][key] != expected:
            raise ValueError("Municipal-waste recipe changed; repeat review.")
    for card in review["reviews"]:
        intern_review(staged, card)
    if any(rule.get("review_ref") and rule["review_ref"] not in staged["reviews"] for _, rule in pending):
        raise ValueError("Municipal-waste portfolio review reference missing.")
    resource.setdefault("recipes", {}).update(deepcopy(review["recipes"]))
    resource["reviews"] = staged["reviews"]
    for parent, compiled in pending:
        parent.clear()
        parent.update(deepcopy(compiled))
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
    dispositions["omitted"] = sum(r.get("approved_omission", False) for r in resource["delete"])
    if resource["coverage"].get("unresolved_identities"):
        dispositions["unresolved"] = resource["coverage"]["unresolved_identities"]
    resource["coverage"]["dispositions"] = dict(dispositions)
