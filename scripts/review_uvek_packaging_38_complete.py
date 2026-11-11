"""Replay the inspected packed-product services and complete PE disposal closure."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_packaging_38_complete_review.json"
REVIEW_SHA256 = "c233e5758bd860400e36c8247cd9bd15129c69c18b3c4f69f47f1d49cec927de"


def digest(value):
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return hashlib.sha256(raw.encode()).hexdigest()


def apply_packaging_38_complete_review(resource, root=ROOT):
    """Keep physical fates, packed-product basis and all older recipe contents."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Complete packaging evidence changed.")
    review = json.loads(raw)
    if (
        review["source_profile"] != {"family": "ecoinvent", "version": "3.8", "system_model": "cutoff"}
        or len(review["records"]) != 2
        or len(review["recipes"]) != 55
        or review["closure_counts"]["source_waste_markets"] != 45
        or review["closure_counts"]["public_nonzero_biosphere_coordinates"] != 612
        or review["closure_counts"]["new_physical_omissions"] != 0
        or len(review["constant_formula_proofs"]) != 6
        or any(w["reference_amounts"] != [1] for w in review["native_2026_witnesses"])
    ):
        raise ValueError("Packaging basis, complete closure or native reference changed.")
    for model in review["models"]:
        if hashlib.sha256((root / model["path"]).read_bytes()).hexdigest() != model["sha256"]:
            raise ValueError("Public PE waste calculation changed.")
    for key, expected in review["recipe_dependencies"].items():
        if digest(resource["recipes"][key]) != expected:
            raise ValueError("An existing packaging dependency changed.")
    for key, omission in review["inherited_source_omissions"].items():
        if resource["recipes"][key].get("source_input_omissions") != omission:
            raise ValueError("Existing HDPE omission boundary changed.")
    parents = []
    for row in review["records"]:
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        if parent not in (row["original_rule"], row["compiled_rule"]):
            raise ValueError("Packaging parent changed before review.")
        if row["source_witness"]["reference_amounts"] != [1] or row["production_witness"]["reference_amounts"] != [1]:
            raise ValueError("Packaging source is not the inspected positive packed-product service.")
        profiles = row["compiled_rule"]["reference_flow_sign_review"]["profiles"]
        if (
            len(profiles) != 1
            or profiles[0]["source_profile"] != review["source_profile"]
            or profiles[0]["conversion_factor"] != 1
        ):
            raise ValueError("Packaging approval expanded beyond its exact profile.")
        if row["retention"]["factory_waste_rows"] != 8 or row["retention"]["market_freight_rows"] != 3:
            raise ValueError("Packaging source inputs changed.")
        parents.append((parent, row["compiled_rule"]))
    for key, recipe in review["recipes"].items():
        if key in resource["recipes"] and resource["recipes"][key] != recipe:
            raise ValueError("Packaging helper conflicts with existing content.")
    cards = {intern_review({"reviews": {}}, card): card for card in review["reviews"]}
    if any(rule["review_ref"] not in cards for _, rule in parents):
        raise ValueError("Packaging review identity changed.")
    for card in cards.values():
        intern_review(resource, card)
    resource["recipes"].update(deepcopy(review["recipes"]))
    for parent, compiled in parents:
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
    dispositions["omitted"] = len(resource["delete"])
    if resource["coverage"].get("unresolved_identities"):
        dispositions["unresolved"] = resource["coverage"]["unresolved_identities"]
    resource["coverage"]["dispositions"] = dict(dispositions)
