"""Replay exact allocated compost stocks and their preserved final delivery."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_fuels_38_compost_review.json"
REVIEW_SHA256 = "fea6033df8a91f20e7f4cbcc99c0d8620938a136f4d51b45d1e80dd5119ef793"


def apply_fuels_38_compost_review(resource, root=ROOT):
    """Require exact positive compost / nonempty negative treatment witnesses."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Compost evidence changed; repeat its allocation review.")
    review = json.loads(raw)
    expected_profile = {"family": "ecoinvent", "version": "3.8", "system_model": "cutoff"}
    if (
        review["source_profile"] != expected_profile
        or len(review["records"]) != 7
        or review["literal_zero_summary"]
        != {
            "producer_count": 6,
            "biosphere_rows": 44,
            "amount": 0,
            "loc": 0,
            "uncertainty_type": 0,
            "nonzero_uncertainty_or_formula_rows": 0,
            "negative_nonempty_treatment_pairs": 6,
        }
    ):
        raise ValueError("Compost source allocation scope changed.")
    pending = []
    for row in review["records"]:
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        compiled = row["compiled_rule"]
        profiles = compiled["reference_flow_sign_review"]["profiles"]
        recipe = review["recipes"][compiled["target"]["recipe_ref"]]
        if (
            parent not in (row["original_rule"], compiled)
            or row["source_witness"]["reference_amounts"] != [1]
            or compiled["source"] != row["source_witness"]["identity"]
            or compiled["source"]["reference product"] != "compost"
            or len(profiles) != 1
            or profiles[0]["source_profile"] != expected_profile
            or profiles[0]["conversion_factor"] != 1
            or profiles[0]["review"]["source_sign"] != 1
            or profiles[0]["review"]["target_sign"] != 1
        ):
            raise ValueError("Compost parent, functional reference or exact profile changed.")
        if row["role"] == "allocated_compost_stock":
            pair = row["cutoff_pair_witness"]
            if (
                pair["compost_identity"] != compiled["source"]
                or pair["waste_reference_amounts"] != [-1]
                or pair["waste_nonzero_exchange_count"] <= 0
                or recipe.get("inputs") != []
                or recipe.get("zero_burden") is not True
                or not recipe.get("zero_burden_reason")
            ):
                raise ValueError("Compost cut-off handover or treatment allocation changed.")
        else:
            if len(recipe["inputs"]) != 8 or sum("recipe" in e for e in recipe["inputs"]) != 6:
                raise ValueError("Compost delivery composition changed.")
            for actual, retained in zip(recipe["inputs"], row["retained_rows"], strict=True):
                if actual != {**retained["statistics"], **retained["target"]}:
                    raise ValueError("Compost shares, freight or uncertainty changed.")
        pending.append((parent, compiled))
    for witness in review["native_2026_witnesses"]:
        if witness["reference_amount"] != 1 or witness["nonproduction_count"] <= 0:
            raise ValueError("Native compost-delivery freight changed.")
    for rid, recipe in review["recipes"].items():
        if rid in resource["recipes"] and resource["recipes"][rid] != recipe:
            raise ValueError("Generated compost helper changed.")
    for card in review["reviews"]:
        key = intern_review({}, card)
        if key in resource.get("reviews", {}) and resource["reviews"][key] != card:
            raise ValueError("Compost allocation review changed.")
    resource["recipes"].update(deepcopy(review["recipes"]))
    for card in review["reviews"]:
        intern_review(resource, card)
    for parent, compiled in pending:
        parent.clear()
        parent.update(deepcopy(compiled))
    resource["coverage"]["mapping_methods"] = dict(
        sorted(Counter(r["mapping_method"] for r in resource["replace"]).items())
    )
    dispositions = Counter(
        "recipe" if "recipe_ref" in r["target"] else r.get("classification", "unclassified")
        for r in resource["replace"]
    )
    dispositions["omitted"] = len(resource["delete"])
    if resource["coverage"].get("unresolved_identities"):
        dispositions["unresolved"] = resource["coverage"]["unresolved_identities"]
    resource["coverage"]["dispositions"] = dict(dispositions)
