"""Replay inspected crop-purpose, country and final-delivery correspondences."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_fuels_38_crops_review.json"
REVIEW_SHA256 = "21a5bba439cad665c9fe97e39c52d0b58c3623d3554f9a385190d29f6aa6f1da"


def apply_fuels_38_crops_review(resource, root=ROOT):
    """Retain fresh-kg seed purpose and every original market entry."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Crop evidence changed; repeat its reference-basis review.")
    review = json.loads(raw)
    profile = {"family": "ecoinvent", "version": "3.8", "system_model": "cutoff"}
    if review["source_profile"] != profile or len(review["records"]) != 2:
        raise ValueError("Crop source scope changed.")
    pending = []
    for row in review["records"]:
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        compiled = row["compiled_rule"]
        if (
            parent not in (row["original_rule"], compiled)
            or row["source_witness"]["reference_amounts"] != [1]
            or compiled["source"] != row["source_witness"]["identity"]
        ):
            raise ValueError("Crop parent or positive fresh-kg reference changed.")
        if row["resource_action"] == "unchanged_rule":
            if (
                compiled != row["original_rule"]
                or compiled["target"]["name"] != "Sugar beet seed IP, at regional storehouse"
            ):
                raise ValueError("Complete propagation-seed stock changed.")
        else:
            entries = compiled["reference_flow_sign_review"]["profiles"]
            recipe = review["recipes"][compiled["target"]["recipe_ref"]]
            if (
                len(entries) != 1
                or entries[0]["source_profile"] != profile
                or entries[0]["conversion_factor"] != 1
                or entries[0]["review"]["source_sign"] != 1
                or entries[0]["review"]["target_sign"] != 1
                or len(recipe["inputs"]) != 10
                or len(row["source_producer_witnesses"]) != 6
                or sum(p["geographic_proxy"] for p in row["source_producer_witnesses"]) != 3
            ):
                raise ValueError("Harvested crop profile, producer shares or delivery changed.")
            for actual, retained in zip(recipe["inputs"], row["retained_rows"], strict=True):
                if actual != {**retained["statistics"], "type": "technosphere", **retained["target"]}:
                    raise ValueError("Crop country entry, freight or uncertainty changed.")
        pending.append((parent, compiled))
    for witness in review["native_2026_witnesses"]:
        if witness["reference_amount"] != 1 or witness["nonproduction_count"] <= 0:
            raise ValueError("Native crop/freight reference changed.")
    for rid, recipe in review["recipes"].items():
        if rid in resource["recipes"] and resource["recipes"][rid] != recipe:
            raise ValueError("Derived crop delivery changed.")
    for card in review["reviews"]:
        key = intern_review({}, card)
        if key in resource.get("reviews", {}) and resource["reviews"][key] != card:
            raise ValueError("Crop proxy review changed.")
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
