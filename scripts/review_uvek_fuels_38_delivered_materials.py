"""Retain inspected LDPE-film and zeolite market delivery entries."""

import hashlib
import json
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_fuels_38_delivered_materials_review.json"
REVIEW_SHA256 = "357ad62ef92e5a5627e2c326b2f76ea40ec0a612a02fde761cf1edb62c7096c0"


def apply_fuels_38_delivered_materials_review(resource, root=ROOT):
    """Add two exact-profile helpers without altering older approved targets."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Delivered-material evidence changed.")
    review = json.loads(raw)
    if len(review["records"]) != 2 or len(review["recipes"]) != 2:
        raise ValueError("Delivered-material scope changed.")
    previous = review["previous_39_zeolite_approval"]
    if (
        hashlib.sha256((root / "brightpath/data" / previous["review_path"]).read_bytes()).hexdigest()
        != previous["review_sha256"]
    ):
        raise ValueError("Previous zeolite review changed.")
    pending = []
    for row in review["records"]:
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        if parent not in (row["original_rule"], row["compiled_rule"]):
            raise ValueError("Delivered-material parent changed.")
        if row["source_witness"]["reference_amounts"] != [1] or any(
            w["reference_amounts"] != [1] for w in row["source_producer_witnesses"]
        ):
            raise ValueError("Material kg reference changed.")
        entry = row["compiled_rule"]["reference_flow_sign_review"]["profiles"][0]
        if (
            entry["source_profile"] != {"family": "ecoinvent", "version": "3.8", "system_model": "cutoff"}
            or entry["target"] != {"recipe_ref": row["recipe_id"]}
            or entry["conversion_factor"] != 1
            or entry["review"]["source_sign"] != 1
            or entry["review"]["target_sign"] != 1
        ):
            raise ValueError("Delivered-material profile, sign or scale changed.")
        recipe = review["recipes"][row["recipe_id"]]
        if len(recipe["inputs"]) != 6 or sum(r["role"] == "producer" for r in row["retained_rows"]) != 2:
            raise ValueError("Material producer or freight entry count changed.")
        for actual, retained in zip(recipe["inputs"], row["retained_rows"], strict=True):
            if actual != {**retained["statistics"], "type": "technosphere", **retained["target"]}:
                raise ValueError("Material delivery quantity or uncertainty changed.")
        if row["rule_id"] == previous["rule_id"]:
            old = row["compiled_rule"]["reference_flow_sign_review"]["profiles"][1]
            if (
                old["source_profile"] != previous["scopes"][0]["source"]
                or old["conversion_factor"] != 1
                or old["review"]["reason"] != previous["inspection"]["boundary"]
                or row["compiled_rule"]["target"] != row["original_rule"]["target"]
            ):
                raise ValueError("Older approved zeolite decision changed.")
        pending.append((parent, row["compiled_rule"]))
    for witness in review["native_2026_witnesses"]:
        if witness["reference_amount"] != 1 or witness["nonproduction_count"] <= 0:
            raise ValueError("Native material/freight reference changed.")
    for rid, recipe in review["recipes"].items():
        if rid in resource["recipes"] and resource["recipes"][rid] != recipe:
            raise ValueError("Delivered-material helper changed.")
    for card in review["reviews"]:
        intern_review(resource, card)
    resource["recipes"].update(deepcopy(review["recipes"]))
    for parent, compiled in pending:
        parent.clear()
        parent.update(deepcopy(compiled))
