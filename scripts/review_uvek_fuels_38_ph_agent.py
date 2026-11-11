"""Replay exact allocated ph_agent stocks and their preserved final delivery."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_fuels_38_ph_agent_review.json"
REVIEW_SHA256 = "916b0b5c5eeec42c02c595bfe87e988171872e93d15b1eeed434c249804c1a31"


def apply_fuels_38_ph_agent_review(resource, root=ROOT):
    """Require exact positive ph_agent / nonempty negative treatment witnesses."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Soil-pH evidence changed; repeat its allocation review.")
    review = json.loads(raw)
    expected_profile = {"family": "ecoinvent", "version": "3.8", "system_model": "cutoff"}
    if (
        review["source_profile"] != expected_profile
        or len(review["records"]) != 4
        or review["source_allocation_summary"]
        != {
            "positive_product_suppliers": 3,
            "nominal_zero_deterministic_biosphere_rows": 75,
            "inherited_background_formula_rows": 50,
            "formulas_identical_to_negative_treatment": 50,
            "read_only_formula_matches_waste_values": 50,
            "case_alias_clarifications": 4,
            "source_inventory_or_parameters_modified": False,
        }
    ):
        raise ValueError("Soil-pH source allocation scope changed.")
    for rid, expected in review["recipe_dependencies"].items():
        actual = resource["recipes"].get(rid)
        digest = hashlib.sha256(
            json.dumps(actual, sort_keys=True, default=str, separators=(",", ":")).encode()
        ).hexdigest()
        if digest != expected:
            raise ValueError("Inherited lime delivery or mining helper changed.")
    inherited = review["inherited_lime_review"]
    if hashlib.sha256((root / "brightpath/data" / inherited["path"]).read_bytes()).hexdigest() != inherited["sha256"]:
        raise ValueError("Inherited lime quantity/allocation review changed.")
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
            or compiled["source"]["reference product"] != "soil pH raising agent, as CaCO3"
            or len(profiles) != 1
            or profiles[0]["source_profile"] != expected_profile
            or profiles[0]["conversion_factor"] != 1
            or profiles[0]["review"]["source_sign"] != 1
            or profiles[0]["review"]["target_sign"] != 1
        ):
            raise ValueError("Soil-pH parent, functional reference or exact profile changed.")
        if row["role"] == "allocated_ph_agent_stock":
            pair = row["cutoff_pair_witness"]
            if (
                pair["product_identity"] != compiled["source"]
                or pair["waste_reference_amounts"] != [-1]
                or pair["waste_nonzero_exchange_count"] <= 0
                or recipe.get("inputs") != []
                or recipe.get("zero_burden") is not True
                or not recipe.get("zero_burden_reason")
            ):
                raise ValueError("Soil-pH cut-off handover or treatment allocation changed.")
        else:
            if len(recipe["inputs"]) != 8 or sum("recipe" in e for e in recipe["inputs"]) != 4:
                raise ValueError("Soil-pH delivery composition changed.")
            for actual, retained in zip(recipe["inputs"], row["retained_rows"], strict=True):
                if actual != {**retained["statistics"], **retained["target"]}:
                    raise ValueError("Soil-pH shares, freight or uncertainty changed.")
        pending.append((parent, compiled))
    for witness in review["native_2026_witnesses"]:
        if witness["reference_amount"] != 1 or witness["nonproduction_count"] <= 0:
            raise ValueError("Native ph_agent-delivery freight changed.")
    for rid, recipe in review["recipes"].items():
        if rid in resource["recipes"] and resource["recipes"][rid] != recipe:
            raise ValueError("Generated ph_agent helper changed.")
    for card in review["reviews"]:
        key = intern_review({}, card)
        if key in resource.get("reviews", {}) and resource["reviews"][key] != card:
            raise ValueError("Soil-pH allocation review changed.")
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
