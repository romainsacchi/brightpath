"""Preserve independently inspected, production-only allocated cut-off stock supply."""

import hashlib
import json
from copy import deepcopy
from pathlib import Path

FIELDS = ("name", "reference product", "location", "unit")
REVIEW_PATH = "brightpath/data/export/uvek_cutoff_stock_review.json"
APPROVAL = json.loads((Path(__file__).resolve().parents[1] / REVIEW_PATH).read_text())
STOCKS = {tuple(row["source"][f] for f in FIELDS): row for row in APPROVAL["rules"]}
if (
    len(STOCKS) != APPROVAL["source_count"]
    or sum(len(row["profiles"]) for row in STOCKS.values()) != APPROVAL["source_profile_count"]
):
    raise ValueError("Cut-off stock scope differs from its independently inspected evidence.")
LIMITATIONS = (
    "Reviewed production-only cut-off stock supply: the exact source supplier is explicitly marked "
    "Recycled Content cut-off and contains only self-linked production +1, with no processing, freight, "
    "biosphere or other exchanges. Preserve this observed allocation boundary through a production-only "
    "foreground supplier. Do not add the former UVEK production/disposal supplier's burdens, avoided "
    "primary-material credits or biogenic uptake. This is not a claim of zero life-cycle impact for "
    "actual recycling or treatment; those processes belong outside this allocated stock and remain "
    "separate. No uncharacterized or missing inventory is assigned zero. Preserve signed material "
    "quantities, uncertainty, formulas, foreground suppliers, production and separately listed emissions "
    "and credits. Only the enumerated exact positive-reference cut-off profiles are approved; other "
    "versions, patch releases, consequential supplies and negative-reference empty treatment stages "
    "remain outside this approval. Export and score all returned foreground helpers."
)


def stock_recipe(source, *, review_path=REVIEW_PATH):
    """Represent the known allocation boundary without inventing an external target process."""
    digest = hashlib.sha256(json.dumps(source).encode()).hexdigest()[:20]
    return f"cutoff-stock-{digest}", {
        "name": f"Allocated cut-off stock, {source[1]}, {source[2]}",
        "reference product": source[1],
        "location": source[2],
        "unit": source[3],
        "inputs": [],
        "zero_burden": True,
        "zero_burden_reason": LIMITATIONS,
        "rationale": LIMITATIONS,
        "evidence": [f"{review_path}: exact positive cut-off production-only supplier and inventory fingerprints."],
    }


def stock_decision(
    rule, observations, inventories, binding, original_review, *, approval_row=None, review_path=REVIEW_PATH
):
    """Require every available source profile to match its positive cut-off allocation evidence."""
    source = tuple(rule["source"][f] for f in FIELDS)
    approval = STOCKS[source] if approval_row is None else approval_row
    expected = sorted((p["label"], p["inventory_sha256"]) for p in approval["profiles"])
    actual = sorted((p["label"], p["inventory_sha256"]) for p in inventories)
    if (
        "Recycled Content cut-off" not in source[0]
        or source != tuple(approval["source"][f] for f in FIELDS)
        or rule["target"] != approval["previous_target"]
        or rule.get("conversion_factor", 1) != 1
        or sorted(observations) != [(label, 1) for label, _ in expected]
        or actual != expected
        or any(
            not p["label"].endswith("-cutoff")
            or p.get("total_exchanges") != 1
            or p.get("only_self_production") is not True
            or p.get("production_amount") != 1
            for p in inventories
        )
    ):
        raise ValueError(
            "Cut-off stock supplier, profile or allocation inventory changed; repeat the scientific review."
        )
    recipe_id, recipe = stock_recipe(source, review_path=review_path)
    profiles = [
        {
            "source_profile": dict(zip(("family", "version", "system_model"), label.split("-"), strict=True)),
            "target": {"recipe_ref": recipe_id},
            "conversion_factor": 1,
            "review": {
                "status": "verified",
                "source_sign": 1,
                "target_sign": 1,
                "reason": LIMITATIONS,
                "evidence": [f"{review_path}#{label}/{digest}"],
            },
        }
        for label, digest in expected
    ]
    return {
        "operation": "recipe",
        "binding": binding,
        "original_rule": deepcopy(rule),
        "original_review": deepcopy(original_review),
        "original_factor": 1,
        "factor": 1,
        "source_observations": [list(row) for row in sorted(observations)],
        "uvek_reference_sign": 1,
        "uvek_native_flow_type": "GENERATED_PRODUCT",
        "original_native_flow_type": approval["previous_native_flow_type"],
        "target": {"recipe_ref": recipe_id},
        "recipes": {recipe_id: recipe},
        "recipe_review": {"rationale": LIMITATIONS, "limitations": [LIMITATIONS], "evidence": [review_path]},
        "mapping_method": "reviewed_cutoff_stock_recipe",
        "review": {
            "status": "unresolved",
            "reason": "Select only an exact inspected positive cut-off stock profile; no other allocation or treatment context is approved.",
            "evidence": [review_path],
            "profiles": profiles,
        },
    }


def zero_supplier_binding(rule, source_profile, recipe_id):
    """Bind a dependency to the exact already approved direct stock correspondence."""
    entry = next(
        (
            p
            for p in rule.get("reference_flow_sign_review", {}).get("profiles", [])
            if p["source_profile"] == source_profile
        ),
        None,
    )
    if (
        entry is None
        or source_profile.get("family") != "ecoinvent"
        or source_profile.get("system_model") != "cutoff"
        or entry["review"].get("status") != "verified"
        or entry["review"].get("source_sign") != 1
        or entry["review"].get("target_sign") != 1
        or entry.get("conversion_factor") != 1
        or entry.get("target", rule["target"]).get("recipe_ref") != recipe_id
        or entry.get("operation")
        or not rule.get("id")
    ):
        raise ValueError("Zero stock dependency has no exact positive cut-off source approval.")
    return {
        "rule_id": rule["id"],
        "source_profile": deepcopy(source_profile),
        "profile_review_sha256": hashlib.sha256(json.dumps(entry, sort_keys=True).encode()).hexdigest(),
    }
