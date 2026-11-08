"""Compose complete processing inventories from already resolved directional components."""

import json
from pathlib import Path

try:
    from review_uvek_market_compositions import composition_decision, composition_recipe, validate_composition_bindings
except ModuleNotFoundError:
    from scripts.review_uvek_market_compositions import (
        composition_decision,
        composition_recipe,
        validate_composition_bindings,
    )

FIELDS = ("name", "reference product", "location", "unit")
REVIEW_PATH = "brightpath/data/export/uvek_processing_composition_review.json"
APPROVAL = json.loads((Path(__file__).resolve().parents[1] / REVIEW_PATH).read_text())
PROCESSING = {tuple(r["source"][f] for f in FIELDS): r for r in APPROVAL["rules"]}
LIMITATIONS = (
    "Reviewed processing-inventory composition: retain all inspected active technosphere inputs through "
    "the already resolved directional component correspondences for this exact profile. The source has "
    "one unit-magnitude self-linked production reference, no active direct biosphere exchanges and no "
    "unsupported exchange types. Retain processing inputs, freight and genuine recovered-product credits; "
    "apply each provider's reference-sign factor independently, distinguishing treatment demand from "
    "negative product credits. No component omission, empty allocation, new technology primitive or "
    "missing-provider fallback is used. The inherited component correspondences approximate technology, "
    "material composition, geography and allocation; resolved signs do not establish scientific or LCIA "
    "equivalence. Internal component uncertainty and parameter formulas are not reconstructed. Preserve "
    "caller-owned signed quantities, supported uncertainty, formulas, foreground suppliers, production "
    "and separately listed emissions, including full original-exchange change records. Net impacts may "
    "legitimately be negative where recovery credits exceed processing burdens. Score and export all "
    "returned helpers. Only enumerated exact profiles are approved; no reverse mapping is inferred."
)


def processing_recipe(source, profile):
    return composition_recipe(
        source, profile, rationale=LIMITATIONS, review_path=REVIEW_PATH, namespace="processing-composition"
    )


def validate_processing_bindings(resource):
    validate_composition_bindings(resource, approval=APPROVAL, label="Processing composition")


def processing_decision(rule, observations, inventories, binding, original_review):
    source = tuple(rule["source"][f] for f in FIELDS)
    return composition_decision(
        rule,
        observations,
        inventories,
        binding,
        original_review,
        approval_row=PROCESSING[source],
        rationale=LIMITATIONS,
        review_path=REVIEW_PATH,
        namespace="processing-composition",
        mapping_method="reviewed_processing_composition",
        profile_reason="Select an exact inspected processing-inventory profile; no unreviewed component or profile fallback is approved.",
    )
