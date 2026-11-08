"""Replay the explicitly approved small-generator fuel-reference normalization."""

import hashlib
import json
import math
from pathlib import Path

if __package__:
    from scripts.review_uvek_pilot_materials import _apply_material_review
else:
    from review_uvek_pilot_materials import _apply_material_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_pilot_generator_fuel_review.json"
REVIEW_SHA256 = "acbf38949d74bc6472f20ea8127343768499950090e078ba3f0498d558610cee"


def apply_pilot_generator_fuel_review(resource, root=ROOT):
    """Preserve audited diesel draw only for the inspected 3.6 cut-off service.

    The common positive-reference authoring helper also handles reviewed
    quantity bases. It keeps an unresolved default and the exact profile gate.
    """
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Pilot generator evidence changed; repeat its review.")
    (decision,) = json.loads(raw)["rules"]
    rule = next((r for r in resource["replace"] if r["id"] == decision["rule_id"]), None)
    if (
        rule is None
        or rule.get("conversion_factor", 1)
        not in (decision["previous_conversion_factor"], decision["conversion_factor"])
        or decision["source_reference_unit"] != "megajoule"
        or decision["native_reference_unit"] != "megajoule"
        or not math.isclose(
            decision["conversion_factor"] * decision["native_diesel_kg_per_reference"],
            decision["source_diesel_kg_per_reference"],
            rel_tol=1e-15,
        )
        or set(decision["native_references"]) != {"2025", "2026"}
        or any(
            r["reference_amount"] != 1 or r["diesel_kg_per_reference"] != decision["native_diesel_kg_per_reference"]
            for r in decision["native_references"].values()
        )
    ):
        raise ValueError("Pilot generator reference or fuel coordinate changed.")
    _apply_material_review(resource, root, REVIEW_PATH, REVIEW_SHA256)
