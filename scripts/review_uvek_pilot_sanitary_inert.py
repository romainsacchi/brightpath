"""Replay inspected sanitary-landfill services with negative waste references."""

import hashlib
import json
from pathlib import Path

if __package__:
    from scripts.review_uvek_pilot_materials import _apply_material_review
else:
    from review_uvek_pilot_materials import _apply_material_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_pilot_sanitary_inert_review.json"
REVIEW_SHA256 = "264913a5308cdb4d4c0a922f673bfad714f605c472cbff567c77f3ff5e3aabc6"


def apply_pilot_sanitary_inert_review(resource, root=ROOT):
    """Correct only inspected whole-service endpoints; preserve wet waste kg."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Pilot sanitary-inert evidence changed; repeat its review.")
    review = json.loads(raw)
    for authority in review["authorization_evidence"]:
        path = root / "brightpath/data" / authority["path"]
        if hashlib.sha256(path.read_bytes()).hexdigest() != authority["sha256"]:
            raise ValueError("Pilot sanitary-inert authority changed; repeat its review.")
    for decision in review["rules"]:
        if (
            decision["source_reference_amount"] != -1
            or decision["conversion_factor"] != -1
            or any(row["reference_amount"] != -1 for row in decision["profiles"])
            or any(row["reference_amount"] != 1 for row in decision["native_references"].values())
        ):
            raise ValueError("Pilot sanitary-inert reference changed.")
    _apply_material_review(resource, root, REVIEW_PATH, REVIEW_SHA256, mapping_method="reviewed_pilot_sanitary_inert")
