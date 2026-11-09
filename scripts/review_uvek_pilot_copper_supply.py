"""Replay the inspected generic-copper market's complete supply correction."""

from pathlib import Path

if __package__:
    from scripts.review_uvek_pilot_materials import _apply_material_review
else:
    from review_uvek_pilot_materials import _apply_material_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_pilot_metal_chemical_supply_review.json"
REVIEW_SHA256 = "d33b402f9e329da4cecb30bf72b34aaac0d528df6b743ca8783bc11f13a26488"


def apply_pilot_copper_supply_review(resource, root=ROOT):
    """Use the existing primary/secondary supply only for inspected 3.6 copper.

    The evidence also inspects two unchanged suppliers. Only its ``rules``
    section authors a raw correction; the other exact approvals stay in Premise.
    """
    _apply_material_review(resource, root, REVIEW_PATH, REVIEW_SHA256)
