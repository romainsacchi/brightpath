"""Replay the inspected positive secondary-aluminium billet correction."""

from pathlib import Path

if __package__:
    from scripts.review_uvek_pilot_materials import _apply_material_review
else:
    from review_uvek_pilot_materials import _apply_material_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_pilot_secondary_aluminium_pcb_review.json"
REVIEW_SHA256 = "10fdcbd22d705300ac0dc2e8929e1b533240158ef2098e534b02d66a6f6813b8"


def apply_pilot_secondary_aluminium_review(resource, root=ROOT):
    """Use old-scrap secondary billets only for the inspected 3.6 source.

    The evidence also inspects an unchanged PCB preparation service. Only its
    ``rules`` section authors a correction; exact admissions remain in Premise.
    """
    _apply_material_review(resource, root, REVIEW_PATH, REVIEW_SHA256)
