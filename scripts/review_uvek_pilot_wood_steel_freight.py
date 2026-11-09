"""Replay closer native suppliers for inspected wood, steel and heavy freight."""

from pathlib import Path

if __package__:
    from scripts.review_uvek_pilot_materials import _apply_material_review
else:
    from review_uvek_pilot_materials import _apply_material_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_pilot_wood_steel_freight_review.json"
REVIEW_SHA256 = "c51ffd9cf18176c5e1bca75460edf6748bbbd555e78c0eae037bcd348da5ba2f"


def apply_pilot_wood_steel_freight_review(resource, root=ROOT):
    """Preserve dry-mass kg, cast EAF steel kg and complete heavy-freight tkm.

    Only the enumerated cut-off profiles acquire verified reference reviews.
    The common authoring helper keeps other source profiles unresolved.
    """
    _apply_material_review(resource, root, REVIEW_PATH, REVIEW_SHA256)
