"""Replay inspected native grid and converter-steel supplier corrections."""

from pathlib import Path

if __package__:
    from scripts.review_uvek_pilot_materials import _apply_material_review
else:
    from review_uvek_pilot_materials import _apply_material_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_pilot_grid_converter_review.json"
REVIEW_SHA256 = "143a88f8b6a9a4b0f89549c13cd73a54b8b1a260af31f505a09621144af38a2b"


def apply_pilot_grid_converter_review(resource, root=ROOT):
    """Retain complete delivered-grid and unrolled-converter process boundaries.

    Only the enumerated cut-off profiles acquire verified reference reviews;
    other profiles keep the common authoring helper's unresolved default.
    """
    _apply_material_review(resource, root, REVIEW_PATH, REVIEW_SHA256)
