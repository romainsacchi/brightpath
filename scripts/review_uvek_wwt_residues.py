"""Replay exact industrial WWT-residue omissions under standing scientific approval."""

import json
from copy import deepcopy
from pathlib import Path

FIELDS = ("name", "reference product", "location", "unit")
REVIEW_PATH = "brightpath/data/export/uvek_wwt_residue_omission_review.json"
APPROVAL = json.loads((Path(__file__).resolve().parents[1] / REVIEW_PATH).read_text())
APPROVED_WWT_RESIDUE_OMISSIONS = {}
for row in APPROVAL["rules"]:
    source = tuple(row["source"][f] for f in FIELDS)
    if (
        row["approval_basis"] != "unsupported-nonbiogenic-industrial-wwt-residue"
        or source in APPROVED_WWT_RESIDUE_OMISSIONS
    ):
        raise ValueError("Unrecognized or duplicate industrial WWT-residue scientific approval.")
    versions = {p["label"].split("-")[1] for p in row["profiles"]}
    APPROVED_WWT_RESIDUE_OMISSIONS[source] = {
        "source_versions": tuple(sorted(versions, key=lambda v: tuple(int(p) for p in v.split(".")))),
        "previous_target": tuple(row["previous_target"][f] for f in FIELDS),
        "previous_native_flow_type": row["previous_native_flow_type"],
        "profiles": deepcopy(row["profiles"]),
        "review": deepcopy(row["review"]),
    }
if (
    len(APPROVED_WWT_RESIDUE_OMISSIONS) != APPROVAL["source_count"]
    or sum(len(a["profiles"]) for a in APPROVED_WWT_RESIDUE_OMISSIONS.values()) != APPROVAL["source_profile_count"]
):
    raise ValueError("Industrial WWT-residue approval scope disagrees with its sparse evidence.")


def validate_wwt_residue_inventories(source, observations, inventories):
    """Require all inspected negative-reference profiles and their unchanged inventories."""
    expected = sorted((p["label"], p["inventory_sha256"]) for p in APPROVED_WWT_RESIDUE_OMISSIONS[source]["profiles"])
    if sorted(observations) != [(label, -1) for label, _ in expected] or sorted(inventories) != expected:
        raise ValueError("Industrial WWT-residue inventory or profile changed; repeat the scientific review.")
