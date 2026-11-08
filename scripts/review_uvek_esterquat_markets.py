"""Bind independently inspected esterquat suppliers to their feedstock-specific market review."""

import json
from collections import defaultdict
from pathlib import Path

FIELDS = ("name", "reference product", "location", "unit")
REVIEW_PATH = "brightpath/data/export/uvek_esterquat_market_review.json"
APPROVAL = json.loads((Path(__file__).resolve().parents[1] / REVIEW_PATH).read_text())
EVIDENCE_SCOPE = defaultdict(set)
for row in APPROVAL["provider_inventories"]:
    EVIDENCE_SCOPE[tuple(row["label"].split("-"))].add(tuple(row["source"][f] for f in FIELDS))
if (
    len(APPROVAL["provider_inventories"]) != APPROVAL["provider_inventory_count"]
    or APPROVAL["provider_inventory_count"] != 44
    or APPROVAL["source_count"] != 2
    or APPROVAL["source_profile_count"] != 28
):
    raise ValueError("Esterquat market supplier review scope changed; repeat the independent inspection.")


def validate_provider_inventories(inventories, observations, native):
    """Reject changed, missing or duplicated suppliers and non-product references."""
    expected = defaultdict(list)
    for row in APPROVAL["provider_inventories"]:
        key = tuple(row["source"][f] for f in FIELDS)
        expected[key].append((row["label"], row["inventory_sha256"]))
        family = row["label"].split("-")[0]
        actual = {sign for label, sign in observations[family].get(key, set()) if label == row["label"]}
        if row["reference_amount"] != 1 or actual != {1}:
            raise ValueError("Esterquat provider product reference changed; repeat the independent review.")
        if family == "uvek":
            entry = native.get(key, {})
            if entry.get("native_flow_type") != "PRODUCT_FLOW" or entry.get("native_reference_is_input") is not False:
                raise ValueError("Esterquat UVEK provider is no longer the inspected product output.")
    if set(inventories) != set(expected) or any(
        sorted(inventories[key]) != sorted(rows) for key, rows in expected.items()
    ):
        raise ValueError("Esterquat provider inventory changed; repeat the independent feedstock review.")
