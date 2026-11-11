"""Extend the immutable canonical baseline with one reviewed same-CAS label."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import (
    dumps_migration_resource,
    normalize_migration_resource,
    validate_migration_resource,
)

REVIEW_PATH = "brightpath/data/export/uvek_quizalofop_synonym_review.json"
BASELINE_SHA256 = "585b86f001109450d0be3f95929cc1cfb9512706a38b5e012493cf2d2fa69c5a"


def build_extended_biosphere_resource(baseline):
    """Authorize only the inspected 3.8 chemical identity; retain other refusals."""
    root = Path(__file__).resolve().parents[1]
    baseline_bytes = dumps_migration_resource(normalize_migration_resource(baseline)).encode()
    if hashlib.sha256(baseline_bytes).hexdigest() != BASELINE_SHA256:
        raise ValueError("Canonical baseline changed; repeat the synonym review.")
    raw_review = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw_review).hexdigest() != "b1ddd0262b44efa5a380144ac60207c2ce0af7594d1799ca40c58a04cd7ea281":
        raise ValueError("Chemical identity evidence changed; repeat the review.")
    review = json.loads(raw_review)
    if (
        review["source_cas"] != review["target_cas"]
        or review["source_cas"] != "76578-14-8"
        or review["source_profile"] != {"family": "ecoinvent", "version": "3.8"}
        or review["source"]["unit"] != review["target"]["unit"]
        or review["source"]["categories"] != review["target"]["categories"]
    ):
        raise ValueError("Chemical identity or receiving context changed.")
    catalog = json.loads((root / "brightpath/data/export/reference_catalogs/ecoinvent__3.10__cutoff.json").read_text())
    target = next(r for r in catalog["biosphere"] if r["flow_id"] == review["target"]["flow_id"])
    if any(target.get(k) != v for k, v in review["target"].items()):
        raise ValueError("Registered target identity changed.")
    result = deepcopy(baseline)
    rows = [r for r in result["unresolved"] if r["id"] == review["rule_id"]]
    if rows != [review["original_unresolved_row"]]:
        raise ValueError("Reviewed unresolved identity changed.")
    row = rows[0]
    decision = next(p for p in row["profiles"] if p["source_profile"] == review["source_profile"])
    if decision["source_flow_ids"] != [review["source_flow_id"]]:
        raise ValueError("Source flow ID changed.")
    decision.pop("reason")
    decision.update(
        status="verified",
        identity_kind="reviewed_publisher_synonym",
        target=review["target"],
        source_cas=review["source_cas"],
        target_cas=review["target_cas"],
        evidence=REVIEW_PATH,
    )
    result["unresolved"].remove(row)
    result["replace"].append(
        {
            "id": row["id"],
            "source": row["source"],
            "target": review["target"],
            "mapping_method": "publisher_flow_identity",
            "confidence": 1.0,
            "biosphere_identity_review": {"status": "verified", "reason": review["scope"], "profiles": row["profiles"]},
        }
    )
    result["coverage"].update(
        mapped_identities=len(result["replace"]),
        unresolved_identities=len(result["unresolved"]),
        mapping_methods=dict(sorted(Counter(r["mapping_method"] for r in result["replace"]).items())),
    )
    result["supplemental_identity_reviews"] = [REVIEW_PATH]
    validate_migration_resource(result)
    return result
