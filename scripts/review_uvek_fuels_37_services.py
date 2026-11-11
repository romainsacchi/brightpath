"""Replay the reviewed 3.7 compressed-air network boundary and service proxies."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_fuels_37_services_review.json"
REVIEW_SHA256 = "9a98471242e311d4c3f6f46247205140c1e99970fd6accf4b5d816cbac24b2e3"


def apply_fuels_37_services_review(resource, root=ROOT):
    """Keep service references and reject compressor-only market reconstruction."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("3.7 service evidence changed.")
    review = json.loads(raw)
    if len(review["records"]) != 4 or len(review["recipes"]) != 1:
        raise ValueError("3.7 service scope changed.")
    pending = []
    for row in review["records"]:
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        if parent not in (row["original_rule"], row["compiled_rule"]):
            raise ValueError("3.7 service parent changed.")
        if (
            row["source_profile"] != {"family": "ecoinvent", "version": "3.7", "system_model": "cutoff"}
            or row["source_witness"]["inspected_profile"]["version"] != "3.7.1"
            or row["source_witness"]["reference_amounts"] != [1]
            or any(w["reference_amounts"] != [1] for w in row["source_producer_witnesses"])
        ):
            raise ValueError("Requested/inspected source or reference changed.")
        if row["resource_action"] == "unchanged_rule":
            if row["compiled_rule"] != row["original_rule"]:
                raise ValueError("Unchanged native service changed.")
        else:
            entry = row["compiled_rule"]["reference_flow_sign_review"]["profiles"]
            if (
                len(entry) != 1
                or entry[0]["source_profile"] != row["source_profile"]
                or entry[0]["conversion_factor"] != 1
                or entry[0]["review"]["source_sign"] != 1
                or entry[0]["review"]["target_sign"] != 1
                or entry[0]["target"] != {"recipe_ref": "fuels-37-compressed-air-complete-network-mix"}
            ):
                raise ValueError("Compressed-air profile or sign changed.")
        pending.append((parent, row["compiled_rule"]))
    recipe = review["recipes"]["fuels-37-compressed-air-complete-network-mix"]
    if len(recipe["inputs"]) != 3 or {r["kind"] for r in review["network_boundary_checks"]} != {
        "average",
        "best",
        "optimised",
    }:
        raise ValueError("Complete compressed-air technology split changed.")
    for edge, proof in zip(recipe["inputs"], review["network_boundary_checks"], strict=True):
        if (
            edge["amount"] != proof["inspected_patch_share"]
            or edge["amount"] != 0.333333333333333
            or edge["unit"] != "cubic meter"
            or "at supply network" not in edge["name"]
            or any(edge.get(k) != v for k, v in proof["identity"].items())
        ):
            raise ValueError("Compressed-air network boundary or share changed.")
    for witness in review["native_2026_witnesses"]:
        if witness["reference_amount"] != 1 or witness["nonproduction_count"] <= 0:
            raise ValueError("Native service reference changed.")
    for key, value in review["recipes"].items():
        if key in resource["recipes"] and resource["recipes"][key] != value:
            raise ValueError("Complete network helper changed.")
    for card in review["reviews"]:
        intern_review(resource, card)
    resource["recipes"].update(deepcopy(review["recipes"]))
    for parent, compiled in pending:
        parent.clear()
        parent.update(deepcopy(compiled))
    dispositions = Counter(
        "recipe" if "recipe_ref" in r["target"] else r.get("classification", "unclassified")
        for r in resource["replace"]
    )
    dispositions["omitted"] = len(resource["delete"])
    if resource["coverage"].get("unresolved_identities"):
        dispositions["unresolved"] = resource["coverage"]["unresolved_identities"]
    resource["coverage"]["dispositions"] = dict(dispositions)
