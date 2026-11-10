"""Replay two inspected cut-off 3.9 treatment decisions without runtime adapters."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_cement_treatment_39_review.json"
REVIEW_SHA256 = "b6c424b3b8c378e24b6541b599cbf52619b3cd7e7489ee8f5fda61fa8482ad45"


def apply_cement_treatment_39_review(resource, root=ROOT):
    """Check all evidence and prior decisions before changing either parent."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Cement treatment evidence changed; repeat its review.")
    review = json.loads(raw)
    for authority in review["authorization_evidence"]:
        if (
            hashlib.sha256((root / "brightpath/data" / authority["path"]).read_bytes()).hexdigest()
            != authority["sha256"]
        ):
            raise ValueError("Cement treatment authority changed; repeat its review.")
    profile = {"family": "ecoinvent", "version": "3.9", "system_model": "cutoff"}
    pe, concrete = review["polyethylene"], review["concrete"]
    selected = {}
    for record in (pe, concrete):
        rule = next(r for r in resource["replace"] if r["id"] == record["rule_id"])
        if rule not in (record["original_rule"], record["compiled_rule"]):
            raise ValueError("Cement treatment parent changed; repeat its review.")
        selected[record["rule_id"]] = rule
    if (
        review["source_profile"] != profile
        or pe["matching_composition_coordinates"] != 44
        or pe["source_witness"]["reference_amounts"] != [-1]
        or pe["native_2026_witness"]["reference_amounts"] != [1]
        or concrete["source_witness"]["reference_amounts"] != [-1]
        or any(
            row["reference_amount"] != 1 or row["nonproduction_count"] <= 0 for row in concrete["native_2026_witnesses"]
        )
        or any(not row["single_deterministic_one_supplier"] for row in concrete["source_market_witnesses"])
    ):
        raise ValueError("Cement treatment reference or service boundary changed.")
    previous = pe["original_rule"]["reference_flow_sign_review"]["profiles"]
    current = pe["compiled_rule"]["reference_flow_sign_review"]["profiles"]
    if current[:-1] != previous or current[-1]["source_profile"] != profile:
        raise ValueError("Previously approved polyethylene profile changed.")
    inputs = concrete["recipe"]["inputs"]
    if (
        len(inputs) != 2
        or sorted(e["amount"] for e in inputs) != [1.48e-9, 1.0]
        or {e["unit"] for e in inputs} != {"unit", "kilogram"}
        or any(e["uncertainty type"] != 2 or e["scale"] <= 0 for e in inputs)
    ):
        raise ValueError("Concrete landfill coefficients or uncertainty changed.")
    rid = concrete["recipe_id"]
    if rid in resource["recipes"] and resource["recipes"][rid] != concrete["recipe"]:
        raise ValueError("Concrete landfill helper changed; repeat its review.")
    # Validation above is deliberately completed before the first mutation.
    resource["recipes"][rid] = deepcopy(concrete["recipe"])
    intern_review(resource, concrete["recipe_review"])
    for record in (pe, concrete):
        rule = selected[record["rule_id"]]
        rule.clear()
        rule.update(deepcopy(record["compiled_rule"]))
    resource["coverage"]["mapping_methods"] = dict(
        sorted(Counter(r["mapping_method"] for r in resource["replace"]).items())
    )
    resource["coverage"]["mean_confidence"] = round(
        sum(r["confidence"] for r in resource["replace"]) / len(resource["replace"]), 6
    )
    dispositions = Counter(
        "recipe" if "recipe_ref" in r["target"] else r.get("classification", "unclassified")
        for r in resource["replace"]
    )
    dispositions["omitted"] = len(resource["delete"])
    if resource["coverage"].get("unresolved_identities"):
        dispositions["unresolved"] = resource["coverage"]["unresolved_identities"]
    resource["coverage"]["dispositions"] = dict(dispositions)
