"""Replay the scoped fixed-allocation waste-oil market and its literal omission."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_fuels_37_waste_oil_review.json"
REVIEW_SHA256 = "d9a38553d8c54b1411f99f7839f2fdb4c75d42d18de1169f77972468dffffa1d"


def apply_fuels_37_waste_oil_review(resource, root=ROOT):
    """Bind the omission to only the inspected requested profile and root."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Allocated waste-oil evidence changed.")
    review = json.loads(raw)
    parent = next(r for r in resource["replace"] if r["id"] == review["rule_id"])
    if parent not in (review["original_rule"], review["compiled_rule"]):
        raise ValueError("Waste-oil parent changed before scoped review.")
    recipe = review["recipes"]["fuels-37-allocated-waste-mineral-oil"]
    evidence = review["allocation_evidence"]
    omission = recipe["source_input_omissions"]
    if (
        review["source_profile"] != {"family": "ecoinvent", "version": "3.7", "system_model": "cutoff"}
        or review["source_witness"]["reference_amounts"] != [-1]
        or review["source_witness"]["inspected_profile"]["version"] != "3.7.1"
        or review["new_profile"]["conversion_factor"] != -1
        or len(omission) != 1
        or omission[0]["source_exchange"]["amount"] != -0.204833837097889
        or omission[0]["source_profile"] != review["source_profile"]
        or omission[0]["source_activity"] != review["original_rule"]["source"]
        or omission[0]["original_index"] != 5
        or evidence["zero_nominal_waste_coordinates"] != 57
        or evidence["zero_bounded_uncertainty_rows"] != 57
        or evidence["common_formula_strings_as_positive_clinker"] != 57
    ):
        raise ValueError("Waste-oil reference, fixed allocation or exact omission scope changed.")
    if [e["amount"] for e in recipe["inputs"]] != [0.01932, 0.0112, 0.397600148816246, 0.397566014085865]:
        raise ValueError("Waste-oil shares or freight changed.")
    if any(w["reference_amount"] != 1 for w in review["native_2026_witnesses"]):
        raise ValueError("Native treatment/freight reference changed.")
    for key, value in review["recipes"].items():
        if key in resource["recipes"] and resource["recipes"][key] != value:
            raise ValueError("Waste-oil recipe conflicts with existing content.")
        resource["recipes"][key] = deepcopy(value)
    for card in review["reviews"]:
        intern_review(resource, card)
    parent.clear()
    parent.update(deepcopy(review["compiled_rule"]))
    counts = Counter(
        "recipe" if "recipe_ref" in r["target"] else r.get("classification", "unclassified")
        for r in resource["replace"]
    )
    counts["omitted"] = len(resource["delete"])
    if resource["coverage"].get("unresolved_identities"):
        counts["unresolved"] = resource["coverage"]["unresolved_identities"]
    resource["coverage"]["dispositions"] = dict(counts)
