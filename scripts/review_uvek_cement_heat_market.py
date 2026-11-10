"""Replay the exact European 3.9 gas-heat market as native plant-gate services."""

import hashlib
import json
import math
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

if __package__:
    from scripts.review_uvek_venting import compiled_venting_rule
else:
    from review_uvek_venting import compiled_venting_rule

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_cement_european_ng_heat_review.json"
REVIEW_SHA256 = "ae949fc3271ca84559456061b8c707d87b1522542b505d924a23a68e34ecffdd"


def apply_cement_heat_market_review(resource, root=ROOT):
    """Validate the entire frozen decision before changing its one parent rule."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Cement gas-heat evidence changed; repeat its scientific review.")
    review = json.loads(raw)
    for authority in review["authorization_evidence"]:
        if (
            hashlib.sha256((root / "brightpath/data" / authority["path"]).read_bytes()).hexdigest()
            != authority["sha256"]
        ):
            raise ValueError("Cement gas-heat authority changed; repeat its review.")
    decision = review["decision"]
    compiled = compiled_venting_rule(decision)
    rule = next(row for row in resource["replace"] if row["id"] == compiled["id"])
    profiles = compiled["reference_flow_sign_review"]["profiles"]
    if (
        compiled != review["compiled_rule"]
        or rule not in (decision["original_rule"], compiled)
        or len(profiles) != 1
        or profiles[0]["source_profile"] != {"family": "ecoinvent", "version": "3.9", "system_model": "cutoff"}
        or profiles[0]["conversion_factor"] != 1
        or review["source_witness"]["reference_amount"] != 1
        or review["source_supplier_count"] != 54
        or len(review["source_producer_witnesses"]) != 54
        or any(row["reference_amounts"] != [1] for row in review["source_producer_witnesses"])
        or any(row["reference_amount"] != 1 for row in review["native_witnesses"])
        or review["district_network_added"]
        or review["extra_efficiency_factor"]
    ):
        raise ValueError("Cement gas-heat parent, references or exact scope changed.")
    inputs = [edge for recipe in decision["recipes"].values() for edge in recipe["inputs"]]
    if (
        len(inputs) != 4
        or any(edge["type"] != "technosphere" or edge["unit"] != "megajoule" or edge["amount"] <= 0 for edge in inputs)
        or any(edge["name"].startswith(("District heat,", "Natural gas,")) for edge in inputs)
        or not math.isclose(sum(edge["amount"] for edge in inputs), review["source_quantity_sum"], abs_tol=1e-14)
    ):
        raise ValueError("Cement gas-heat quantity or useful-heat boundary changed.")
    for key, recipe in decision["recipes"].items():
        if key in resource["recipes"] and resource["recipes"][key] != recipe:
            raise ValueError("Cement gas-heat helper changed; repeat its review.")
    resource["recipes"].update(deepcopy(decision["recipes"]))
    intern_review(resource, decision["recipe_review"])
    rule.clear()
    rule.update(compiled)
    resource["coverage"]["mapping_methods"] = dict(
        sorted(Counter(row["mapping_method"] for row in resource["replace"]).items())
    )
    resource["coverage"]["mean_confidence"] = round(
        sum(row["confidence"] for row in resource["replace"]) / len(resource["replace"]), 6
    )
    dispositions = Counter(
        "recipe" if "recipe_ref" in row["target"] else row.get("classification", "unclassified")
        for row in resource["replace"]
    )
    dispositions["omitted"] = len(resource["delete"])
    if resource["coverage"].get("unresolved_identities"):
        dispositions["unresolved"] = resource["coverage"]["unresolved_identities"]
    resource["coverage"]["dispositions"] = dict(dispositions)
