"""Replay exact sand-delivery and flight-hour profiles with prior bindings intact."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath import TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_fuels_37_sand_hour_review.json"
REVIEW_SHA256 = "b55346b1312a0e15b8d2a02bb9c44b7f7f412d3cd56a917a196edf76ea215d5f"


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def apply_fuels_37_sand_hour_review(resource, root=ROOT):
    """Keep kg/hour references and distinguish the existing at-mine profile."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Sand/hour evidence changed.")
    review = json.loads(raw)
    for prior in review["prior_inspections"]:
        if hashlib.sha256((root / "brightpath/data" / prior["path"]).read_bytes()).hexdigest() != prior["sha256"]:
            raise ValueError("An earlier sand/hour inspection changed.")
    for row in review["records"]:
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        old, new = row["original_rule"], row["compiled_rule"]
        if parent not in (old, new):
            raise ValueError("Sand/hour parent changed.")
        if (
            row["source_profile"] != {"family": "ecoinvent", "version": "3.7", "system_model": "cutoff"}
            or row["source_witness"]["reference_amounts"] != [1]
            or row["source_witness"]["inspected_profile"]["version"] != "3.7.1"
            or row["new_profile"]["conversion_factor"] != 1
            or any(w["reference_amount"] != 1 for w in row["native_2026_witnesses"])
        ):
            raise ValueError("Sand/hour profile, sign or reference changed.")
        for proof in row["preserved_profiles"]:
            selected = select_profile_sign_rules({"replace": [new]}, TechnosphereProfile(**proof["source_profile"]))[
                "replace"
            ][0]
            if digest(selected) != proof["selected_rule_sha256"]:
                raise ValueError("An earlier flight-hour profile changed.")
        for proof in row["preserved_physical_bindings"]:
            selected = select_profile_sign_rules({"replace": [new]}, TechnosphereProfile(**proof["source_profile"]))[
                "replace"
            ][0]
            if (
                any(selected[k] != proof[k] for k in ("source", "target"))
                or selected.get("conversion_factor", 1) != proof["conversion_factor"]
            ):
                raise ValueError("The earlier sand supply or quantity factor changed.")
        parent.clear()
        parent.update(deepcopy(new))
    for key, recipe in review["recipes"].items():
        if key in resource["recipes"] and resource["recipes"][key] != recipe:
            raise ValueError("Sand delivery recipe conflicts with existing content.")
        resource["recipes"][key] = deepcopy(recipe)
    for card in review["reviews"]:
        intern_review(resource, card)
    counts = Counter(
        "recipe" if "recipe_ref" in r["target"] else r.get("classification", "unclassified")
        for r in resource["replace"]
    )
    counts["omitted"] = len(resource["delete"])
    if resource["coverage"].get("unresolved_identities"):
        counts["unresolved"] = resource["coverage"]["unresolved_identities"]
    resource["coverage"]["dispositions"] = dict(counts)
