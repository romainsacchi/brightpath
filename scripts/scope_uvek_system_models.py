"""Publish cut-off UVEK routes while preserving historical scientific audit inputs.

Historical review ledgers contain both ecoinvent models. They remain evidence
for replay, but only the cut-off projection is an executable migration resource.
"""

import json
from collections import Counter
from copy import deepcopy

FIELDS = ("name", "reference product", "location", "unit")
VERSIONS = ("3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12")


def cutoff_uvek_resource(resource, catalog_directory):
    """Remove incompatible profiles, exclusive identities and unused dependencies."""
    if resource["axis"] != "technosphere" or resource["target_profile"] != {
        "family": "uvek",
        "version": "2025",
        "system_model": "cutoff",
    }:
        raise ValueError("The cut-off scope requires the forward UVEK 2025 technosphere resource.")
    result = deepcopy(resource)
    allowed = set()
    for version in VERSIONS:
        catalog = json.loads((catalog_directory / f"ecoinvent__{version}__cutoff.json").read_text(encoding="utf-8"))
        allowed.update((r["name"], r["reference_product"], r["location"], r["unit"]) for r in catalog["technosphere"])
    result["source_profiles"] = [p for p in result["source_profiles"] if p["system_model"] == "cutoff"]
    if not result["source_profiles"]:
        raise ValueError("The resource has no cut-off source profiles.")
    for operation in ("replace", "disaggregate", "delete"):
        result[operation] = [r for r in result[operation] if tuple(r["source"][f] for f in FIELDS) in allowed]
        for rule in result[operation]:
            review = rule.get("reference_flow_sign_review", {})
            if "profiles" not in review:
                continue
            review["profiles"] = [p for p in review["profiles"] if p["source_profile"]["system_model"] == "cutoff"]
            if not review["profiles"]:
                raise ValueError("A retained identity lacks a cut-off sign-review profile.")
            verified = [
                p for p in review["profiles"] if p["review"]["status"] == "verified" and p.get("operation") != "delete"
            ]
            if verified:
                first = verified[0]
                rule["target"] = deepcopy(first.get("target", rule["target"]))
                rule["conversion_factor"] = first["conversion_factor"]
    recipes = result.get("recipes", {})
    retained = set()

    def retain(recipe_id):
        if recipe_id in retained:
            return
        retained.add(recipe_id)
        for exchange in recipes[recipe_id]["inputs"]:
            if "recipe" in exchange:
                retain(exchange["recipe"])

    for rule in result["replace"]:
        if "recipe_ref" in rule["target"]:
            retain(rule["target"]["recipe_ref"])
        for profile in rule.get("reference_flow_sign_review", {}).get("profiles", []):
            if "recipe_ref" in profile.get("target", {}):
                retain(profile["target"]["recipe_ref"])
    result["recipes"] = {key: value for key, value in recipes.items() if key in retained}
    references = {
        rule["review_ref"]
        for operation in ("replace", "disaggregate", "delete")
        for rule in result[operation]
        if "review_ref" in rule
    }
    references.update(
        p["review_ref"]
        for rule in result["replace"]
        for p in rule.get("reference_flow_sign_review", {}).get("profiles", [])
        if "review_ref" in p
    )
    result["reviews"] = {key: value for key, value in result["reviews"].items() if key in references}
    coverage = result["coverage"]
    coverage.update(
        source_identities=len(allowed),
        mapped_identities=sum(len(result[k]) for k in ("replace", "disaggregate", "delete")),
        omitted_identities=len(result["delete"]),
        mapping_methods=dict(sorted(Counter(r["mapping_method"] for r in result["replace"]).items())),
        mean_confidence=round(sum(r["confidence"] for r in result["replace"]) / len(result["replace"]), 6),
    )
    if "dispositions" in coverage:
        coverage["dispositions"] = dict(
            Counter(
                "recipe" if "recipe_ref" in r["target"] else r.get("classification", "unclassified")
                for r in result["replace"]
            )
        )
        coverage["dispositions"]["omitted"] = len(result["delete"])
    result["description"] = (
        "Heuristic compatibility mapping from packaged ecoinvent 3.x cut-off technosphere identities "
        "to existing UVEK 2025 cut-off activities. Cross-system-model conversion is unsupported. "
        "Reference-sign review does not establish scientific or LCIA equivalence."
    )
    return result
