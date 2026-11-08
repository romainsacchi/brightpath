"""Author exact-profile, mass-conserving refrigerant venting service recipes."""

import json
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

FIELDS = ("name", "reference product", "location", "unit")
REVIEW_PATH = "brightpath/data/export/uvek_refrigerant_venting_review.json"
APPROVAL = json.loads((Path(__file__).resolve().parents[1] / REVIEW_PATH).read_text())
VENTING_SOURCES = {tuple(row["source"][f] for f in FIELDS): row for row in APPROVAL["rules"]}
CHEMICALS = {
    "r12": ("CFC-12", "Dichlorodifluoromethane", ["air"]),
    "r134a": ("HFC-134a", "1,1,1,2-Tetrafluoroethane", ["air"]),
    "r600a": ("isobutane", "Isobutane", ["air", "urban air close to ground"]),
}
NATIVE_EMISSIONS = {
    "r12": "Methane, dichlorodifluoro-, CFC-12",
    "r134a": "Ethane, 1,1,1,2-tetrafluoro-, HFC-134a",
    "r600a": "Isobutane",
}


def venting_recipe(refrigerant, indicator):
    """Define physical release coefficients independently of background production."""
    chemical, emission, categories = CHEMICALS[refrigerant]
    recipe_id = f"venting-{refrigerant}" + ("-waste-indicator" if indicator else "")
    inputs = [{"name": emission, "categories": categories, "unit": "kilogram", "amount": 1, "type": "biosphere"}]
    binding = {
        "profile": {"family": "uvek", "version": "2026"},
        "target": {
            "name": NATIVE_EMISSIONS[refrigerant],
            "categories": ["emissions to air", "unspecified"],
            "unit": "kilogram",
        },
        "reason": "Approved exact chemical identity and kilogram air-emission correspondence for this venting recipe; the native target catalog must contain the declared flow.",
    }
    if refrigerant == "r600a":
        binding["information_loss"] = (
            "Approved venting-compartment approximation: ecoinvent's urban-air isobutane release is represented by UVEK 2026's unspecified-air Isobutane flow. Preserve released mass; population-specific characterization may differ."
        )
    inputs[0]["biosphere_targets"] = [binding]
    if indicator:
        inputs.append(
            {
                "name": "Non-hazardous waste disposed",
                "categories": ["inventory indicator", "waste"],
                "unit": "kilogram",
                "amount": 1,
                "type": "biosphere",
                "approved_omission_if_unavailable": True,
                "omission_rationale": "The approved 3.12 venting model retains its waste-disposal inventory indicator where the target biosphere supports it. Otherwise exclude the indicator with a warning; the refrigerant air emission must remain represented.",
            }
        )
    recipe = {
        "name": f"Venting of used refrigerant {chemical}",
        "reference product": f"venting service for used refrigerant {chemical}",
        "location": "GLO",
        "unit": "kilogram",
        "inputs": inputs,
        "rationale": f"User-approved physical venting model: one kilogram of {chemical} released to air per kilogram vented, without manufacturing, recovery or destruction. Mass conservation establishes this coefficient; no LCIA score equivalence is asserted. Preserve the original air compartment subject to an explicitly reported target-compartment approximation.",
        "evidence": [
            f"{REVIEW_PATH}: approved 2026-11-05; inspected source inventories are fingerprint-bound, with no active processing inputs. Recipe coefficients follow the definition of complete venting and mass conservation."
        ],
    }
    return recipe_id, recipe


def venting_decision(
    rule,
    observations,
    inventories,
    binding,
    original_review,
    *,
    approval=None,
    recipe_factory=venting_recipe,
    review_path=REVIEW_PATH,
    model="venting",
    limitations=None,
    decision_reason=None,
    original_native_flow_type="PRODUCT_FLOW",
):
    """Reject changed suppliers, signs, profile coverage or inspected inventories."""
    source = tuple(rule["source"][f] for f in FIELDS)
    approval = VENTING_SOURCES[source] if approval is None else approval
    profiles = approval["profiles"]
    expected = sorted((p["label"], -1) for p in profiles)
    hashes = sorted((p["label"], p["inventory_sha256"]) for p in profiles)
    if (
        rule["target"] != approval["previous_target"]
        or rule.get("conversion_factor", 1) != 1
        or sorted(observations) != expected
        or sorted(inventories) != hashes
    ):
        raise ValueError(
            f"{model.title()} supplier, profile, reference or inventory changed; repeat the scientific review."
        )
    recipes, entries = {}, []
    for p in profiles:
        family, version, system_model = p["label"].split("-")
        recipe_id, recipe = recipe_factory(approval["refrigerant"], p["waste_indicator"])
        recipes[recipe_id] = recipe
        entries.append(
            {
                "source_profile": {"family": family, "version": version, "system_model": system_model},
                "target": {"recipe_ref": recipe_id},
                "conversion_factor": -1,
                "review": {
                    "status": "verified",
                    "source_sign": -1,
                    "target_sign": 1,
                    "reason": decision_reason
                    or "An inspected negative waste reference becomes a positive venting-service reference. Preserve the refrigerant identity and its one-to-one air release; manufacturing is not a venting substitute.",
                    "evidence": [f"{review_path}#{p['label']}/{p['inventory_sha256']}"],
                },
            }
        )
    base_id, base_recipe = recipe_factory(approval["refrigerant"], False)
    recipes[base_id] = base_recipe
    review = {
        "rationale": base_recipe["rationale"],
        "limitations": limitations
        or [
            "Complete venting is modelled, not collection, reclamation, destruction or new refrigerant manufacture. Target biosphere compartment approximations and unsupported inventory indicators must be reported. Source-profile approval does not extend to uninspected patch releases or reverse routes."
        ],
        "evidence": base_recipe["evidence"],
    }
    return {
        "operation": "recipe",
        "binding": binding,
        "original_rule": deepcopy(rule),
        "original_review": deepcopy(original_review),
        "original_factor": 1,
        "factor": -1,
        "source_observations": [list(p) for p in expected],
        "uvek_reference_sign": 1,
        "uvek_native_flow_type": "GENERATED_SERVICE",
        "original_native_flow_type": original_native_flow_type,
        "review": {
            "status": "unresolved",
            "reason": f"Only enumerated exact source profiles approve this physical {model} recipe; no missing-profile fallback is permitted.",
            "evidence": [review_path],
            "profiles": entries,
        },
        "target": {"recipe_ref": base_id},
        "recipes": recipes,
        "recipe_review": review,
    }


def compiled_venting_rule(decision):
    """Reproduce the complete reviewed rule for stale-resource detection."""
    rule = deepcopy(decision["original_rule"])
    rule.update(
        target=deepcopy(decision["target"]),
        conversion_factor=decision["factor"],
        reference_flow_sign_review=deepcopy(decision["review"]),
        review_ref=intern_review({}, decision["recipe_review"]),
        mapping_method=decision.get("mapping_method", "reviewed_venting_recipe"),
        confidence=1,
        report_correspondence=True,
    )
    if decision.get("recipe_information_loss"):
        rule.update(classification="proxy", technosphere_information_loss=decision["recipe_information_loss"])
    return rule
