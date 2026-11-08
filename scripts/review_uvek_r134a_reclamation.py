"""Author exact-profile R134a reclamation services and cut-off product suppliers."""

import json
from copy import deepcopy
from pathlib import Path

try:
    from review_uvek_refrigerant_disposal import DISPOSAL_TARGET
    from review_uvek_venting import FIELDS, venting_recipe
except ModuleNotFoundError:
    from scripts.review_uvek_refrigerant_disposal import DISPOSAL_TARGET
    from scripts.review_uvek_venting import FIELDS, venting_recipe

REVIEW_PATH = "brightpath/data/export/uvek_r134a_reclamation_review.json"
APPROVAL = json.loads((Path(__file__).resolve().parents[1] / REVIEW_PATH).read_text())
RECLAMATION_SOURCES = {tuple(row["source"][f] for f in FIELDS): row for row in APPROVAL["rules"]}
ELECTRICITY_TARGET = {
    "name": "Electricity, medium voltage, at grid",
    "reference product": "Electricity, medium voltage, at grid",
    "location": "GLO",
    "unit": "kilowatt hour",
}
REFRIGERANT_TARGET = {
    "name": "Refrigerant R134a, at plant",
    "reference product": "Refrigerant R134a, at plant",
    "location": "RER",
    "unit": "kilogram",
}
LIMITATIONS = "Approved R134a-reclamation proxy: average GLO medium-voltage grid electricity replaces the source electricity supply; CH average hazardous-waste incineration at 25% water replaces its regional incineration mix. In consequential profiles only, 0.899 kg of RER primary R134a production approximates the credited market supplied by 25% RER and 75% RoW primary production. Electricity mix, geography, treatment composition, allocation, transport, marginal supply and other background burdens may differ. Preserve the separate 0.001 kg direct refrigerant release; no additional disposal-service leakage, moisture scaling, universal recycling credit or equivalent LCIA score is inferred. Cut-off profiles have no avoided-production credit."
ZERO_REASON = "Approved only for the seven inspected positive-reference cut-off R134a reclamation-product profiles: their source suppliers have production +1 kg and no active processing inputs or biosphere exchanges. Preserve material quantities through this production-only supplier. The reclamation treatment service separately retains processing burdens. This observed allocation boundary does not establish zero impact for reclamation or for other recycled products; missing and consequential profile decisions remain blocked."


def reclamation_recipe(model):
    """Keep processing and leakage separate from the model-specific recovery credit."""
    _, recipe = venting_recipe("r134a", False)
    recipe["name"] = f"Reclamation of used HFC-134a, {model} accounting"
    recipe["reference product"] = f"reclamation service for used HFC-134a, {model} accounting"
    recipe["inputs"][0]["amount"] = 0.001
    for binding in recipe["inputs"][0]["biosphere_targets"]:
        binding["reason"] = binding["reason"].replace("venting recipe", "reclamation recipe")
    recipe["inputs"].extend(
        [
            {**deepcopy(ELECTRICITY_TARGET), "amount": 0.0466666666666667, "type": "technosphere"},
            {**deepcopy(DISPOSAL_TARGET), "amount": 0.1, "type": "technosphere"},
        ]
    )
    if model == "consequential":
        recipe["inputs"].append({**deepcopy(REFRIGERANT_TARGET), "amount": -0.899, "type": "technosphere"})
    recipe["rationale"] = (
        f"Approved {model} reclamation service per kg accepted: 0.0466666666666667 kWh electricity, 0.1 kg average hazardous-waste incineration and 0.001 kg HFC-134a released to air. "
        + (
            "Preserve the 0.899 kg primary-refrigerant recovery credit. "
            if model == "consequential"
            else "No recovered-product substitution credit is applied. "
        )
        + LIMITATIONS
    )
    recipe["evidence"] = [
        f"{REVIEW_PATH}: 14 exact inspected waste-reference profiles, seven fingerprint-bound credited primary markets and seven inspected empty positive-reference cut-off coproduct suppliers. Only sparse identities, allocation facts and fingerprints are published."
    ]
    return f"reclamation-r134a-{model}", recipe


def recovered_product_recipe():
    """Preserve the inspected empty cut-off supplier without discarding material flows."""
    return "recovered-r134a-cutoff", {
        "name": "Recovered HFC-134a, cut-off allocation",
        "reference product": "recovered HFC-134a, cut-off allocation",
        "location": "GLO",
        "unit": "kilogram",
        "inputs": [],
        "zero_burden": True,
        "zero_burden_reason": ZERO_REASON,
        "rationale": ZERO_REASON,
        "evidence": [
            f"{REVIEW_PATH}: seven exact cut-off coproduct inventories have one positive production exchange and no active additional exchanges. Processing is retained on the separate waste-treatment service. This is an explicit inspected allocation decision, not a missing-provider fallback."
        ],
    }


def reclamation_decision(rule, observations, inventories, binding, original_review):
    """Bind both service and product approvals to their exact inspected inventories."""
    source = tuple(rule["source"][f] for f in FIELDS)
    approval = RECLAMATION_SOURCES[source]
    profiles = approval["profiles"]
    sign = approval["source_reference_sign"]
    expected_inventories = sorted(
        (
            {"source": list(source), **{k: v for k, v in p.items() if k != "label"}} | {"label": p["label"]}
            for p in profiles
        ),
        key=lambda p: p["label"],
    )
    if (
        rule["target"] != approval["previous_target"]
        or rule.get("conversion_factor", 1) != 1
        or sorted(observations) != sorted((p["label"], sign) for p in profiles)
        or sorted(inventories, key=lambda p: p["label"]) != expected_inventories
    ):
        raise ValueError(
            "Reclamation supplier, allocation profile, inventory or credited market changed; repeat the review."
        )
    service = approval["kind"] == "service"
    recipes, entries = {}, []
    for p in profiles:
        family, version, model = p["label"].split("-")
        if not service and model != "cutoff":
            raise ValueError("Empty recovered-product approval is restricted to inspected cut-off profiles.")
        recipe_id, recipe = reclamation_recipe(model) if service else recovered_product_recipe()
        recipes[recipe_id] = recipe
        entries.append(
            {
                "source_profile": {"family": family, "version": version, "system_model": model},
                "target": {"recipe_ref": recipe_id},
                "conversion_factor": sign,
                "review": {
                    "status": "verified",
                    "source_sign": sign,
                    "target_sign": 1,
                    "reason": (
                        f"Preserve inspected {model} reclamation accounting, including its explicit recovery-credit disposition."
                        if service
                        else ZERO_REASON
                    ),
                    "evidence": [f"{REVIEW_PATH}#{p['label']}/{p['inventory_sha256']}"],
                },
            }
        )
    base_id, base = reclamation_recipe("cutoff") if service else recovered_product_recipe()
    review = {
        "rationale": (
            (
                "Approved R134a reclamation accounting: retain electricity, 0.1 kg incineration and 0.001 kg direct leakage, and apply the 0.899 kg recovered-product credit only in consequential profiles. "
                + LIMITATIONS
            )
            if service
            else ZERO_REASON
        ),
        "limitations": [LIMITATIONS if service else ZERO_REASON],
        "evidence": base["evidence"],
    }
    decision = {
        "operation": "recipe",
        "binding": binding,
        "original_rule": deepcopy(rule),
        "original_review": deepcopy(original_review),
        "original_factor": 1,
        "factor": sign,
        "source_observations": [list(row) for row in sorted(observations)],
        "uvek_reference_sign": 1,
        "uvek_native_flow_type": "GENERATED_SERVICE" if service else "GENERATED_PRODUCT",
        "original_native_flow_type": "PRODUCT_FLOW",
        "target": {"recipe_ref": base_id},
        "recipes": recipes,
        "recipe_review": review,
        "mapping_method": "reviewed_r134a_reclamation_recipe" if service else "reviewed_r134a_cutoff_product_recipe",
        "review": {
            "status": "unresolved",
            "reason": "Select only an exact inspected source profile; reclamation credits and empty product suppliers cannot be inferred across system models or patch releases.",
            "evidence": [REVIEW_PATH],
            "profiles": entries,
        },
    }
    if service:
        decision["recipe_information_loss"] = LIMITATIONS
    return decision
