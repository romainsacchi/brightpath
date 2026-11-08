"""Author fingerprint-bound refrigerant disposal proxies with explicit leakage."""

import json
from copy import deepcopy
from pathlib import Path

try:
    from review_uvek_venting import CHEMICALS, FIELDS, venting_decision, venting_recipe
except ModuleNotFoundError:
    from scripts.review_uvek_venting import CHEMICALS, FIELDS, venting_decision, venting_recipe

REVIEW_PATH = "brightpath/data/export/uvek_refrigerant_disposal_review.json"
APPROVAL = json.loads((Path(__file__).resolve().parents[1] / REVIEW_PATH).read_text())
DISPOSAL_SOURCES = {tuple(row["source"][f] for f in FIELDS): row for row in APPROVAL["rules"]}
DISPOSAL_TARGET = APPROVAL["target"]
LIMITATIONS = "Approved refrigerant-disposal proxy: CH average hazardous-waste incineration at 25% water replaces the source regional hazardous-waste treatment mix for 0.999 kg per kg accepted. Preserve the separately modelled 0.001 kg refrigerant air release, without claiming that it establishes destruction efficiency. Geography, average waste composition, plant operation, emissions control, residues, energy recovery, allocation and background supplies may differ. No moisture or heating-value scaling is inferred. Refrigerant-specific destruction performance, fluorine/chlorine balances and LCIA-score equivalence are not established or quantified. Collection, reclamation and treatment-market freight are outside this direct-service approval."


def disposal_recipe(refrigerant, indicator):
    """Combine a disclosed incineration proxy and the inspected direct release assumption."""
    _, recipe = venting_recipe(refrigerant, indicator)
    chemical = CHEMICALS[refrigerant][0]
    recipe_id = f"disposal-{refrigerant}" + ("-waste-indicator" if indicator else "")
    recipe["name"] = f"Final disposal of used refrigerant {chemical}"
    recipe["reference product"] = f"disposal service for used refrigerant {chemical}"
    recipe["inputs"][0]["amount"] = 0.001
    for exchange in recipe["inputs"]:
        for binding in exchange.get("biosphere_targets", []):
            binding["reason"] = binding["reason"].replace("venting recipe", "disposal recipe")
            if binding.get("information_loss"):
                binding["information_loss"] = binding["information_loss"].replace(
                    "venting-compartment", "disposal-compartment"
                )
        if exchange.get("omission_rationale"):
            exchange["omission_rationale"] = exchange["omission_rationale"].replace("venting model", "disposal model")
    recipe["inputs"].append({**deepcopy(DISPOSAL_TARGET), "amount": 0.999, "type": "technosphere"})
    recipe["rationale"] = (
        f"User-approved disposal service per kg of {chemical}: 0.999 kg of CH average hazardous-waste incineration plus the source assumption of 0.001 kg direct {chemical} air release. {LIMITATIONS}"
    )
    recipe["evidence"] = [
        f"{REVIEW_PATH}: approved 2026-11-05. All 42 exact inspected source/profile inventories have reference production -1 kg, hazardous-waste incineration supplies totaling -0.999 kg and +0.001 kg of the identified refrigerant to air. Only 3.12 adds a +1 kg waste-disposal inventory indicator. Sparse identifiers, profile coverage and exchange-record fingerprints are retained; no full source inventories are published.",
        "Read-only UVEK 2025 inspection: bafu-165198, CH/kg, production +1, verified native WASTE_FLOW input reference and average hazardous waste at 25% water. The provider is a disposal proxy, not refrigerant manufacture, cable treatment or a validated refrigerant-specific destruction process.",
    ]
    return recipe_id, recipe


def disposal_decision(rule, observations, inventories, binding, original_review, previous):
    """Preserve the prior sign correction while approving the inspected disposal model."""
    source = tuple(rule["source"][f] for f in FIELDS)
    approval = DISPOSAL_SOURCES[source]
    normalized = deepcopy(rule)
    # The older R600a cable proxy already acquired factor -1 in the sign audit.
    # Bind to its pre-correction factor so historical projection and replay agree.
    original_factor = previous.get("original_factor", 1)
    if original_factor != 1 or rule.get("conversion_factor", 1) not in (1, previous.get("factor", 1)):
        raise ValueError("Disposal prior factor changed; repeat the scientific review.")
    normalized.pop("conversion_factor", None)
    decision = venting_decision(
        normalized,
        observations,
        inventories,
        binding,
        original_review,
        approval=approval,
        recipe_factory=disposal_recipe,
        review_path=REVIEW_PATH,
        model="disposal",
        limitations=[LIMITATIONS],
        original_native_flow_type=approval["previous_native_flow_type"],
        decision_reason="The inspected -1 kg waste reference becomes +1 kg of disposal service. Retain the 0.001 kg direct refrigerant air-release assumption and use the separately approved 0.999 kg hazardous-incineration proxy. Missing source profiles remain blocked.",
    )
    decision.update(mapping_method="reviewed_refrigerant_disposal_recipe", recipe_information_loss=LIMITATIONS)
    return decision
