"""Author approved gas-combustion services with explicit energy-recovery credits."""

import hashlib
import json
from copy import deepcopy
from pathlib import Path

FIELDS = ("name", "reference product", "location", "unit")
REVIEW_PATH = "brightpath/data/export/uvek_industrial_gas_review.json"
APPROVAL = json.loads((Path(__file__).resolve().parents[1] / REVIEW_PATH).read_text())
GAS_SOURCES = {tuple(r["source"][f] for f in FIELDS): r for r in APPROVAL["rules"]}
PRIMITIVES = {tuple(r["target"][f] for f in FIELDS): r for r in APPROVAL["primitives"]}
LIMITATIONS = (
    "Approved industrial-gas energy-recovery proxy: RER UVEK combustion represents the source regional "
    "power plant per MJ of waste gas. Preserve the inspected electricity and heat credit quantities through "
    "the existing UVEK energy correspondences; these approximate consequential marginal energy supplies. "
    "For coke-oven gas, subtract exactly the burner's embedded 1 MJ coke-oven-gas production input to "
    "isolate waste-gas combustion. This fuel-boundary correction is not an additional recovered-fuel credit. "
    "Combustion composition, emissions, infrastructure, cooling, geography, internal uncertainty and "
    "parameterization differ; LCIA equivalence is not established or quantified. Net impacts may legitimately "
    "be negative. Preserve caller-owned amounts, uncertainty, formulas, foreground suppliers and separate "
    "emissions. Only enumerated consequential profiles are approved; cut-off product references, gas markets "
    "and missing/patch profiles cannot inherit these treatment recipes. Include generated helpers in scoring "
    "and export. No reverse reconstruction is inferred."
)


def gas_recipe(source, profile):
    """Keep fuel combustion, fuel-boundary cancellation and energy credits distinct."""
    coke = source[1] == "coal gas"
    name = "Coke oven gas" if coke else "Blast furnace gas"
    burner = next(
        r["target"] for r in APPROVAL["primitives"] if r["target"]["name"] == f"{name}, burned in power plant"
    )
    inputs = [{**deepcopy(burner), "amount": 1, "type": "technosphere"}]
    if coke:
        fuel = next(r["target"] for r in APPROVAL["primitives"] if r["target"]["name"] == "Coke oven gas, at plant")
        inputs.append({**deepcopy(fuel), "amount": -1, "type": "technosphere"})
    inputs.extend(deepcopy(profile["credits"]))
    digest = hashlib.sha256(json.dumps([source, inputs], sort_keys=True).encode()).hexdigest()[:20]
    return f"industrial-gas-{digest}", {
        "name": f"{name} waste-gas combustion with energy recovery, {source[2]}",
        "reference product": f"{name.lower()} treatment with energy recovery",
        "location": source[2],
        "unit": "megajoule",
        "inputs": inputs,
        "rationale": f"User-approved waste-gas treatment per MJ, retaining its signed electricity and heat recovery quantities. {LIMITATIONS}",
        "evidence": [
            f"{REVIEW_PATH}: 721 inspected negative-reference consequential inventories; sparse source fingerprints and derived UVEK credit inputs. No full source inventories are published."
        ],
    }


def validate_credit_correspondences(resource):
    """Do not silently change the mappings used for approved recovered-energy credits."""
    rules = {r["id"]: r for r in resource["replace"]}
    for entry in APPROVAL["credit_correspondences"]:
        rule = rules.get(entry["rule_id"], {})
        binding = {
            "source": rule.get("source"),
            "target": rule.get("target"),
            "conversion_factor": rule.get("conversion_factor", 1),
        }
        if hashlib.sha256(json.dumps(binding, sort_keys=True).encode()).hexdigest() != entry["binding_sha256"]:
            raise ValueError("Industrial-gas energy-credit correspondence changed; repeat the scientific review.")


def gas_decision(rule, observations, inventories, primitive_inventories, binding, original_review):
    """Require the exact supplier, source inventories and primitive process boundaries."""
    source = tuple(rule["source"][f] for f in FIELDS)
    approval = GAS_SOURCES[source]
    expected = sorted((p["label"], p["inventory_sha256"]) for p in approval["profiles"])
    if (
        rule["target"] != approval["previous_target"]
        or rule.get("conversion_factor", 1) != 1
        or sorted(observations) != [(label, -1) for label, _ in expected]
        or sorted(inventories) != expected
        or any(
            primitive_inventories.get(target) != [("uvek-2025-cutoff", row["inventory_sha256"])]
            for target, row in PRIMITIVES.items()
        )
    ):
        raise ValueError("Industrial-gas supplier, credit profile or combustion boundary changed; repeat the review.")
    recipes, profiles = {}, []
    for p in approval["profiles"]:
        family, version, model = p["label"].split("-")
        if model != "consequential":
            raise ValueError("Gas-treatment credit approval is restricted to consequential inventories.")
        recipe_id, recipe = gas_recipe(source, p)
        recipes[recipe_id] = recipe
        profiles.append(
            {
                "source_profile": {"family": family, "version": version, "system_model": model},
                "target": {"recipe_ref": recipe_id},
                "conversion_factor": -1,
                "review": {
                    "status": "verified",
                    "source_sign": -1,
                    "target_sign": 1,
                    "reason": "Retain inspected electricity/heat recovery quantities with the approved combustion and fuel-boundary proxies.",
                    "evidence": [f"{REVIEW_PATH}#{p['label']}/{p['inventory_sha256']}"],
                },
            }
        )
    return {
        "operation": "recipe",
        "binding": binding,
        "original_rule": deepcopy(rule),
        "original_review": deepcopy(original_review),
        "original_factor": 1,
        "factor": -1,
        "source_observations": [list(row) for row in sorted(observations)],
        "uvek_reference_sign": 1,
        "uvek_native_flow_type": "GENERATED_SERVICE",
        "original_native_flow_type": approval["previous_native_flow_type"],
        "target": deepcopy(profiles[0]["target"]),
        "recipes": recipes,
        "recipe_review": {"rationale": LIMITATIONS, "limitations": [LIMITATIONS], "evidence": [REVIEW_PATH]},
        "recipe_information_loss": LIMITATIONS,
        "mapping_method": "reviewed_industrial_gas_recipe",
        "review": {
            "status": "unresolved",
            "reason": "Select only an exact inspected consequential gas-treatment profile; no missing-profile fallback is approved.",
            "evidence": [REVIEW_PATH],
            "profiles": profiles,
        },
    }
