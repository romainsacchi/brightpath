"""Independently inspected market boundaries composed from approved gas services."""

import hashlib
import json
import math
from copy import deepcopy
from pathlib import Path

FIELDS = ("name", "reference product", "location", "unit")
REVIEW_PATH = "brightpath/data/export/uvek_gas_market_review.json"
APPROVAL = json.loads((Path(__file__).resolve().parents[1] / REVIEW_PATH).read_text())
MARKETS = {tuple(r["source"][f] for f in FIELDS): r for r in APPROVAL["rules"]}
EVIDENCE_SCOPE = {tuple(APPROVAL["primitive"]["target"][f] for f in FIELDS)} | set(MARKETS)
EVIDENCE_SCOPE.update(
    tuple(p["empty_supplier"][f] for f in FIELDS)
    for r in APPROVAL["rules"]
    for p in r["profiles"]
    if "empty_supplier" in p
)
LIMITATIONS = (
    "Reviewed model-specific gas-market representation. Consequential gas markets mix the already approved "
    "per-MJ gas-treatment services, including their combustion proxies and electricity/heat recovery credits. "
    "Negative inputs to negative-reference providers represent treatment demand, not avoided product supply. "
    "Cut-off blast-furnace gas has an inspected one-MJ supply chain ending in an empty allocated coproduct: "
    "retain its quantity in an explicit zero-burden allocation helper. This is not a missing-provider fallback. "
    "Cut-off coke-oven gas is a positive-reference allocated product, represented by one MJ of UVEK coke-oven "
    "gas production; it is not waste treatment or zero burden. Allocation, producer mix, geography, emissions "
    "and internal uncertainty differ; LCIA equivalence is not established. Preserve caller quantity, signed "
    "demand, uncertainty, formulas, foreground suppliers and separate emissions. Only enumerated profiles "
    "are approved. Export and score the complete returned inventory with its generated helpers."
)
ZERO_REASON = (
    "Inspected positive-reference cut-off blast-furnace-gas market supplies exactly one MJ of its allocated "
    "Recycled Content cut-off coproduct, which has only a positive production exchange and no active inputs "
    "or emissions. The market has no other active inputs or emissions. Preserve its quantity as zero-burden "
    "allocated supply; this is an observed cut-off boundary, not missing data or a consequential treatment model."
)


def market_recipe(source, profile):
    """Keep positive products separate from negative-reference treatment mixes."""
    kind = profile["kind"]
    if kind == "treatment_mix":
        inputs = deepcopy(profile["inputs"])
        if any(e["amount"] <= 0 or e["recipe"] not in APPROVAL["dependencies"] for e in inputs) or not math.isclose(
            math.fsum(e["amount"] for e in inputs), 1
        ):
            raise ValueError("Reviewed gas-treatment mix changed; repeat the scientific review.")
    elif kind == "production_proxy":
        inputs = [{**APPROVAL["primitive"]["target"], "amount": 1, "type": "technosphere"}]
    elif kind == "zero_supply":
        inputs = []
    else:
        raise ValueError("Unreviewed gas market boundary.")
    digest = hashlib.sha256(json.dumps([source, kind, inputs], sort_keys=True).encode()).hexdigest()[:20]
    recipe = {
        "name": f"{source[1]} market, {kind.replace('_', ' ')}",
        "reference product": source[1],
        "location": source[2],
        "unit": "megajoule",
        "inputs": inputs,
        "rationale": ZERO_REASON if kind == "zero_supply" else LIMITATIONS,
        "evidence": [f"{REVIEW_PATH}: exact inventories and independently inspected system-model boundaries."],
    }
    if kind == "zero_supply":
        recipe.update(zero_burden=True, zero_burden_reason=ZERO_REASON)
    return f"gas-market-{digest}", recipe


def validate_market_dependencies(resource):
    """Do not let changed child services silently inherit the market approval."""
    for key, digest in APPROVAL["dependencies"].items():
        recipe = resource["recipes"].get(key)
        if hashlib.sha256(json.dumps(recipe, sort_keys=True).encode()).hexdigest() != digest:
            raise ValueError("Gas-market treatment dependency changed; repeat the scientific review.")


def market_decision(rule, observations, inventories, binding, original_review):
    """Require all exact source and upstream allocation fingerprints before compiling."""
    source = tuple(rule["source"][f] for f in FIELDS)
    approval = MARKETS[source]
    expected = sorted((p["label"], p["inventory_sha256"]) for p in approval["profiles"])
    primitive = APPROVAL["primitive"]
    if (
        rule["target"] != approval["previous_target"]
        or rule.get("conversion_factor", 1) != 1
        or sorted(inventories.get(source, [])) != expected
        or sorted(observations) != [(label, -1 if label.endswith("consequential") else 1) for label, _ in expected]
        or inventories.get(tuple(primitive["target"][f] for f in FIELDS))
        != [("uvek-2025-cutoff", primitive["inventory_sha256"])]
    ):
        raise ValueError("Gas-market supplier, profile or primitive changed; repeat the scientific review.")
    for p in approval["profiles"]:
        if p["kind"] == "zero_supply":
            supplier = tuple(p["empty_supplier"][f] for f in FIELDS)
            if (p["label"], p["empty_supplier_inventory_sha256"]) not in inventories.get(supplier, []):
                raise ValueError("Gas-market empty allocation changed; repeat the scientific review.")
    recipes, profiles = {}, []
    for p in approval["profiles"]:
        family, version, model = p["label"].split("-")
        if (p["kind"] == "treatment_mix") != (model == "consequential"):
            raise ValueError("Gas-market boundary does not match its system model.")
        recipe_id, recipe = market_recipe(source, p)
        recipes[recipe_id] = recipe
        sign = -1 if model == "consequential" else 1
        profiles.append(
            {
                "source_profile": {"family": family, "version": version, "system_model": model},
                "target": {"recipe_ref": recipe_id},
                "conversion_factor": sign,
                "review": {
                    "status": "verified",
                    "source_sign": sign,
                    "target_sign": 1,
                    "reason": LIMITATIONS,
                    "evidence": [f"{REVIEW_PATH}#{p['label']}/{p['inventory_sha256']}"],
                },
            }
        )
    # A nonzero default prevents zero-allocation approval from leaking to any
    # profile lacking its own explicit, inspected cut-off decision.
    default = next(p["target"] for p in profiles if p["source_profile"]["system_model"] == "consequential")
    return {
        "operation": "recipe",
        "binding": binding,
        "original_rule": deepcopy(rule),
        "original_review": deepcopy(original_review),
        "original_factor": 1,
        "factor": -1,
        "source_observations": [list(r) for r in sorted(observations)],
        "uvek_reference_sign": 1,
        "uvek_native_flow_type": "GENERATED_SERVICE",
        "original_native_flow_type": approval["previous_native_flow_type"],
        "target": deepcopy(default),
        "recipes": recipes,
        "recipe_review": {"rationale": LIMITATIONS, "limitations": [LIMITATIONS], "evidence": [REVIEW_PATH]},
        "recipe_information_loss": LIMITATIONS,
        "mapping_method": "reviewed_gas_market_recipe",
        "review": {
            "status": "unresolved",
            "reason": "Select an exact independently inspected gas-market allocation profile; no default is approved.",
            "evidence": [REVIEW_PATH],
            "profiles": profiles,
        },
    }
