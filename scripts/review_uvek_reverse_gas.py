"""Independent reverse combustion reconstructions; never infer them from forward rules."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

FIELDS = ("name", "reference product", "location", "unit")
REVIEW_PATH = "brightpath/data/export/uvek_reverse_industrial_gas_review.json"
APPROVAL = json.loads((Path(__file__).resolve().parents[1] / REVIEW_PATH).read_text())
METHOD = "reviewed_industrial_gas_reverse"
SERVICES = {
    tuple(row["source"][f] for f in FIELDS): row for row in APPROVAL["rules"] if row["kind"] == "matching_service_proxy"
}
EVIDENCE_SCOPE = {
    ("uvek", "2025", "cutoff"): {tuple(r["source"][f] for f in FIELDS) for r in APPROVAL["rules"]},
    ("ecoinvent", "3.12", "cutoff"): {tuple(r["target"][f] for f in FIELDS) for r in APPROVAL["target_inventories"]},
}
LIMITATIONS = (
    "User-approved reverse gas-combustion reconstruction: combine allocated German ecoinvent 3.12 cut-off "
    "electricity and heat outputs to represent one MJ of gas combustion. Their weighted fuel inputs sum to "
    "one MJ. Exclude upstream blast-furnace-gas supply to match that UVEK burner; retain coke-oven-gas supply "
    "to match the coke-oven burner. Do not apply the separate foreground fuel correction twice. This is an "
    "explicit RER-to-DE technology/geography proxy. Allocation, infrastructure, cooling, emissions, internal "
    "uncertainty and supply models differ; LCIA equivalence is not established or quantified. Preserve caller "
    "signs, uncertainty, formulas and separately recorded electricity/heat credits. Include all generated "
    "helpers in scoring and export; no absolute-value transformation or inferred inverse is used."
)
SERVICE_LIMITATIONS = (
    "Independently reviewed reverse refinery-flaring service proxy: preserve the incoming MJ with factor -1, "
    "converting UVEK's positive physical-combustion reference to the negative ecoinvent 3.12 cut-off waste "
    "treatment reference. UVEK's native PRODUCT_FLOW output denotes flaring as a service, not fuel production. "
    "Both GLO/MJ processes cover direct refinery-flare emissions and exclude upstream fuel, flare infrastructure "
    "and recovered-energy credits. Ecoinvent substitutes nine active biosphere exchanges for UVEK's ten. "
    "Emission factors, pollutant coverage, speciation and underlying plant/geographic representation differ; "
    "climate, toxicity and other LCIA equivalence are not established or quantified. Preserve caller signs, "
    "uncertainty, formulas, foreground suppliers and separate emissions without taking an absolute value. "
    "This exact source/target pair was inspected independently; natural-gas flares and other treatment or "
    "production references are outside this approval."
)


def identity(record):
    return tuple(record.get(f) for f in FIELDS)


def validate_reverse_gas_inventories(inventories):
    """Revalidate primitive boundaries and allocated target inventories independently."""
    for row in APPROVAL["rules"]:
        if inventories.get(identity(row["source"])) != [("uvek-2025-cutoff", row["source_inventory_sha256"])]:
            raise ValueError("Reverse gas source inventory changed; repeat the independent review.")
    for row in APPROVAL["target_inventories"]:
        if inventories.get(identity(row["target"])) != [("ecoinvent-3.12-cutoff", row["inventory_sha256"])]:
            raise ValueError("Reverse gas allocation or target inventory changed; repeat the independent review.")


def additions():
    """Compile only independently reviewed exact combustion and supply dispositions."""
    rules, recipes, reviews = [], {}, {}
    for row in APPROVAL["rules"]:
        source = row["source"]
        source_digest = hashlib.sha256(json.dumps(identity(source)).encode()).hexdigest()[:16]
        reconstruction = row["kind"] == "reconstruction"
        service = row["kind"] == "matching_service_proxy"
        if row["kind"] not in ("reconstruction", "matching_product_proxy", "matching_service_proxy") or (
            service
            and (
                row.get("source_reference_sign") != 1
                or row.get("target_reference_sign") != -1
                or row.get("conversion_factor") != -1
                or row.get("source_native_flow_type") != "PRODUCT_FLOW"
                or row.get("source_native_reference_is_input") is not False
            )
        ):
            raise ValueError("Unrecognized reverse gas disposition or service reference convention.")
        rationale = (
            LIMITATIONS
            if reconstruction
            else (
                "Approved matching-product reverse screening proxy: preserve one MJ and its incoming sign. "
                + (
                    "UVEK high-voltage electricity supply is represented by the unique ecoinvent high-voltage "
                    "market or market group for the same region and kWh. Supply mix, imports, losses, grid "
                    "infrastructure and emissions differ; this is a supply proxy, not a production-technology "
                    "or marginal-generation equivalence. "
                    if source["name"] == "Electricity, high voltage, at grid"
                    else (
                        "UVEK GLO coke-oven-gas production is represented by the ecoinvent GLO coal-gas supply market, "
                        "whose suppliers are allocated coke-production coproducts. Coking mix and allocation differ. "
                        if source["name"] == "Coke oven gas, at plant"
                        else "UVEK RER heavy-fuel-oil industrial-furnace heat is represented by the matching ecoinvent "
                        "Europe-without-Switzerland industrial-furnace heat service. Geographic scope, infrastructure "
                        "and emissions differ. "
                    )
                )
                + "This independently reviewed commodity proxy is not a reverse inference; LCIA equivalence is "
                "not established. Preserve foreground suppliers, uncertainty, formulas and separate emissions."
            )
        )
        if service:
            rationale = SERVICE_LIMITATIONS
        review = {
            "rationale": rationale,
            "evidence": [
                f"{REVIEW_PATH}: independent source and target inventory fingerprints, reference-unit and fuel-boundary checks."
            ],
        }
        review_ref = intern_review({"reviews": reviews}, review)
        if reconstruction:
            recipe_id = f"reverse-industrial-gas-{source_digest}"
            recipes[recipe_id] = {
                "name": f"{source['name']}, reconstructed from allocated DE coproducts",
                "reference product": source["reference product"],
                "location": "DE",
                "unit": "megajoule",
                "inputs": deepcopy(row["inputs"]),
                "rationale": rationale,
                "evidence": review["evidence"],
            }
            target = {"recipe_ref": recipe_id}
        else:
            target = deepcopy(row["target"])
        rules.append(
            {
                "id": f"uvek-ei312-{source_digest}",
                "source": deepcopy(source),
                "target": target,
                "conversion_factor": -1 if service else 1,
                "classification": "proxy",
                "mapping_method": METHOD,
                "review_ref": review_ref,
            }
        )
    return rules, recipes, reviews


def legacy_projection(resource):
    """Keep the frozen publisher baseline protected while validating later additions separately."""
    rules, recipes, reviews = additions()
    expected = {identity(r["source"]): r for r in rules}
    present = set()
    for rule in resource["replace"]:
        key = identity(rule["source"])
        if key not in expected:
            if rule.get("mapping_method") == METHOD:
                raise ValueError("Unapproved reverse gas source outside the independently reviewed scope.")
            continue
        normalized = {k: v for k, v in rule.items() if k != "reference_flow_sign_review"}
        if normalized != expected[key] or resource["reviews"].get(rule["review_ref"]) != reviews[rule["review_ref"]]:
            raise ValueError("Reviewed reverse gas correspondence changed; repeat the independent review.")
        present.add(key)
    if present:
        # Permit only whole, ordered review batches: the original four routes
        # remain a valid pre-extension baseline, never an arbitrary subset.
        batches = APPROVAL.get("extension_batches", [[r["source"] for r in APPROVAL["rules"]]])
        cumulative, allowed = set(), set()
        for batch in batches:
            cumulative.update(identity(source) for source in batch)
            allowed.add(frozenset(cumulative))
        if (
            cumulative != expected.keys()
            or frozenset(present) not in allowed
            or any(resource["recipes"].get(k) != v for k, v in recipes.items())
        ):
            raise ValueError("Reviewed reverse gas reconstruction changed or is incomplete.")
    return {
        **resource,
        "replace": [r for r in resource["replace"] if identity(r["source"]) not in present],
        "recipes": {k: v for k, v in resource["recipes"].items() if k not in recipes},
    }


def apply_reverse_gas(resource, review, source_identities, target_identities):
    """Add reviewed routes after publisher correspondence review, without weakening its baseline."""
    legacy_projection(resource)
    rules, recipes, reviews = additions()
    for rule in rules:
        if identity(rule["source"]) not in source_identities:
            raise ValueError("Reviewed reverse gas source is missing from the exact catalog.")
        if "recipe_ref" not in rule["target"] and identity(rule["target"]) not in target_identities:
            raise ValueError("Reviewed reverse commodity target is missing from the exact catalog.")
    for recipe in recipes.values():
        if any(identity(e) not in target_identities for e in recipe["inputs"]):
            raise ValueError("Reviewed reverse combustion dependency is missing from the exact catalog.")
    keys = {identity(r["source"]) for r in rules}
    resource["replace"] = [r for r in resource["replace"] if identity(r["source"]) not in keys] + deepcopy(rules)
    resource["replace"].sort(key=lambda r: identity(r["source"]))
    resource["recipes"].update(deepcopy(recipes))
    resource["reviews"].update(deepcopy(reviews))
    updates = {
        identity(r["source"]): {
            "source": r["source"],
            "rule_id": r["id"],
            "disposition": "recipe" if "recipe_ref" in r["target"] else "proxy",
        }
        for r in rules
    }
    review[:] = [deepcopy(updates.get(identity(r["source"]), r)) for r in review]
    resource["coverage"].update(
        mapped_identities=len(resource["replace"]) + len(resource["delete"]),
        unresolved_identities=len(source_identities) - len(resource["replace"]) - len(resource["delete"]),
        dispositions=dict(Counter(r["disposition"] for r in review)),
    )
