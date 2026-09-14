"""Review seedling reference amounts and CHP allocation against local inventories."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from collections import Counter
from pathlib import Path

if __package__:
    from .review_uvek_mapping import FIELDS, identity
else:
    from review_uvek_mapping import FIELDS, identity

WOOD_SOURCE = "heat and power co-generation, wood chips, 6667 kW, state-of-the-art 2014"
DIESEL_SOURCE = "heat and power co-generation, diesel, 200kW electrical, SCR-NOx reduction"
WOOD_TARGET = "xx Electricity, at cogen ORC 1400kWth, wood, emission control, allocation exergy"
DIESEL_TARGET = "Electricity, at cogen 300kWth, diesel, allocation exergy"
SEEDLING_PREFIX = "Tree seedling, from {condition} greenhouse, 1000 units, at tree nursery"


def activity_summary(activity):
    """Extract sparse evidence, never a complete proprietary inventory."""
    production = list(activity.production())
    if len(production) != 1:
        raise ValueError(f"Expected one reference production exchange: {activity.key}")
    reference = production[0]
    amount = reference.get("amount")
    if not isinstance(amount, (int, float)) or not math.isfinite(amount) or amount <= 0:
        raise ValueError(f"Invalid reference production amount: {activity.key}")
    if tuple(reference.get("input", ())) != activity.key or reference.get("unit") != activity.get("unit"):
        raise ValueError(f"Reference production identity mismatch: {activity.key}")
    totals = {"electricity_kwh": 0.0, "heat_mj": 0.0, "machine_diesel_mj": 0.0, "diesel_kg": 0.0, "wood_kg": 0.0}
    fuel_providers = []
    technosphere = list(activity.technosphere())
    for exchange in technosphere:
        supplier = exchange.input
        name = supplier.get("name", "").lower()
        product = supplier.get("reference product", "").lower()
        unit = supplier.get("unit")
        quantity = float(exchange["amount"]) / amount
        if quantity <= 0:
            continue
        if unit == "kilowatt hour" and ("electricity" in product or "electricity" in name):
            totals["electricity_kwh"] += quantity
        if unit == "megajoule" and (
            product.startswith("heat") or "market for heat," in name or "market group for heat," in name
        ):
            totals["heat_mj"] += quantity
        if unit == "megajoule" and "diesel" in name and "machine" in name:
            totals["machine_diesel_mj"] += quantity
        if unit == "kilogram" and any(fuel in product for fuel in ("diesel", "light fuel oil")):
            totals["diesel_kg"] += quantity
        if unit == "kilogram" and "wood chips" in product:
            totals["wood_kg"] += quantity
        if unit == "megajoule" and "burned in cogen" in name:
            fuel_providers.append(
                {
                    "identity": {field: supplier.get(field) for field in FIELDS},
                    "code": supplier.get("code"),
                    "amount_mj_per_reference_unit": quantity,
                    "biosphere_exchanges": len(list(supplier.biosphere())),
                    "technosphere_exchanges": len(list(supplier.technosphere())),
                }
            )
    comment = (activity.get("comment") or "").lower()
    return {
        "identity": {field: activity.get(field) for field in FIELDS},
        "database": activity.key[0],
        "code": activity.key[1],
        "reference_amount": amount,
        "reference_unit": reference.get("unit"),
        "technosphere_exchanges": len(technosphere),
        "biosphere_exchanges": len(list(activity.biosphere())),
        "normalized_inputs": totals,
        "fuel_providers": fuel_providers,
        "documentation_flags": {
            "mentions_1000_seedlings": "1000" in comment and "seedling" in comment,
            "mentions_exergy_allocation": "allocation exergy" in comment or "exergy allocation" in comment,
            "mentions_orc": "organic rankine" in comment or "orc" in comment,
            "mentions_sncr": "sncr" in comment,
            "mentions_electrostatic_precipitator": "electrostatic precipitator" in comment,
        },
    }


def target_for(source):
    name = source["name"]
    if name == WOOD_SOURCE:
        target_name, location = WOOD_TARGET, "CH"
    elif name == DIESEL_SOURCE:
        target_name, location = DIESEL_TARGET, "CH"
    elif name in ("tree seedling production, in heated greenhouse", "tree seedling production, in unheated greenhouse"):
        condition = "unheated" if "unheated" in name else "heated"
        target_name, location = SEEDLING_PREFIX.format(condition=condition), "RER"
    else:
        raise ValueError(f"Source is outside the inspected priority families: {source}")
    return {"name": target_name, "reference product": target_name, "location": location, "unit": source["unit"]}


def assess(source, target, branch):
    """Choose only when the expected physical evidence is present."""
    source_identity = source["identity"]
    if target["identity"] != target_for(source_identity):
        raise ValueError("Candidate does not match the inspected technology and greenhouse condition")
    if source["reference_amount"] != 1 or target["reference_amount"] != 1:
        raise ValueError("Unexpected production normalization; review scaling before substitution")
    if source_identity["unit"] != target["identity"]["unit"]:
        raise ValueError("Unexpected unit change")
    if source_identity["name"].startswith("tree seedling"):
        if source_identity["reference product"] != "tree seedling, for planting" or source_identity["unit"] != "unit":
            raise ValueError("Unexpected seedling reference product")
        if not source["documentation_flags"]["mentions_1000_seedlings"]:
            raise ValueError("Seedling source reference-batch documentation is missing")
        expected = {
            "electricity_kwh": 0.0704,
            "machine_diesel_mj": 0.0184,
            "heat_mj": 0.0027 if "unheated" in source_identity["name"] else 0.801,
        }
        for field, value in expected.items():
            if not all(
                math.isclose(record["normalized_inputs"][field], value, rel_tol=1e-8, abs_tol=1e-12)
                for record in (source, target)
            ):
                raise ValueError(f"Seedling normalized-input fingerprint differs: {field}")
        reason = "Source documentation retains a 1000-seedling batch description, while both imported inventories produce one reference unit and have matching normalized electricity, diesel and heat inputs. Use the same greenhouse condition with factor 1.0, not 0.001 or 1000."
        return (
            "accept_seedling_reference_basis",
            0.8 if source_identity["location"] == "RER" else 0.65,
            reason,
            [
                "Reference-basis compatibility checked for these imported inventories, not a general rule for datasets named 1000 units.",
                "Background suppliers and geographic representativeness differ; full inventory equivalence is not claimed.",
            ],
        )
    if (
        source_identity["reference product"] != "electricity, high voltage"
        or source_identity["unit"] != "kilowatt hour"
    ):
        raise ValueError("Expected high-voltage electricity co-product")
    if branch["technosphere_exchanges"] != 1 or branch["biosphere_exchanges"] != 0:
        raise ValueError("Branch allocation-to-heat burden pattern changed; inspect again")
    if branch["fuel_providers"] or any(branch["normalized_inputs"].values()):
        raise ValueError("Branch now carries operating inputs; inspect allocation again")
    if source["biosphere_exchanges"] == 0:
        raise ValueError("Source operating burdens are missing")
    if source_identity["name"] == WOOD_SOURCE:
        if not source["documentation_flags"]["mentions_orc"] or not source["documentation_flags"]["mentions_sncr"]:
            raise ValueError("Expected ORC and emission-control technology is not documented")
        if (
            source["normalized_inputs"]["wood_kg"] <= 0
            or target["normalized_inputs"]["wood_kg"] <= 0
            or target["biosphere_exchanges"] == 0
        ):
            raise ValueError("Wood combustion inventory evidence is missing")
        return (
            "accept_wood_operating_proxy",
            0.45,
            "Reject the branch's electricity allocation-to-heat variant, which has only electricity-specific infrastructure and no direct combustion burdens. The full-catalog ORC wood/emission-control candidate carries fuel and emissions and better preserves source technology than the previous grid proxy.",
            [
                "Low-confidence proxy: target is deprecated and its 1400 kW thermal rating differs from the source 6667 kW fuel-input rating.",
                "Target uses exergy allocation; the source comment does not independently establish its numerical allocation split. No allocation equivalence is claimed.",
                "Source emission factors were updated in 2014; the older target's operating factors and regional conditions differ.",
                "The target's emission-control designation does not establish identical particulate-filter or SNCR performance.",
            ],
        )
    providers = target["fuel_providers"]
    if (
        not source["documentation_flags"]["mentions_exergy_allocation"]
        or not target["documentation_flags"]["mentions_exergy_allocation"]
    ):
        raise ValueError("Exergy allocation is not documented for both diesel inventories")
    if (
        source["normalized_inputs"]["diesel_kg"] <= 0
        or not providers
        or not all(provider["biosphere_exchanges"] > 0 for provider in providers)
    ):
        raise ValueError("Diesel operating burdens are not established, including indirect suppliers")
    return (
        "accept_diesel_allocation_proxy",
        0.55,
        "Reject the branch's infrastructure-only allocation-to-heat electricity. The active full-catalog diesel CHP candidate documents exergy allocation, as does the source, and includes operating fuel and emissions through its combustion supplier.",
        [
            "Proxy: source rating is 200 kW electric with SCR; target rating is 300 kW thermal. Capacity and emission-control equivalence are not established.",
            "A zero direct-biosphere count at the target is not zero combustion impact: the fuel-burning supplier carries the emissions.",
            "Production-point electricity is used rather than adding a grid-transmission wrapper and its 3.3% electricity input uplift.",
        ],
    )


def collect(project, database_name, identities):
    import bw2data as bd

    if project not in bd.projects:
        raise ValueError(f"Missing project: {project}")
    bd.projects.set_current(project)
    if database_name not in bd.databases:
        raise ValueError(f"Missing database: {database_name}")
    matches = {}
    for activity in bd.Database(database_name):
        key = tuple(activity.get(field) for field in FIELDS)
        if key not in identities:
            continue
        if key in matches:
            raise ValueError(f"Ambiguous activity: {key}")
        matches[key] = activity_summary(activity)
    if matches.keys() != identities:
        raise ValueError(f"Missing exact activities: {identities - matches.keys()}")
    metadata = bd.databases[database_name]
    provenance = {
        "project": project,
        "database": database_name,
        "number": metadata.get("number"),
        "modified": str(metadata.get("modified")),
        "source_workbook_sha256": metadata.get("source_workbook_sha256"),
    }
    return matches, provenance


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("resource", type=Path)
    parser.add_argument("followup_report", type=Path)
    parser.add_argument("output_directory", type=Path)
    parser.add_argument("--source-project", required=True)
    parser.add_argument("--source-database", required=True)
    parser.add_argument("--target-project", required=True)
    parser.add_argument("--target-database", required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    resource = json.loads(args.resource.read_text())
    indexed = {identity(row["source"]): row for row in resource["replace"]}
    with args.followup_report.open(newline="") as handle:
        pending = [
            row
            for row in csv.DictReader(handle)
            if row["followup_outcome"] in ("needs_quantity_basis", "needs_allocation_inventory")
        ]
    if len(pending) != 62 or len({identity(row) for row in pending}) != 62:
        raise ValueError("Expected the scoped 62-entry priority batch")
    source_keys = {identity(row) for row in pending}
    target_keys = {identity(target_for(row)) for row in pending} | {
        identity(json.loads(row["candidate_target"])) for row in pending
    }
    catalog_path = root / "brightpath/data/export/reference_catalogs/uvek__2025__cutoff.json"
    catalog = json.loads(catalog_path.read_text())
    catalog_keys = {
        (row["name"], row["reference_product"], row["location"], row["unit"]) for row in catalog["technosphere"]
    }
    if not target_keys <= catalog_keys:
        raise ValueError("Chosen or branch target absent from exact UVEK catalog")
    sources, source_provenance = collect(args.source_project, args.source_database, source_keys)
    targets, target_provenance = collect(args.target_project, args.target_database, target_keys)
    evidence = {
        "source_profile": {"family": "ecoinvent", "version": "3.10", "system_model": "cutoff"},
        "target_profile": catalog["profile"],
        "source_database": source_provenance,
        "target_database": target_provenance,
        "source_activities": sorted(sources.values(), key=lambda row: identity(row["identity"])),
        "target_activities": sorted(targets.values(), key=lambda row: identity(row["identity"])),
    }
    evidence_raw = json.dumps(evidence, indent=2, sort_keys=True) + "\n"
    evidence_hash = hashlib.sha256(evidence_raw.encode()).hexdigest()
    decisions, audit = [], []
    for row in pending:
        source = {field: row[field] for field in FIELDS}
        key = identity(source)
        current = indexed[key]
        if (
            current["mapping_method"] != "heuristic_similarity"
            or current["confidence"] >= 0.5
            or current["target"] != json.loads(row["previous_target"])
        ):
            raise ValueError(f"Stale or previously reviewed priority source: {key}")
        candidate = target_for(source)
        branch_target = json.loads(row["candidate_target"])
        outcome, confidence, reason, limitations = assess(
            sources[key], targets[identity(candidate)], targets[identity(branch_target)]
        )
        if source["location"] != candidate["location"]:
            limitations.append("Geographic proxy.")
        selection_source = "branch_after_inventory_check" if candidate == branch_target else "full_uvek_catalog"
        inventory_evidence = {
            "path": "docs/uvek_priority_inventory_evidence.json",
            "sha256": evidence_hash,
            "source_code": sources[key]["code"],
            "target_code": targets[identity(candidate)]["code"],
            "source_profile": evidence["source_profile"],
            "target_profile": evidence["target_profile"],
            "selection_source": selection_source,
            "amount_factor": 1.0,
        }
        decisions.append(
            {
                "source": source,
                "previous_target": current["target"],
                "target": candidate,
                "previous_confidence": current["confidence"],
                "confidence": confidence,
                "mapping_method": "uvek_inventory_review",
                "review_outcome": outcome,
                "review_version": 4,
                "branch_method": row["candidate_method"],
                "branch_reasoning": row["candidate_reasoning"],
                "branch_target": branch_target,
                "reason": reason,
                "limitations": limitations,
                "inventory_evidence": inventory_evidence,
            }
        )
        audit.append(
            source
            | {
                "previous_target": json.dumps(current["target"], sort_keys=True),
                "branch_target": json.dumps(branch_target, sort_keys=True),
                "selected_target": json.dumps(candidate, sort_keys=True),
                "outcome": outcome,
                "previous_confidence": current["confidence"],
                "confidence": confidence,
                "amount_factor": 1.0,
                "selection_source": selection_source,
                "reason": reason,
                "limitations": "; ".join(limitations),
            }
        )
    summary = {
        "review_version": 4,
        "reviewed": len(decisions),
        "outcomes": dict(Counter(row["review_outcome"] for row in decisions)),
        "selection_sources": dict(Counter(row["inventory_evidence"]["selection_source"] for row in decisions)),
        "selected_still_below_0.5": sum(row["confidence"] < 0.5 for row in decisions),
        "source_entries": len(resource["replace"]),
        "low_confidence_before": sum(row["confidence"] < 0.5 for row in resource["replace"]),
        "evidence_sha256": evidence_hash,
        "input_hashes": {
            label: hashlib.sha256(path.read_bytes()).hexdigest()
            for label, path in (
                ("resource", args.resource),
                ("followup_report", args.followup_report),
                ("target_catalog", catalog_path),
            )
        },
    }
    summary["low_confidence_after"] = summary["low_confidence_before"] - sum(
        row["confidence"] >= 0.5 for row in decisions
    )
    args.output_directory.mkdir(parents=True, exist_ok=True)
    (args.output_directory / "uvek_priority_inventory_evidence.json").write_text(evidence_raw)
    (args.output_directory / "uvek_priority_decisions.json").write_text(
        json.dumps({"review_version": 4, "decisions": decisions}, indent=2, sort_keys=True) + "\n"
    )
    (args.output_directory / "uvek_priority_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n"
    )
    with (args.output_directory / "uvek_priority_review.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(audit[0]))
        writer.writeheader()
        writer.writerows(audit)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
