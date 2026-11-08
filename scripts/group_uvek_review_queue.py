"""Group pending UVEK correspondences for review; never approve or alter mappings."""

import argparse
import hashlib
import json
from collections import Counter
from pathlib import Path


def review_family(rule):
    """Assign a screening family from source metadata, without claiming equivalence."""
    name = rule["source"]["name"].lower()
    product = rule["source"]["reference product"].lower()
    if rule["reference_flow_sign_review"].get("profiles"):
        return "Model-dependent recycled materials and coproduct allocation"
    if "to generic market for" in name or "to market for" in name:
        return "Explicit recovery or substitution credits"
    if any(
        term in name
        for term in (
            "cement production",
            "clinker production",
            "concrete production",
            "concrete block production",
            "fly ash brick production",
            "flyash brick production",
            "granulated blast furnace slag production",
        )
    ):
        return "Mineral recovery and cement/concrete co-processing"
    if product in (
        "blast furnace gas",
        "coal gas",
        "bagasse, from sugarcane",
        "bagasse, from sweet sorghum",
        "tallow, unrefined",
        "stalk",
        "coal slurry",
    ):
        return "Energy recovery and combustible byproducts"
    if any(term in name for term in ("venting", "antifouling paint emissions", "water discharge from petroleum")):
        return "Direct releases and discharge boundaries"
    if any(
        term in product
        for term in ("bottom ash", "residues, mswi", "sewage sludge", "leachate", "refinery sludge", "digester sludge")
    ):
        return "Ash, sludge and leachate treatment"
    if product.startswith(
        (
            "used ",
            "decommissioned ",
            "electronics scrap",
            "waste electric",
            "metal part of electronics",
            "precious metal from electronics",
            "residue from mechanical treatment, liquid crystal",
        )
    ) and not any(term in product for term in ("used vegetable", "used tyre", "used metallurgical")):
        return "Equipment, vehicle and electronics end-of-life"
    if any(term in name for term in ("recycling", "sorting", "shredding", "purification", "esterification")) or any(
        term in product
        for term in (
            "scrap",
            "flakes, recycled",
            "plastic, mixed, recycled",
            "waste paper, sorted",
            "waste wood",
            "waste mixed aggregates",
        )
    ):
        return "Material sorting and recycling services"
    if name.startswith(("market ", "treatment of ")) and any(
        term in name for term in ("landfill", "open dump", "open burning", "incineration")
    ):
        return "Other waste disposal and its markets"
    return "Other industrial waste markets and chemical treatment"


def build_queue(resource, source_digest):
    """Account for every wholly pending identity exactly once, separately from approvals."""
    pending = []
    for rule in resource["replace"]:
        review = rule.get("reference_flow_sign_review", {})
        if review.get("status") != "unresolved":
            continue
        if review.get("profiles") and all(entry["review"]["status"] == "verified" for entry in review["profiles"]):
            continue
        partial = any(entry["review"]["status"] == "verified" for entry in review.get("profiles", []))
        pending.append(
            {
                "id": rule["id"],
                "source": rule["source"],
                "family": review_family(rule),
                "review_state": "partially_approved" if partial else "unreviewed",
            }
        )
    counts = Counter(row["family"] for row in pending)
    return {
        "schema_version": 1,
        "generator": "scripts/group_uvek_review_queue.py",
        "resource": resource["name"],
        "resource_sha256": source_digest,
        "basis": "Source-metadata screening only; these families are not scientific approvals and never authorize execution. Inspection may split or regroup them. Existing approvals apply only after matching scientific evidence is established.",
        "additional_approval_count": None,
        "approval_count_reason": "The number of distinct new modelling choices requires scientific inspection; neither identity count nor screening-family count predicts user approval requests.",
        "pending_identities": len(pending),
        "fully_pending_identities": sum(row["review_state"] == "unreviewed" for row in pending),
        "partially_approved_identities": sum(row["review_state"] == "partially_approved" for row in pending),
        "families": dict(sorted(counts.items())),
        "rules": sorted(pending, key=lambda row: (row["family"], row["id"])),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    source = args.root / "brightpath/data/migrations/uvek/ecoinvent-to-uvek-2025.json"
    raw = source.read_bytes()
    queue = build_queue(json.loads(raw), hashlib.sha256(raw).hexdigest())
    output = args.root / "docs/uvek_forward_review_queue.json"
    output.write_text(json.dumps(queue, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(
        f"Grouped {queue['pending_identities']} pending identities into {len(queue['families'])} screening families; additional approval count is undetermined."
    )


if __name__ == "__main__":
    main()
