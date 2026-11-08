"""Review the exact unspecified pure-water discharge to native UVEK separately."""

import argparse
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review, rule_identifier
from scripts.review_uvek_2026_biosphere import REPORT as NATIVE_REVIEW
from scripts.review_uvek_2026_biosphere import ROOT, WATER_REPORT_URL, digest, identity, native_metadata
from scripts.review_uvek_2026_land import REPORT as LAND_REVIEW
from scripts.review_uvek_2026_land import resource_hash

REPORT = ROOT / "docs/uvek_2026_water_review.json"
METHOD = "reviewed_unspecified_water_volume_mass"
SOURCE = {"name": "Water", "categories": ["water"], "unit": "cubic meter"}
TARGET = {"name": "Water", "categories": ["emissions to water", "unspecified"], "unit": "kilogram"}
WATER_UUID = "2404b41a-2eed-4e9d-8ab6-783946fdf5d6"
LIMITATION = (
    "Only pure Water discharged to the unspecified water compartment is eligible. "
    "Use the documented 1000 kg/m3 LCA water convention; this is not a wastewater-treatment "
    "service or a wastewater-composition estimate. Preserve signed quantities, uncertainty "
    "and formulas through normal biosphere scaling. No geography, river/lake split, resource "
    "withdrawal, regional Water flow, omission or background-treatment input is inferred."
)


def validate_decision(decision, ecoinvent, native):
    """Require the same chemical, unspecified emission compartment and exact units."""
    source_matches = [row for row in ecoinvent if identity(row) == identity(SOURCE)]
    target_matches = [row for row in native if identity(row) == identity(TARGET)]
    if (
        decision.get("source") != SOURCE
        or decision.get("target") != TARGET
        or decision.get("source_uuid") != WATER_UUID
        or decision.get("source_cas") != ["7732-18-5"]
        or decision.get("target_cas") != ["7732-18-5"]
        or decision.get("conversion_factor") != 1000
        or decision.get("density_kg_per_m3") != 1000
        or decision.get("target_uses") != 1858
        or len(source_matches) != 1
        or source_matches[0].get("flow_id") != WATER_UUID
        or len(target_matches) != 1
        or "biosphere_information_loss" in decision
    ):
        raise ValueError("Unspecified water correspondence changed; repeat the independent review.")


def apply_water_review(resources, path, catalog_directory):
    """Add one authored ecoinvent-to-UVEK rule and verify the existing reverse edge."""
    report = json.loads(path.read_text())
    sources = report["sources"]
    native_review = json.loads(NATIVE_REVIEW.read_text())
    if (
        report.get("schema_version") != 1
        or report.get("direction") != "ecoinvent-3.10-biosphere-to-uvek-2026-biosphere"
        or sources["native_review_sha256"] != digest(NATIVE_REVIEW)
        or sources["land_review_sha256"] != digest(LAND_REVIEW)
        or sources["water_convention"] != native_review["sources"]["water_convention"]
        or sources["water_convention"]["density_kg_per_m3"] != 1000
    ):
        raise ValueError("Unspecified water review schema or pinned evidence changed.")
    catalogs = {}
    for family, filename in (("ecoinvent", "ecoinvent__3.10__cutoff.json"), ("native", "uvek__2026__cutoff.json")):
        catalog_path = catalog_directory / filename
        if sources[f"{family}_catalog_sha256"] != digest(catalog_path):
            raise ValueError("Unspecified water exact catalog changed.")
        catalogs[family] = json.loads(catalog_path.read_text())["biosphere"]
    validate_decision(report["decision"], catalogs["ecoinvent"], catalogs["native"])
    result = deepcopy(resources)
    incoming = next(r for r in result if r["axis"] == "biosphere" and r["source_profiles"][0]["family"] == "ecoinvent")
    outgoing = next(r for r in result if r["axis"] == "biosphere" and r["source_profiles"][0]["family"] == "uvek")
    if (
        resource_hash(incoming) != sources["baseline_incoming_sha256"]
        or resource_hash(outgoing) != sources["baseline_outgoing_sha256"]
        or incoming["provenance"]["sha256"] != sources["native_archive_sha256"]
    ):
        raise ValueError("Unspecified water directional baseline changed.")
    reverse = [r for r in outgoing["replace"] if identity(r["source"]) == identity(TARGET)]
    if (
        len(reverse) != 1
        or reverse[0] != report["independent_reverse_check"]
        or reverse[0]["target"] != {**SOURCE, "uuid": WATER_UUID}
        or reverse[0].get("conversion_factor") != 0.001
        or reverse[0].get("biosphere_information_loss")
    ):
        raise ValueError("Unspecified water reverse correspondence needs an independent review.")
    if sum(identity(row) == identity(SOURCE) for row in incoming["unresolved"]) != 1 or any(
        identity(row["source"]) == identity(SOURCE) for row in incoming["replace"]
    ):
        raise ValueError("Unspecified water source is no longer uniquely unresolved.")
    reference = intern_review(
        incoming,
        {
            "rationale": "Independently reviewed pure-water identity and unspecified emission compartment. "
            + LIMITATION,
            "limitations": [LIMITATION],
            "evidence": [f"{path.name} SHA-256 {digest(path)}", WATER_REPORT_URL, NATIVE_REVIEW.name],
        },
    )
    incoming["replace"].append(
        {
            "id": rule_identifier(incoming["name"], SOURCE),
            "source": deepcopy(SOURCE),
            "target": deepcopy(TARGET),
            "classification": "correspondence",
            "conversion_factor": 1000.0,
            "mapping_method": METHOD,
            "review_ref": reference,
            "report_correspondence": True,
        }
    )
    incoming["replace"].sort(key=lambda row: identity(row["source"]))
    incoming["unresolved"] = [row for row in incoming["unresolved"] if identity(row) != identity(SOURCE)]
    incoming["coverage"].update(
        mapped_identities=len(incoming["replace"]),
        unresolved_identities=len(incoming["unresolved"]),
        mapping_methods=dict(sorted(Counter(row["mapping_method"] for row in incoming["replace"]).items())),
        directional_additions=incoming["coverage"].get("directional_additions", 0) + 1,
    )
    incoming["provenance"]["water_review"] = {"file": path.name, "sha256": digest(path), "sources": sources}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("--output", type=Path, default=REPORT)
    args = parser.parse_args()
    from scripts.generate_uvek_2026_resources import build_resources

    directory = ROOT / "brightpath/data/export/reference_catalogs"
    _catalog, resources = build_resources(args.archive, directory, water_review=None)
    incoming = next(
        r for r in resources if r["axis"] == "biosphere" and r["source_profiles"][0]["family"] == "ecoinvent"
    )
    outgoing = next(r for r in resources if r["axis"] == "biosphere" and r["source_profiles"][0]["family"] == "uvek")
    metadata = native_metadata(args.archive)[identity(TARGET)]
    native_review = json.loads(NATIVE_REVIEW.read_text())
    if digest(args.archive) != native_review["sources"]["native_archive_sha256"]:
        raise ValueError("Unspecified water native archive changed; repeat the independent review.")
    decision = {
        "source": SOURCE,
        "target": TARGET,
        "source_uuid": WATER_UUID,
        "source_cas": ["7732-18-5"],
        "target_cas": sorted(metadata["cas"]),
        "target_uses": metadata["uses"],
        "conversion_factor": 1000,
        "density_kg_per_m3": 1000,
    }
    report = {
        "schema_version": 1,
        "generator": "scripts/review_uvek_2026_water.py",
        "direction": incoming["name"],
        "authorization": "Standing independently inspected matching-flow and physical-quantity approval; no omission or compartment approximation.",
        "sources": {
            "native_archive_sha256": digest(args.archive),
            "native_review_sha256": digest(NATIVE_REVIEW),
            "land_review_sha256": digest(LAND_REVIEW),
            "ecoinvent_catalog_sha256": digest(directory / "ecoinvent__3.10__cutoff.json"),
            "native_catalog_sha256": digest(directory / "uvek__2026__cutoff.json"),
            "baseline_incoming_sha256": resource_hash(incoming),
            "baseline_outgoing_sha256": resource_hash(outgoing),
            "baseline_coverage": incoming["coverage"],
            "water_convention": native_review["sources"]["water_convention"],
        },
        "decision": decision,
        "independent_reverse_check": next(r for r in outgoing["replace"] if identity(r["source"]) == identity(TARGET)),
    }
    validate_decision(
        decision,
        json.loads((directory / "ecoinvent__3.10__cutoff.json").read_text())["biosphere"],
        json.loads((directory / "uvek__2026__cutoff.json").read_text())["biosphere"],
    )
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
