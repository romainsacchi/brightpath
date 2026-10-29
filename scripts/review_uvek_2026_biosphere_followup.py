"""Review remaining native flows using exact publisher contexts and chemical identifiers."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import dumps_migration_resource
from brightpath.units import normalize_unit
from scripts.review_uvek_2026_biosphere import REPORT as INITIAL_REVIEW
from scripts.review_uvek_2026_biosphere import ROOT, apply_review, digest, identity, native_metadata, workbook_rows

REPORT = ROOT / "docs/uvek_2026_biosphere_followup.json"
GUIDANCE_URL = "https://eplca.jrc.ec.europa.eu/permalink/GLAD_Mapper/GLAD_Instructions.pdf"
CONTEXT_METHOD = "publisher_context_alias_uuid_correspondence"
CAS_METHOD = "publisher_cas_uuid_correspondence"
SUFFIX_METHOD = "reviewed_cubic_meter_name_suffix"
METHODS = {CONTEXT_METHOD, CAS_METHOD, SUFFIX_METHOD}
MAIN = {
    "emissions to air": "air",
    "emissions to water": "water",
    "emissions to soil": "soil",
    "resources": "natural resource",
}
SUB = {
    "unspecified": "",
    "high. pop.": "urban air close to ground",
    "low. pop.": "non-urban air or from high stacks",
    "low. pop., long-term": "low population density, long-term",
    "stratosphere + troposphere": "lower stratosphere + upper troposphere",
    "river": "surface water",
    "lake": "surface water",
    "river, long-term": "surface water, long-term",
    "groundwater": "ground-",
    "groundwater, long-term": "ground-, long-term",
    "fossilwater": "fossil well",
}
QUALIFIER = re.compile(
    r"fossil|biogenic|origin|measured|equiv|total|bound|unspecified|mixture|compounds|sum|dioxin|congener", re.I
)
REGION = re.compile(r",\s*(?:[A-Z]{2}(?:-[A-Z0-9]+)?|GLO|RER|RoW)(?:\s|,|$)")
SUFFIX_IDENTITIES = {
    ("Water, cooling, unspecified natural origin/m3", ("resources", "in water"), "cubic meter"),
    ("Water, unspecified natural origin/m3", ("resources", "in water"), "cubic meter"),
    ("Water/m3", ("emissions to air", "low. pop."), "cubic meter"),
    ("Wood, unspecified, standing/m3", ("resources", "biotic"), "cubic meter"),
}


def context(source):
    categories = source["categories"]
    if len(categories) > 2 or categories[0] not in MAIN:
        return ()
    return tuple(
        value for value in [MAIN[categories[0]], *[SUB.get(value, value) for value in categories[1:]]] if value
    )


def publisher_context(row):
    return tuple(
        value for value in (row["Compartment v3"], row["Subcompartment v3"]) if value not in ("", "unspecified")
    )


def legacy_context(row):
    root = row["Category v2.2"].lower()
    root = {"resource": "natural resource", "resources": "natural resource"}.get(root, root)
    subcategory = row["SubCategory v2.2"].lower()
    subcategory = {
        "high population density": "urban air close to ground",
        "low population density": "non-urban air or from high stacks",
        "low population density, long-term": "low population density, long-term",
        **SUB,
    }.get(subcategory, subcategory)
    return tuple(value for value in (root, subcategory) if value)


def valid_cas(value):
    """Require a non-placeholder CAS registry number with its check digit."""
    if not re.fullmatch(r"[1-9]\d{1,6}-\d{2}-\d", value):
        return False
    digits = value.replace("-", "")
    return sum(index * int(digit) for index, digit in enumerate(reversed(digits[:-1]), 1)) % 10 == int(digits[-1])


def target_row(row):
    return {"name": row["name"], "categories": row["categories"], "unit": row["unit"], "uuid": row["flow_id"]}


def candidate(source, metadata, rows, catalog, *, protected=False):
    """Match one publisher UUID; never discard geography, chemical basis or CAS conflicts."""
    cas = metadata["cas"]
    if protected or len(cas) > 1:
        return "source_cas_conflict", None
    categories = context(source)
    if not categories:
        return "unsupported_native_context", None
    if identity(source) in SUFFIX_IDENTITIES:
        matches = [row for row in catalog if identity(row) == (source["name"][:-3], categories, source["unit"])]
        if len(matches) == 1:
            return SUFFIX_METHOD, {"target": target_row(matches[0])}
    matching_context = [
        row
        for row in rows
        if row["Elementary Name v2.2"]
        and legacy_context(row) == categories
        and publisher_context(row) == categories
        and normalize_unit(row["Unit"]) == source["unit"]
    ]
    matches = [
        row
        for row in matching_context
        if row["Elementary Name v2.2"] == source["name"]
        and (not cas or not row["CAS"] or cas == {str(row["CAS"]).lstrip("0")})
    ]
    method = CONTEXT_METHOD
    if not matches:
        if source["categories"][0] == "resources" or QUALIFIER.search(source["name"]) or REGION.search(source["name"]):
            return "source_identity_requires_manual_review", None
        if len(cas) != 1 or not valid_cas(next(iter(cas))):
            return "no_unique_valid_source_cas", None
        # A molecular CAS alone does not identify fossil origin, mixture/TEQ basis,
        # geographic qualifiers or chemical speciation. Reject those cases.
        matches = [
            row
            for row in matching_context
            if str(row["CAS"]).lstrip("0") in cas
            and row["Formula"]
            and not QUALIFIER.search(row["Elementary Name v2.2"] + " " + row["Elementary Flow Name v3"])
            and not REGION.search(row["Elementary Name v2.2"] + " " + row["Elementary Flow Name v3"])
            and (not metadata["formulas"] or metadata["formulas"] == {row["Formula"]})
        ]
        method = CAS_METHOD
    uuids = {row["UUID Elementary Flow v3"] for row in matches}
    if len(uuids) != 1:
        return "publisher_uuid_missing_or_ambiguous", None
    targets = [row for row in catalog if row.get("flow_id") == next(iter(uuids))]
    if len(targets) != 1 or targets[0]["unit"] != source["unit"] or tuple(targets[0]["categories"]) != categories:
        return "current_uuid_missing_or_incompatible", None
    target = targets[0]
    if method == CAS_METHOD and (QUALIFIER.search(target["name"]) or REGION.search(target["name"])):
        return "target_identity_requires_manual_review", None
    return method, {"target": target_row(target), "publisher_rows": sorted(matches, key=lambda row: row["row"])}


def validate_decision(decision, exact_targets, target_uuids):
    source, target = decision["source"], decision["target"]
    if decision.get("conversion_factor", 1) != 1:
        raise ValueError("Follow-up correspondences cannot change quantity units.")
    metadata = {"cas": set(decision["source_cas"]), "formulas": set(decision["source_formulas"])}
    rows = decision.get("publisher_rows", [])
    method, expected = candidate(source, metadata, rows, list(exact_targets.values()))
    if method != decision["reason"] or expected is None or expected["target"] != target:
        raise ValueError("Follow-up identity fails independent evidence checks.")
    coarsens = source["categories"] in (["emissions to water", "lake"], ["emissions to water", "river"])
    if bool(decision.get("biosphere_information_loss")) != coarsens:
        raise ValueError("Follow-up compartment coarsening must be reported.")


def apply_followup(resources, path, catalog_directory):
    report = json.loads(path.read_text())
    if report["sources"]["initial_review_sha256"] != digest(INITIAL_REVIEW):
        raise ValueError("Initial biosphere review digest mismatch.")
    protected = {
        identity(row["source"])
        for row in json.loads(INITIAL_REVIEW.read_text())["decisions"]
        if row["reason"] == "source_cas_conflict"
    }
    if any("target" in row and identity(row["source"]) in protected for row in report["decisions"]):
        raise ValueError("Follow-up cannot override previously identified CAS conflicts.")
    return apply_review(
        resources,
        path,
        catalog_directory,
        decision_validator=validate_decision,
        provenance_key="biosphere_followup_review",
        additional_evidence=(GUIDANCE_URL,),
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("workbook", type=Path)
    parser.add_argument("--output", type=Path, default=REPORT)
    args = parser.parse_args()
    from scripts.generate_uvek_2026_resources import build_resources

    directory = ROOT / "brightpath/data/export/reference_catalogs"
    _catalog, resources = build_resources(args.archive, directory, biosphere_followup=None)
    baseline = next(
        row for row in resources if row["axis"] == "biosphere" and row["source_profiles"][0]["family"] == "uvek"
    )
    initial = json.loads(INITIAL_REVIEW.read_text())
    protected = {identity(row["source"]) for row in initial["decisions"] if row["reason"] == "source_cas_conflict"}
    metadata, rows = native_metadata(args.archive), workbook_rows(args.workbook)
    target_path = directory / "ecoinvent__3.10__cutoff.json"
    catalog = json.loads(target_path.read_text())["biosphere"]
    decisions = []
    for source in baseline["unresolved"]:
        meta = metadata[identity(source)]
        reason, match = candidate(source, meta, rows, catalog, protected=identity(source) in protected)
        decision = {
            "source": source,
            "source_cas": sorted(meta["cas"]),
            "source_formulas": sorted(meta["formulas"]),
            "uses": meta["uses"],
            "reason": reason,
        }
        if match:
            decision.update(match)
            if source["categories"] in (["emissions to water", "lake"], ["emissions to water", "river"]):
                decision["biosphere_information_loss"] = (
                    "The target surface-water compartment does not retain the source lake/river distinction."
                )
        decisions.append(decision)
    report = {
        "schema_version": 1,
        "generator": "scripts/review_uvek_2026_biosphere_followup.py",
        "direction": baseline["name"],
        "sources": {
            **deepcopy(initial["sources"]),
            "native_archive_sha256": digest(args.archive),
            "target_catalog_sha256": digest(target_path),
            "initial_review_sha256": digest(INITIAL_REVIEW),
            "baseline_forward_sha256": hashlib.sha256(dumps_migration_resource(baseline).encode()).hexdigest(),
            "baseline_coverage": baseline["coverage"],
            "mapping_guidance": GUIDANCE_URL,
        },
        "summary": {
            "reviewed_identities": len(decisions),
            "accepted_identities": sum("target" in row for row in decisions),
            "remaining_unresolved": sum("target" not in row for row in decisions),
            "accepted_exchange_uses": sum(row["uses"] for row in decisions if "target" in row),
            "information_loss_rules": sum("biosphere_information_loss" in row for row in decisions),
            "mapping_methods": dict(sorted(Counter(row["reason"] for row in decisions if "target" in row).items())),
            "blocked_reasons": dict(sorted(Counter(row["reason"] for row in decisions if "target" not in row).items())),
        },
        "decisions": decisions,
    }
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False) + "\n")
    with args.output.with_suffix(".csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "status",
                "name",
                "categories",
                "unit",
                "cas",
                "exchange_occurrences",
                "reason",
                "target_name",
                "target_categories",
                "uuid",
                "information_loss",
            ],
        )
        writer.writeheader()
        for row in sorted(decisions, key=lambda row: (-row["uses"], identity(row["source"]))):
            source, target = row["source"], row.get("target", {})
            writer.writerow(
                {
                    "status": "accepted" if target else "blocked",
                    "name": source["name"],
                    "categories": " / ".join(source["categories"]),
                    "unit": source["unit"],
                    "cas": " | ".join(row["source_cas"]),
                    "exchange_occurrences": row["uses"],
                    "reason": row["reason"],
                    "target_name": target.get("name", ""),
                    "target_categories": " / ".join(target.get("categories", [])),
                    "uuid": target.get("uuid", ""),
                    "information_loss": row.get("biosphere_information_loss", ""),
                }
            )
    print(json.dumps(report["summary"], sort_keys=True))


if __name__ == "__main__":
    main()
