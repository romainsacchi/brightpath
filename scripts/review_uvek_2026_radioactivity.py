"""Review exact publisher radioactivity correspondences with explicit SI scaling."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import dumps_migration_resource
from scripts.review_uvek_2026_biosphere import ROOT, apply_review, digest, identity, workbook_rows
from scripts.review_uvek_2026_biosphere_followup import CONTEXT_METHOD
from scripts.review_uvek_2026_biosphere_followup import REPORT as PARENT_REVIEW
from scripts.review_uvek_2026_biosphere_followup import candidate as publisher_candidate

REPORT = ROOT / "docs/uvek_2026_radioactivity_review.json"
METHOD = "publisher_becquerel_to_kilobecquerel"
SI_URL = "https://www.bipm.org/en/publications/si-brochure/"
UNIT_URL = "https://www.epa.gov/radiation/radiation-terms-and-units"
LOSS = "The target surface-water compartment does not retain the source lake/river distinction."


def candidate(source, metadata, rows, catalog):
    """Accept exact historical names and UUIDs, with the sole unit factor Bq/kBq."""
    if source["unit"] != "Becquerel":
        return "outside_becquerel_review", None
    reason, match = publisher_candidate({**source, "unit": "kilo Becquerel"}, metadata, rows, catalog)
    # CAS synonyms are outside this batch. Preserve the named nuclide (including
    # its metastable suffix), or the publisher's exact radioactive group label.
    if reason != CONTEXT_METHOD or match is None:
        return reason if match is None else "historical_name_change_requires_review", None
    return METHOD, {**match, "conversion_factor": 0.001}


def validate_decision(decision, exact_targets, target_uuids):
    """Verify identity, publisher context, current UUID and SI factor independently."""
    metadata = {"cas": set(decision["source_cas"]), "formulas": set(decision["source_formulas"])}
    reason, expected = candidate(decision["source"], metadata, decision["publisher_rows"], list(exact_targets.values()))
    if (
        reason != decision["reason"]
        or expected is None
        or expected["target"] != decision["target"]
        or decision.get("conversion_factor") != 0.001
    ):
        raise ValueError("Radioactivity correspondence fails exact publisher/SI checks.")
    coarsens = decision["source"]["categories"] in (
        ["emissions to water", "lake"],
        ["emissions to water", "river"],
    )
    if decision.get("biosphere_information_loss") != (LOSS if coarsens else None):
        raise ValueError("Radioactivity compartment coarsening must be reported.")


def apply_radioactivity_review(resources, path, catalog_directory):
    report = json.loads(path.read_text())
    if report["sources"]["parent_review_sha256"] != digest(PARENT_REVIEW):
        raise ValueError("Radioactivity parent review digest mismatch.")
    return apply_review(
        resources,
        path,
        catalog_directory,
        decision_validator=validate_decision,
        provenance_key="radioactivity_review",
        additional_evidence=(SI_URL, UNIT_URL),
        quantity_limitation="Only exact Becquerel to kilo Becquerel correspondences use the SI factor 0.001.",
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("workbook", type=Path)
    parser.add_argument("--output", type=Path, default=REPORT)
    args = parser.parse_args()
    from scripts.generate_uvek_2026_resources import build_resources

    directory = ROOT / "brightpath/data/export/reference_catalogs"
    _catalog, resources = build_resources(args.archive, directory, radioactivity_review=None)
    baseline = next(
        row for row in resources if row["axis"] == "biosphere" and row["source_profiles"][0]["family"] == "uvek"
    )
    parent = json.loads(PARENT_REVIEW.read_text())
    if digest(args.archive) != parent["sources"]["native_archive_sha256"]:
        raise ValueError("Native archive differs from the reviewed release.")
    metadata = {identity(row["source"]): row for row in parent["decisions"] if "target" not in row}
    rows = workbook_rows(args.workbook)
    target_path = directory / "ecoinvent__3.10__cutoff.json"
    catalog = json.loads(target_path.read_text())["biosphere"]
    decisions = []
    for source in baseline["unresolved"]:
        meta = metadata[identity(source)]
        reason, match = candidate(
            source, {"cas": set(meta["source_cas"]), "formulas": set(meta["source_formulas"])}, rows, catalog
        )
        if source["unit"] != "Becquerel":
            reason = meta["reason"]
        decision = {
            "source": source,
            "source_cas": meta["source_cas"],
            "source_formulas": meta["source_formulas"],
            "uses": meta["uses"],
            "reason": reason,
        }
        if match:
            decision.update(match)
            if source["categories"] in (["emissions to water", "lake"], ["emissions to water", "river"]):
                decision["biosphere_information_loss"] = LOSS
        decisions.append(decision)
    report = {
        "schema_version": 1,
        "generator": "scripts/review_uvek_2026_radioactivity.py",
        "direction": baseline["name"],
        "sources": {
            **deepcopy(parent["sources"]),
            "parent_review_sha256": digest(PARENT_REVIEW),
            "baseline_forward_sha256": hashlib.sha256(dumps_migration_resource(baseline).encode()).hexdigest(),
            "baseline_coverage": baseline["coverage"],
            "si_units": SI_URL,
            "kilobecquerel_definition": UNIT_URL,
        },
        "summary": {
            "reviewed_identities": len(decisions),
            "accepted_identities": sum("target" in row for row in decisions),
            "remaining_unresolved": sum("target" not in row for row in decisions),
            "accepted_exchange_uses": sum(row["uses"] for row in decisions if "target" in row),
            "information_loss_rules": sum("biosphere_information_loss" in row for row in decisions),
            "blocked_reasons": dict(sorted(Counter(row["reason"] for row in decisions if "target" not in row).items())),
        },
        "decisions": decisions,
    }
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False) + "\n")
    with args.output.with_suffix(".csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "status",
                "name",
                "categories",
                "unit",
                "exchange_occurrences",
                "reason",
                "target_name",
                "target_categories",
                "target_unit",
                "uuid",
                "conversion_factor",
                "information_loss",
            ]
        )
        for row in sorted(decisions, key=lambda row: (-row["uses"], identity(row["source"]))):
            source, target = row["source"], row.get("target", {})
            writer.writerow(
                [
                    "accepted" if target else "blocked",
                    source["name"],
                    " / ".join(source["categories"]),
                    source["unit"],
                    row["uses"],
                    row["reason"],
                    target.get("name", ""),
                    " / ".join(target.get("categories", [])),
                    target.get("unit", ""),
                    target.get("uuid", ""),
                    row.get("conversion_factor", ""),
                    row.get("biosphere_information_loss", ""),
                ]
            )
    print(json.dumps(report["summary"], sort_keys=True))


if __name__ == "__main__":
    main()
