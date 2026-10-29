"""Review unresolved native UVEK biosphere links against pinned publisher evidence.

Write reference metadata and dispositions only. Never copy inventory amounts.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter, defaultdict
from copy import deepcopy
from pathlib import Path
from zipfile import ZipFile

from lxml import etree

from brightpath.units import normalize_unit

ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "docs/uvek_2026_biosphere_review.json"
WORKBOOK_URL = (
    "https://support.ecoinvent.org/hubfs/Knowledge%20Base/Database/Releases/"
    "correspondence_file_elementary-exchanges_v2.2-v3.0_20130904.xls"
)
WORKBOOK_SHA256 = "1656bd66bd69610e8f76a80949ba9a19f20b7ef4a33b477fd28dc324a3a2cc65"
CHANGE_REPORT_URL = (
    "https://support.ecoinvent.org/hubfs/Knowledge%20Base/Database/Releases/"
    "report_of_changes_ecoinvent_2.2_to_3.0_20130904.pdf"
)
WATER_REPORT_URL = (
    "https://support.ecoinvent.org/hubfs/Knowledge%20Base/Database/Releases/"
    "Change%20Report%20v3.10%20-%2020231214.pdf"
)
MAIN_ALIASES = {
    "emissions to air": "air",
    "emissions to water": "water",
    "emissions to soil": "soil",
    "resources": "resource",
}
SUB_ALIASES = {
    "high. pop.": "high population density",
    "low. pop.": "low population density",
    "low. pop., long-term": "low population density, long-term",
    "stratosphere + troposphere": "lower stratosphere + upper troposphere",
}
HEADERS = (
    "DS ID v2.2",
    "Elementary Name v2.2",
    "Category v2.2",
    "SubCategory v2.2",
    "CAS",
    "Formula",
    "Unit",
    "UUID Elementary Flow v3",
    "Elementary Flow Name v3",
    "Compartment v3",
    "Subcompartment v3",
    "Synonyms",
)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def identity(row):
    return row["name"], tuple(row["categories"]), row["unit"]


def native_metadata(archive):
    """Collect source identity/CAS evidence and usage counts without amounts."""
    metadata = defaultdict(lambda: {"cas": set(), "formulas": set(), "uses": 0})
    with ZipFile(archive) as zipped:
        for filename in sorted(zipped.namelist()):
            if not filename.lower().endswith(".xml") or Path(filename).name == "ElementaryFlows.xml":
                continue
            parser = etree.XMLParser(resolve_entities=False, load_dtd=False, no_network=True)
            root = etree.fromstring(zipped.read(filename), parser=parser)
            if root.getroottree().docinfo.doctype:
                raise ValueError("DTD declarations are not supported.")
            for element in root.iter():
                if not isinstance(element.tag, str) or etree.QName(element).localname != "exchange":
                    continue
                groups = [
                    child.text for child in element if etree.QName(child).localname in {"inputGroup", "outputGroup"}
                ]
                if "4" not in groups:
                    continue
                key = (
                    element.get("name", "").strip(),
                    tuple(element.get(field) for field in ("category", "subCategory") if element.get(field)),
                    normalize_unit(element.get("unit", "")),
                )
                metadata[key]["uses"] += 1
                if element.get("CASNumber"):
                    metadata[key]["cas"].add(element.get("CASNumber").lstrip("0"))
                if element.get("formula"):
                    metadata[key]["formulas"].add(element.get("formula").strip())
    return metadata


def workbook_rows(path):
    """Read only the publisher workbook bound to this review."""
    import xlrd

    if digest(path) != WORKBOOK_SHA256:
        raise ValueError("Publisher elementary-flow workbook digest mismatch.")
    sheet = xlrd.open_workbook(path).sheet_by_name("ElementaryExchanges")
    if tuple(sheet.row_values(0)) != HEADERS:
        raise ValueError("Unexpected publisher elementary-flow workbook headers.")
    return [
        {"row": index + 1, **dict(zip(HEADERS, sheet.row_values(index), strict=True))}
        for index in range(1, sheet.nrows)
    ]


def publisher_key(source):
    categories = source["categories"]
    subcategory = categories[1] if len(categories) == 2 else "unspecified"
    return (
        source["name"],
        MAIN_ALIASES.get(categories[0], categories[0]),
        SUB_ALIASES.get(subcategory, subcategory),
        source["unit"],
    )


def publisher_candidate(source, metadata, rows, targets):
    """Require one source row, compatible CAS, one current UUID, units and context."""
    matches = rows.get(publisher_key(source), ())
    if len(matches) != 1:
        return "publisher_source_missing_or_ambiguous", None
    row = matches[0]
    if row_source_key(row) != publisher_key(source):
        return "publisher_source_mismatch", None
    source_cas = metadata["cas"]
    publisher_cas = str(row["CAS"]).lstrip("0")
    if len(source_cas) > 1 or (source_cas and publisher_cas and source_cas != {publisher_cas}):
        return "source_cas_conflict", None
    current = targets.get(row["UUID Elementary Flow v3"], ())
    if len(current) != 1:
        return "current_uuid_missing_or_ambiguous", None
    target = current[0]
    categories = [
        value for value in (row["Compartment v3"], row["Subcompartment v3"]) if value not in ("", "unspecified")
    ]
    if target["categories"] != categories:
        return "current_compartment_changed", None
    if source["unit"] != target["unit"]:
        return "unit_change_requires_separate_review", None
    return "publisher_elementary_uuid_correspondence", {
        "target": {
            "name": target["name"],
            "categories": target["categories"],
            "unit": target["unit"],
            "uuid": target["flow_id"],
        },
        "publisher_row": row,
    }


def row_source_key(row):
    return (
        row["Elementary Name v2.2"],
        row["Category v2.2"].lower(),
        row["SubCategory v2.2"].lower(),
        normalize_unit(row["Unit"]),
    )


def validate_decision(decision, exact_targets, target_uuids):
    """Check authored decisions independently before adding executable rules."""
    source, target = decision["source"], decision["target"]
    exact = exact_targets.get(identity(target))
    if exact is None or exact["flow_id"] != target.get("uuid"):
        raise ValueError("Reviewed target disagrees with the exact catalog UUID.")
    method = decision["reason"]
    if method == "publisher_elementary_uuid_correspondence":
        reason, candidate = publisher_candidate(
            source,
            {"cas": set(decision["source_cas"])},
            {publisher_key(source): [decision["publisher_row"]]},
            target_uuids,
        )
        if reason != method or candidate["target"] != target or "conversion_factor" in decision:
            raise ValueError("Publisher correspondence fails independent checks.")
    elif method == "reviewed_lake_river_collapse":
        if (
            source["categories"] not in (["emissions to water", "lake"], ["emissions to water", "river"])
            or target["categories"] != ["water", "surface water"]
            or source["name"] != target["name"]
            or source["unit"] != target["unit"]
            or len(decision["source_cas"]) > 1
            or "conversion_factor" in decision
        ):
            raise ValueError("Unsupported compartment collapse.")
    elif method == "reviewed_water_mass_volume":
        subcategory = {"groundwater": "ground-", "ocean": "ocean", "river": "surface water", "unspecified": ""}.get(
            source["categories"][-1]
        )
        categories = ["water", subcategory] if subcategory else ["water"]
        if (
            source["name"] != "Water"
            or target["name"] != "Water"
            or source["categories"][0] != "emissions to water"
            or subcategory is None
            or target["categories"] != categories
            or (source["unit"], target["unit"]) != ("kilogram", "cubic meter")
            or decision["source_cas"] != ["7732-18-5"]
            or decision.get("conversion_factor") != 0.001
        ):
            raise ValueError("Unsupported water mass/volume conversion.")
    else:
        raise ValueError("Unknown reviewed biosphere mapping method.")
    loses_compartment = source["categories"] in (["emissions to water", "lake"], ["emissions to water", "river"])
    if bool(decision.get("biosphere_information_loss")) != loses_compartment:
        raise ValueError("Compartment coarsening needs an explicit information-loss finding.")


def apply_review(
    resources,
    path,
    catalog_directory,
    *,
    decision_validator=validate_decision,
    provenance_key="biosphere_review",
    additional_evidence=(),
    quantity_limitation="Only the reviewed pure-water rules use the 1000 kg/m3 LCA density convention.",
):
    """Apply a source-bound forward review without modifying any prior rule or reverse edge."""
    from brightpath.migrations.schema import dumps_migration_resource, intern_review, rule_identifier

    report = json.loads(path.read_text())
    if (
        report.get("schema_version") != 1
        or report.get("direction") != "uvek-2026-biosphere-to-ecoinvent-3.10-biosphere"
    ):
        raise ValueError("Unsupported UVEK biosphere review.")
    result = deepcopy(resources)
    resource = next(
        row for row in result if row["axis"] == "biosphere" and row["source_profiles"][0]["family"] == "uvek"
    )
    sources = report["sources"]
    if sources["baseline_forward_sha256"] != hashlib.sha256(dumps_migration_resource(resource).encode()).hexdigest():
        raise ValueError("UVEK biosphere review baseline mismatch.")
    if sources["native_archive_sha256"] != resource["provenance"]["sha256"]:
        raise ValueError("UVEK biosphere review archive mismatch.")
    if sources["publisher_workbook"]["sha256"] != WORKBOOK_SHA256:
        raise ValueError("UVEK biosphere review publisher digest mismatch.")
    catalog_path = catalog_directory / "ecoinvent__3.10__cutoff.json"
    if sources["target_catalog_sha256"] != digest(catalog_path):
        raise ValueError("UVEK biosphere review target catalog mismatch.")
    catalog = json.loads(catalog_path.read_text())
    exact_targets = {identity(row): row for row in catalog["biosphere"]}
    target_uuids = defaultdict(list)
    for target in catalog["biosphere"]:
        target_uuids[target["flow_id"]].append(target)
    expected = {identity(row) for row in resource["unresolved"]}
    seen, accepted = set(), set()
    review_digest = digest(path)
    review = intern_review(
        resource,
        {
            "rationale": "Publisher-backed reference-flow correspondence with exact source and target catalogs; no LCIA equivalence is asserted.",
            "evidence": [
                f"{path.name} SHA-256 {review_digest}",
                sources["publisher_workbook"]["url"],
                sources["compartment_merge"]["url"],
                sources["water_convention"]["url"],
                *additional_evidence,
            ],
            "limitations": [
                "Forward only; no lake/river allocation is inferred in reverse.",
                quantity_limitation,
                "Conflicting CAS identities and unmatched flows remain blocked.",
            ],
        },
    )
    for decision in report["decisions"]:
        key = identity(decision["source"])
        if key in seen or key not in expected:
            raise ValueError("Biosphere review has duplicate or unexpected sources.")
        seen.add(key)
        if "target" not in decision:
            continue
        decision_validator(decision, exact_targets, target_uuids)
        rule = {
            "id": rule_identifier(resource["name"], decision["source"]),
            "source": decision["source"],
            "target": decision["target"],
            "classification": "correspondence",
            "mapping_method": decision["reason"],
            "review_ref": review,
            "report_correspondence": True,
        }
        for field in ("conversion_factor", "biosphere_information_loss"):
            if field in decision:
                rule[field] = decision[field]
        resource["replace"].append(rule)
        accepted.add(key)
    if seen != expected or len(accepted) != report["summary"]["accepted_identities"]:
        raise ValueError("Biosphere review does not completely disposition the unresolved catalog.")
    resource["replace"].sort(key=lambda row: identity(row["source"]))
    resource["unresolved"] = [row for row in resource["unresolved"] if identity(row) not in accepted]
    resource["coverage"].update(
        mapped_identities=len(resource["replace"]),
        unresolved_identities=len(resource["unresolved"]),
        mapping_methods=dict(sorted(Counter(row["mapping_method"] for row in resource["replace"]).items())),
        directional_additions=resource["coverage"].get("directional_additions", 0) + len(accepted),
        information_loss_rules=sum("biosphere_information_loss" in row for row in resource["replace"]),
    )
    resource["provenance"] = {
        **resource["provenance"],
        provenance_key: {"file": path.name, "sha256": review_digest, "sources": sources},
    }
    if not any(row["name"] == "Source publisher terms" for row in resource["licenses"]):
        resource["licenses"].append(
            {
                "name": "Source publisher terms",
                "scope": "ecoinvent correspondence reference metadata; no standalone reuse license is asserted",
                "path": WORKBOOK_URL,
            }
        )
    return result


def build_review(baseline, catalog, source_catalog, metadata, rows):
    """Disposition every unresolved source; accept no fuzzy names or CAS conflicts."""
    rows_by_source, targets = defaultdict(list), defaultdict(list)
    for row in rows:
        rows_by_source[row_source_key(row)].append(row)
    for row in catalog["biosphere"]:
        if row.get("flow_id"):
            targets[row["flow_id"]].append(row)
    exact_targets = {identity(row): row for row in catalog["biosphere"]}
    collapse = {}
    for group in baseline["provenance"]["biosphere_matching"]["ambiguous"]:
        target = group["ecoinvent"]
        for source in group["native_candidates"]:
            # Land-class mergers need a separate interpretation of the UVEK land uses.
            if source["categories"] not in (["emissions to water", "lake"], ["emissions to water", "river"]):
                continue
            if source["name"] != target["name"] or source["unit"] != target["unit"]:
                continue
            current = exact_targets[identity(target)]
            collapse[identity(source)] = {**target, "uuid": current["flow_id"]}
    decisions = []
    for source in baseline["unresolved"]:
        source_metadata = metadata[identity(source)]
        reason, candidate = publisher_candidate(source, source_metadata, rows_by_source, targets)
        if (
            candidate is None
            and reason != "source_cas_conflict"
            and identity(source) in collapse
            and len(source_metadata["cas"]) <= 1
        ):
            reason, candidate = "reviewed_lake_river_collapse", {"target": collapse[identity(source)]}
        # Pure water only: explicit LCA water-density convention, not a general mass/volume alias.
        if (
            candidate is None
            and reason != "source_cas_conflict"
            and source["name"] == "Water"
            and source["unit"] == "kilogram"
            and source_metadata["cas"] == {"7732-18-5"}
            and source["categories"][0] == "emissions to water"
        ):
            subcategory = {"groundwater": "ground-", "river": "surface water", "unspecified": ""}.get(
                source["categories"][1], source["categories"][1]
            )
            key = ("Water", ("water", subcategory) if subcategory else ("water",), "cubic meter")
            if key in exact_targets:
                target = exact_targets[key]
                reason, candidate = "reviewed_water_mass_volume", {
                    "target": {
                        "name": target["name"],
                        "categories": target["categories"],
                        "unit": target["unit"],
                        "uuid": target["flow_id"],
                    },
                    "conversion_factor": 0.001,
                }
        decision = {
            "source": source,
            "source_cas": sorted(source_metadata["cas"]),
            "uses": source_metadata["uses"],
            "reason": reason,
        }
        if candidate is not None:
            decision.update(candidate)
            if source["categories"] in (["emissions to water", "lake"], ["emissions to water", "river"]):
                decision["biosphere_information_loss"] = (
                    "The target surface-water compartment does not retain the source lake/river distinction."
                )
        decisions.append(decision)
    return {
        "schema_version": 1,
        "generator": "scripts/review_uvek_2026_biosphere.py",
        "direction": "uvek-2026-biosphere-to-ecoinvent-3.10-biosphere",
        "summary": {
            "reviewed_identities": len(decisions),
            "accepted_identities": sum("target" in row for row in decisions),
            "remaining_unresolved": sum("target" not in row for row in decisions),
            "accepted_exchange_uses": sum(row["uses"] for row in decisions if "target" in row),
            "mapping_methods": dict(sorted(Counter(row["reason"] for row in decisions if "target" in row).items())),
            "blocked_reasons": dict(sorted(Counter(row["reason"] for row in decisions if "target" not in row).items())),
        },
        "decisions": decisions,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("workbook", type=Path)
    parser.add_argument("--output", type=Path, default=REPORT)
    args = parser.parse_args()
    from brightpath.migrations.schema import dumps_migration_resource
    from scripts.generate_uvek_2026_resources import build_resources

    directory = ROOT / "brightpath/data/export/reference_catalogs"
    source_catalog, resources = build_resources(args.archive, directory, biosphere_review=None)
    baseline = next(
        row for row in resources if row["axis"] == "biosphere" and row["source_profiles"][0]["family"] == "uvek"
    )
    catalog_path = directory / "ecoinvent__3.10__cutoff.json"
    metadata = native_metadata(args.archive)
    if set(metadata) != {identity(row) for row in source_catalog["biosphere"]}:
        raise ValueError("Native metadata does not cover the exact source catalog.")
    report = build_review(
        baseline, json.loads(catalog_path.read_text()), source_catalog, metadata, workbook_rows(args.workbook)
    )
    report["sources"] = {
        "native_archive_sha256": digest(args.archive),
        "target_catalog_sha256": digest(catalog_path),
        "baseline_forward_sha256": hashlib.sha256(dumps_migration_resource(baseline).encode()).hexdigest(),
        "publisher_workbook": {"url": WORKBOOK_URL, "sha256": WORKBOOK_SHA256, "sheet": "ElementaryExchanges"},
        "compartment_merge": {"url": CHANGE_REPORT_URL, "section": "8.1"},
        "water_convention": {"url": WATER_REPORT_URL, "page": 94, "density_kg_per_m3": 1000},
    }
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False) + "\n")
    with args.output.with_suffix(".csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "status",
                "source_name",
                "source_categories",
                "source_unit",
                "source_cas",
                "exchange_occurrences",
                "reason",
                "target_name",
                "target_categories",
                "target_unit",
                "target_uuid",
                "conversion_factor",
                "information_loss",
            ],
        )
        writer.writeheader()
        for decision in sorted(report["decisions"], key=lambda row: (-row["uses"], identity(row["source"]))):
            source, target = decision["source"], decision.get("target", {})
            writer.writerow(
                {
                    "status": "accepted" if target else "blocked",
                    "source_name": source["name"],
                    "source_categories": " / ".join(source["categories"]),
                    "source_unit": source["unit"],
                    "source_cas": " | ".join(decision["source_cas"]),
                    "exchange_occurrences": decision["uses"],
                    "reason": decision["reason"],
                    "target_name": target.get("name", ""),
                    "target_categories": " / ".join(target.get("categories", [])),
                    "target_unit": target.get("unit", ""),
                    "target_uuid": target.get("uuid", ""),
                    "conversion_factor": decision.get("conversion_factor", ""),
                    "information_loss": decision.get("biosphere_information_loss", ""),
                }
            )
    print(json.dumps(report["summary"], sort_keys=True))


if __name__ == "__main__":
    main()
