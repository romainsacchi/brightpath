"""Build UVEK 2026 reference identities and explicit bidirectional migrations.

Read the original ecoSpold1 ZIP without importing a Brightway database. Only
reference metadata is written; exchange amounts and full inventories stay local.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import Counter, defaultdict
from copy import deepcopy
from pathlib import Path
from zipfile import ZipFile

from lxml import etree

from brightpath.migrations.schema import dumps_migration_resource, intern_review, rule_identifier
from brightpath.units import normalize_unit

GENERATOR = "scripts/generate_uvek_2026_resources.py"
TECH_FIELDS = ("name", "reference product", "location", "unit")
DEFAULT_UNIT_REVIEW = Path(__file__).resolve().parents[1] / "docs/uvek_2026_unit_review.json"
DEFAULT_BIOSPHERE_REVIEW = Path(__file__).resolve().parents[1] / "docs/uvek_2026_biosphere_review.json"
DEFAULT_BIOSPHERE_FOLLOWUP = Path(__file__).resolve().parents[1] / "docs/uvek_2026_biosphere_followup.json"
DEFAULT_RADIOACTIVITY_REVIEW = Path(__file__).resolve().parents[1] / "docs/uvek_2026_radioactivity_review.json"
DEFAULT_LAND_REVIEW = Path(__file__).resolve().parents[1] / "docs/uvek_2026_land_review.json"
DEFAULT_PROCESS_REVIEW = Path(__file__).resolve().parents[1] / "docs/uvek_2026_process_review.json"
UNIT_REVIEW_CHECKS = {
    "same_source_filename",
    "same_source_uuid",
    "same_process_number",
    "same_name_product_location",
    "target_identity_matches_catalog",
    "reference_output_amount_unchanged",
    "technosphere_flow_ids_and_coefficients_unchanged",
    "technosphere_names_and_resolved_locations_unchanged",
}


def read_archive(path: Path) -> dict:
    """Extract native, unit-normalized reference identities from an ecoSpold1 ZIP.

    Both namespaced and unqualified ecoSpold1 XML are accepted. Original flow
    categories, including ``unspecified``, are retained. No UUIDs are invented.
    """
    technosphere, biosphere = {}, set()
    count = 0
    with ZipFile(path) as archive:
        for name in sorted(archive.namelist()):
            if not name.lower().endswith(".xml") or Path(name).name == "ElementaryFlows.xml":
                continue
            parser = etree.XMLParser(resolve_entities=False, load_dtd=False, no_network=True)
            root = etree.fromstring(archive.read(name), parser=parser)
            if root.getroottree().docinfo.doctype:
                raise ValueError(f"DTD declarations are not supported: {name}")
            if etree.QName(root).localname != "ecoSpold":
                raise ValueError(f"Not an ecoSpold1 document: {name}")
            for element in root.iter():
                if isinstance(element.tag, str):
                    element.tag = etree.QName(element).localname
            for dataset in root.findall("dataset"):
                reference = dataset.find("metaInformation/processInformation/referenceFunction")
                geography = dataset.find("metaInformation/processInformation/geography")
                if reference is None or geography is None:
                    raise ValueError(f"Missing process metadata: {name}")
                exchanges = dataset.findall("flowData/exchange")
                outputs = [exchange for exchange in exchanges if exchange.findtext("outputGroup") == "0"]
                if len(outputs) != 1 or dataset.findall("flowData/allocation"):
                    raise ValueError(f"Expected a single unallocated reference output: {name}")
                identity = (
                    reference.get("name", "").strip(),
                    outputs[0].get("name", "").strip(),
                    geography.get("location", ""),
                    normalize_unit(reference.get("unit", "")),
                )
                if not all(identity) or normalize_unit(outputs[0].get("unit", "")) != identity[3]:
                    raise ValueError(f"Incomplete or inconsistent reference output: {name}")
                if identity in technosphere:
                    raise ValueError(f"Duplicate process identity: {identity!r}")
                technosphere[identity] = {"file": name, "number": dataset.get("number", "")}
                count += 1
                for exchange in exchanges:
                    if "4" not in (exchange.findtext("inputGroup"), exchange.findtext("outputGroup")):
                        continue
                    categories = tuple(
                        exchange.get(field) for field in ("category", "subCategory") if exchange.get(field)
                    )
                    flow = (exchange.get("name", "").strip(), categories, normalize_unit(exchange.get("unit", "")))
                    if not all(flow):
                        raise ValueError(f"Incomplete biosphere identity: {name}")
                    biosphere.add(flow)
    if not count or not biosphere:
        raise ValueError("The archive must contain processes and native biosphere exchanges.")
    return {
        "schema_version": 2,
        "profile": {"family": "uvek", "version": "2026", "system_model": "cutoff"},
        "provenance": {
            "source": path.name,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "format": "ecospold1",
            "generator": GENERATOR,
            "process_datasets": count,
            "biosphere": "Native UVEK 2026 names and categories; units normalized only.",
            "license": "Source archive terms apply; no license declaration supplied in the archive.",
        },
        "technosphere": [
            dict(zip(("name", "reference_product", "location", "unit"), identity, strict=True))
            for identity in sorted(technosphere)
        ],
        "biosphere": [bio_row(identity) for identity in sorted(biosphere)],
    }


def bio_row(identity: tuple) -> dict:
    return {"name": identity[0], "categories": list(identity[1]), "unit": identity[2]}


def identities(catalog: dict, axis: str) -> set[tuple]:
    if axis == "technosphere":
        return {tuple(row[key] for key in ("name", "reference_product", "location", "unit")) for row in catalog[axis]}
    return {(row["name"], tuple(row["categories"]), row["unit"]) for row in catalog[axis]}


def _process_label(identity: tuple) -> tuple:
    """Remove only the legacy location suffix repeated in two 2025 names."""
    location = re.escape(identity[2])
    return (
        tuple(re.sub(rf" \{{{location}\}}(?: U)? - {location}$", "", value) for value in identity[:2]) + identity[2:3]
    )


def technosphere_pairs(old: set[tuple], new: set[tuple]) -> list[tuple]:
    """Match exact identities, then unique labels at unchanged locations.

    Unit changes remain explicit rules without a factor so the runtime's strict
    missing-factor policy rejects their use. No generic year/hour or gas-volume
    conversion is assumed.
    """
    pairs = [(identity, identity, "exact_identity", None) for identity in sorted(old & new)]
    left, right = defaultdict(list), defaultdict(list)
    for identity in sorted(old - new):
        left[_process_label(identity)].append(identity)
    for identity in sorted(new - old):
        right[_process_label(identity)].append(identity)
    for label in sorted(left.keys() & right.keys()):
        if len(left[label]) == len(right[label]) == 1:
            source, target = left[label][0], right[label][0]
            method = "unit_change_requires_factor" if source[3] != target[3] else "legacy_location_suffix"
            aliases = {"person kilometer": "person-kilometer", "kmy": "kilometer-year"}
            factor = None
            if source[3] != target[3] and aliases.get(source[3], source[3]) == aliases.get(target[3], target[3]):
                method, factor = "unit_spelling_alias", 1.0
            pairs.append((source, target, method, factor))
    return sorted(pairs)


def biosphere_pairs(native: set[tuple], ecoinvent: set[tuple]) -> tuple[list[tuple], dict]:
    """Match native compartments through explicit aliases and bw2io name rules.

    Retain only one-to-one matches for the two authored directions. Lake/river
    collapses and all other ambiguous correspondences remain unresolved.
    """
    from bw2io import __version__ as bw2io_version
    from bw2io.data import get_biosphere_2_3_name_migration_data

    name_rules = {}
    name_migration = get_biosphere_2_3_name_migration_data()
    for key, target in name_migration["data"]:
        if key[3] == "biosphere":
            name_rules[(key[0], tuple(key[1]), key[2])] = target
    main = {
        "emissions to air": "air",
        "emissions to water": "water",
        "emissions to soil": "soil",
        "resources": "natural resource",
    }
    sub = {
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
    candidates = defaultdict(list)
    for source in sorted(native):
        categories = source[1]
        categories = (main.get(categories[0], categories[0]),) + tuple(
            sub.get(value, value) for value in categories[1:] if value != "unspecified"
        )
        target = (source[0], categories, source[2])
        changes = name_rules.get(target, {})
        target = (changes.get("name", target[0]), categories, changes.get("unit", target[2]))
        if target in ecoinvent:
            candidates[target].append((source, changes.get("multiplier")))
    pairs, ambiguous = [], []
    for target, sources in sorted(candidates.items()):
        if len(sources) == 1:
            source, factor = sources[0]
            pairs.append((source, target, "native_compartment_and_bw2io_name", factor))
        else:
            ambiguous.append(
                {"ecoinvent": bio_row(target), "native_candidates": [bio_row(source) for source, _ in sources]}
            )
    return pairs, {
        "bw2io_version": str(bw2io_version),
        "source": "https://github.com/brightway-lca/brightway2-io/blob/main/bw2io/data/__init__.py",
        "function": "get_biosphere_2_3_name_migration_data",
        "name_migration_sha256": hashlib.sha256(json.dumps(name_migration, sort_keys=True).encode()).hexdigest(),
        "license": "BSD-3-Clause",
        "ambiguous": ambiguous,
    }


def migration_resource(axis, source_profile, target_profile, source_identities, target_identities, pairs, provenance):
    """Build a v3 directed resource and explicit unresolved dispositions."""
    kind = "cutoff" if axis == "technosphere" else "biosphere"
    source_id = f"{source_profile['family']}-{source_profile['version']}-{kind}"
    target_id = f"{target_profile['family']}-{target_profile['version']}-{kind}"
    resource = {
        "schema": "brightpath.migration",
        "schema_version": 3,
        "name": f"{source_id}-to-{target_id}",
        "status": "active",
        "axis": axis,
        "source_id": source_id,
        "target_id": target_id,
        "source_profiles": [source_profile],
        "target_profile": target_profile,
        "reverse_policy": "explicit_only",
        "replace": [],
        "disaggregate": [],
        "delete": [],
        "recipes": {},
        "reviews": {},
        "coverage": {},
        "provenance": provenance,
        "generator": GENERATOR,
        "licenses": [
            {"name": "BSD-3-Clause", "scope": "BrightPath generator, authored mappings and bw2io name migration data"}
        ],
    }
    review = intern_review(
        resource,
        {
            "rationale": "Reference identity correspondence only; background coefficients and LCIA results may change.",
            "evidence": ["Packaged UVEK 2025 catalog", "UVEK 2026 original ecoSpold1 archive identified by SHA-256"],
            "limitations": [
                "Unmatched and ambiguous identities are unresolved.",
                "Unit changes require a separately documented numeric factor.",
            ],
        },
    )
    row = (lambda identity: dict(zip(TECH_FIELDS, identity, strict=True))) if axis == "technosphere" else bio_row
    for source, target, method, factor in sorted(pairs):
        rule = {
            "id": rule_identifier(resource["name"], row(source)),
            "source": row(source),
            "target": row(target),
            "mapping_method": method,
            "classification": "correspondence",
            "review_ref": review,
        }
        if factor is not None:
            rule["conversion_factor"] = factor
        resource["replace"].append(rule)
    mapped = {source for source, *_ in pairs}
    resource["coverage"] = {
        "source_identities": len(source_identities),
        "target_identities": len(target_identities),
        "mapped_identities": len(mapped),
        "unresolved_identities": len(source_identities - mapped),
        "mapping_methods": dict(Counter(pair[2] for pair in pairs)),
        "unit_changes_without_factor": sum(
            source[-1] != target[-1] and factor is None for source, target, _, factor in pairs
        ),
    }
    resource["unresolved"] = [row(identity) for identity in sorted(source_identities - mapped)]
    return resource


def apply_unit_review(resources: list[dict], path: Path) -> list[dict]:
    """Copy release resources and apply only source-bound, reviewed label factors.

    The historical review identifies the unreviewed forward resource by its
    digest. Both directed resources receive independently matched identities;
    neither general unit aliases nor the source catalogs are changed.
    """
    raw = path.read_bytes()
    report = json.loads(raw)
    if report.get("schema_version") != 1 or report.get("runtime_rules_changed") is not False:
        raise ValueError("Expected an original read-only unit review with schema version 1.")
    result = deepcopy(resources)
    forward = next(resource for resource in result if resource["name"] == "uvek-2025-cutoff-to-uvek-2026-cutoff")
    backward = next(resource for resource in result if resource["name"] == "uvek-2026-cutoff-to-uvek-2025-cutoff")
    baseline_digest = hashlib.sha256(dumps_migration_resource(forward).encode()).hexdigest()
    sources = report["sources"]
    if sources["migration_resource"]["sha256"] != baseline_digest:
        raise ValueError("The unit review does not match the unreviewed migration resource.")
    if sources["native_archive"]["sha256"] != forward["provenance"]["sha256"]:
        raise ValueError("The unit review does not match the original UVEK archive.")
    candidates = {
        rule["id"]: rule for rule in forward["replace"] if rule["mapping_method"] == "unit_change_requires_factor"
    }
    rows = report["processes"]
    if len(rows) != len(candidates) or {row["rule_id"] for row in rows} != candidates.keys():
        raise ValueError("The unit review must cover every unfactored rule exactly once.")
    indexed_reverse = {tuple(rule["source"][field] for field in TECH_FIELDS): rule for rule in backward["replace"]}
    accepted = []
    for row in rows:
        rule = candidates[row["rule_id"]]
        if row["source"] != rule["source"] or row["target"] != rule["target"]:
            raise ValueError("A unit review identity differs from the migration rule.")
        if row["status"] == "manual_review_required":
            if row["recommended_forward_factor"] is not None or row["recommended_reverse_factor"] is not None:
                raise ValueError("A blocked review must not propose a factor.")
            continue
        if row["status"] != "supported_label_correspondence":
            raise ValueError("Unknown unit review status.")
        if set(row.get("checks", {})) != UNIT_REVIEW_CHECKS or not all(
            value is True for value in row["checks"].values()
        ):
            raise ValueError("A unit label correspondence needs every source check to pass.")
        for field in ("recommended_forward_factor", "recommended_reverse_factor"):
            if type(row[field]) not in (int, float) or row[field] != 1:
                raise ValueError("This review authorizes only explicit factor-of-1 label correspondences.")
        if row["old_reference_amount"] != row["native_reference_amount"] or row["old_reference_amount"] <= 0:
            raise ValueError("Reviewed reference quantities must be equal and positive.")
        reverse = indexed_reverse[tuple(rule["target"][field] for field in TECH_FIELDS)]
        if reverse["target"] != rule["source"] or reverse["mapping_method"] != "unit_change_requires_factor":
            raise ValueError("The independent reverse rule does not match the unit review.")
        accepted.append((row, rule, reverse))
    statuses = dict(Counter(row["status"] for row in rows))
    if report["summary"]["reviewed"] != len(rows) or report["summary"]["statuses"] != statuses:
        raise ValueError("Unit review summary differs from its process dispositions.")
    review_digest = hashlib.sha256(raw).hexdigest()
    for resource, rule_index in ((forward, 1), (backward, 2)):
        review = intern_review(
            resource,
            {
                "rationale": "Release-specific reference-unit label correspondence: same source process UUID and number, unchanged reference output quantity, supplier identities and per-reference technosphere coefficients. Preserve demand and uncertainty with factor 1.",
                "evidence": [
                    f"{path.name} (SHA-256 {review_digest}); match each rule's unit_review_rule_id to the historical review.",
                    f"Source workbook SHA-256 {sources['workbook']['sha256']}",
                    f"Original XML archive SHA-256 {sources['native_archive']['sha256']}",
                    f"Unreviewed forward resource SHA-256 {baseline_digest}",
                ],
                "limitations": [
                    "These factors are scoped to the reviewed release identities, not general physical unit conversions.",
                    "The washing-machine reference unit remains unapproved; biosphere mappings and LCIA equivalence are assessed independently.",
                ],
            },
        )
        for item in accepted:
            row, rule = item[0], item[rule_index]
            rule.update(
                conversion_factor=1.0,
                mapping_method="reviewed_unit_label_correspondence",
                review_ref=review,
                unit_review_rule_id=row["rule_id"],
            )
        resource["coverage"]["mapping_methods"] = dict(Counter(rule["mapping_method"] for rule in resource["replace"]))
        resource["coverage"]["unit_changes_without_factor"] = sum(
            rule["source"]["unit"] != rule["target"]["unit"] and "conversion_factor" not in rule
            for rule in resource["replace"]
        )
        resource["provenance"] = {
            **resource["provenance"],
            "unit_review": {
                "file": path.name,
                "sha256": review_digest,
                "baseline_forward_sha256": baseline_digest,
                "applied_correspondences": len(accepted),
                "blocked_correspondences": len(rows) - len(accepted),
            },
        }
    return result


def build_resources(
    archive: Path,
    catalog_directory: Path,
    unit_review: Path | None = DEFAULT_UNIT_REVIEW,
    biosphere_review: Path | None = DEFAULT_BIOSPHERE_REVIEW,
    biosphere_followup: Path | None = DEFAULT_BIOSPHERE_FOLLOWUP,
    radioactivity_review: Path | None = DEFAULT_RADIOACTIVITY_REVIEW,
    land_review: Path | None = DEFAULT_LAND_REVIEW,
    process_review: Path | None = DEFAULT_PROCESS_REVIEW,
) -> tuple[dict, list[dict]]:
    """Generate a catalog and both explicit directions for each independent axis."""
    catalog = read_archive(archive)
    old = json.loads((catalog_directory / "uvek__2025__cutoff.json").read_text(encoding="utf-8"))
    ecoinvent = json.loads((catalog_directory / "ecoinvent__3.10__cutoff.json").read_text(encoding="utf-8"))
    old_tech, new_tech = identities(old, "technosphere"), identities(catalog, "technosphere")
    ei_bio, native_bio = identities(ecoinvent, "biosphere"), identities(catalog, "biosphere")
    tech_pairs = technosphere_pairs(old_tech, new_tech)
    bio_pairs, bio_metadata = biosphere_pairs(native_bio, ei_bio)
    provenance = deepcopy(catalog["provenance"])
    provenance["comparison_catalog_sha256"] = hashlib.sha256(
        (catalog_directory / "uvek__2025__cutoff.json").read_bytes()
    ).hexdigest()
    provenance["ecoinvent_biosphere_catalog_sha256"] = hashlib.sha256(
        (catalog_directory / "ecoinvent__3.10__cutoff.json").read_bytes()
    ).hexdigest()
    provenance["biosphere_matching"] = bio_metadata
    resources = []
    for axis, source_profile, target_profile, source, target, pairs in (
        ("technosphere", old["profile"], catalog["profile"], old_tech, new_tech, tech_pairs),
        (
            "biosphere",
            {"family": "uvek", "version": "2026"},
            {"family": "ecoinvent", "version": "3.10"},
            native_bio,
            ei_bio,
            bio_pairs,
        ),
    ):
        resources.append(migration_resource(axis, source_profile, target_profile, source, target, pairs, provenance))
        reverse = [
            (target, source, method, 1 / factor if factor is not None else None)
            for source, target, method, factor in pairs
        ]
        resources.append(migration_resource(axis, target_profile, source_profile, target, source, reverse, provenance))
    resources = apply_unit_review(resources, unit_review) if unit_review is not None else resources
    if process_review is not None and unit_review is not None:
        from scripts.review_uvek_2026_processes import apply_process_review

        resources = apply_process_review(resources, process_review, catalog_directory)
    if biosphere_review is not None:
        from scripts.review_uvek_2026_biosphere import apply_review

        resources = apply_review(resources, biosphere_review, catalog_directory)
    if biosphere_followup is not None and biosphere_review is not None:
        from scripts.review_uvek_2026_biosphere_followup import apply_followup

        resources = apply_followup(resources, biosphere_followup, catalog_directory)
        if radioactivity_review is not None:
            from scripts.review_uvek_2026_radioactivity import apply_radioactivity_review

            resources = apply_radioactivity_review(resources, radioactivity_review, catalog_directory)
            if land_review is not None:
                from scripts.review_uvek_2026_land import apply_land_review

                resources = apply_land_review(resources, land_review, catalog_directory)
    return catalog, resources


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("--catalog-directory", type=Path, default=Path("brightpath/data/export/reference_catalogs"))
    parser.add_argument("--migration-directory", type=Path, default=Path("brightpath/data/migrations/uvek"))
    review_options = parser.add_mutually_exclusive_group()
    review_options.add_argument("--unit-review", type=Path, default=DEFAULT_UNIT_REVIEW)
    parser.add_argument("--biosphere-review", type=Path, default=DEFAULT_BIOSPHERE_REVIEW)
    parser.add_argument("--biosphere-followup", type=Path, default=DEFAULT_BIOSPHERE_FOLLOWUP)
    parser.add_argument("--radioactivity-review", type=Path, default=DEFAULT_RADIOACTIVITY_REVIEW)
    parser.add_argument("--land-review", type=Path, default=DEFAULT_LAND_REVIEW)
    parser.add_argument("--process-review", type=Path, default=DEFAULT_PROCESS_REVIEW)
    review_options.add_argument(
        "--unreviewed",
        action="store_true",
        help="Reproduce the historical baseline without applying unit review factors.",
    )
    args = parser.parse_args()
    catalog, resources = build_resources(
        args.archive,
        args.catalog_directory,
        None if args.unreviewed else args.unit_review,
        biosphere_review=None if args.unreviewed else args.biosphere_review,
        biosphere_followup=None if args.unreviewed else args.biosphere_followup,
        radioactivity_review=None if args.unreviewed else args.radioactivity_review,
        land_review=None if args.unreviewed else args.land_review,
        process_review=None if args.unreviewed else args.process_review,
    )
    (args.catalog_directory / "uvek__2026__cutoff.json").write_text(
        json.dumps(catalog, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    for resource in resources:
        path = args.migration_directory / (resource["name"] + ".json")
        path.write_text(dumps_migration_resource(resource), encoding="utf-8")
        print(path.name, resource["coverage"])


if __name__ == "__main__":
    main()
