"""Add four independently inspected chemical synonyms in their exact air context."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review, rule_identifier
from scripts.review_uvek_2026_biosphere import digest, identity
from scripts.review_uvek_2026_land import resource_hash

REPORT = Path(__file__).resolve().parents[1] / "brightpath/data/export/uvek_2026_chemical_alias_review.json"
METHOD = "reviewed_exact_chemical_cas_and_air_context"
DECISIONS_SHA256 = "0e3fa9575c8d2b799dda984c1384dffa42af833b3f0899c958be7472e11030dc"
PAIRS = {
    "Hydrochloric acid": ("Hydrogen chloride", "007647-01-0"),
    "Pentachlorobenzene": ("Benzene, pentachloro-", "000608-93-5"),
    "Hexachlorobenzene": ("Benzene, hexachloro-", "000118-74-1"),
    "Pentachlorophenol": ("Phenol, pentachloro-", "000087-86-5"),
}
RATIONALE = (
    "Independently inspected exact chemical synonyms with the same reported CAS, kilogram quantity "
    "and explicit urban/high-population air compartment. Original source flow metadata and native "
    "UVEK archive definitions were checked. HCl is the emitted substance mass, not a purchased "
    "aqueous solution quantity; no dilution factor is applied. Preserve signed amounts, uncertainty, "
    "formulas, metadata and separately listed emissions. The independently reviewed publisher-UUID "
    "reverse rules remain unchanged. No ionic-metal speciation, dioxin-equivalent mixture, water "
    "mass/volume, receiving-water proxy or pollutant omission is inferred. Physical correspondence "
    "does not assert equal characterization factors in source and target LCIA methods."
)


def decisions_digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def validate_report(report, catalogs):
    """Require the four exact substance/CAS/unit/compartment decisions and frozen catalogs."""
    rows = report.get("decisions", [])
    if (
        report.get("schema_version") != 1
        or report.get("direction") != "ecoinvent-3.10-biosphere-to-uvek-2026-biosphere"
        or len(rows) != 4
        or decisions_digest(rows) != DECISIONS_SHA256
        or len({identity(r["target"]) for r in rows}) != 4
    ):
        raise ValueError("Chemical alias scope changed; repeat the independent review.")
    for family, filename in (("ecoinvent", "ecoinvent__3.10__cutoff.json"), ("native", "uvek__2026__cutoff.json")):
        if digest(catalogs / filename) != report["sources"][f"{family}_catalog_sha256"]:
            raise ValueError("Chemical alias exact catalog changed; repeat the independent review.")
    eco = json.loads((catalogs / "ecoinvent__3.10__cutoff.json").read_text())["biosphere"]
    native = json.loads((catalogs / "uvek__2026__cutoff.json").read_text())["biosphere"]
    eco = {identity(r): r for r in eco}
    native = {identity(r) for r in native}
    for row in rows:
        source, target = row["source"], row["target"]
        if (
            PAIRS.get(source["name"]) != (target["name"], row["source_CAS"])
            or row["source_CAS"] != row["native_CAS"]
            or source["categories"] != ["air", "urban air close to ground"]
            or target["categories"] != ["emissions to air", "high. pop."]
            or source["unit"] != target["unit"]
            or source["unit"] != "kilogram"
            or row["conversion_factor"] != 1
            or eco.get(identity(source), {}).get("flow_id") != row["source_uuid"]
            or identity(target) not in native
            or row["native_definition"]["output_group"] != "4"
        ):
            raise ValueError("Chemical alias CAS, unit, compartment or definition changed; repeat the review.")


def compiled_additions(resource, report, path):
    """Compile only the inspected forward synonyms and their traceable evidence."""
    reviews = {}
    review_ref = intern_review(
        {"reviews": reviews},
        {
            "rationale": RATIONALE,
            "limitations": ["Source and target LCIA characterization tables can differ."],
            "evidence": [f"{path.name} SHA-256 {digest(path)}"],
        },
    )
    return [
        {
            "id": rule_identifier(resource["name"], row["source"]),
            "source": deepcopy(row["source"]),
            "target": deepcopy(row["target"]),
            "conversion_factor": 1.0,
            "classification": "correspondence",
            "mapping_method": METHOD,
            "review_ref": review_ref,
            "report_correspondence": True,
        }
        for row in report["decisions"]
    ], reviews


def apply_chemical_alias_review(resources, path, catalog_directory):
    """Add four forward rules while preserving every independently reviewed reverse rule."""
    report = json.loads(path.read_text())
    validate_report(report, catalog_directory)
    result = deepcopy(resources)
    incoming = next(r for r in result if r["axis"] == "biosphere" and r["source_profiles"][0]["family"] == "ecoinvent")
    outgoing = next(r for r in result if r["axis"] == "biosphere" and r["source_profiles"][0]["family"] == "uvek")
    sources = report["sources"]
    if (
        resource_hash(incoming) != sources["baseline_incoming_sha256"]
        or resource_hash(outgoing) != sources["baseline_outgoing_sha256"]
        or incoming["provenance"]["sha256"] != sources["native_archive_sha256"]
    ):
        raise ValueError("Chemical alias directional baseline changed; repeat the independent review.")
    reverse = {identity(r["source"]): r for r in outgoing["replace"]}
    for row in report["decisions"]:
        rule = reverse.get(identity(row["target"]))
        if decisions_digest(rule) != row["independent_reverse_binding_sha256"]:
            raise ValueError("Chemical alias independent reverse binding changed; repeat its review.")
    keys = {identity(r["source"]) for r in report["decisions"]}
    if sum(identity(r) in keys for r in incoming["unresolved"]) != 4 or any(
        identity(r["source"]) in keys for r in incoming["replace"]
    ):
        raise ValueError("Chemical alias source identities must be uniquely unresolved before the review.")
    rules, reviews = compiled_additions(incoming, report, path)
    incoming["replace"] = sorted([*incoming["replace"], *rules], key=lambda r: identity(r["source"]))
    incoming["reviews"].update(reviews)
    incoming["unresolved"] = [r for r in incoming["unresolved"] if identity(r) not in keys]
    incoming["coverage"].update(
        mapped_identities=len(incoming["replace"]),
        unresolved_identities=len(incoming["unresolved"]),
        mapping_methods=dict(sorted(Counter(r["mapping_method"] for r in incoming["replace"]).items())),
        directional_additions=incoming["coverage"].get("directional_additions", 0) + 4,
    )
    incoming["provenance"]["chemical_alias_review"] = {"file": path.name, "sha256": digest(path), "sources": sources}
    return result


def before_chemical_alias_review(resources, path=REPORT):
    """Remove only this cohort for exact replay of the earlier review boundaries."""
    result = deepcopy(resources)
    incoming = next(r for r in result if r["axis"] == "biosphere" and r["source_profiles"][0]["family"] == "ecoinvent")
    if "chemical_alias_review" not in incoming["provenance"]:
        return result
    report = json.loads(path.read_text())
    rules, reviews = compiled_additions(incoming, report, path)
    added = [r for r in incoming["replace"] if r.get("mapping_method") == METHOD]
    if sorted(added, key=lambda r: r["id"]) != sorted(rules, key=lambda r: r["id"]) or any(
        incoming["reviews"].get(k) != v for k, v in reviews.items()
    ):
        raise ValueError("Chemical alias historical projection changed; repeat the independent review.")
    incoming["replace"] = [r for r in incoming["replace"] if r.get("mapping_method") != METHOD]
    incoming["unresolved"] = sorted([*incoming["unresolved"], *(r["source"] for r in rules)], key=identity)
    incoming["reviews"] = {k: v for k, v in incoming["reviews"].items() if k not in reviews}
    incoming["coverage"] = deepcopy(report["sources"]["baseline_incoming_coverage"])
    incoming["provenance"].pop("chemical_alias_review")
    if resource_hash(incoming) != report["sources"]["baseline_incoming_sha256"]:
        raise ValueError("Chemical alias projection altered the previous independent baseline.")
    return result
