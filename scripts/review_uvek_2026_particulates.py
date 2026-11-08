"""Match exact particulate size bins and air compartments without inferring fractions."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review, rule_identifier
from scripts.review_uvek_2026_biosphere import digest, identity
from scripts.review_uvek_2026_land import resource_hash

REPORT = Path(__file__).resolve().parents[1] / "brightpath/data/export/uvek_2026_particulate_review.json"
METHOD = "reviewed_exact_particulate_size_and_air_context"
DECISIONS_SHA256 = "3d80784cd2dc1d79f0b41f468bbf8040bf06624a961ae16fb36956b9de2804db"
CONTEXTS = {
    ("air",): ("emissions to air", "unspecified"),
    ("air", "urban air close to ground"): ("emissions to air", "high. pop."),
    ("air", "non-urban air or from high stacks"): ("emissions to air", "low. pop."),
    ("air", "low population density, long-term"): ("emissions to air", "low. pop., long-term"),
    ("air", "lower stratosphere + upper troposphere"): ("emissions to air", "stratosphere + troposphere"),
}
NAMES = {
    "Particulate Matter, < 2.5 um": "Particulates, < 2.5 um",
    "Particulate Matter, > 2.5 um and < 10um": "Particulates, > 2.5 um, and < 10um",
    "Particulate Matter, > 10 um": "Particulates, > 10 um",
}
RATIONALE = (
    "Independently inspected exact particulate label/size-bin correspondence. Preserve signed kilograms, "
    "the explicit <2.5 um, >2.5-and-<10 um or >10 um bin, and each urban, nonurban, unspecified, "
    "long-term or stratosphere/troposphere air context. No size fractions, PM10 total, density, time "
    "or compartment shares are inferred. Original ecoinvent 3.10 and 3.12 flow definitions and native "
    "UVEK archive definitions were inspected. The independently authored publisher-UUID reverse "
    "correspondences remain unchanged. This is not an ionic-metal or generic-particle proxy. "
    "Preserve supported caller uncertainty, formulas, metadata and separately listed emissions; "
    "do not omit pollutants or merge separately reported bins. Physical flow correspondence does "
    "not assert that source and target LCIA method tables contain equal characterization factors."
)


def decisions_digest(rows):
    return hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest()


def validate_report(report, catalogs):
    """Freeze 13 exact size/context pairs and the actual source/native definition evidence."""
    rows = report.get("decisions", [])
    if (
        report.get("schema_version") != 1
        or len(rows) != 13
        or decisions_digest(rows) != DECISIONS_SHA256
        or len({identity(r["target"]) for r in rows}) != 13
    ):
        raise ValueError("Particulate size/context scope changed; repeat the independent review.")
    for family, filename in (("ecoinvent", "ecoinvent__3.10__cutoff.json"), ("native", "uvek__2026__cutoff.json")):
        if digest(catalogs / filename) != report["sources"][f"{family}_catalog_sha256"]:
            raise ValueError("Particulate exact catalog changed; repeat the independent review.")
    eco = json.loads((catalogs / "ecoinvent__3.10__cutoff.json").read_text())["biosphere"]
    native = json.loads((catalogs / "uvek__2026__cutoff.json").read_text())["biosphere"]
    eco = {identity(r): r for r in eco}
    native = {identity(r) for r in native}
    for row in rows:
        source, target = row["source"], row["target"]
        if (
            NAMES.get(source["name"]) != target["name"]
            or CONTEXTS.get(tuple(source["categories"])) != tuple(target["categories"])
            or source["unit"] != target["unit"]
            or source["unit"] != "kilogram"
            or row["conversion_factor"] != 1
            or eco.get(identity(source), {}).get("flow_id") != row["source_uuid"]
            or identity(target) not in native
            or {r["label"] for r in row["ecoinvent_definition_profiles"]}
            != {"ecoinvent-3.10-cutoff", "ecoinvent-3.12-cutoff"}
        ):
            raise ValueError("Particulate size, unit, compartment or definition changed; repeat the review.")


def compiled_additions(resource, report, path):
    """Compile explicitly inspected physical correspondences, with their review evidence."""
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


def apply_particulate_review(resources, path, catalog_directory):
    """Add the missing ecoinvent direction while independently preserving every existing reverse rule."""
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
        raise ValueError("Particulate directional baseline changed; repeat the independent review.")
    reverse = {identity(r["source"]): r for r in outgoing["replace"]}
    for row in report["decisions"]:
        rule = reverse.get(identity(row["target"]))
        if decisions_digest(rule) != row["independent_reverse_binding_sha256"]:
            raise ValueError("Particulate independent reverse binding changed; repeat its review.")
    keys = {identity(r["source"]) for r in report["decisions"]}
    if sum(identity(r) in keys for r in incoming["unresolved"]) != 13 or any(
        identity(r["source"]) in keys for r in incoming["replace"]
    ):
        raise ValueError("Particulate source identities must be uniquely unresolved before the review.")
    rules, reviews = compiled_additions(incoming, report, path)
    incoming["replace"] = sorted([*incoming["replace"], *rules], key=lambda r: identity(r["source"]))
    incoming["reviews"].update(reviews)
    incoming["unresolved"] = [r for r in incoming["unresolved"] if identity(r) not in keys]
    incoming["coverage"].update(
        mapped_identities=len(incoming["replace"]),
        unresolved_identities=len(incoming["unresolved"]),
        mapping_methods=dict(sorted(Counter(r["mapping_method"] for r in incoming["replace"]).items())),
        directional_additions=incoming["coverage"].get("directional_additions", 0) + 13,
    )
    incoming["provenance"]["particulate_review"] = {"file": path.name, "sha256": digest(path), "sources": sources}
    return result


def before_particulate_review(resources, path=REPORT):
    """Project only these exact additions away so all earlier scientific guards remain intact."""
    from scripts.review_uvek_2026_chemical_aliases import before_chemical_alias_review

    result = before_chemical_alias_review(resources)
    incoming = next(r for r in result if r["axis"] == "biosphere" and r["source_profiles"][0]["family"] == "ecoinvent")
    if "particulate_review" not in incoming["provenance"]:
        return result
    report = json.loads(path.read_text())
    rules, reviews = compiled_additions(incoming, report, path)
    added = [r for r in incoming["replace"] if r.get("mapping_method") == METHOD]
    if sorted(added, key=lambda r: r["id"]) != sorted(rules, key=lambda r: r["id"]) or any(
        incoming["reviews"].get(k) != v for k, v in reviews.items()
    ):
        raise ValueError("Particulate historical projection changed; repeat the independent review.")
    incoming["replace"] = [r for r in incoming["replace"] if r.get("mapping_method") != METHOD]
    incoming["unresolved"] = sorted([*incoming["unresolved"], *(r["source"] for r in rules)], key=identity)
    incoming["reviews"] = {k: v for k, v in incoming["reviews"].items() if k not in reviews}
    incoming["coverage"] = deepcopy(report["sources"]["baseline_incoming_coverage"])
    incoming["provenance"].pop("particulate_review")
    if resource_hash(incoming) != report["sources"]["baseline_incoming_sha256"]:
        raise ValueError("Particulate projection altered the previous independent baseline.")
    return result
