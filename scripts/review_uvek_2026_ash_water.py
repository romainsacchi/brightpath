"""Apply the explicitly approved native water screening proxies in one direction."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review, rule_identifier
from scripts.review_uvek_2026_biosphere import digest, identity
from scripts.review_uvek_2026_land import resource_hash

REPORT = Path(__file__).resolve().parents[1] / "brightpath/data/export/uvek_2026_ash_water_review.json"
METHOD = "reviewed_native_water_screening_proxy"
DECISIONS_SHA256 = "98d92374dfcaa2788e1d625fd5eb771651e497b5e77cb546eefc2cd8073eeff7"
LIMITATIONS = (
    "Explicitly approved native UVEK 2026 water-emission screening proxy. Preserve signed kilograms "
    "and supported caller uncertainty, formulas and metadata. Ionic metal labels may become the "
    "corresponding generic element label: oxidation-state/speciation detail and source/native CAS "
    "differences are disclosed, not chemical identity equivalence. Source surface-water emissions "
    "use native unspecified water; no river/lake split or share is inferred. Retain the exact "
    "long-term groundwater compartment where specified. Named nitrate, phosphate, chloride, fluoride "
    "and sulfate retain their own reported mass; do not convert them to elemental N/P/Cl/F/S. "
    "DOC remains the same dissolved-organic-carbon quantity, not COD or BOD. LCIA factors and toxicity "
    "results can differ and equivalence is not established. No emissions are omitted. No inverse "
    "from native generic elements to source ions is inferred; reverse coverage is independently reviewed."
)


def validate_report(report, catalogs):
    """Require the exact 56 approved source/target/CAS/compartment decisions."""
    decisions = report.get("decisions", [])
    if (
        report.get("schema_version") != 1
        or report.get("direction") != "ecoinvent-3.10-biosphere-to-uvek-2026-biosphere"
        or len(decisions) != 56
        or hashlib.sha256(json.dumps(decisions, sort_keys=True).encode()).hexdigest() != DECISIONS_SHA256
    ):
        raise ValueError("Approved native water proxy scope changed; repeat the scientific review.")
    for family, filename in (("ecoinvent", "ecoinvent__3.10__cutoff.json"), ("native", "uvek__2026__cutoff.json")):
        path = catalogs / filename
        if digest(path) != report["sources"][f"{family}_catalog_sha256"]:
            raise ValueError("Approved native water exact catalog changed; repeat the scientific review.")
    source_rows = json.loads((catalogs / "ecoinvent__3.10__cutoff.json").read_text())["biosphere"]
    target_rows = json.loads((catalogs / "uvek__2026__cutoff.json").read_text())["biosphere"]
    for decision in decisions:
        sources = [r for r in source_rows if identity(r) == identity(decision["source"])]
        targets = [r for r in target_rows if identity(r) == identity(decision["target"])]
        if (
            len(sources) != 1
            or sources[0].get("flow_id") != decision["source_uuid"]
            or len(targets) != 1
            or decision["conversion_factor"] != 1
            or decision["source"]["unit"] != "kilogram"
            or decision["target"]["unit"] != "kilogram"
        ):
            raise ValueError("Approved native water identity or mass basis changed; repeat the scientific review.")


def compiled_additions(resource, report, path):
    """Keep the original chemical identifiers and receiving compartment in every loss."""
    reviews = {}
    review_ref = intern_review(
        {"reviews": reviews},
        {
            "rationale": "User approved these 56 explicit screening proxies with warnings on 2026-11-06. "
            + LIMITATIONS,
            "limitations": [LIMITATIONS],
            "evidence": [f"{path.name} SHA-256 {digest(path)}"],
        },
    )
    rules = []
    for row in report["decisions"]:
        loss = (
            f"Source {row['source']['name']}, CAS {row['source_cas'] or 'unspecified'}, UUID {row['source_uuid']}, "
            f"compartment {'/'.join(row['source']['categories'])}; target {row['target']['name']} in "
            f"{'/'.join(row['target']['categories'])}, native reported CAS {row['native_reported_cas'] or 'unspecified'}. "
            + LIMITATIONS
        )
        rules.append(
            {
                "id": rule_identifier(resource["name"], row["source"]),
                "source": deepcopy(row["source"]),
                "target": deepcopy(row["target"]),
                "conversion_factor": 1.0,
                "classification": "proxy",
                "mapping_method": METHOD,
                "review_ref": review_ref,
                "biosphere_information_loss": loss,
            }
        )
    return rules, reviews


def apply_ash_water_review(resources, path, catalog_directory):
    """Add reviewed forward proxies while preserving all other directional rules."""
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
        raise ValueError("Native water proxy directional baseline changed; repeat the scientific review.")
    keys = {identity(r["source"]) for r in report["decisions"]}
    if (
        len(keys) != 56
        or sum(identity(r) in keys for r in incoming["unresolved"]) != 56
        or any(identity(r["source"]) in keys for r in incoming["replace"])
    ):
        raise ValueError("Native water proxies are not uniquely unresolved; repeat the scientific review.")
    rules, reviews = compiled_additions(incoming, report, path)
    incoming["replace"] = sorted([*incoming["replace"], *rules], key=lambda r: identity(r["source"]))
    incoming["reviews"].update(reviews)
    incoming["unresolved"] = [r for r in incoming["unresolved"] if identity(r) not in keys]
    incoming["coverage"].update(
        mapped_identities=len(incoming["replace"]),
        unresolved_identities=len(incoming["unresolved"]),
        mapping_methods=dict(sorted(Counter(r["mapping_method"] for r in incoming["replace"]).items())),
        directional_additions=incoming["coverage"].get("directional_additions", 0) + 56,
    )
    incoming["provenance"]["ash_water_review"] = {"file": path.name, "sha256": digest(path), "sources": sources}
    return result


def before_ash_water_review(resources, path=REPORT):
    """Reconstruct the exact previous baseline without relaxing historical guards."""
    from scripts.review_uvek_2026_particulates import before_particulate_review

    report = json.loads(path.read_text())
    result = before_particulate_review(resources)
    incoming = next(r for r in result if r["axis"] == "biosphere" and r["source_profiles"][0]["family"] == "ecoinvent")
    if "ash_water_review" not in incoming["provenance"]:
        return result
    rules, reviews = compiled_additions(incoming, report, path)
    added = [r for r in incoming["replace"] if r.get("mapping_method") == METHOD]
    if sorted(added, key=lambda r: r["id"]) != sorted(rules, key=lambda r: r["id"]) or any(
        incoming["reviews"].get(k) != v for k, v in reviews.items()
    ):
        raise ValueError("Native water proxy projection changed; repeat the scientific review.")
    incoming["replace"] = [r for r in incoming["replace"] if r.get("mapping_method") != METHOD]
    incoming["unresolved"] = sorted([*incoming["unresolved"], *(r["source"] for r in rules)], key=identity)
    incoming["reviews"] = {k: v for k, v in incoming["reviews"].items() if k not in reviews}
    incoming["coverage"] = deepcopy(report["sources"]["baseline_coverage"])
    incoming["provenance"].pop("ash_water_review")
    if resource_hash(incoming) != report["sources"]["baseline_incoming_sha256"]:
        raise ValueError("Native water proxy projection altered the previous independent baseline.")
    return result
