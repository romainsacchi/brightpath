"""Generate a complete catalog disposition without approving similarity guesses."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from collections import Counter, defaultdict
from functools import lru_cache
from pathlib import Path

from brightpath.background.uvek_export import IDENTITY_FIELDS, RESOURCE_NAME, identity, validate_resource
from brightpath.units import normalize_unit


def load(path):
    return json.loads(Path(path).read_text())


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False, sort_keys=True, allow_nan=False) + "\n")


def clean(value):
    return " ".join(re.sub(r"\s*\{[^{}]+\}\s*[US]?\s*$", "", value).casefold().split())


def source_key(name, location, unit):
    return clean(name), location, normalize_unit(unit)


@lru_cache(maxsize=65536)
def tokens(value):
    """Index distinctive supplier terms rather than process boilerplate."""
    return frozenset(re.findall(r"[a-z0-9]+", clean(value))) - {
        "at",
        "in",
        "of",
        "for",
        "the",
        "and",
        "plant",
        "production",
        "market",
        "group",
        "from",
        "with",
        "by",
    }


def candidate_index(targets):
    index = defaultdict(set)
    for target in targets:
        for token in tokens(target[0] + " " + target[1]):
            index[target[3], token].add(target)
    return index


def ranked_candidates(source, index):
    """Suggest review candidates; ranking scores are not mapping confidence."""
    source_tokens = tokens(source[0] + " " + source[1])
    candidates = set().union(*(index[source[3], token] for token in source_tokens)) if source_tokens else set()

    def score(target):
        target_tokens = tokens(target[0] + " " + target[1])
        similarity = len(source_tokens & target_tokens) / max(1, len(source_tokens | target_tokens))
        return -similarity, -(source[2] == target[2]), target

    return sorted(candidates, key=score)[:5]


def extract_evidence(args):
    """Reduce user-supplied public PV provenance to standalone rule evidence."""
    source_path = Path(args.pv_suppliers)
    candidates = []
    for row in csv.DictReader(source_path.open()):
        candidates.append(
            {
                "source_name": row["source_supplier"],
                "source_location": row["source_location"],
                "source_unit": row["source_unit"],
                "target": {
                    "name": row["ecoinvent_activity"],
                    "reference product": row["ecoinvent_reference_product"],
                    "location": row["ecoinvent_location"],
                    "unit": row["ecoinvent_unit"],
                },
                "factor": float(row["amount_factor"]),
                "classification": "correspondence" if row["mapping_type"] == "Exact correspondence" else "proxy",
                "rationale": row["rationale"],
                "evidence": [
                    "IEA PVPS T12-33:2026 https://doi.org/10.69766/WJTE1771",
                    f"{source_path.name}; sheets: {row['source_sheets']}",
                ],
            }
        )
    aliases = []
    for entry in load(args.pv_audit):
        if entry["method"] == "explicit_catalog_label_alias" and entry["type"] == "technosphere":
            aliases.append({"original": entry["source"], "canonical": entry["target"]})
    from brightpath import (
        BackgroundContext,
        BiosphereProfile,
        BrightwayInventory,
        FormatProfile,
        InventoryContext,
        TechnosphereProfile,
    )

    context = InventoryContext(
        FormatProfile("brightway_excel"),
        BackgroundContext(TechnosphereProfile("ecoinvent", "3.12", "cutoff"), BiosphereProfile("ecoinvent", "3.12")),
    )
    datasets = BrightwayInventory.from_excel(args.pv_workbook, context=context).data
    naming = {entry["source_id"]: entry for entry in load(args.pv_naming)}
    recipes = []
    for entry in load(args.pv_summary)["externalized_supporting_datasets"]:
        label = naming[entry["source_id"]]
        matches = [
            dataset
            for dataset in datasets
            if dataset["name"] == label["name"] and dataset["location"] == label["location"]
        ]
        if len(matches) != 1:
            raise ValueError(f"Cannot identify public support recipe: {entry['source_id']}")
        dataset = matches[0]
        inputs = []
        for exchange in dataset["exchanges"]:
            if exchange["type"] == "production":
                continue
            inputs.append(
                {key: value for key, value in exchange.items() if key not in {"input", "database", "code", "output"}}
            )
        recipes.append(
            {
                "source": dict(zip(IDENTITY_FIELDS, entry["native_supplier"], strict=True)),
                "recipe": {
                    "name": dataset["name"],
                    "reference product": dataset["reference product"],
                    "location": dataset["location"],
                    "unit": dataset["unit"],
                    "inputs": inputs,
                    "rationale": dataset.get("comment") or inputs[0].get("comment", "Public supporting recipe."),
                    "evidence": [
                        dataset.get("source", "IEA PVPS T12-33:2026 supporting inventories"),
                        entry["source_id"],
                    ],
                },
            }
        )
    paths = [source_path, Path(args.pv_audit), Path(args.pv_workbook), Path(args.pv_naming), Path(args.pv_summary)]
    return {
        "schema_version": 1,
        "candidates": candidates,
        "aliases": aliases,
        "recipes": recipes,
        "input_sha256": {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths},
    }


def build(root, evidence):
    directory = root / "brightpath/data"
    source_catalog = load(directory / "export/reference_catalogs/uvek__2025__cutoff.json")
    target_catalog = load(directory / "export/reference_catalogs/ecoinvent__3.12__cutoff.json")
    sources = {
        identity({**row, "reference product": row["reference_product"]}): row for row in source_catalog["technosphere"]
    }
    targets = {
        identity({**row, "reference product": row["reference_product"]}) for row in target_catalog["technosphere"]
    }
    candidates = defaultdict(list)
    overrides = load(root / "docs/uvek_export_overrides.json")
    for candidate in [*evidence["candidates"], *overrides["candidates"]]:
        candidates[source_key(candidate["source_name"], candidate["source_location"], candidate["source_unit"])].append(
            candidate
        )
    aliases = defaultdict(set)
    for alias in evidence["aliases"]:
        original, canonical = alias["original"], alias["canonical"]
        aliases[source_key(canonical["name"], canonical["location"], canonical["unit"])].add(
            source_key(original["name"], original["location"], original["unit"])
        )
    recipes, recipe_rules = {}, {}
    for entry in evidence["recipes"]:
        source = identity(entry["source"])
        if source not in sources:
            raise ValueError(f"Recipe source is not in UVEK catalog: {source}")
        recipe_id = hashlib.sha256(json.dumps(source).encode()).hexdigest()[:16]
        recipes[recipe_id] = entry["recipe"]
        recipe_rules[source] = {
            "recipe": recipe_id,
            "classification": "proxy",
            "rationale": entry["recipe"]["rationale"],
            "evidence": entry["recipe"]["evidence"],
        }
    rules, unresolved, review = [], [], []
    target_index = candidate_index(targets)
    forward = load(directory / "migrations/uvek/ecoinvent-to-uvek-2025.json")
    suggestions = defaultdict(set)
    for rule in forward["replace"]:
        if identity(rule["source"]) in targets:
            suggestions[identity(rule["target"])].add(identity(rule["source"]))
    for source in sorted(sources):
        source_record = dict(zip(IDENTITY_FIELDS, source, strict=True))
        key = source_key(source[0], source[2], source[3])
        matches = list(candidates[key])
        for alias in sorted(aliases[key]):
            matches.extend(candidates[alias])
        matches = [candidate for candidate in matches if identity(candidate["target"]) in targets]
        signatures = {(identity(candidate["target"]), candidate["factor"]) for candidate in matches}
        decision = recipe_rules.get(source)
        if decision is None and len(signatures) == 1:
            selected = matches[0]
            decision = {field: selected[field] for field in ("target", "factor", "classification", "rationale")}
            decision["evidence"] = sorted({reference for candidate in matches for reference in candidate["evidence"]})
            if any(candidate["classification"] == "proxy" for candidate in matches):
                decision["classification"] = "proxy"
        if decision is not None:
            rule_id = "uvek-ei312-" + hashlib.sha256(json.dumps(source).encode()).hexdigest()[:16]
            rules.append({"id": rule_id, "source": source_record, **decision})
            review.append(
                {
                    "source": source_record,
                    "disposition": "recipe" if "recipe" in decision else decision["classification"],
                    "rule_id": rule_id,
                }
            )
        else:
            reason = (
                "Conflicting source-backed targets or factors require adjudication."
                if len(signatures) > 1
                else "No reviewed directional correspondence; similarity and reversed forward rules are not approval."
            )
            unresolved.append({"source": source_record, "reason": reason})
            review.append(
                {
                    "source": source_record,
                    "disposition": "unresolved",
                    "reason": reason,
                    "candidates": [
                        dict(zip(IDENTITY_FIELDS, target, strict=True))
                        for target in ranked_candidates(source, target_index)
                    ],
                    "candidate_method": "unit-constrained token ranking, not approval",
                    "reverse_candidate_count": len(suggestions[source]),
                }
            )
    for recipe in recipes.values():
        for exchange in recipe["inputs"]:
            if exchange["type"] == "technosphere" and identity(exchange) not in targets:
                raise ValueError(f"Public recipe has a non-background dependency: {identity(exchange)}")
    coverage = {
        "source_identities": len(sources),
        "approved_identities": len(rules),
        "unresolved_identities": len(unresolved),
        "dispositions": dict(Counter(entry["disposition"] for entry in review)),
    }
    resource = {
        "schema_version": 1,
        "status": "active",
        "axis": "technosphere",
        "quality": "reviewed",
        "name": "uvek-2025-to-ecoinvent-3.12-cutoff",
        "source_id": "uvek-2025-cutoff",
        "target_id": "ecoinvent-3.12-cutoff",
        "source_profile": {"family": "uvek", "version": "2025", "system_model": "cutoff"},
        "target_profile": {"family": "ecoinvent", "version": "3.12", "system_model": "cutoff"},
        "rules": rules,
        "recipes": recipes,
        "unresolved": unresolved,
        "coverage": coverage,
        "evidence_sha256": hashlib.sha256(
            json.dumps({"source": evidence, "overrides": overrides}, sort_keys=True).encode()
        ).hexdigest(),
    }
    validate_resource(resource)
    return resource, {"coverage": coverage, "review": review, "scientific_equivalence": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--evidence", type=Path)
    for flag in ("pv-suppliers", "pv-audit", "pv-workbook", "pv-naming", "pv-summary"):
        parser.add_argument("--" + flag, type=Path)
    args = parser.parse_args()
    evidence_path = args.evidence or args.root / "docs/uvek_export_evidence.json"
    if args.pv_suppliers:
        write(evidence_path, extract_evidence(args))
    resource, review = build(args.root, load(evidence_path))
    path = args.root / "brightpath/data/migrations/uvek" / RESOURCE_NAME
    write(path, resource)
    write(args.root / "docs/uvek_export_review.json", review)
    manifest_path = args.root / "brightpath/data/migrations/RESOURCE_MANIFEST.json"
    manifest = load(manifest_path)
    relative = "uvek/" + RESOURCE_NAME
    manifest["resources"] = [entry for entry in manifest["resources"] if entry["path"] != relative]
    manifest["resources"].append(
        {
            "path": relative,
            "name": resource["name"],
            "source_id": resource["source_id"],
            "target_id": resource["target_id"],
            "schema_version": 1,
            "status": "active",
            "licenses": [],
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "size": path.stat().st_size,
        }
    )
    manifest["resources"].sort(key=lambda entry: entry["path"])
    write(manifest_path, manifest)
    print(json.dumps(resource["coverage"], indent=2))


if __name__ == "__main__":
    main()
