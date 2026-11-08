"""Preserve allocated digester-sludge stock and its exact material-market freight."""

import hashlib
import json
import pickle
import sqlite3
from copy import deepcopy
from pathlib import Path

from brightpath import TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules

try:
    from review_uvek_cutoff_stocks import stock_decision
    from review_uvek_market_compositions import composition_decision
except ModuleNotFoundError:
    from scripts.review_uvek_cutoff_stocks import stock_decision
    from scripts.review_uvek_market_compositions import composition_decision

FIELDS = ("name", "reference product", "location", "unit")
ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_digester_stock_review.json"
APPROVAL = json.loads((ROOT / REVIEW_PATH).read_text())
REVIEW_SHA256 = "b9229347301403f146803cdf99630855885d32701bae75d9ec90a2da5334c92c"
SOURCES = {tuple(r["source"][f] for f in FIELDS): r for r in APPROVAL["rules"]}
MARKER = "digester_stock_correction"


def identity(row):
    return tuple(row[f] for f in FIELDS)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def inventory_record(label, metadata, blobs):
    """Fingerprint full vectors and descriptions without distributing source inventories."""
    exchanges = [pickle.loads(blob) for blob in blobs]
    (reference,) = [e for e in exchanges if e["type"] == "production"]
    if reference["amount"] != 1 or reference["input"] != reference["output"]:
        raise ValueError("Digester-stock evidence reference changed.")
    return {
        "label": label,
        "source": {f: metadata[f] for f in FIELDS},
        "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
        "activity_comment_sha256": hashlib.sha256(str(metadata.get("comment") or "").encode()).hexdigest(),
        "product_information_sha256": hashlib.sha256(
            str(metadata.get("product_information") or "").encode()
        ).hexdigest(),
        "reference_amount": 1,
        "total_exchanges": len(exchanges),
        "only_self_production": len(exchanges) == 1,
        "production_amount": 1,
    }


def collect_inventories(brightway_root):
    """Read every exact source and native witness from the original databases in read-only mode."""
    records = []
    for entry in APPROVAL["collection_profiles"]:
        label = "-".join(entry["profile"])
        wanted = {identity(r["source"]) for r in APPROVAL["inventories"] if r["label"] == label}
        path = brightway_root / entry["project"] / "lci/databases.db"
        found = set()
        with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as connection:
            for name in sorted({s[0] for s in wanted}):
                for (raw,) in connection.execute(
                    "SELECT data FROM activitydataset WHERE database=? AND name=?", (entry["database"], name)
                ):
                    metadata = pickle.loads(raw)
                    if identity(metadata) not in wanted:
                        continue
                    blobs = [
                        blob
                        for (blob,) in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (entry["database"], metadata["code"]),
                        )
                    ]
                    records.append(inventory_record(label, metadata, blobs))
                    found.add(identity(metadata))
        if found != wanted:
            raise ValueError("Digester-stock source/provider scope changed.")
    return records


def validate_inventories(records, observations):
    """Require exact vectors and a strict positive, self-production-only stock boundary."""
    if digest(APPROVAL) != REVIEW_SHA256 or APPROVAL["authorized_system_model"] != "cutoff":
        raise ValueError("Digester-stock scientific review changed.")
    if sorted(map(digest, records)) != sorted(map(digest, APPROVAL["inventories"])) or len(records) != 43:
        raise ValueError("Digester-stock original inventories or descriptions changed.")
    for source, row in SOURCES.items():
        for p in row["profiles"]:
            (witness,) = [r for r in records if identity(r["source"]) == source and r["label"] == p["label"]]
            if row["kind"] == "stock" and (
                witness["total_exchanges"] != 1 or witness["only_self_production"] is not True
            ):
                raise ValueError("Digester sludge is no longer an allocated production-only stock.")
            observations["ecoinvent"][source].add((p["label"], 1))


def validate_bindings(resource, previous):
    """Freeze retained old helpers, exact freight rules and independently authored prior decisions."""
    retired = APPROVAL.get("retired_helper_bodies", {})
    applied = all(MARKER in previous.get(row["id"], {}) for row in SOURCES.values())
    for rid, expected in APPROVAL["existing_recipe_bindings"].items():
        if rid in retired:
            if digest(retired[rid]) != expected or (applied and rid in resource["recipes"]):
                raise ValueError("A retired digester-sludge helper changed or became executable again.")
            if rid not in resource["recipes"]:
                continue
        if digest(resource["recipes"].get(rid)) != expected:
            raise ValueError("An existing digester-sludge helper changed.")
    selected = {}
    for _source, row in SOURCES.items():
        prior = previous[row["id"]]
        base = prior.get(MARKER, {}).get("base_decision", prior)
        if digest(base) != row["prior_decision_sha256"]:
            raise ValueError("A prior digester-sludge decision changed.")
        for p in row["profiles"]:
            label = p["label"]
            if label not in selected:
                selected[label] = {
                    identity(r["source"]): r
                    for r in select_profile_sign_rules(resource, TechnosphereProfile(*label.split("-")))["replace"]
                }
            for binding in p["bindings"]:
                if binding.get("kind") == "new_stock_dependency":
                    continue
                if digest(selected[label].get(tuple(binding["source"]))) != binding["binding_sha256"]:
                    raise ValueError("Digester-sludge freight decision changed.")


def forward_decision(rule, observations, binding, original_review, prior):
    """Replace only the enumerated allocated stock and material-market decisions."""
    row = SOURCES[identity(rule["source"])]
    base = deepcopy(prior.get(MARKER, {}).get("base_decision", prior))
    if digest(base) != row["prior_decision_sha256"]:
        raise ValueError("Digester-stock prior decision changed.")
    inventory = [r for r in APPROVAL["inventories"] if identity(r["source"]) == identity(rule["source"])]
    if row["kind"] == "stock":
        decision = stock_decision(
            rule,
            {(r["label"], 1) for r in inventory},
            inventory,
            binding,
            original_review,
            approval_row=row,
            review_path=REVIEW_PATH,
        )
    else:
        approval = {**row, "authorized_system_model": "cutoff", "inputs": [], "pending_profiles": []}
        approval["profiles"] = [
            {**p, "inputs": p["recipe"]["inputs"], "rationale": APPROVAL["limitations"]} for p in row["profiles"]
        ]
        decision = composition_decision(
            rule,
            {(r["label"], 1) for r in inventory},
            [(r["label"], r["inventory_sha256"]) for r in inventory],
            binding,
            original_review,
            approval_row=approval,
            rationale=APPROVAL["limitations"],
            review_path=REVIEW_PATH,
            namespace="digester-sludge-market",
        )
    # Retired parent bodies remain in APPROVAL and the retained base decision.
    # They must not return as executable definitions during normal replay.
    retained = {
        key: deepcopy(value)
        for key, value in base.get("recipes", {}).items()
        if key not in APPROVAL.get("retired_helper_bodies", {})
    }
    decision["recipes"] = {**retained, **decision["recipes"]}
    decision["source_observations"] = [
        list(pair) for pair in sorted(set(observations) | {tuple(p) for p in decision["source_observations"]})
    ]
    decision[MARKER] = {"review": REVIEW_PATH, "base_decision": base}
    return decision
