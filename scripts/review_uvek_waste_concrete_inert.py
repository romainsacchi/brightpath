"""Retain Swiss waste-concrete landfill fate and exact cut-off collection."""

import hashlib
import json
import pickle
import sqlite3
from copy import deepcopy
from pathlib import Path

from brightpath import TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules

try:
    from review_uvek_market_compositions import composition_decision
except ModuleNotFoundError:
    from scripts.review_uvek_market_compositions import composition_decision

FIELDS = ("name", "reference product", "location", "unit")
ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_waste_concrete_inert_review.json"
APPROVAL = json.loads((ROOT / REVIEW_PATH).read_text())
REVIEW_SHA256 = "5a846688a1d6bee2e58ed686e843b3573a82a4ab1eadd2146b3de7686096bb2b"
SOURCES = {tuple(r["source"][f] for f in FIELDS): r for r in APPROVAL["rules"]}
LIMITATIONS = APPROVAL["limitations"]


def identity(row):
    return tuple(row[f] for f in FIELDS)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def inventory_record(label, metadata, blobs):
    """Fingerprint complete source or native component without redistributing inventories."""
    exchanges = [pickle.loads(blob) for blob in blobs]
    (reference,) = [e for e in exchanges if e["type"] == "production"]
    expected = -1 if identity(metadata) in SOURCES else 1
    if reference["amount"] != expected or reference["input"] != reference["output"]:
        raise ValueError("Waste-concrete source or complete component reference changed.")
    return {
        "label": label,
        "source": {f: metadata[f] for f in FIELDS},
        "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
        "metadata_sha256": digest(metadata),
        "reference_amount": expected,
        "exchange_count": len(exchanges),
    }


def collect_inventories(brightway_root):
    """Read all 38 exact witnesses through read-only original SQLite connections."""
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
                        b
                        for b, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (entry["database"], metadata["code"]),
                        )
                    ]
                    records.append(inventory_record(label, metadata, blobs))
                    found.add(identity(metadata))
        if found != wanted:
            raise ValueError("Waste-concrete exact source/component/profile scope changed.")
    return records


def validate_inventories(records, observations, native):
    """Bind disposal fate and full generic components independently of reference signs."""
    if digest(APPROVAL) != REVIEW_SHA256 or APPROVAL["authorized_system_model"] != "cutoff":
        raise ValueError("Waste-concrete scientific review changed.")
    if len(records) != 38 or sorted(map(digest, records)) != sorted(map(digest, APPROVAL["inventories"])):
        raise ValueError("Waste-concrete full inventory or metadata changed.")
    # The exact patch releases are freshly collected here, even when the old
    # historical reference scan recorded only the corresponding minor release.
    for record in records:
        if identity(record["source"]) in SOURCES:
            observations["ecoinvent"][identity(record["source"])].add((record["label"], -1))
    for source, row in SOURCES.items():
        expected = {(p["label"], -1) for p in row["profiles"]}
        if {
            (label, sign) for label, sign in observations["ecoinvent"][source] if label.endswith("-cutoff")
        } != expected:
            raise ValueError("Waste-concrete source reference or profile scope changed.")
    reverse = APPROVAL["independent_reverse"]
    if hashlib.sha256((ROOT / reverse["resource_path"]).read_bytes()).hexdigest() != reverse["resource_sha256"]:
        raise ValueError("Waste-concrete independent reverse resource changed.")


def validate_bindings(resource):
    """Require the qualified complete components and independently reviewed collection."""
    for recipe, expected in APPROVAL["recipe_dependencies"].items():
        if digest(resource["recipes"].get(recipe)) != expected:
            raise ValueError("Waste-concrete collection recipe changed.")
    selected = {}
    for row in SOURCES.values():
        for profile in row["profiles"]:
            label = profile["label"]
            if label not in selected:
                selected[label] = {
                    identity(r["source"]): r
                    for r in select_profile_sign_rules(resource, TechnosphereProfile(*label.split("-")))["replace"]
                }
            for binding in profile["bindings"]:
                if "internal_recipe" in binding:
                    continue  # Its full derived body is frozen by REVIEW_SHA256.
                if digest(selected[label].get(tuple(binding["source"]))) != binding["binding_sha256"]:
                    raise ValueError("Waste-concrete operation, facility or collection binding changed.")


def forward_decision(rule, observations, binding, original_review):
    """Preserve landfill operation, facility share and collection without moisture scaling."""
    row = SOURCES[identity(rule["source"])]
    if rule.get("conversion_factor", 1) not in (1, -1):
        raise ValueError("Waste-concrete original conversion factor changed.")
    original = deepcopy(rule)
    original["conversion_factor"] = row["original_factor"]
    inventories = [r for r in APPROVAL["inventories"] if identity(r["source"]) == identity(rule["source"])]
    return composition_decision(
        original,
        observations,
        [(r["label"], r["inventory_sha256"]) for r in inventories],
        binding,
        original_review,
        approval_row=row,
        rationale=LIMITATIONS,
        review_path=REVIEW_PATH,
        namespace="waste-concrete-inert-service",
        mapping_method="reviewed_waste_concrete_inert_reconstruction",
    )
