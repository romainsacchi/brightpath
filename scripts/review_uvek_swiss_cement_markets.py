"""Retain product delivery around complete Swiss CEM II production proxies."""

import hashlib
import json
import pickle
import sqlite3
from pathlib import Path

from brightpath import TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules

try:
    from review_uvek_market_compositions import composition_decision
except ModuleNotFoundError:
    from scripts.review_uvek_market_compositions import composition_decision

FIELDS = ("name", "reference product", "location", "unit")
ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_swiss_cement_market_review.json"
APPROVAL = json.loads((ROOT / REVIEW_PATH).read_text())
REVIEW_SHA256 = "97c32131ee6c0411cf9bb29e14e27ada63a181ab304da954a5f9dc0caa8a4944"
SOURCES = {tuple(r["source"][f] for f in FIELDS): r for r in APPROVAL["rules"]}
LIMITATIONS = APPROVAL["limitations"]
MARKER = "swiss_cement_delivery_review"


def identity(row):
    return tuple(row[f] for f in FIELDS)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def inventory_record(label, metadata, blobs):
    """Fingerprint the exact positive-product function and full source metadata."""
    exchanges = [pickle.loads(blob) for blob in blobs]
    (reference,) = [e for e in exchanges if e["type"] == "production"]
    if reference["amount"] != 1 or reference["input"] != reference["output"]:
        raise ValueError("Swiss cement positive-product reference changed.")
    return {
        "label": label,
        "source": {f: metadata[f] for f in FIELDS},
        "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
        "metadata_sha256": digest(metadata),
        "activity_comment_sha256": hashlib.sha256(str(metadata.get("comment") or "").encode()).hexdigest(),
        "product_information_sha256": hashlib.sha256(
            str(metadata.get("product_information") or "").encode()
        ).hexdigest(),
        "reference_amount": 1,
        "total_exchanges": len(exchanges),
    }


def collect_inventories(brightway_root):
    """Read the 26 exact market/producer/native witnesses without writing original databases."""
    records = []
    for entry in APPROVAL["collection_profiles"]:
        label = "-".join(entry["profile"])
        wanted = {identity(r["source"]) for r in APPROVAL["inventories"] if r["label"] == label}
        if not wanted:
            continue
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
            raise ValueError("Swiss cement exact source/product/profile scope changed.")
    return records


def validate_inventories(records, observations, native):
    """Freeze cement products separately from same-named allocated ash functions."""
    if digest(APPROVAL) != REVIEW_SHA256 or APPROVAL["authorized_system_model"] != "cutoff":
        raise ValueError("Swiss cement scientific review changed.")
    if len(records) != 26 or sorted(map(digest, records)) != sorted(map(digest, APPROVAL["inventories"])):
        raise ValueError("Swiss cement full inventories or metadata changed.")
    for record in records:
        side = "uvek" if record["label"].startswith("uvek-") else "ecoinvent"
        observations[side][identity(record["source"])].add((record["label"], 1))
    for role in APPROVAL["native_roles"]:
        if any(
            native.get(identity(role["source"]), {}).get(f) != role[f]
            for f in ("native_flow_type", "native_reference_is_input", "flow_property_name", "unit_name")
        ):
            raise ValueError("Swiss cement native product role changed.")
    reverse = APPROVAL["independent_reverse"]
    if hashlib.sha256((ROOT / reverse["resource_path"]).read_bytes()).hexdigest() != reverse["resource_sha256"]:
        raise ValueError("Independent reverse cement resource changed.")


def validate_bindings(resource):
    """Require the separately reviewed whole production and exact freight selections."""
    selected = {}
    for row in APPROVAL["rules"]:
        for profile in row["profiles"]:
            label = profile["label"]
            if label not in selected:
                selected[label] = {
                    identity(r["source"]): r
                    for r in select_profile_sign_rules(resource, TechnosphereProfile(*label.split("-")))["replace"]
                }
            for binding in profile["bindings"]:
                if digest(selected[label].get(tuple(binding["source"]))) != binding["binding_sha256"]:
                    raise ValueError("Swiss cement production or freight binding changed.")


def forward_decision(rule, observations, binding, original_review):
    """Compose the positive product and delivery without changing its incoming sign."""
    row = SOURCES[identity(rule["source"])]
    inventories = [r for r in APPROVAL["inventories"] if identity(r["source"]) == identity(rule["source"])]
    return composition_decision(
        rule,
        observations,
        [(r["label"], r["inventory_sha256"]) for r in inventories],
        binding,
        original_review,
        approval_row=row,
        rationale=LIMITATIONS,
        review_path=REVIEW_PATH,
        namespace="swiss-cement-delivery",
        mapping_method="reviewed_swiss_cement_market_delivery",
    )
