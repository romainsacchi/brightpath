"""Retain copper-specific ash releases and collection in exact cut-off profiles."""

import hashlib
import json
import pickle
import sqlite3
from copy import deepcopy
from pathlib import Path

from brightpath import TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.migrations.canonical_biosphere import select_canonical_biosphere_rules

try:
    from review_uvek_market_compositions import composition_decision
except ModuleNotFoundError:
    from scripts.review_uvek_market_compositions import composition_decision

FIELDS = ("name", "reference product", "location", "unit")
ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_copper_ash_review.json"
APPROVAL = json.loads((ROOT / REVIEW_PATH).read_text())
REVIEW_SHA256 = "cee3a0772d00dc7e43855974dd653adce00f800bacc5bfeb9897d56808d6db62"
SOURCES = {tuple(r["source"][f] for f in FIELDS): r for r in APPROVAL["rules"]}
LIMITATIONS = APPROVAL["limitations"]


def identity(row):
    return tuple(row[f] for f in FIELDS)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def inventory_record(label, metadata, blobs):
    """Pin the whole inventory, reference coordinate and parameter metadata."""
    exchanges = [pickle.loads(blob) for blob in blobs]
    (reference,) = [e for e in exchanges if e["type"] == "production"]
    if reference["amount"] != -1 or reference["input"] != reference["output"]:
        raise ValueError("Copper-ash negative one-kg reference changed.")
    return {
        "label": label,
        "source": {f: metadata[f] for f in FIELDS},
        "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
        "metadata_sha256": digest(metadata),
        "reference_amount": -1,
        "total_exchanges": len(exchanges),
    }


def collect_inventories(brightway_root):
    """Read four exact source inventories without writing original databases."""
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
            raise ValueError("Copper-ash exact source/profile scope changed.")
    return records


def validate_inventories(records, observations, native):
    """Keep copper-ash disposal distinct from original-waste treatment and landfarming."""
    if digest(APPROVAL) != REVIEW_SHA256 or APPROVAL["authorized_system_model"] != "cutoff":
        raise ValueError("Copper-ash scientific review changed.")
    if len(records) != 4 or sorted(map(digest, records)) != sorted(map(digest, APPROVAL["inventories"])):
        raise ValueError("Copper-ash full inventories or metadata changed.")
    for row in SOURCES.values():
        expected = {(p["label"], -1) for p in row["profiles"]}
        observed = {
            pair
            for pair in observations["ecoinvent"].get(identity(row["source"]), set())
            if pair[0].endswith("-cutoff")
        }
        if observed != expected:
            raise ValueError("Copper-ash source reference or profile scope changed.")
    for role in APPROVAL["native_roles"]:
        if any(
            native.get(identity(role["source"]), {}).get(f) != role[f]
            for f in ("native_flow_type", "native_reference_is_input", "flow_property_name", "unit_name")
        ):
            raise ValueError("Copper-ash native component role changed.")
    for frozen in (APPROVAL["component_review"], APPROVAL["independent_reverse"]):
        path = frozen.get("path", frozen.get("resource_path"))
        expected = frozen.get("sha256", frozen.get("resource_sha256"))
        if hashlib.sha256((ROOT / path).read_bytes()).hexdigest() != expected:
            raise ValueError("Copper-ash component review or independent reverse resource changed.")


def validate_bindings(resource):
    """Require exact approved component, freight and copper water-flow correspondences."""
    canonical = json.loads(
        (ROOT / "brightpath/data/migrations/uvek/ecoinvent-to-ecoinvent-3.10-biosphere.json").read_text()
    )
    native = json.loads(
        (ROOT / "brightpath/data/migrations/uvek/ecoinvent-3.10-biosphere-to-uvek-2026-biosphere.json").read_text()
    )

    def bio_key(row):
        return row["name"], tuple(row["categories"]), row["unit"]

    native_rules = {bio_key(r["source"]): r for r in native["replace"]}
    for row in SOURCES.values():
        for profile in row["profiles"]:
            label = profile["label"]
            selected = {
                identity(r["source"]): r
                for r in select_profile_sign_rules(resource, TechnosphereProfile(*label.split("-")))["replace"]
            }
            bio_rules = {
                bio_key(r["source"]): r
                for r in select_canonical_biosphere_rules(canonical, label.split("-")[1])["replace"]
            }
            for binding in profile["bindings"]:
                if "internal_recipe" in binding:
                    # Its full body is frozen by REVIEW_SHA256 and built by this review.
                    continue
                if "source_biosphere" in binding:
                    key = bio_key(binding["source_biosphere"])
                    if (
                        digest(bio_rules.get(key)) != binding["canonical_binding_sha256"]
                        or digest(native_rules.get(key)) != binding["native_binding_sha256"]
                    ):
                        raise ValueError("Copper-ash physical-flow binding changed.")
                elif digest(selected.get(tuple(binding["source"]))) != binding["binding_sha256"]:
                    raise ValueError("Copper-ash operating, facility or collection binding changed.")


def forward_decision(rule, observations, binding, original_review):
    """Replace incomplete ash proxies with their own releases, operation and collection."""
    source = identity(rule["source"])
    row = SOURCES[source]
    if rule.get("conversion_factor", 1) not in (1, -1):
        raise ValueError("Copper-ash original conversion factor changed.")
    original = deepcopy(rule)
    original["conversion_factor"] = row["original_factor"]
    inventories = [r for r in APPROVAL["inventories"] if identity(r["source"]) == source]
    return composition_decision(
        original,
        observations,
        [(r["label"], r["inventory_sha256"]) for r in inventories],
        binding,
        original_review,
        approval_row=row,
        rationale=LIMITATIONS,
        review_path=REVIEW_PATH,
        namespace="copper-ash-service",
        mapping_method="reviewed_copper_ash_source_reconstruction",
    )
