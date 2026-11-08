"""Retain exact allocated copper stock without importing PV-recycling burdens or credits."""

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
except ModuleNotFoundError:
    from scripts.review_uvek_cutoff_stocks import stock_decision

FIELDS = ("name", "reference product", "location", "unit")
ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_copper_stock_review.json"
APPROVAL = json.loads((ROOT / REVIEW_PATH).read_text())
REVIEW_SHA256 = "6c9bdfbd908435bb02cb0c2b935067857def43b4271a9abc2c6ea0b44a3b91a1"
SOURCES = {tuple(r["source"][f] for f in FIELDS): r for r in APPROVAL["rules"]}
VERSIONS = {"3.6", "3.7", "3.8", "3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12"}
EVIDENCE_SCOPE = {}
for row in APPROVAL["inventories"]:
    profile = tuple(row["label"].split("-"))
    if profile not in {("ecoinvent", v, "cutoff") for v in VERSIONS} | {("uvek", "2025", "cutoff")}:
        raise ValueError("Unreviewed copper-stock evidence profile.")
    EVIDENCE_SCOPE.setdefault(profile, set()).add(tuple(row["source"][f] for f in FIELDS))


def identity(row):
    return tuple(row[f] for f in FIELDS)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def inventory_record(source, blobs, comment, product_information):
    """Pin the entire allocation/reference boundary and the prior nonempty native process."""
    rows = [pickle.loads(b) for b in blobs]
    (reference,) = [r for r in rows if r["type"] == "production"]
    if reference["amount"] not in (-1, 1) or tuple(reference["input"]) != tuple(reference["output"]):
        raise ValueError("Copper-stock positive reference changed; repeat the scientific review.")
    return {
        "source": list(source),
        "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
        "activity_comment_sha256": hashlib.sha256(str(comment or "").encode()).hexdigest(),
        "product_information_sha256": hashlib.sha256(str(product_information or "").encode()).hexdigest(),
        "reference_amount": int(reference["amount"]),
        "total_exchanges": len(rows),
        "only_self_production": len(rows) == 1,
        "production_amount": int(reference["amount"]),
    }


def collect_extra_inventories(brightway_root):
    """Read both original patches, without inferring any other patch or system model."""
    result = []
    for entry in APPROVAL["extra_profiles"]:
        scope = EVIDENCE_SCOPE[tuple(entry["profile"])]
        path = brightway_root / entry["project"] / "lci/databases.db"
        with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as con:
            found = set()
            for name in sorted({s[0] for s in scope}):
                for (raw,) in con.execute(
                    "SELECT data FROM activitydataset WHERE database=? AND name=?", (entry["database"], name)
                ):
                    metadata = pickle.loads(raw)
                    source = identity(metadata)
                    if source not in scope:
                        continue
                    blobs = [
                        b
                        for b, in con.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (entry["database"], metadata["code"]),
                        )
                    ]
                    result.append(
                        {
                            **inventory_record(
                                source, blobs, metadata.get("comment"), metadata.get("product_information")
                            ),
                            "label": "-".join(entry["profile"]),
                        }
                    )
                    found.add(source)
            if found != scope:
                raise ValueError("Copper-stock exact patch scope changed; repeat the scientific review.")
    return result


def validate_inventories(inventories, observations, native):
    """Require all nine self-only source stocks and the complete former PV target."""
    expected = {
        json.dumps({**r, "source": list(identity(r["source"]))}, sort_keys=True) for r in APPROVAL["inventories"]
    }
    actual = {json.dumps({**r, "source": list(s)}, sort_keys=True) for s, rows in inventories.items() for r in rows}
    if actual != expected or len(expected) != 73 or sum(map(len, inventories.values())) != 73:
        raise ValueError("Copper-stock inventory or description changed; repeat the scientific review.")
    for source, row in SOURCES.items():
        wanted = {(p["label"], 1) for p in row["profiles"]}
        observed = {
            (label, sign) for label, sign in observations["ecoinvent"].get(source, set()) if label.endswith("-cutoff")
        }
        if observed != wanted or {(p["label"], p["reference_amount"]) for p in inventories[source]} != wanted:
            raise ValueError("Copper-stock exact source scope changed; repeat the scientific review.")
        if any(
            p["total_exchanges"] != 1 or p["only_self_production"] is not True or p["production_amount"] != 1
            for p in inventories[source]
        ):
            raise ValueError("Copper-stock allocation boundary changed; repeat the scientific review.")
    (role,) = APPROVAL["native_roles"]
    source = identity(role["source"])
    if (
        native.get(source, {}).get("native_flow_type") != role["native_flow_type"]
        or native[source].get("native_reference_is_input") is not role["native_reference_is_input"]
        or observations["uvek"].get(source) != {("uvek-2025-cutoff", 1)}
    ):
        raise ValueError("Former PV-copper native role changed; repeat the scientific review.")


def validate_bindings(resource):
    """Guard the standing material-stock scope and prohibit an inferred physical recycling inverse."""
    if (
        digest(APPROVAL) != REVIEW_SHA256
        or APPROVAL["authorized_system_model"] != "cutoff"
        or (APPROVAL["source_count"], APPROVAL["source_profile_count"], APPROVAL["inventory_count"]) != (1, 9, 73)
        or len(SOURCES) != 1
    ):
        raise ValueError("Copper-stock allocation approval changed; repeat the scientific review.")
    (source,) = SOURCES
    if source != (
        "copper scrap, sorted, pressed, Recycled Content cut-off",
        "copper scrap, sorted, pressed",
        "GLO",
        "kilogram",
    ):
        raise ValueError("Unreviewed copper allocation-stock context.")
    if (
        APPROVAL["dependent_parent_source_count"] != 9
        or APPROVAL["dependent_parent_profile_count"] != 63
        or len(APPROVAL["direct_stock_transformations"]) != 42
        or len(APPROVAL["parent_rule_bindings"]) != 63
        or not APPROVAL["historical_consequential_coefficients_unchanged"]
    ):
        raise ValueError("Copper-stock dependency scope changed; repeat the scientific review.")
    by_profile = {}
    for entry in APPROVAL["parent_rule_bindings"]:
        label = entry["label"]
        if label not in by_profile:
            selected = select_profile_sign_rules(resource, TechnosphereProfile(*label.split("-")))
            by_profile[label] = {identity(r["source"]): r for r in selected["replace"]}
        if digest(by_profile[label].get(identity(entry["source"]))) != entry["binding_sha256"]:
            raise ValueError("Copper-stock parent binding changed; repeat the scientific review.")
    reverse = APPROVAL["independent_reverse_resource"]
    path = ROOT / reverse["path"]
    if hashlib.sha256(path.read_bytes()).hexdigest() != reverse["sha256"]:
        raise ValueError("Copper-stock independent reverse coverage changed; repeat its review.")
    if any(
        r["source"] == APPROVAL["unsupported_native_reverse_source"] for r in json.loads(path.read_text())["replace"]
    ):
        raise ValueError("A new inverse PV-recycling correspondence requires independent review.")


def forward_decision(rule, observations, inventories, binding, original_review):
    """Keep signed stock quantities while removing invented PV-recycling burdens or avoided credits."""
    decision = stock_decision(
        rule,
        {(label, sign) for label, sign in observations if label.endswith("-cutoff")},
        inventories,
        binding,
        deepcopy(original_review),
        approval_row=SOURCES[identity(rule["source"])],
        review_path=REVIEW_PATH,
    )
    decision["source_observations"] = [list(pair) for pair in sorted(observations)]
    return decision
