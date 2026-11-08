"""Compile inspected cable-processing and complete Swiss freight service boundaries."""

import hashlib
import json
import pickle
import sqlite3
from copy import deepcopy
from pathlib import Path

from brightpath import TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules

FIELDS = ("name", "reference product", "location", "unit")
ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_cable_rail_review.json"
APPROVAL = json.loads((ROOT / REVIEW_PATH).read_text())
REVIEW_SHA256 = "001fef2fa3fffd4f2eaf135071d77699cf871ee4b8df2d8b4d531d96cac39b93"
SOURCES = {tuple(r["source"][f] for f in FIELDS): r for r in APPROVAL["root_rules"]}
EVIDENCE_SCOPE = {}
for row in APPROVAL["inventories"]:
    EVIDENCE_SCOPE.setdefault(tuple(row["label"].split("-")), set()).add(tuple(row["source"][f] for f in FIELDS))


def identity(row):
    return tuple(row[f] for f in FIELDS)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def validate_review():
    """Require the complete reviewed scope, including separately approved FAE limitations."""
    if (
        digest(APPROVAL) != REVIEW_SHA256
        or len(SOURCES) != 4
        or APPROVAL["root_profile_count"] != 34
        or APPROVAL["parent_profile_count"] != 321
        or len(APPROVAL["inventories"]) != 535
        or APPROVAL["authorized_system_model"] != "cutoff"
        or APPROVAL["approval"]["decision"] != "Approve scoped proxy with warnings"
        or len(APPROVAL["existing_freight_patch_cases"]) != 4
    ):
        raise ValueError("Cable/rail review scope changed; repeat the scientific review.")


def inventory_record(source, blobs, comment, product_information):
    """Pin every original exchange, including zero rows, without distributing raw inventories."""
    refs = [e for e in map(pickle.loads, blobs) if e["type"] == "production"]
    if len(refs) != 1 or refs[0]["amount"] not in (-1, 1) or refs[0]["input"] != refs[0]["output"]:
        raise ValueError("Cable/rail reference changed; repeat the scientific review.")
    return {
        "source": list(source),
        "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
        "activity_comment_sha256": hashlib.sha256(str(comment or "").encode()).hexdigest(),
        "product_information_sha256": hashlib.sha256(str(product_information or "").encode()).hexdigest(),
        "reference_amount": refs[0]["amount"],
        "total_exchanges": len(blobs),
    }


def collect_extra_inventories(brightway_root):
    """Read original exact patch databases only; never substitute their base release."""
    records = []
    for entry in APPROVAL["extra_profiles"]:
        scope = EVIDENCE_SCOPE[tuple(entry["profile"])]
        path = brightway_root / entry["project"] / "lci/databases.db"
        found = set()
        with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as connection:
            for name in sorted({s[0] for s in scope}):
                for (raw,) in connection.execute(
                    "SELECT data FROM activitydataset WHERE database=? AND name=?", (entry["database"], name)
                ):
                    md = pickle.loads(raw)
                    source = identity(md)
                    if source not in scope:
                        continue
                    blobs = [
                        b
                        for b, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (entry["database"], md["code"]),
                        )
                    ]
                    records.append(
                        {
                            **inventory_record(source, blobs, md.get("comment"), md.get("product_information")),
                            "label": "-".join(entry["profile"]),
                        }
                    )
                    found.add(source)
        if found != scope:
            raise ValueError("Cable/rail patch coverage changed; repeat the scientific review.")
    return records


def validate_inventories(inventories):
    """Require all complete source, parent, native-service and lifecycle witnesses."""
    validate_review()
    expected = {
        json.dumps({**r, "source": list(identity(r["source"]))}, sort_keys=True) for r in APPROVAL["inventories"]
    }
    actual = {
        json.dumps({**r, "source": list(source)}, sort_keys=True) for source, rows in inventories.items() for r in rows
    }
    if expected != actual or sum(map(len, inventories.values())) != 535:
        raise ValueError("Cable/rail full inventory evidence changed; repeat the scientific review.")


def extend_parent_patch_evidence(observations, market_inventories, inventories):
    """Expose only two newly inspected cable-market profiles to the existing composition author."""
    if not inventories:
        return
    validate_inventories(inventories)
    source = ("market for used cable", "used cable", "GLO", "kilogram")
    for record in inventories[source]:
        if record["label"] not in {"ecoinvent-3.9.1-cutoff", "ecoinvent-3.10.1-cutoff"}:
            continue
        if record["reference_amount"] != -1:
            raise ValueError("Cable-market patch reference changed; repeat the scientific review.")
        observations["ecoinvent"][source].add((record["label"], -1))
        market_inventories[source].append((record["label"], record["inventory_sha256"]))


def forward_decision(rule, observations, binding, original_review):
    """Compile exact cut-off helpers; retain old model observations as historical evidence only."""
    validate_review()
    row = SOURCES[identity(rule["source"])]
    if binding != row["binding_sha256"] or rule.get("conversion_factor", 1) != row["original_factor"]:
        raise ValueError("Cable/rail original correspondence changed; repeat the scientific review.")
    observed = set(observations)
    observed.update((p["label"], p["source_reference"]) for p in row["profiles"])
    if sorted(map(list, observed)) != row["source_observations"]:
        raise ValueError("Cable/rail source references changed; repeat the scientific review.")
    profiles = []
    for p in row["profiles"]:
        body = APPROVAL["library"][p["target"]["recipe_ref"]]
        profiles.append(
            {
                "source_profile": dict(zip(("family", "version", "system_model"), p["label"].split("-"), strict=True)),
                "target": deepcopy(p["target"]),
                "conversion_factor": p["conversion_factor"],
                "review": {
                    "status": "verified",
                    "source_sign": p["source_reference"],
                    "target_sign": 1,
                    "reason": body["rationale"],
                    "evidence": [f"{REVIEW_PATH}#{p['label']}/{p['inventory_sha256']}"],
                },
            }
        )
    profiles.sort(key=lambda p: tuple(p["source_profile"][f] for f in ("family", "version", "system_model")))
    first = row["profiles"][0]
    rationale = APPROVAL["library"][first["target"]["recipe_ref"]]["rationale"]
    recipes = (
        deepcopy(APPROVAL["library"])
        if row["library_owner"]
        else {
            p["target"]["recipe_ref"]: deepcopy(APPROVAL["library"][p["target"]["recipe_ref"]]) for p in row["profiles"]
        }
    )
    return {
        "operation": "recipe",
        "binding": binding,
        "original_rule": deepcopy(rule),
        "original_review": deepcopy(original_review),
        "original_factor": row["original_factor"],
        "factor": first["conversion_factor"],
        "source_observations": sorted(map(list, observed)),
        "uvek_reference_sign": 1,
        "uvek_native_flow_type": "GENERATED_SERVICE",
        "original_native_flow_type": row["original_native_flow_type"],
        "target": deepcopy(first["target"]),
        "recipes": recipes,
        "recipe_review": {"rationale": rationale, "limitations": [rationale], "evidence": [REVIEW_PATH]},
        "recipe_information_loss": rationale,
        "mapping_method": "reviewed_" + row["kind"].replace("-", "_"),
        "review": {
            "status": "unresolved",
            "reason": "Select an exact independently inspected cut-off cable/wire-plastic/freight profile.",
            "evidence": [REVIEW_PATH],
            "profiles": profiles,
        },
    }


def validate_existing_freight_bindings(resource):
    """Retain the already reviewed actual-patch whole-freight suppliers unchanged."""
    selected = {}
    for row in APPROVAL["existing_freight_patch_cases"]:
        label = row["label"]
        if label not in selected:
            view = select_profile_sign_rules(resource, TechnosphereProfile(*label.split("-")))
            selected[label] = {identity(r["source"]): r for r in view["replace"]}
        if digest(selected[label].get(identity(row["source"]))) != row["binding_sha256"]:
            raise ValueError("Existing cable-market freight correspondence changed; repeat the scientific review.")
