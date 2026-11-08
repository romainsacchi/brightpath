"""Retain exact source-only gas flaring where an operating proxy would change emissions."""

import hashlib
import json
import pickle
import sqlite3
from copy import deepcopy
from pathlib import Path

FIELDS = ("name", "reference product", "location", "unit")
ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_remaining_gas_flare_review.json"
REVIEW_SHA256 = "4272c347e7e4a4a8b5b9f015007f54b4b4621639bd250c618c40600877b236a6"
if hashlib.sha256((ROOT / REVIEW_PATH).read_bytes()).hexdigest() != REVIEW_SHA256:
    raise ValueError("Source gas-flare review changed; repeat the scientific review.")
APPROVAL = json.loads((ROOT / REVIEW_PATH).read_text())
SOURCES = {tuple(r["source"][f] for f in FIELDS): r for r in APPROVAL["rules"]}
LIMITATIONS = APPROVAL["limitations"]


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def identity(row):
    return tuple(row[f] for f in FIELDS)


def inventory_record(label, md, blobs):
    rows = [pickle.loads(b) for b in blobs]
    refs = [e for e in rows if e["type"] == "production"]
    if (
        len(refs) != 1
        or refs[0]["input"] != refs[0]["output"]
        or refs[0]["amount"] != (1 if label.startswith("uvek-") else -1)
    ):
        raise ValueError("Source gas-flare reference changed; repeat the scientific review.")
    return {
        "label": label,
        "source": {f: md[f] for f in FIELDS},
        "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
        "activity_comment_sha256": hashlib.sha256(str(md.get("comment") or "").encode()).hexdigest(),
        "product_information_sha256": hashlib.sha256(str(md.get("product_information") or "").encode()).hexdigest(),
        "reference_amount": refs[0]["amount"],
        "total_exchanges": len(rows),
        "formula_rows": sum(bool(e.get("formula")) for e in rows),
        "formula_sha256": digest(sorted(e.get("formula") or "" for e in rows)),
    }


def collect_inventories(brightway_root):
    """Read complete-vector fingerprints from the ten explicit original profiles."""
    result = []
    for entry in APPROVAL["collection_profiles"]:
        label = "-".join(entry["profile"])
        wanted = {identity(r["source"]) for r in APPROVAL["inventories"] if r["label"] == label}
        path = brightway_root / entry["project"] / "lci/databases.db"
        found = set()
        with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as con:
            for name in sorted({s[0] for s in wanted}):
                for (raw,) in con.execute(
                    "SELECT data FROM activitydataset WHERE database=? AND name=?", (entry["database"], name)
                ):
                    md = pickle.loads(raw)
                    if identity(md) not in wanted:
                        continue
                    blobs = [
                        b
                        for b, in con.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (entry["database"], md["code"]),
                        )
                    ]
                    result.append(inventory_record(label, md, blobs))
                    found.add(identity(md))
        if found != wanted:
            raise ValueError("Source gas-flare profile coverage changed; repeat the scientific review.")
    return result


def extend_observations(records, observations):
    for row in records:
        source = identity(row["source"])
        if source in SOURCES:
            observations["ecoinvent"][source].add((row["label"], row["reference_amount"]))


def validate_inventories(records, observations, native):
    if len(records) != 37 or sorted(map(digest, records)) != sorted(map(digest, APPROVAL["inventories"])):
        raise ValueError("Source gas-flare inventory or formula metadata changed; repeat the scientific review.")
    for source, row in SOURCES.items():
        if {
            (label, sign) for label, sign in observations["ecoinvent"].get(source, set()) if label.endswith("-cutoff")
        } != {(p["label"], -1) for p in row["profiles"]}:
            raise ValueError("Source gas-flare reference observations changed.")
    witness = next(r for r in APPROVAL["inventories"] if r["label"] == "uvek-2025-cutoff")
    role = native.get(identity(witness["source"]), {})
    if role.get("native_flow_type") != "PRODUCT_FLOW" or role.get("native_reference_is_input") is not False:
        raise ValueError("Retained native gas-flare role changed.")
    for path, expected in APPROVAL["canonical_identity_dependency_sha256"].items():
        if hashlib.sha256((ROOT / path).read_bytes()).hexdigest() != expected:
            raise ValueError("Gas-flare canonical biosphere changed; repeat the scientific review.")
    reverse = APPROVAL["independent_reverse"]
    if hashlib.sha256((ROOT / reverse["resource_path"]).read_bytes()).hexdigest() != reverse["resource_sha256"]:
        raise ValueError("Independent reverse gas-flare resource changed.")


def forward_decision(rule, observations, binding, previous, original_review):
    row = SOURCES[identity(rule["source"])]
    cutoff = {(label, sign) for label, sign in observations if label.endswith("-cutoff")}
    if (
        rule["target"] != row["previous_target"]
        or rule.get("conversion_factor", 1) not in ((1, -1 / 36) if "sweet" in row["source"]["name"] else (1,))
        or cutoff != {(p["label"], -1) for p in row["profiles"]}
    ):
        raise ValueError("Source gas-flare supplier, amount or exact scope changed.")
    profiles, recipes = [], {}
    for p in row["profiles"]:
        if "retained_native_profile" in p:
            entry = deepcopy(p["retained_native_profile"])
        else:
            recipes[p["recipe_id"]] = deepcopy(p["recipe"])
            recipes.update(deepcopy(p.get("dependency_recipes", {})))
            entry = {
                "source_profile": dict(zip(("family", "version", "system_model"), p["label"].split("-"), strict=True)),
                "target": {"recipe_ref": p["recipe_id"]},
                "conversion_factor": -1,
                "review": {
                    "status": "verified",
                    "source_sign": -1,
                    "target_sign": 1,
                    "reason": LIMITATIONS,
                    "evidence": [f"{REVIEW_PATH}#{p['label']}/{p['inventory_sha256']}"],
                },
            }
        profiles.append(entry)
    retained = next((p for p in row["profiles"] if "retained_native_profile" in p), None)
    first = (
        {"target": retained["target"], "conversion_factor": -1 / 36}
        if retained
        else next(p for p in profiles if "recipe_ref" in p["target"])
    )
    original = deepcopy(previous.get("original_rule", rule))
    if "original_rule" not in previous and "original_target" in previous:
        original["target"] = deepcopy(previous["original_target"])
        if previous.get("original_factor", 1) == 1:
            original.pop("conversion_factor", None)
        else:
            original["conversion_factor"] = previous["original_factor"]
    return {
        "operation": "recipe",
        "binding": binding,
        "original_rule": original,
        "original_review": deepcopy(previous.get("original_review", original_review)),
        "original_factor": 1,
        "factor": first["conversion_factor"],
        "source_observations": [list(p) for p in sorted(cutoff)],
        "uvek_reference_sign": 1,
        "uvek_native_flow_type": "GENERATED_SERVICE",
        "original_native_flow_type": "PRODUCT_FLOW",
        "target": deepcopy(first["target"]),
        "recipes": recipes,
        "recipe_review": {"rationale": LIMITATIONS, "limitations": [LIMITATIONS], "evidence": [REVIEW_PATH]},
        "recipe_information_loss": LIMITATIONS,
        "mapping_method": "reviewed_source_gas_flare_service",
        "review": {
            "status": "unresolved",
            "reason": "Select an exact inspected cut-off flare or market profile; no cross-model or missing-profile fallback.",
            "evidence": [REVIEW_PATH],
            "profiles": profiles,
        },
    }
