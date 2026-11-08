"""Retain complete Ghanaian uncontrolled-refrigerator emissions and existing waste treatments."""

import hashlib
import json
import pickle
import sqlite3
from copy import deepcopy
from pathlib import Path

FIELDS = ("name", "reference product", "location", "unit")
ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_ghana_uncontrolled_review.json"
REVIEW_SHA256 = "3305a7ab677bb5a88598f5ba7248af63e1d5c075100195840e2defe7544b48ca"
if hashlib.sha256((ROOT / REVIEW_PATH).read_bytes()).hexdigest() != REVIEW_SHA256:
    raise ValueError("Ghanaian uncontrolled refrigerator-disposal review changed; repeat the scientific review.")
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
    if len(refs) != 1 or refs[0]["input"] != refs[0]["output"] or refs[0]["amount"] != -1:
        raise ValueError("Ghanaian uncontrolled refrigerator-disposal reference changed; repeat the scientific review.")
    if (
        md.get("parameters")
        or len(rows) != 13
        or sum(e["type"] == "biosphere" for e in rows) != 5
        or sum(e["type"] == "technosphere" for e in rows) != 7
    ):
        raise ValueError("Ghanaian uncontrolled dismantling full boundary changed; repeat the source review.")
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
    """Read complete-vector fingerprints from the nine explicit original profiles."""
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
            raise ValueError(
                "Ghanaian uncontrolled refrigerator-disposal profile coverage changed; repeat the scientific review."
            )
    return result


def extend_observations(records, observations):
    for row in records:
        observations["ecoinvent"][identity(row["source"])].add((row["label"], row["reference_amount"]))


def validate_inventories(records, observations):
    if len(records) != 9 or sorted(map(digest, records)) != sorted(map(digest, APPROVAL["inventories"])):
        raise ValueError(
            "Ghanaian uncontrolled refrigerator source vector or metadata changed; repeat the scientific review."
        )
    for source, row in SOURCES.items():
        actual = {
            (label, sign) for label, sign in observations["ecoinvent"].get(source, set()) if label.endswith("-cutoff")
        }
        if actual != {(p["label"], -1) for p in row["profiles"]}:
            raise ValueError("Ghanaian uncontrolled refrigerator source exact profiles or reference changed.")
    for path, expected in APPROVAL["canonical_identity_dependency_sha256"].items():
        if hashlib.sha256((ROOT / path).read_bytes()).hexdigest() != expected:
            raise ValueError("Existing canonical publisher identity review changed.")
    reverse = APPROVAL["independent_reverse"]
    if hashlib.sha256((ROOT / reverse["resource_path"]).read_bytes()).hexdigest() != reverse["resource_sha256"]:
        raise ValueError("Independent reverse resource changed; no inverse is inferred.")


def forward_decision(rule, observations, binding, original_review):
    row = SOURCES[identity(rule["source"])]
    cutoff = {(label, sign) for label, sign in observations if label.endswith("-cutoff")}
    if (
        rule["target"] != row["previous_target"]
        or rule.get("conversion_factor", 1) != 1
        or cutoff != {(p["label"], -1) for p in row["profiles"]}
    ):
        raise ValueError("Ghanaian uncontrolled refrigerator source target, factor or exact scope changed.")
    profiles, recipes = [], {}
    for p in row["profiles"]:
        recipes[p["recipe_id"]] = deepcopy(p["recipe"])
        profiles.append(
            {
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
        )
    return {
        "operation": "recipe",
        "recipe_information_loss": LIMITATIONS,
        "binding": binding,
        "original_rule": deepcopy(rule),
        "original_review": deepcopy(original_review),
        "original_factor": 1,
        "factor": -1,
        "source_observations": [list(p) for p in sorted(cutoff)],
        "uvek_reference_sign": 1,
        "uvek_native_flow_type": "GENERATED_SERVICE",
        "original_native_flow_type": row["previous_native_flow_type"],
        "target": deepcopy(profiles[0]["target"]),
        "recipes": recipes,
        "recipe_review": {"rationale": LIMITATIONS, "limitations": [LIMITATIONS], "evidence": [REVIEW_PATH]},
        "mapping_method": "reviewed_ghana_uncontrolled_inventory",
        "review": {
            "status": "unresolved",
            "reason": "Select an exact inspected cut-off uncontrolled-dismantling source profile; no missing-profile or cross-model fallback.",
            "evidence": [REVIEW_PATH],
            "profiles": profiles,
        },
    }


def validate_bindings(resource):
    """Require every exact selected component and allocated-stock body to remain reviewed."""
    from brightpath import TechnosphereProfile
    from brightpath.background.reference_signs import select_profile_sign_rules

    for row in SOURCES.values():
        for profile in row["profiles"]:
            selected = select_profile_sign_rules(resource, TechnosphereProfile(*profile["label"].split("-")))
            by = {identity(r["source"]): r for r in selected["replace"]}
            for binding in profile["component_bindings"]:
                if digest(by.get(identity(binding["source"]))) != binding["binding_sha256"]:
                    raise ValueError("Ghanaian uncontrolled refrigerator component changed; repeat its review.")
    for rid, expected in APPROVAL["component_helper_sha256"].items():
        if digest(resource["recipes"].get(rid)) != expected:
            raise ValueError("Ghanaian recovered-material allocation helper changed; repeat its review.")
