"""Correct the output grade of one independently inspected whole-concrete proxy."""

import hashlib
import json
import pickle
import sqlite3
from copy import deepcopy
from pathlib import Path

FIELDS = ("name", "reference product", "location", "unit")
ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_concrete_grade_review.json"
APPROVAL_SHA256 = "20cd75c55b9b7676bbe6e6f8da2c819d825d7e3de79431140dcd88ada8334c9e"
if hashlib.sha256((ROOT / REVIEW_PATH).read_bytes()).hexdigest() != APPROVAL_SHA256:
    raise ValueError("Concrete grade approval changed; repeat the scientific review.")
APPROVAL = json.loads((ROOT / REVIEW_PATH).read_text())
SOURCE = tuple(APPROVAL["source"][f] for f in FIELDS)
TARGET = tuple(APPROVAL["target"][f] for f in FIELDS)
PREVIOUS = tuple(APPROVAL["previous_target"][f] for f in FIELDS)
LIMITATIONS = (
    "Reviewed concrete output-grade correction: use the complete UVEK CEM III A, NPK B, primary "
    "at-plant concrete process per +1 cubic meter for the six inspected ecoinvent cut-off CEM III/A "
    "25 MPa producer profiles. Published NPK B is C25/30; the previous NPK A target is C20/25. "
    "This remains a whole-process screening proxy, not a reconstruction of the source formulation: "
    "the source uses about 316.59 kg cement/m3 and the target 280 kg/m3. Swiss primary-aggregate "
    "concrete approximates the global source; binder supplier mix, material doses, operating inputs, "
    "emissions, infrastructure, water, transport, vintage and LCIA equivalence are not established. "
    "The target binder uses no steelmaking burden for slag, consistent with the inspected source "
    "cut-off stock boundary; slag grinding and delivery still have burdens. Do not rescale by "
    "density, cement content, strength or lifetime, or add source process burdens to the complete "
    "target. Preserve signed caller quantities, uncertainty, formulas, foreground suppliers, "
    "production and separate emissions. No source market shares or freight are removed by this "
    "producer-only correction. Other binder families, waste-service coproducts, consequential "
    "profiles and reverse correspondences are outside this approval."
)


def identity(row):
    return tuple(row[f] for f in FIELDS)


def inventory_record(label, source, metadata, blobs):
    rows = [pickle.loads(b) for b in blobs]
    refs = [e for e in rows if e["type"] == "production"]
    if (
        len(refs) != 1
        or type(refs[0]["amount"]) not in (int, float)
        or refs[0]["amount"] != 1
        or refs[0]["input"] != refs[0]["output"]
    ):
        raise ValueError("Concrete grade reference changed; repeat the scientific review.")
    return {
        "label": label,
        "source": dict(zip(FIELDS, source, strict=True)),
        "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
        "activity_comment_sha256": hashlib.sha256(str(metadata.get("comment") or "").encode()).hexdigest(),
        "product_information_sha256": hashlib.sha256(
            str(metadata.get("product_information") or "").encode()
        ).hexdigest(),
        "reference_amount": 1,
        "total_exchanges": len(rows),
    }


def collect_inventories(brightway_root):
    """Collect only the eight frozen complete-vector hashes from original read-only databases."""
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
                    source = identity(md)
                    if source not in wanted:
                        continue
                    blobs = [
                        b
                        for b, in con.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (entry["database"], md["code"]),
                        )
                    ]
                    result.append(inventory_record(label, source, md, blobs))
                    found.add(source)
        if found != wanted:
            raise ValueError("Concrete grade source coverage changed; repeat the scientific review.")
    return result


def validate_inventories(records, observations, native):
    expected = sorted(json.dumps(r, sort_keys=True) for r in APPROVAL["inventories"])
    if sorted(json.dumps(r, sort_keys=True) for r in records) != expected or len(expected) != 8:
        raise ValueError("Concrete grade inventory or metadata changed; repeat the scientific review.")
    if {(label, sign) for label, sign in observations["ecoinvent"].get(SOURCE, set()) if label.endswith("-cutoff")} != {
        (label, 1) for label in APPROVAL["profiles"]
    }:
        raise ValueError("Concrete grade source reference coverage changed.")
    target = native.get(TARGET, {})
    if (
        observations["uvek"].get(TARGET) != {("uvek-2025-cutoff", 1)}
        or target.get("native_flow_type") != "PRODUCT_FLOW"
        or target.get("native_reference_is_input") is not False
    ):
        raise ValueError("Concrete grade native product role changed.")
    reverse = APPROVAL["independent_reverse"]
    if hashlib.sha256((ROOT / reverse["resource_path"]).read_bytes()).hexdigest() != reverse["resource_sha256"]:
        raise ValueError("Independent reverse resource changed; review it independently.")


def approved_rule(rule):
    """Change only the grade of this identity, without changing the output amount."""
    if identity(rule["source"]) != SOURCE:
        return rule
    if (
        identity(rule["target"]) not in (PREVIOUS, TARGET)
        or type(rule.get("conversion_factor", 1)) not in (int, float)
        or rule.get("conversion_factor", 1) != 1
    ):
        raise ValueError("Concrete grade supplier or unit factor changed.")
    return {**rule, "target": dict(zip(FIELDS, TARGET, strict=True))}


def forward_decision(rule, observations, uvek_signs, native, binding, previous, original_review):
    if identity(rule["source"]) != SOURCE or identity(rule["target"]) != TARGET or uvek_signs != {1}:
        raise ValueError("Concrete grade directional correspondence changed.")
    evidence = [REVIEW_PATH]
    return {
        "binding": binding,
        "original_factor": 1,
        "factor": 1,
        "original_target": dict(zip(FIELDS, PREVIOUS, strict=True)),
        "source_observations": [list(p) for p in sorted(observations)],
        "uvek_reference_sign": 1,
        "uvek_native_flow_type": "PRODUCT_FLOW",
        "original_review": deepcopy(previous.get("original_review", original_review)),
        "original_information_loss": previous.get(
            "original_information_loss", rule.get("technosphere_information_loss")
        ),
        "supplier_proxy_review": {
            "rationale": APPROVAL["authorization"] + " " + LIMITATIONS,
            "limitations": [LIMITATIONS],
            "evidence": evidence,
        },
        "review": {
            "status": "unresolved",
            "reason": "Select only an exact inspected cut-off concrete producer profile.",
            "evidence": evidence,
            "profiles": [
                {
                    "source_profile": dict(zip(("family", "version", "system_model"), label.split("-"), strict=True)),
                    "conversion_factor": 1,
                    "review": {
                        "status": "verified",
                        "source_sign": 1,
                        "target_sign": 1,
                        "reason": LIMITATIONS,
                        "evidence": evidence,
                    },
                }
                for label in APPROVAL["profiles"]
            ],
        },
    }
