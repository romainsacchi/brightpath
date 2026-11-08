"""Retain complete uncontrolled-disposal inventories and their separately bounded operating services."""

import hashlib
import json
import pickle
import sqlite3
from collections import Counter
from copy import deepcopy
from pathlib import Path

try:
    from review_uvek_market_compositions import composition_decision, validate_composition_bindings
except ModuleNotFoundError:
    from scripts.review_uvek_market_compositions import composition_decision, validate_composition_bindings

FIELDS = ("name", "reference product", "location", "unit")
ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_uncontrolled_disposal_review.json"
APPROVAL = json.loads((ROOT / REVIEW_PATH).read_text())
SOURCES = {tuple(r["source"][f] for f in FIELDS): r for r in APPROVAL["rules"]}
REVIEW_SHA256 = "709eba4f861df138d352e6de6905f98d6bf602b2453b9be857b4ea60f12a6eb9"
VERSIONS = {"3.6", "3.7", "3.8", "3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12"}
LIMITATIONS = APPROVAL["limitations"]


def identity(row):
    return tuple(row[f] for f in FIELDS)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


if (
    digest(APPROVAL) != REVIEW_SHA256
    or APPROVAL["authorized_system_model"] != "cutoff"
    or APPROVAL["source_count"] != 37
    or len(SOURCES) != 37
    or APPROVAL["source_profile_count"] != 226
    or APPROVAL["inventory_count"] != 310
    or any(s[3] != "kilogram" for s in SOURCES)
    or Counter(p["source_sign"] for r in SOURCES.values() for p in r["profiles"]) != {-1: 215, 1: 11}
    or any(
        p["source_sign"] == 1 and p["label"] != "ecoinvent-3.9-cutoff" for r in SOURCES.values() for p in r["profiles"]
    )
):
    raise ValueError("Uncontrolled-disposal scope, source roles or scientific evidence changed; repeat the review.")
EVIDENCE_SCOPE = {}
for row in APPROVAL["inventories"]:
    profile = tuple(row["label"].split("-"))
    if profile not in {("ecoinvent", v, "cutoff") for v in VERSIONS} | {("uvek", "2025", "cutoff")}:
        raise ValueError("Unreviewed uncontrolled-disposal evidence profile.")
    EVIDENCE_SCOPE.setdefault(profile, set()).add(identity(row["source"]))


def inventory_record(source, blobs, comment, product_information):
    """Pin full trusted original Brightway vectors; publish fingerprints, not source inventories."""
    (reference,) = [e for e in map(pickle.loads, blobs) if e["type"] == "production"]
    if reference["amount"] not in (-1, 1) or tuple(reference["input"]) != tuple(reference["output"]):
        raise ValueError("Uncontrolled-disposal inventory reference changed; repeat the scientific review.")
    return {
        "source": list(source),
        "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
        "activity_comment_sha256": hashlib.sha256(str(comment or "").encode()).hexdigest(),
        "product_information_sha256": hashlib.sha256(str(product_information or "").encode()).hexdigest(),
        "reference_amount": reference["amount"],
    }


def collect_extra_inventories(brightway_root):
    """Inspect original exact patch databases without inheriting missing source counterparts."""
    records = []
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
                    records.append(
                        {
                            **inventory_record(
                                source, blobs, metadata.get("comment"), metadata.get("product_information")
                            ),
                            "label": "-".join(entry["profile"]),
                        }
                    )
                    found.add(source)
            if found != scope:
                raise ValueError("Uncontrolled-disposal exact patch scope changed; repeat the scientific review.")
    return records


def validate_inventories(inventories, observations, native):
    """Require every full physical source and complete native/reverse operating inventory."""
    expected = {
        json.dumps({**r, "source": list(identity(r["source"]))}, sort_keys=True) for r in APPROVAL["inventories"]
    }
    actual = {json.dumps({**r, "source": list(s)}, sort_keys=True) for s, rows in inventories.items() for r in rows}
    if actual != expected or len(expected) != 310 or sum(map(len, inventories.values())) != 310:
        raise ValueError("Uncontrolled-disposal inventory or description changed; repeat the scientific review.")
    for source, row in SOURCES.items():
        wanted = {(p["label"], p["source_sign"]) for p in row["profiles"]}
        observed = {
            (label, sign) for label, sign in observations["ecoinvent"].get(source, set()) if label.endswith("-cutoff")
        }
        if observed != wanted or {(r["label"], r["reference_amount"]) for r in inventories[source]} != wanted:
            raise ValueError("Uncontrolled-disposal exact source reference changed; repeat the scientific review.")
    for role in [*APPROVAL["native_roles"], *APPROVAL.get("companion_native_roles", [])]:
        supplier = identity(role["source"])
        if (
            native.get(supplier, {}).get("native_flow_type") != "PRODUCT_FLOW"
            or native[supplier].get("native_reference_is_input") is not False
            or observations["uvek"].get(supplier) != {("uvek-2025-cutoff", 1)}
        ):
            raise ValueError("Uncontrolled-disposal full operating supplier role changed; repeat its review.")


def validate_bindings(resource):
    """Retain all physical releases, original reference signs and reviewed operating boundaries."""
    if digest(APPROVAL) != REVIEW_SHA256:
        raise ValueError("Uncontrolled-disposal coefficients, evidence or approval changed; repeat the review.")
    validate_composition_bindings(resource, approval=APPROVAL, label="Uncontrolled disposal")
    companion = APPROVAL.get("companion_whole_freight_review_sha256")
    if (
        companion
        and hashlib.sha256((ROOT / "brightpath/data/export/uvek_whole_freight_review.json").read_bytes()).hexdigest()
        != companion
    ):
        raise ValueError("Companion whole-freight evidence changed; repeat the textile function review.")
    profiles = [p for r in SOURCES.values() for p in r["profiles"]]
    if (
        len(profiles) != 226
        or any(r["pending_profiles"] for r in SOURCES.values())
        or sum(e["type"] == "biosphere" for p in profiles for e in p["inputs"]) != 22133
        or sum(e["type"] == "technosphere" for p in profiles for e in p["inputs"]) != 288
        or sum(e.get("approved_omission_if_unavailable", False) for p in profiles for e in p["inputs"]) != 300
    ):
        raise ValueError("Uncontrolled-disposal physical flow or operating service coverage changed.")
    for path, expected in APPROVAL["canonical_identity_dependency_sha256"].items():
        if hashlib.sha256((ROOT / path).read_bytes()).hexdigest() != expected:
            raise ValueError(
                "Canonical publisher identity review changed; repeat uncontrolled-disposal identity checks."
            )
    catalog = json.loads((ROOT / "brightpath/data/export/reference_catalogs/ecoinvent__3.10__cutoff.json").read_text())
    by_uuid = {r["flow_id"]: r for r in catalog["biosphere"]}
    for flow in APPROVAL["canonical_biosphere_definitions"]:
        if {k: by_uuid.get(flow["flow_id"], {}).get(k) for k in flow} != flow:
            raise ValueError("Uncontrolled-disposal canonical biosphere identity changed; repeat its review.")
    proofs = {(r["label"], identity(r["source"])): r for r in APPROVAL["inventories"]}
    reverse = json.loads((ROOT / "brightpath/data/migrations/uvek/uvek-2025-to-ecoinvent-3.12-cutoff.json").read_text())
    for entry in APPROVAL["reverse_bindings"]:
        rule = next((r for r in reverse["replace"] if r["source"] == entry["source"]), None)
        if (
            digest(rule) != entry["binding_sha256"]
            or entry["source_inventory_sha256"]
            != proofs["uvek-2025-cutoff", identity(entry["source"])]["inventory_sha256"]
            or entry["target_inventory_sha256"]
            != proofs["ecoinvent-3.12-cutoff", identity(entry["target"])]["inventory_sha256"]
        ):
            raise ValueError("Independent reverse operating correspondence changed; repeat the full-service review.")


def forward_decision(rule, observations, inventories, binding, original_review):
    """Compile only the enumerated faithful uncontrolled-disposal source functions."""
    decision = composition_decision(
        rule,
        {(label, sign) for label, sign in observations if label.endswith("-cutoff")},
        [(r["label"], r["inventory_sha256"]) for r in inventories],
        binding,
        deepcopy(original_review),
        approval_row=SOURCES[identity(rule["source"])],
        rationale=LIMITATIONS,
        review_path=REVIEW_PATH,
        namespace="uncontrolled-disposal",
        mapping_method="reviewed_uncontrolled_disposal_service",
        profile_reason=SOURCES[identity(rule["source"])].get(
            "profile_scope_note",
            "Only the217 independently inspected exact cut-off profiles and their actual reference signs are approved.",
        ),
    )
    decision["source_observations"] = [list(pair) for pair in sorted(observations)]
    return decision


def extend_companion_market_evidence(observations, market_inventories, inventories):
    """Expose only fully pinned actual textile-market patch profiles to the market author."""
    if not inventories:
        return
    for expected in APPROVAL.get("companion_market_profiles", []):
        source = identity(expected["source"])
        rows = [r for r in inventories[source] if r["label"] == expected["label"]]
        if (
            len(rows) != 1
            or rows[0]["inventory_sha256"] != expected["inventory_sha256"]
            or rows[0]["reference_amount"] != -1
        ):
            raise ValueError("Companion textile-market inventory changed; repeat the scientific review.")
        if expected["label"].split("-")[1] not in {"3.9.1", "3.10.1"}:
            continue
        observations["ecoinvent"][source].add((expected["label"], -1))
        record = (expected["label"], expected["inventory_sha256"])
        if record not in market_inventories[source]:
            market_inventories[source].append(record)
