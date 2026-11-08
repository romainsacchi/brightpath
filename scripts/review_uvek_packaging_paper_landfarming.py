"""Retain complete wet-sludge landfarming releases and its separately bounded spreading service."""

import hashlib
import json
import pickle
import sqlite3
from copy import deepcopy
from pathlib import Path

try:
    from review_uvek_market_compositions import composition_decision, validate_composition_bindings
except ModuleNotFoundError:
    from scripts.review_uvek_market_compositions import composition_decision, validate_composition_bindings

FIELDS = ("name", "reference product", "location", "unit")
ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_packaging_paper_landfarming_review.json"
APPROVAL = json.loads((ROOT / REVIEW_PATH).read_text())
SOURCES = {tuple(r["source"][f] for f in FIELDS): r for r in APPROVAL["rules"]}
PROFILES_SHA256 = "974eed06cff710003ef1f008720b76364c85c20ca6574808181f423561239ffa"
VERSIONS = {"3.10", "3.10.1", "3.11", "3.12"}
LIMITATIONS = APPROVAL["limitations"]
if (
    APPROVAL["authorized_system_model"] != "cutoff"
    or APPROVAL["source_count"] != 1
    or APPROVAL["source_profile_count"] != 4
    or APPROVAL["inventory_count"] != 25
    or len(SOURCES) != 1
    or next(iter(SOURCES))[0] != "treatment of sewage sludge, 97% water, WWT-SLF, waste packaging paper, landfarming"
    or next(iter(SOURCES))[2:] != ("GLO", "kilogram")
):
    raise ValueError("Packaging-paper landfarming scope changed; repeat the scientific review.")
EVIDENCE_SCOPE = {}
for row in APPROVAL["inventories"]:
    profile = tuple(row["label"].split("-"))
    if profile not in {("ecoinvent", v, "cutoff") for v in VERSIONS} | {("uvek", "2025", "cutoff")}:
        raise ValueError("Unreviewed packaging-paper landfarming evidence profile.")
    EVIDENCE_SCOPE.setdefault(profile, set()).add(tuple(row["source"][f] for f in FIELDS))


def identity(row):
    return tuple(row[f] for f in FIELDS)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def inventory_record(source, blobs, comment, product_information):
    """Pin full actual vectors and descriptions without distributing raw licensed inventories."""
    (reference,) = [e for e in map(pickle.loads, blobs) if e["type"] == "production"]
    if reference["amount"] not in (-1, 1) or tuple(reference["input"]) != tuple(reference["output"]):
        raise ValueError("Landfarming evidence reference changed; repeat the scientific review.")
    return {
        "source": list(source),
        "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
        "activity_comment_sha256": hashlib.sha256(str(comment or "").encode()).hexdigest(),
        "product_information_sha256": hashlib.sha256(str(product_information or "").encode()).hexdigest(),
        "reference_amount": reference["amount"],
    }


def collect_extra_inventories(brightway_root):
    """Read the original exact patch, never a generated scenario or inherited profile."""
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
                raise ValueError("Landfarming exact patch scope changed; repeat the scientific review.")
    return records


def validate_inventories(inventories, observations, native):
    """Require all four source profiles and complete tanker market/production inventories."""
    expected = {
        json.dumps({**r, "source": list(identity(r["source"]))}, sort_keys=True) for r in APPROVAL["inventories"]
    }
    actual = {json.dumps({**r, "source": list(s)}, sort_keys=True) for s, rows in inventories.items() for r in rows}
    if actual != expected or len(expected) != 25 or sum(map(len, inventories.values())) != 25:
        raise ValueError("Landfarming inventory or description changed; repeat the scientific review.")
    for source, row in SOURCES.items():
        wanted = {(p["label"], -1) for p in row["profiles"]}
        observed = {
            (label, sign) for label, sign in observations["ecoinvent"].get(source, set()) if label.endswith("-cutoff")
        }
        if observed != wanted or {(r["label"], r["reference_amount"]) for r in inventories[source]} != wanted:
            raise ValueError("Landfarming exact source reference changed; repeat the scientific review.")
    (role,) = APPROVAL["native_roles"]
    supplier = identity(role["source"])
    if (
        native.get(supplier, {}).get("native_flow_type") != "PRODUCT_FLOW"
        or native[supplier].get("native_reference_is_input") is not False
        or observations["uvek"].get(supplier) != {("uvek-2025-cutoff", 1)}
    ):
        raise ValueError("Landfarming full tanker service role changed; repeat the scientific review.")


def validate_bindings(resource):
    """Keep the 39 releases, wet-sludge basis and service volume independently traceable."""
    validate_composition_bindings(resource, approval=APPROVAL, label="Packaging-paper landfarming")
    (row,) = APPROVAL["rules"]
    if (
        digest(row["profiles"]) != PROFILES_SHA256
        or len(row["profiles"]) != 4
        or row["pending_profiles"]
        or len(APPROVAL["canonical_biosphere_definitions"]) != 39
    ):
        raise ValueError("Landfarming quantities, emissions or profile scope changed; repeat the scientific review.")
    catalog = json.loads((ROOT / "brightpath/data/export/reference_catalogs/ecoinvent__3.10__cutoff.json").read_text())
    by_uuid = {r["flow_id"]: r for r in catalog["biosphere"]}
    for flow in APPROVAL["canonical_biosphere_definitions"]:
        actual = by_uuid.get(flow["flow_id"], {})
        if {k: actual.get(k) for k in flow} != flow:
            raise ValueError("Landfarming canonical biosphere definition changed; repeat the scientific review.")
    proofs = {(r["label"], identity(r["source"])): r for r in APPROVAL["inventories"]}
    reverse = json.loads((ROOT / "brightpath/data/migrations/uvek/uvek-2025-to-ecoinvent-3.12-cutoff.json").read_text())
    entry = APPROVAL["reverse_binding"]
    rule = next((r for r in reverse["replace"] if r["source"] == entry["source"]), None)
    if (
        digest(rule) != entry["binding_sha256"]
        or entry["source_inventory_sha256"] != proofs["uvek-2025-cutoff", identity(entry["source"])]["inventory_sha256"]
        or entry["target_inventory_sha256"]
        != proofs["ecoinvent-3.12-cutoff", identity(entry["target"])]["inventory_sha256"]
    ):
        raise ValueError("Landfarming independent reverse tanker supplier changed; repeat its review.")


def forward_decision(rule, observations, inventories, binding, original_review):
    """Compile the complete exact wet-sludge inventory in an independent canonical biosphere."""
    decision = composition_decision(
        rule,
        {(label, sign) for label, sign in observations if label.endswith("-cutoff")},
        [(r["label"], r["inventory_sha256"]) for r in inventories],
        binding,
        deepcopy(original_review),
        approval_row=SOURCES[identity(rule["source"])],
        rationale=LIMITATIONS,
        review_path=REVIEW_PATH,
        namespace="packaging-paper-landfarming",
        mapping_method="reviewed_packaging_paper_landfarming_service",
        profile_reason="Only the four independently inspected exact cut-off landfarming profiles are approved.",
    )
    decision["source_observations"] = [list(pair) for pair in sorted(observations)]
    return decision
