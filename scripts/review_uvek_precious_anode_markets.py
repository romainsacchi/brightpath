"""Bind positive precious-metal markets to the matching complete refinery product."""

import hashlib
import json
import pickle
import sqlite3
from pathlib import Path

try:
    from review_uvek_market_compositions import composition_decision
except ModuleNotFoundError:
    from scripts.review_uvek_market_compositions import composition_decision

FIELDS = ("name", "reference product", "location", "unit")
ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_precious_anode_market_review.json"
REVIEW_SHA256 = "2c14b1bec1de15241545aeecbe8f2ae30b4b44b36f2ee2542beec43bc7047a95"
if hashlib.sha256((ROOT / REVIEW_PATH).read_bytes()).hexdigest() != REVIEW_SHA256:
    raise ValueError("Precious-metal market review changed; repeat the scientific review.")
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
    if len(refs) != 1 or refs[0]["input"] != refs[0]["output"] or refs[0]["amount"] != 1:
        raise ValueError("Precious-metal market/refinery reference changed; repeat the scientific review.")
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
            raise ValueError("Precious-metal market/refinery profile coverage changed; repeat the scientific review.")
    return result


def extend_observations(records, observations):
    """Add only the independently inspected market reference observations."""
    for row in records:
        source = identity(row["source"])
        if source in SOURCES:
            observations["ecoinvent"][source].add((row["label"], row["reference_amount"]))


def validate_inventories(records, observations, native):
    """Freeze every market, nonempty refinery and native output boundary."""
    if len(records) != 30 or sorted(map(digest, records)) != sorted(map(digest, APPROVAL["inventories"])):
        raise ValueError("Precious-metal market/refinery inventory or metadata changed; repeat the scientific review.")
    for source, row in SOURCES.items():
        actual = {
            (label, sign) for label, sign in observations["ecoinvent"].get(source, set()) if label.endswith("-cutoff")
        }
        if actual != {(p["label"], 1) for p in row["profiles"]}:
            raise ValueError("Precious-metal market exact-profile product observations changed.")
    target = native.get(identity(APPROVAL["target"]), {})
    if target.get("native_flow_type") != "PRODUCT_FLOW" or target.get("native_reference_is_input") is not False:
        raise ValueError("Precious-metal native refinery is no longer the inspected positive product.")
    reverse = APPROVAL["independent_reverse"]
    if hashlib.sha256((ROOT / reverse["resource_path"]).read_bytes()).hexdigest() != reverse["resource_sha256"]:
        raise ValueError("Independent reverse resource changed; review it independently.")


def forward_decision(rule, observations, records, binding, original_review):
    """Preserve exact supplier shares and positive product coordinates without normalization."""
    source = identity(rule["source"])
    inventories = [(r["label"], r["inventory_sha256"]) for r in records if identity(r["source"]) == source]
    return composition_decision(
        rule,
        observations,
        inventories,
        binding,
        original_review,
        approval_row=SOURCES[source],
        rationale=LIMITATIONS,
        review_path=REVIEW_PATH,
        namespace="precious-anode-market",
        mapping_method="reviewed_precious_anode_market",
        profile_reason="Select an exact inspected cut-off precious-metal market; no missing-profile or cross-model fallback.",
    )
