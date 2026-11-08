"""Complete inspected actual-patch profiles of previously approved stock and handoff identities."""

import hashlib
import json
import pickle
import sqlite3
from copy import deepcopy
from pathlib import Path

from brightpath import TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules

try:
    from review_uvek_cutoff_stocks import STOCKS, stock_decision
except ModuleNotFoundError:
    from scripts.review_uvek_cutoff_stocks import STOCKS, stock_decision

FIELDS = ("name", "reference product", "location", "unit")
ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_complete_patch_boundary_review.json"
APPROVAL = json.loads((ROOT / REVIEW_PATH).read_text())
REVIEW_SHA256 = "9014fd1c34e141b18c8e4a9079ee7a6d23c57c689c96a91d6538ffb2a63a4010"


def identity(row):
    return tuple(row[f] for f in FIELDS)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


CASES = {(r["label"], identity(r["source"])): r for r in APPROVAL["rules"]}
PATCH_STOCKS = {source for (_, source), r in CASES.items() if r["kind"] == "stock"}
HANDOFFS = {source for (_, source), r in CASES.items() if r["kind"] == "handoff"}
SOURCES = PATCH_STOCKS | HANDOFFS
EVIDENCE_SCOPE = {}
for row in APPROVAL["inventories"]:
    EVIDENCE_SCOPE.setdefault(tuple(row["label"].split("-")), set()).add(identity(row["source"]))


def validate_review():
    """Require the exact 61 inspected boundaries; no other patch inherits a decision."""
    if (
        digest(APPROVAL) != REVIEW_SHA256
        or len(CASES) != 61
        or len(PATCH_STOCKS) != 20
        or len(HANDOFFS) != 14
        or len(APPROVAL["inventories"]) != 136
        or {r["label"] for r in CASES.values()} != {"ecoinvent-3.9.1-cutoff", "ecoinvent-3.10.1-cutoff"}
        or hashlib.sha256((ROOT / "brightpath/data/export/uvek_cutoff_stock_review.json").read_bytes()).hexdigest()
        != APPROVAL["existing_stock_review_sha256"]
    ):
        raise ValueError("Exact patch boundary scope changed; repeat the scientific review.")
    proofs = {(r["label"], identity(r["source"])): r for r in APPROVAL["inventories"]}
    for (label, source), row in CASES.items():
        proof = proofs[label, source]
        if proof["inventory_sha256"] != row["inventory_sha256"] or source[3] != "kilogram":
            raise ValueError("Exact patch source boundary changed; repeat the scientific review.")
        if row["kind"] == "stock":
            if proof["total_exchanges"] != 1 or proof["reference_amount"] != 1 or source not in STOCKS:
                raise ValueError("Exact patch positive stock changed; repeat the scientific review.")
        else:
            endpoint = proofs[label, identity(row["endpoint_source"])]
            if (
                proof["total_exchanges"] != 2
                or proof["reference_amount"] != -1
                or endpoint["total_exchanges"] != 1
                or endpoint["reference_amount"] != 1
                or endpoint["inventory_sha256"] != row["endpoint_inventory_sha256"]
            ):
                raise ValueError("Exact patch recovery chain changed; repeat the scientific review.")


def inventory_record(source, blobs, comment, product_information):
    """Pin complete actual source vectors without distributing the licensed inventories."""
    refs = [e for e in map(pickle.loads, blobs) if e["type"] == "production"]
    if len(refs) != 1 or refs[0]["amount"] not in (-1, 1) or tuple(refs[0]["input"]) != tuple(refs[0]["output"]):
        raise ValueError("Exact patch reference changed; repeat the scientific review.")
    return {
        "source": list(source),
        "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
        "activity_comment_sha256": hashlib.sha256(str(comment or "").encode()).hexdigest(),
        "product_information_sha256": hashlib.sha256(str(product_information or "").encode()).hexdigest(),
        "reference_amount": refs[0]["amount"],
        "total_exchanges": len(blobs),
    }


def collect_extra_inventories(brightway_root):
    """Read both actual patch databases; never substitute the base release."""
    records = []
    for entry in APPROVAL["extra_profiles"]:
        scope = EVIDENCE_SCOPE[tuple(entry["profile"])]
        path = brightway_root / entry["project"] / "lci/databases.db"
        found = set()
        with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as con:
            for name in sorted({s[0] for s in scope}):
                for (raw,) in con.execute(
                    "SELECT data FROM activitydataset WHERE database=? AND name=?", (entry["database"], name)
                ):
                    md = pickle.loads(raw)
                    source = identity(md)
                    if source not in scope:
                        continue
                    blobs = [
                        b
                        for b, in con.execute(
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
            raise ValueError("Exact patch inventory scope changed; repeat the scientific review.")
    return records


def validate_inventories(inventories):
    """Require all original core, actual patch and empty downstream endpoint fingerprints."""
    validate_review()
    expected = {
        json.dumps({**r, "source": list(identity(r["source"]))}, sort_keys=True) for r in APPROVAL["inventories"]
    }
    actual = {json.dumps({**r, "source": list(s)}, sort_keys=True) for s, rows in inventories.items() for r in rows}
    if expected != actual or sum(map(len, inventories.values())) != 136:
        raise ValueError("Exact patch full inventory evidence changed; repeat the scientific review.")


def validate_bindings(resource):
    """Freeze the independently approved core actions and the existing stock helper bodies."""
    validate_review()
    selected = {}
    for row in CASES.values():
        label = row["core_label"]
        if label not in selected:
            view = select_profile_sign_rules(resource, TechnosphereProfile(*label.split("-")))
            selected[label] = {identity(r["source"]): r for op in ("replace", "delete") for r in view[op]}
        if digest(selected[label].get(identity(row["source"]))) != row["core_binding_sha256"]:
            raise ValueError("Exact patch approved core binding changed; repeat the scientific review.")
        if (
            row["kind"] == "stock"
            and digest(resource["recipes"].get(row["existing_stock_recipe"])) != row["existing_stock_recipe_sha256"]
        ):
            raise ValueError("Exact patch stock recipe changed; repeat the scientific review.")


def extend_handoff_observations(approvals, omission_observations):
    """Add only independently inspected patch labels to the existing exact handoff whitelist."""
    validate_review()
    for (label, source), row in CASES.items():
        if row["kind"] != "handoff":
            continue
        previous = approvals[source]
        if (row["core_label"], -1) not in previous["observations"]:
            raise ValueError("Exact patch handoff lacks its approved core case; repeat the scientific review.")
        previous["observations"] = sorted(set(map(tuple, previous["observations"])) | {(label, -1)})
        omission_observations[source] = previous["observations"]


def annotate_handoff_profiles(source, profiles, previous):
    """Preserve old decisions exactly and attach actual-patch evidence only to the new entries."""
    if source not in HANDOFFS:
        return profiles
    old = {"-".join(p["source_profile"][f] for f in ("family", "version", "system_model")): p for p in previous}
    result = []
    for entry in profiles:
        label = "-".join(entry["source_profile"][f] for f in ("family", "version", "system_model"))
        row = CASES.get((label, source))
        if row and row["kind"] == "handoff":
            if entry.get("operation") != "delete" or not entry.get("approved_omission"):
                raise ValueError("Exact patch handoff action changed; repeat the scientific review.")
            citation = f"{REVIEW_PATH}#{label}/{row['inventory_sha256']}"
            if citation not in entry["review"]["evidence"]:
                entry["review"]["evidence"].append(citation)
        elif label in old:
            if any(entry.get(f) != old[label].get(f) for f in ("operation", "approved_omission", "review_ref")):
                raise ValueError("Existing handoff action changed; repeat the scientific review.")
            entry = deepcopy(old[label])
        result.append(entry)
    return result


def stock_patch_decision(rule, observations, core_inventories, patch_inventories, binding, original_review):
    """Reuse the existing empty allocation-stock helper; retain every previously approved profile."""
    validate_review()
    source = identity(rule["source"])
    approval = deepcopy(STOCKS[source])
    rows = [r for (_, s), r in CASES.items() if s == source and r["kind"] == "stock"]
    approval["profiles"] += [{"label": r["label"], "inventory_sha256": r["inventory_sha256"]} for r in rows]
    labels = {r["label"] for r in rows}
    additional = [
        {**r, "only_self_production": True, "production_amount": r["reference_amount"]}
        for r in patch_inventories
        if r["label"] in labels
    ]
    result = stock_decision(
        rule, observations, [*core_inventories, *additional], binding, original_review, approval_row=approval
    )
    for entry in result["review"]["profiles"]:
        label = "-".join(entry["source_profile"][f] for f in ("family", "version", "system_model"))
        if label in labels:
            row = CASES[label, source]
            entry["review"]["evidence"].append(f"{REVIEW_PATH}#{label}/{row['inventory_sha256']}")
    return result
