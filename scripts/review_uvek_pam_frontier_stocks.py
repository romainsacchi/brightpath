"""Retain exact positive aluminium/iron allocation stocks without importing scrap-preparation burdens."""

import hashlib
import json
import pickle
import sqlite3
from copy import deepcopy
from pathlib import Path

try:
    from review_uvek_cutoff_stocks import stock_decision
except ModuleNotFoundError:
    from scripts.review_uvek_cutoff_stocks import stock_decision

FIELDS = ("name", "reference product", "location", "unit")
ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_pam_frontier_stock_review.json"
APPROVAL = json.loads((ROOT / REVIEW_PATH).read_text())
REVIEW_SHA256 = "629ca2f1d59ac820a9ca553017fdc7b18426762728b2bd1113eb725e5ff053db"
SOURCES = {tuple(r["source"][f] for f in FIELDS): r for r in APPROVAL["rules"]}
VERSIONS = {"3.6", "3.7", "3.8", "3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12"}
EVIDENCE_SCOPE = {}
for row in APPROVAL["inventories"]:
    profile = tuple(row["label"].split("-"))
    if profile not in {("ecoinvent", v, "cutoff") for v in VERSIONS} | {("uvek", "2025", "cutoff")}:
        raise ValueError("Unreviewed scrap-stock evidence profile.")
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
        raise ValueError("Scrap-stock positive reference changed; repeat the scientific review.")
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
                raise ValueError("Scrap-stock exact patch scope changed; repeat the scientific review.")
    return result


def validate_inventories(inventories, observations, native):
    """Require both nine-profile stocks and both complete former preparation suppliers."""
    expected = {
        json.dumps({**r, "source": list(identity(r["source"]))}, sort_keys=True) for r in APPROVAL["inventories"]
    }
    actual = {json.dumps({**r, "source": list(s)}, sort_keys=True) for s, rows in inventories.items() for r in rows}
    if actual != expected or len(expected) != 20 or sum(map(len, inventories.values())) != 20:
        raise ValueError("Scrap-stock inventory or description changed; repeat the scientific review.")
    for source, row in SOURCES.items():
        wanted = {(p["label"], 1) for p in row["profiles"]}
        observed = {
            (label, sign) for label, sign in observations["ecoinvent"].get(source, set()) if label.endswith("-cutoff")
        }
        if observed != wanted or {(p["label"], p["reference_amount"]) for p in inventories[source]} != wanted:
            raise ValueError("Scrap-stock exact source scope changed; repeat the scientific review.")
        if any(
            p["total_exchanges"] != 1 or p["only_self_production"] is not True or p["production_amount"] != 1
            for p in inventories[source]
        ):
            raise ValueError("Scrap-stock allocation boundary changed; repeat the scientific review.")
    for role in APPROVAL["native_roles"]:
        source = identity(role["source"])
        if observations["uvek"].get(source) != {("uvek-2025-cutoff", 1)} or any(
            native.get(source, {}).get(f) != role[f]
            for f in ("native_flow_type", "native_reference_is_input", "flow_property_name", "unit_name")
        ):
            raise ValueError("Former scrap-preparation native role changed; repeat the scientific review.")


def validate_bindings(resource):
    """Freeze the exact allocation decision; unrelated same-target preparation recipes retain their burdens."""
    if (
        digest(APPROVAL) != REVIEW_SHA256
        or APPROVAL["authorized_system_model"] != "cutoff"
        or (APPROVAL["source_count"], APPROVAL["source_profile_count"], APPROVAL["inventory_count"]) != (2, 18, 20)
        or APPROVAL["source_identified_parent_profiles"] != 0
        or APPROVAL["unrelated_existing_same_target_recipe_bodies"] != 19
    ):
        raise ValueError("Scrap-stock scope changed; repeat the scientific review.")
    if len(APPROVAL["existing_recipe_bindings"]) != APPROVAL["existing_recipe_binding_count"] or any(
        digest(resource["recipes"].get(recipe_id)) != expected
        for recipe_id, expected in APPROVAL["existing_recipe_bindings"].items()
    ):
        raise ValueError("Unrelated scrap-preparation recipes changed; repeat the scientific review.")


def forward_decision(rule, observations, inventories, binding, original_review):
    """Keep signed stock quantities without adding preparation burdens from a different allocation boundary."""
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
