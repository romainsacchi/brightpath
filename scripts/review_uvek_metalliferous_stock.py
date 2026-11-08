"""Preserve the inspected positive sludge allocation stock without fresh lithium burdens."""

import hashlib
import json
import pickle
import sqlite3
from pathlib import Path

try:
    from review_uvek_cutoff_stocks import stock_decision
except ModuleNotFoundError:
    from scripts.review_uvek_cutoff_stocks import stock_decision

FIELDS = ("name", "reference product", "location", "unit")
ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_metalliferous_stock_review.json"
REVIEW_SHA256 = "67f74375422ca2148ede9bde16b1e34f5b4fdc334312ed77057096c2a70c52cd"
if hashlib.sha256((ROOT / REVIEW_PATH).read_bytes()).hexdigest() != REVIEW_SHA256:
    raise ValueError("Metalliferous stock review changed; repeat the allocation review.")
APPROVAL = json.loads((ROOT / REVIEW_PATH).read_text())
SOURCE = tuple(APPROVAL["source"][f] for f in FIELDS)


def identity(row):
    return tuple(row[f] for f in FIELDS)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def inventory_record(label, metadata, blobs):
    """Reject anything except the exact self-linked positive production-only stock."""
    rows = [pickle.loads(b) for b in blobs]
    if (
        len(rows) != 1
        or rows[0]["type"] != "production"
        or rows[0]["amount"] != 1
        or rows[0]["input"] != rows[0]["output"]
    ):
        raise ValueError("Metalliferous stock allocation boundary changed; repeat the scientific review.")
    return {
        "label": label,
        "source": {f: metadata[f] for f in FIELDS},
        "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
        "activity_comment_sha256": hashlib.sha256(str(metadata.get("comment") or "").encode()).hexdigest(),
        "product_information_sha256": hashlib.sha256(
            str(metadata.get("product_information") or "").encode()
        ).hexdigest(),
        "reference_amount": 1,
        "production_amount": 1,
        "total_exchanges": 1,
        "only_self_production": True,
    }


def collect_inventories(brightway_root):
    """Read all nine original cut-off databases, never inherit a patch or write a database."""
    result = []
    for entry in APPROVAL["collection_profiles"]:
        path = brightway_root / entry["project"] / "lci/databases.db"
        with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as con:
            matches = []
            for (raw,) in con.execute(
                "SELECT data FROM activitydataset WHERE database=? AND name=?", (entry["database"], SOURCE[0])
            ):
                md = pickle.loads(raw)
                if identity(md) != SOURCE:
                    continue
                blobs = [
                    b
                    for b, in con.execute(
                        "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                        (md["database"], md["code"]),
                    )
                ]
                matches.append(inventory_record("-".join(entry["profile"]), md, blobs))
            if len(matches) != 1:
                raise ValueError("Metalliferous stock exact profile changed; repeat the allocation review.")
            result.extend(matches)
    return result


def extend_observations(records, observations):
    for row in records:
        observations["ecoinvent"][SOURCE].add((row["label"], row["reference_amount"]))


def validate_inventories(records, observations):
    if len(records) != 9 or sorted(map(digest, records)) != sorted(map(digest, APPROVAL["inventories"])):
        raise ValueError("Metalliferous stock inventory or metadata changed; repeat the scientific review.")
    if {(label, sign) for label, sign in observations["ecoinvent"].get(SOURCE, set()) if label.endswith("-cutoff")} != {
        (p["label"], 1) for p in APPROVAL["profiles"]
    }:
        raise ValueError("Metalliferous stock exact positive reference scope changed.")
    reverse = APPROVAL["independent_reverse"]
    if hashlib.sha256((ROOT / reverse["resource_path"]).read_bytes()).hexdigest() != reverse["resource_sha256"]:
        raise ValueError("Independent reverse resource changed; do not infer an inverse.")


def forward_decision(rule, observations, records, binding, original_review):
    """Reuse the established exact positive-stock implementation and warnings."""
    return stock_decision(
        rule,
        {(label, sign) for label, sign in observations if label.endswith("-cutoff")},
        records,
        binding,
        original_review,
        approval_row=APPROVAL,
        review_path=REVIEW_PATH,
    )


def collect_parent_inventories(brightway_root):
    """Fingerprint the two newly inspected exact patch stock markets independently."""
    wanted = {r["label"]: r for r in APPROVAL["parent_patch_inventories"]}
    result = []
    for entry in APPROVAL["collection_profiles"]:
        label = "-".join(entry["profile"])
        if label not in wanted:
            continue
        expected = wanted[label]
        path = brightway_root / entry["project"] / "lci/databases.db"
        with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as con:
            matches = []
            for (raw,) in con.execute(
                "SELECT data FROM activitydataset WHERE database=? AND name=?",
                (entry["database"], expected["source"]["name"]),
            ):
                md = pickle.loads(raw)
                if identity(md) != identity(expected["source"]):
                    continue
                blobs = [
                    b
                    for b, in con.execute(
                        "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                        (md["database"], md["code"]),
                    )
                ]
                exchanges = [pickle.loads(b) for b in blobs]
                refs = [e for e in exchanges if e["type"] == "production"]
                if (
                    len(exchanges) != 6
                    or len(refs) != 1
                    or refs[0]["amount"] != 1
                    or refs[0]["input"] != refs[0]["output"]
                    or sum(e["type"] == "technosphere" for e in exchanges) != 5
                ):
                    raise ValueError("Metalliferous stock market boundary changed; repeat the scientific review.")
                matches.append(
                    {
                        "label": label,
                        "source": {f: md[f] for f in FIELDS},
                        "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
                        "activity_comment_sha256": hashlib.sha256(str(md.get("comment") or "").encode()).hexdigest(),
                        "product_information_sha256": hashlib.sha256(
                            str(md.get("product_information") or "").encode()
                        ).hexdigest(),
                        "reference_amount": 1,
                        "total_exchanges": 6,
                    }
                )
            if len(matches) != 1:
                raise ValueError("Metalliferous stock market exact patch changed.")
            result.extend(matches)
    return result


def validate_parent_inventories(records):
    if len(records) != 2 or sorted(map(digest, records)) != sorted(map(digest, APPROVAL["parent_patch_inventories"])):
        raise ValueError(
            "Metalliferous stock market exact inventory or metadata changed; repeat the scientific review."
        )


def accepts_revised_parent_binding(resource, previous_binding, selected_rule):
    """Permit only the source-bound stock correction in a previously frozen freight parent."""
    expected = next(
        (
            p
            for p in APPROVAL["parent_rule_bindings"]
            if p["label"] == previous_binding["label"]
            and p["source"] == previous_binding["source"]
            and p["previous_whole_freight_binding_sha256"] == previous_binding["binding_sha256"]
        ),
        None,
    )
    if expected is None:
        return False
    if (
        digest(selected_rule) != expected["binding_sha256"]
        or digest(resource["recipes"].get(expected["recipe_id"])) != expected["recipe_sha256"]
    ):
        raise ValueError("Metalliferous parent stock/freight replacement changed; repeat the scientific review.")
    try:
        from review_uvek_cutoff_stocks import stock_recipe
    except ModuleNotFoundError:
        from scripts.review_uvek_cutoff_stocks import stock_recipe
    recipe_id, recipe = stock_recipe(SOURCE, review_path=REVIEW_PATH)
    if resource["recipes"].get(recipe_id) != recipe:
        raise ValueError("Metalliferous positive stock allocation helper changed.")
    return True
