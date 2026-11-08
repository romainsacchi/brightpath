"""Compile exact PET groups from already reviewed regional markets without added operations."""

import hashlib
import json
import pickle
import sqlite3
from copy import deepcopy
from pathlib import Path

try:
    from review_uvek_market_compositions import composition_decision, composition_recipe, validate_composition_bindings
except ModuleNotFoundError:
    from scripts.review_uvek_market_compositions import (
        composition_decision,
        composition_recipe,
        validate_composition_bindings,
    )

FIELDS = ("name", "reference product", "location", "unit")
ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_pet_market_group_review.json"
APPROVAL = json.loads((ROOT / REVIEW_PATH).read_text())
REVIEW_SHA256 = "ab5b025eb6f9e3f16708a37f17a8e262a184c02eeca2f5906649a22e7af90a89"
SOURCES = {tuple(r["source"][f] for f in FIELDS): r for r in APPROVAL["rules"]}
VERSIONS = {"3.6", "3.7", "3.8", "3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12"}
LIMITATIONS = APPROVAL["limitations"]
EVIDENCE_SCOPE = {}
for row in APPROVAL["inventories"]:
    profile = tuple(row["label"].split("-"))
    if profile not in {("ecoinvent", v, "cutoff") for v in VERSIONS}:
        raise ValueError("Unreviewed PET group evidence profile.")
    EVIDENCE_SCOPE.setdefault(profile, set()).add(tuple(row["source"][f] for f in FIELDS))


def identity(row):
    return tuple(row[f] for f in FIELDS)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def inventory_record(source, blobs, comment, product_information):
    """Pin complete exact source/child vectors and descriptions without distributing raw inventories."""
    (reference,) = [e for e in map(pickle.loads, blobs) if e["type"] == "production"]
    if reference["amount"] != -1 or tuple(reference["input"]) != tuple(reference["output"]):
        raise ValueError("PET market-group reference changed; repeat the scientific review.")
    return {
        "source": list(source),
        "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
        "activity_comment_sha256": hashlib.sha256(str(comment or "").encode()).hexdigest(),
        "product_information_sha256": hashlib.sha256(str(product_information or "").encode()).hexdigest(),
        "reference_amount": -1,
    }


def collect_extra_inventories(brightway_root):
    """Read both original exact patch databases, with no inherited patch authorization."""
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
                raise ValueError("PET group exact patch scope changed; repeat the scientific review.")
    return records


def validate_inventories(inventories, observations, native):
    """Require all eighteen actual groups and every complete regional child inventory."""
    expected = {
        json.dumps({**r, "source": list(identity(r["source"]))}, sort_keys=True) for r in APPROVAL["inventories"]
    }
    actual = {json.dumps({**r, "source": list(s)}, sort_keys=True) for s, rows in inventories.items() for r in rows}
    if actual != expected or len(expected) != 342 or sum(map(len, inventories.values())) != 342:
        raise ValueError("PET group inventory or description changed; repeat the scientific review.")
    for source, row in SOURCES.items():
        wanted = {(p["label"], -1) for p in row["profiles"]}
        observed = {
            (label, sign) for label, sign in observations["ecoinvent"].get(source, set()) if label.endswith("-cutoff")
        }
        if observed != wanted or {(r["label"], r["reference_amount"]) for r in inventories[source]} != wanted:
            raise ValueError("PET group source profile/reference changed; repeat the scientific review.")


def validate_bindings(resource):
    """Freeze existing complete child graphs and independently checked directional limits."""
    if (
        digest(APPROVAL) != REVIEW_SHA256
        or APPROVAL["authorized_system_model"] != "cutoff"
        or (APPROVAL["source_count"], APPROVAL["source_profile_count"], APPROVAL["inventory_count"]) != (2, 18, 342)
        or len(SOURCES) != 2
        or len(APPROVAL["child_checks"]) != 333
        or len(APPROVAL["child_inventory_review_bindings"]) != 324
        or not APPROVAL["no_added_group_operations"]
        or not APPROVAL["no_omission_or_normalization"]
    ):
        raise ValueError("PET group quantities, children or approval scope changed; repeat the scientific review.")
    validate_composition_bindings(resource, approval=APPROVAL, label="PET group")
    for path, expected in APPROVAL["shared_scientific_review_sha256"].items():
        if hashlib.sha256((ROOT / path).read_bytes()).hexdigest() != expected:
            raise ValueError("PET group child scientific review changed; repeat the scientific review.")
    reverse = APPROVAL["independent_reverse_resource"]
    if hashlib.sha256((ROOT / reverse["path"]).read_bytes()).hexdigest() != reverse["sha256"]:
        raise ValueError("PET group independent reverse resource changed; repeat the scientific review.")
    reverse_rules = json.loads((ROOT / reverse["path"]).read_text())["replace"]
    if any(r["source"] in APPROVAL["unsupported_reverse_sources"] for r in reverse_rules):
        raise ValueError("A new inverse PET-treatment correspondence needs independent review.")
    proofs = {(r["label"], identity(r["source"])): r for r in APPROVAL["inventories"]}
    by_parent = {}
    for child in APPROVAL["child_checks"]:
        source = identity(child["source"])
        if (
            child["source_amount"] >= 0
            or child["source_reference_amount"] != -1
            or child["conversion_factor"] != -1
            or proofs[child["label"], source]["inventory_sha256"] != child["source_inventory_sha256"]
        ):
            raise ValueError("PET group child quantity/reference changed; repeat the scientific review.")
        by_parent.setdefault((child["label"], identity(child["parent"])), []).append(child)
    for source, row in SOURCES.items():
        if (
            source[0] != "market group for waste polyethylene terephthalate"
            or source[1] != "waste polyethylene terephthalate"
            or source[2] not in {"RER", "Europe without Switzerland"}
            or source[3] != "kilogram"
            or row["pending_profiles"]
        ):
            raise ValueError("Unreviewed PET group source scope.")
        for profile in row["profiles"]:
            children = by_parent[profile["label"], source]
            expected = [{"recipe": c["target"]["recipe_ref"], "amount": -c["source_amount"]} for c in children]
            if (
                profile["inputs"] != expected
                or profile["omissions"]
                or len(children) != (35 if source[2] == "Europe without Switzerland" else 2)
            ):
                raise ValueError("PET group weights or extra operations changed; repeat the scientific review.")
            for child in children:
                if child["kind"] == "local_reviewed_group":
                    child_source = identity(child["source"])
                    child_profile = next(p for p in SOURCES[child_source]["profiles"] if p["label"] == profile["label"])
                    rid, recipe = composition_recipe(
                        child_source,
                        child_profile,
                        rationale=LIMITATIONS,
                        review_path=REVIEW_PATH,
                        namespace="pet-market-group",
                    )
                    if child["target"] != {"recipe_ref": rid} or APPROVAL["local_recipes"].get(rid) != recipe:
                        raise ValueError("PET nested group changed; repeat the scientific review.")


def forward_decision(rule, observations, inventories, binding, original_review):
    """Compile grouping only, retaining original shares and each reviewed child's full graph."""
    decision = composition_decision(
        rule,
        {(label, sign) for label, sign in observations if label.endswith("-cutoff")},
        [(r["label"], r["inventory_sha256"]) for r in inventories],
        binding,
        deepcopy(original_review),
        approval_row=SOURCES[identity(rule["source"])],
        rationale=LIMITATIONS,
        review_path=REVIEW_PATH,
        namespace="pet-market-group",
        mapping_method="reviewed_pet_market_group_service",
        profile_reason="Only the eighteen independently inspected original cut-off group profiles are approved; no other version, patch or system-model inheritance is inferred.",
    )
    needed = {e["recipe"] for recipe in decision["recipes"].values() for e in recipe["inputs"] if "recipe" in e}
    for rid in sorted(needed - APPROVAL["recipe_dependencies"].keys()):
        decision["recipes"][rid] = deepcopy(APPROVAL["local_recipes"][rid])
    decision["source_observations"] = [list(pair) for pair in sorted(observations)]
    return decision
