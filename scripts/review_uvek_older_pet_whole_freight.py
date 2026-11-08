"""Resolve exact older PET transport mixtures and the positive polyester transfer."""

import hashlib
import json
import pickle
import sqlite3
from copy import deepcopy
from pathlib import Path

from brightpath import TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules

try:
    import review_uvek_pet_waste_markets as legacy
    from review_uvek_market_compositions import composition_decision
except ModuleNotFoundError:
    from scripts import review_uvek_pet_waste_markets as legacy
    from scripts.review_uvek_market_compositions import composition_decision

FIELDS = ("name", "reference product", "location", "unit")
ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_older_pet_whole_freight_review.json"
APPROVAL = json.loads((ROOT / REVIEW_PATH).read_text())
REVIEW_SHA256 = "2d5142461c8ab9b4151d7528e066775d4e94cbcedb058bc5564c9b9df753a700"
SOURCES = {tuple(row["source"][f] for f in FIELDS): row for row in APPROVAL["rules"]}
LIMITATIONS = APPROVAL["limitations"]


def identity(row):
    return tuple(row[f] for f in FIELDS)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def inventory_record(label, metadata, blobs):
    """Pin complete vectors and metadata without redistributing source inventories."""
    exchanges = [pickle.loads(blob) for blob in blobs]
    (reference,) = [e for e in exchanges if e["type"] == "production"]
    if reference["amount"] not in (-1, 1) or reference["input"] != reference["output"]:
        raise ValueError("Older PET whole-service reference changed.")
    return {
        "label": label,
        "source": {f: metadata[f] for f in FIELDS},
        "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
        "metadata_sha256": digest(metadata),
        "reference_amount": reference["amount"],
        "total_exchanges": len(exchanges),
    }


def collect_inventories(brightway_root):
    """Read the 440 exact source, whole-freight and native lifecycle witnesses read-only."""
    records = []
    for entry in APPROVAL["collection_profiles"]:
        label = "-".join(entry["profile"])
        wanted = {identity(row["source"]) for row in APPROVAL["inventories"] if row["label"] == label}
        if not wanted:
            continue
        path = brightway_root / entry["project"] / "lci/databases.db"
        found = set()
        with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as connection:
            for name in sorted({s[0] for s in wanted}):
                for (raw,) in connection.execute(
                    "SELECT data FROM activitydataset WHERE database=? AND name=?", (entry["database"], name)
                ):
                    metadata = pickle.loads(raw)
                    if identity(metadata) not in wanted:
                        continue
                    blobs = [
                        b
                        for b, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (entry["database"], metadata["code"]),
                        )
                    ]
                    records.append(inventory_record(label, metadata, blobs))
                    found.add(identity(metadata))
        if found != wanted:
            raise ValueError("Older PET exact source, modal-service or lifecycle scope changed.")
    return records


def validate_inventories(records, observations, native):
    """Keep positive transfer, negative waste and complete freight functions distinct."""
    if digest(APPROVAL) != REVIEW_SHA256 or APPROVAL["authorized_system_model"] != "cutoff":
        raise ValueError("Older PET scientific review changed.")
    if len(records) != 440 or sorted(map(digest, records)) != sorted(map(digest, APPROVAL["inventories"])):
        raise ValueError("Older PET full inventory or metadata changed.")
    for record in records:
        source = identity(record["source"])
        if source in SOURCES:
            observations["ecoinvent"][source].add((record["label"], record["reference_amount"]))
    for source, row in SOURCES.items():
        expected = {(p["label"], p["source_sign"]) for p in row["profiles"]}
        actual = {(label, sign) for label, sign in observations["ecoinvent"][source] if label.endswith("-cutoff")}
        if row["legacy_decision_sha256"]:
            expected |= {(p["label"], p["source_sign"]) for p in legacy.SOURCES[source]["profiles"]}
        if actual != expected:
            raise ValueError("Older PET source coordinate or exact profile scope changed.")
    for name, expected in APPROVAL["shared_review_sha256"].items():
        if hashlib.sha256((ROOT / name).read_bytes()).hexdigest() != expected:
            raise ValueError("Older PET shared whole-function or independent reverse review changed.")


def validate_bindings(resource):
    """Freeze separately reviewed treatment functions, prior choices and exact helper bodies."""
    for recipe, expected in APPROVAL["recipe_dependencies"].items():
        if digest(resource["recipes"].get(recipe)) != expected:
            raise ValueError("Older PET existing treatment helper changed.")
    selected = {}
    for binding in APPROVAL["bindings"]:
        if "internal_recipe" in binding:
            continue  # The new 3.9 RoW body is frozen by this review, never a fallback.
        label = binding["label"]
        if label not in selected:
            selected[label] = {
                identity(row["source"]): row
                for row in select_profile_sign_rules(resource, TechnosphereProfile(*label.split("-")))["replace"]
            }
        if digest(selected[label].get(identity(binding["source"]))) != binding["selected_binding_sha256"]:
            raise ValueError("Older PET source treatment, transfer or freight binding changed.")


def forward_decision(rule, observations, inventories, binding, original_review):
    """Refine five pending PET profiles without rewriting the four existing helper bodies."""
    row = SOURCES[identity(rule["source"])]
    if not row["legacy_decision_sha256"]:
        return composition_decision(
            rule,
            observations,
            [(record["label"], record["inventory_sha256"]) for record in inventories],
            binding,
            original_review,
            approval_row=row,
            rationale=LIMITATIONS,
            review_path=REVIEW_PATH,
            namespace="polyester-pet-transfer",
            mapping_method="reviewed_polyester_pet_transfer",
        )
    decision = legacy.forward_decision(rule, observations, inventories, binding, original_review)
    if digest(decision) != row["legacy_decision_sha256"]:
        raise ValueError("Older PET legacy parent decision changed; repeat the scoped review.")
    replacements = {}
    for profile in row["profiles"]:
        source_profile = dict(zip(("family", "version", "system_model"), profile["label"].split("-"), strict=True))
        (recipe,) = [name for name, body in row["recipes"].items() if body["inputs"] == profile["inputs"]]
        replacements[profile["label"]] = {
            "source_profile": source_profile,
            "target": {"recipe_ref": recipe},
            "conversion_factor": -1,
            "review": {
                "status": "verified",
                "source_sign": -1,
                "target_sign": 1,
                "reason": LIMITATIONS,
                "evidence": [f"{REVIEW_PATH}#{profile['label']}/{profile['inventory_sha256']}"],
            },
        }
    for index, profile in enumerate(decision["review"]["profiles"]):
        label = "-".join(profile["source_profile"][f] for f in ("family", "version", "system_model"))
        if label in replacements:
            if profile["review"]["status"] != "unresolved":
                raise ValueError("Older PET extension may replace only its five pending profiles.")
            decision["review"]["profiles"][index] = replacements.pop(label)
    if replacements:
        raise ValueError("Older PET extension profile is absent from the frozen legacy decision.")
    decision["recipes"].update(deepcopy(row["recipes"]))
    decision["mapping_method"] = "reviewed_older_pet_whole_freight"
    decision["recipe_information_loss"] = LIMITATIONS
    decision["recipe_review"] = {"rationale": LIMITATIONS, "limitations": [LIMITATIONS], "evidence": [REVIEW_PATH]}
    decision["review"]["reason"] = "Exact enumerated cut-off profiles only; unknown profiles remain unresolved."
    return decision
