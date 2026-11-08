"""Compose inspected waste markets from reviewed treatment children and complete freight."""

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
REVIEW_PATH = "brightpath/data/export/uvek_industrial_waste_market_review.json"
APPROVAL = json.loads((ROOT / REVIEW_PATH).read_text())
SOURCES = {tuple(row["source"][f] for f in FIELDS): row for row in APPROVAL["rules"]}
RULES_SHA256 = "bdcacf1e95d9b2a96fc8f4c8375d4a10ba38cfd71fddce834da0a8508fb4a25b"
ROAD_PROOFS_SHA256 = "04551e46bdabb240545900b192e5b0fe658d6dff655890722a6ebd2aa8b37246"
if (
    len(SOURCES) != 14
    or APPROVAL["authorized_system_model"] != "cutoff"
    or APPROVAL["source_count"] != 14
    or APPROVAL["source_profile_count"] != 84
    or APPROVAL["inventory_count"] != 1067
    or sum(len(row["profiles"]) for row in SOURCES.values()) != 84
    or any(row["pending_profiles"] for row in SOURCES.values())
    or any(not source[0].startswith("market for ") or source[3] != "kilogram" for source in SOURCES)
):
    raise ValueError("Industrial-waste market scope changed; repeat the scientific review.")
EVIDENCE_SCOPE = {}
for row in APPROVAL["inventories"]:
    profile = tuple(row["label"].split("-"))
    if profile not in {("ecoinvent", v, "cutoff") for v in ("3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12")} | {
        ("uvek", "2025", "cutoff")
    }:
        raise ValueError("Unreviewed industrial-waste market evidence profile.")
    EVIDENCE_SCOPE.setdefault(profile, set()).add(tuple(row["source"][f] for f in FIELDS))
LIMITATIONS = APPROVAL["limitations"]


def identity(row):
    return tuple(row[f] for f in FIELDS)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def transport(name, location):
    return dict(zip(FIELDS, (name, name, location, "ton kilometer"), strict=True))


ROAD = transport("Transport, freight, lorry, fleet average", "RER")


def inventory_record(source, blobs, comment, product_information):
    """Pin actual vectors, descriptions and reference magnitudes without source inventories."""
    (reference,) = [e for e in map(pickle.loads, blobs) if e["type"] == "production"]
    if reference["amount"] not in (-1, 1) or tuple(reference["input"]) != tuple(reference["output"]):
        raise ValueError("Industrial-waste-market evidence reference changed; repeat the scientific review.")
    return {
        "source": list(source),
        "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
        "activity_comment_sha256": hashlib.sha256(str(comment or "").encode()).hexdigest(),
        "product_information_sha256": hashlib.sha256(str(product_information or "").encode()).hexdigest(),
        "reference_amount": reference["amount"],
    }


def collect_extra_inventories(brightway_root):
    """Read original exact patch inventories, never premise scenarios or mutable projects."""
    records = []
    for entry in APPROVAL["extra_profiles"]:
        scope = EVIDENCE_SCOPE[tuple(entry["profile"])]
        path = brightway_root / entry["project"] / "lci/databases.db"
        with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as connection:
            found = set()
            for name in sorted({s[0] for s in scope}):
                for (raw,) in connection.execute(
                    "SELECT data FROM activitydataset WHERE database=? AND name=?", (entry["database"], name)
                ):
                    metadata = pickle.loads(raw)
                    source = identity(metadata)
                    if source not in scope:
                        continue
                    blobs = [
                        b
                        for b, in connection.execute(
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
                raise ValueError("Industrial-waste-market patch scope changed; repeat the scientific review.")
    return records


def validate_inventories(inventories, observations, native):
    """Require all actual source, freight and independently inspected reverse inventories."""
    expected = {
        json.dumps({**r, "source": list(identity(r["source"]))}, sort_keys=True) for r in APPROVAL["inventories"]
    }
    actual = {json.dumps({**r, "source": list(s)}, sort_keys=True) for s, rows in inventories.items() for r in rows}
    if actual != expected or len(expected) != 1067 or sum(map(len, inventories.values())) != 1067:
        raise ValueError("Industrial-waste market inventory or metadata changed; repeat the scientific review.")
    for source, row in SOURCES.items():
        wanted = {(p["label"], -1) for p in [*row["profiles"], *row["pending_profiles"]]}
        observed = {
            (label, sign) for label, sign in observations["ecoinvent"].get(source, set()) if label.endswith("-cutoff")
        }
        if wanted != observed or {(r["label"], r["reference_amount"]) for r in inventories[source]} != wanted:
            raise ValueError("Industrial-waste market exact references changed; repeat the scientific review.")
    if len(APPROVAL["native_roles"]) != 5:
        raise ValueError("Industrial-waste market native scope changed.")
    for row in APPROVAL["native_roles"]:
        source = identity(row["source"])
        if (
            native.get(source, {}).get("native_flow_type") != "PRODUCT_FLOW"
            or native[source].get("native_reference_is_input") is not False
            or observations["uvek"].get(source) != {("uvek-2025-cutoff", 1)}
        ):
            raise ValueError("Industrial-waste market native supplier role changed; repeat the scientific review.")


def validate_bindings(resource):
    """Freeze child functions and quantities, full road-service evidence and independent reverse."""
    validate_composition_bindings(resource, approval=APPROVAL, label="Industrial-waste market")
    if digest(APPROVAL["rules"]) != RULES_SHA256 or digest(APPROVAL["full_road_service_proofs"]) != ROAD_PROOFS_SHA256:
        raise ValueError("Industrial-waste market quantities or full road scope changed; repeat the scientific review.")
    for path, expected in APPROVAL["scientific_child_review_sha256"].items():
        if hashlib.sha256((ROOT / path).read_bytes()).hexdigest() != expected:
            raise ValueError("Industrial-waste market child review changed; repeat the scientific review.")
    proofs = {(r["label"], identity(r["source"])): r for r in APPROVAL["inventories"]}
    for entry in APPROVAL["full_road_service_proofs"]:
        proof = proofs[entry["label"], identity(entry["source"])]
        if (
            proof["inventory_sha256"] != entry["inventory_sha256"]
            or proof["reference_amount"] != 1
            or entry["vehicle_maintenance_and_road_verified"] is not True
        ):
            raise ValueError("Industrial-waste market full road service changed; repeat the scientific review.")
    choices = {(r["label"], identity(r["source"])): r for r in APPROVAL["freight_choices"]}
    if len(choices) != 30 or len(APPROVAL["full_road_service_proofs"]) != 384:
        raise ValueError("Industrial-waste market freight scope changed; repeat the scientific review.")
    children_by_profile = {(r["label"], identity(r["parent"])): r for r in APPROVAL["child_checks"]}
    if len(children_by_profile) != 84:
        raise ValueError("Industrial-waste market child scope changed; repeat the scientific review.")
    used = set()
    for row in SOURCES.values():
        for profile in row["profiles"]:
            child = children_by_profile[profile["label"], identity(row["source"])]
            proof = proofs[profile["label"], identity(child["source"])]
            if (
                child["source_amount"] != -1
                or child["conversion_factor"] != -1
                or proof["reference_amount"] != -1
                or proof["inventory_sha256"] != child["source_inventory_sha256"]
                or child["method"]
                not in {"reviewed_residual_landfill_service", "reviewed_industrial_landfarming_service"}
                or [e for e in profile["inputs"] if "recipe" in e] != [{"recipe": child["target_recipe"], "amount": 1}]
                or profile["bindings"][0]["source"] != list(identity(child["source"]))
            ):
                raise ValueError(
                    "Industrial-waste market child evidence or quantity changed; repeat the scientific review."
                )
            expected = []
            for entry in profile["freight_choices"]:
                key = profile["label"], tuple(entry["source"])
                choice = choices[key]
                if (
                    digest(choice) != entry["choice_sha256"]
                    or choice["source_inventory_sha256"] != proofs[key]["inventory_sha256"]
                    or choice["conversion_factor"] != 1
                    or choice["target"] != ROAD
                    or choice["kind"] != "road"
                    or entry["amount"] <= 0
                    or "lorry" not in key[1][0]
                    or "unspecified" not in key[1][0]
                ):
                    raise ValueError("Industrial-waste market freight evidence changed; repeat the scientific review.")
                expected.append({**ROAD, "type": "technosphere", "amount": entry["amount"]})
                used.add(key)
            actual = [e for e in profile["inputs"] if e.get("unit") == "ton kilometer"]
            children = [e for e in profile["inputs"] if "recipe" in e]
            if (
                actual != expected
                or len(children) != 1
                or children[0]["amount"] != 1
                or any(e.get("type") == "biosphere" for e in profile["inputs"])
            ):
                raise ValueError("Industrial-waste market treatment/freight quantities or biosphere boundary changed.")
    if used != set(choices):
        raise ValueError("Industrial-waste market unused or missing road choice.")
    reverse = json.loads((ROOT / "brightpath/data/migrations/uvek/uvek-2025-to-ecoinvent-3.12-cutoff.json").read_text())
    entry = APPROVAL["reverse_binding"]
    rule = next((r for r in reverse["replace"] if r["source"] == ROAD), None)
    if (
        digest(rule) != entry["binding_sha256"]
        or entry["source"] != ROAD
        or entry["source_inventory_sha256"] != proofs["uvek-2025-cutoff", identity(entry["source"])]["inventory_sha256"]
        or entry["target_inventory_sha256"]
        != proofs["ecoinvent-3.12-cutoff", identity(entry["target"])]["inventory_sha256"]
    ):
        raise ValueError("Industrial-waste market reverse road supplier changed; repeat the independent review.")


def forward_decision(rule, observations, inventories, binding, original_review):
    """Compile only the exact complete treatment/freight compositions without new omissions."""
    source = identity(rule["source"])
    decision = composition_decision(
        rule,
        {(label, sign) for label, sign in observations if label.endswith("-cutoff")},
        [(r["label"], r["inventory_sha256"]) for r in inventories],
        binding,
        deepcopy(original_review),
        approval_row=SOURCES[source],
        rationale=LIMITATIONS,
        review_path=REVIEW_PATH,
        namespace="industrial-waste-market",
        mapping_method="reviewed_industrial_waste_market_service",
        profile_reason="Only the 84 exact independently inspected cut-off market profiles are approved.",
    )
    decision["source_observations"] = [list(pair) for pair in sorted(observations)]
    return decision
