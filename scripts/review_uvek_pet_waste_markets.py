"""Compile exact PET-waste mixtures while retaining treatment, freight and unresolved boundaries."""

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
REVIEW_PATH = "brightpath/data/export/uvek_pet_waste_market_review.json"
APPROVAL = json.loads((ROOT / REVIEW_PATH).read_text())
REVIEW_SHA256 = "7326abb9cf42a88eeef075b399e8b3ea4aa53e03538f6980a1cb0feaef13a585"
SOURCES = {tuple(r["source"][f] for f in FIELDS): r for r in APPROVAL["rules"]}
LIMITATIONS = APPROVAL["limitations"]
VERSIONS = {"3.6", "3.7", "3.8", "3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12"}
EVIDENCE_SCOPE = {}
for row in APPROVAL["inventories"]:
    profile = tuple(row["label"].split("-"))
    if profile not in {("ecoinvent", v, "cutoff") for v in VERSIONS} | {("uvek", "2025", "cutoff")}:
        raise ValueError("Unreviewed PET-waste market evidence profile.")
    EVIDENCE_SCOPE.setdefault(profile, set()).add(tuple(row["source"][f] for f in FIELDS))


def identity(row):
    return tuple(row[f] for f in FIELDS)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def inventory_record(source, blobs, comment, product_information):
    """Fingerprint actual full vectors and descriptions without publishing licensed inventories."""
    (reference,) = [e for e in map(pickle.loads, blobs) if e["type"] == "production"]
    if reference["amount"] not in (-1, 1) or tuple(reference["input"]) != tuple(reference["output"]):
        raise ValueError("PET-waste market reference changed; repeat the scientific review.")
    return {
        "source": list(source),
        "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
        "activity_comment_sha256": hashlib.sha256(str(comment or "").encode()).hexdigest(),
        "product_information_sha256": hashlib.sha256(str(product_information or "").encode()).hexdigest(),
        "reference_amount": reference["amount"],
    }


def collect_extra_inventories(brightway_root):
    """Read the original two exact patch databases without mutation or version inheritance."""
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
                raise ValueError("PET-waste market exact patch scope changed; repeat the scientific review.")
    return records


def validate_inventories(inventories, observations, native):
    """Require original full source/native vectors, descriptions, reference signs and native roles."""
    expected = {
        json.dumps({**r, "source": list(identity(r["source"]))}, sort_keys=True) for r in APPROVAL["inventories"]
    }
    actual = {json.dumps({**r, "source": list(s)}, sort_keys=True) for s, rows in inventories.items() for r in rows}
    if actual != expected or len(expected) != 1958 or sum(map(len, inventories.values())) != 1958:
        raise ValueError("PET-waste inventory or metadata changed; repeat the scientific review.")
    for source, row in SOURCES.items():
        wanted = {(p["label"], -1) for p in [*row["profiles"], *row["pending_profiles"]]}
        observed = {
            (label, sign) for label, sign in observations["ecoinvent"].get(source, set()) if label.endswith("-cutoff")
        }
        if observed != wanted or {(r["label"], r["reference_amount"]) for r in inventories[source]} != wanted:
            raise ValueError("PET-waste exact source profile/reference changed; repeat the scientific review.")
    for role in APPROVAL["native_roles"]:
        source = identity(role["source"])
        if (
            native.get(source, {}).get("native_flow_type") != role["native_flow_type"]
            or native[source].get("native_reference_is_input") is not role["native_reference_is_input"]
            or observations["uvek"].get(source) != {("uvek-2025-cutoff", 1)}
        ):
            raise ValueError("PET-waste native complete-service role changed; repeat the scientific review.")


def validate_bindings(resource):
    """Freeze exact parent/child quantities and reviewed full-function and independent reverse boundaries."""
    if (
        digest(APPROVAL) != REVIEW_SHA256
        or APPROVAL["authorized_system_model"] != "cutoff"
        or (
            len(SOURCES),
            APPROVAL["source_profile_count"],
            APPROVAL["approved_source_profile_count"],
            APPROVAL["pending_source_profile_count"],
        )
        != (42, 378, 373, 5)
    ):
        raise ValueError("PET-waste scientific scope changed; repeat the independent review.")
    validate_composition_bindings(resource, approval=APPROVAL, label="PET-waste market")
    for path, expected in APPROVAL["shared_scientific_review_sha256"].items():
        if hashlib.sha256((ROOT / path).read_bytes()).hexdigest() != expected:
            raise ValueError("Shared PET-waste child scientific review changed.")
    proofs = {(r["label"], identity(r["source"])): r for r in APPROVAL["inventories"]}
    children = {}
    for entry in APPROVAL["child_checks"]:
        key = entry["label"], identity(entry["source"])
        if (
            entry["source_amount"] >= 0
            or entry["conversion_factor"] != -1
            or proofs[key]["reference_amount"] != entry["source_reference_amount"]
            or entry["source_reference_amount"] != -1
            or proofs[key]["inventory_sha256"] != entry["source_inventory_sha256"]
        ):
            raise ValueError("PET-waste treatment reference, inventory or quantity changed.")
        target = entry["target"]
        if "recipe_ref" in target:
            if target["recipe_ref"] not in APPROVAL["recipe_dependencies"] or entry["scoped_patch"]:
                raise ValueError("Unreviewed PET-waste helper dependency.")
            expected = {"recipe": target["recipe_ref"], "amount": -entry["source_amount"]}
        else:
            if (
                "polyethylene terephtalate, 0.2% water" not in target["name"]
                or not identity(entry["source"])[0].endswith((", municipal incineration", ", sanitary landfill"))
                or proofs["uvek-2025-cutoff", identity(target)]["reference_amount"] != 1
            ):
                raise ValueError("Unreviewed PET-waste material/function proxy.")
            expected = {**target, "type": "technosphere", "amount": -entry["source_amount"]}
        children.setdefault((entry["label"], identity(entry["parent"])), []).append(expected)
    choices = {(r["label"], identity(r["source"])): r for r in APPROVAL["freight_choices"]}
    used = set()
    for row in SOURCES.values():
        for p in row["profiles"]:
            if p["omissions"] or any(e.get("type") == "biosphere" for e in p["inputs"]):
                raise ValueError("A PET-waste market cannot omit a component or invent a direct release.")
            freight = []
            for entry in p["freight_choices"]:
                key = p["label"], tuple(entry["source"])
                choice = choices[key]
                if (
                    digest(choice) != entry["choice_sha256"]
                    or entry["amount"] <= 0
                    or choice["conversion_factor"] != 1
                    or choice["source_inventory_sha256"] != proofs[key]["inventory_sha256"]
                    or proofs[key]["reference_amount"] != 1
                ):
                    raise ValueError("PET-waste complete freight or original weight changed.")
                # No approved parent in this cohort calls rail; its steam-containing profiles stay pending.
                if choice["kind"] not in {"road", "barge", "sea"} or "recipe_ref" in choice["target"]:
                    raise ValueError("Unreviewed PET-waste rail or partial operating service.")
                freight.append({**choice["target"], "type": "technosphere", "amount": entry["amount"]})
                used.add(key)
            expected = children.get((p["label"], identity(row["source"])), []) + freight
            # Original source exchange order can interleave freight and treatments.
            if sorted(map(digest, expected)) != sorted(map(digest, p["inputs"])):
                raise ValueError("PET-waste market treatment/freight coefficients changed.")
    if used != set(choices):
        raise ValueError("Unused or missing PET-waste freight choices.")
    for entry in APPROVAL["full_road_service_proofs"]:
        if (
            not all(entry[k] for k in ("has_vehicle", "has_maintenance", "has_road", "has_fuel"))
            or proofs[entry["label"], identity(entry["source"])]["inventory_sha256"] != entry["inventory_sha256"]
        ):
            raise ValueError("PET-waste complete road boundary changed.")
    reverse = json.loads((ROOT / "brightpath/data/migrations/uvek/uvek-2025-to-ecoinvent-3.12-cutoff.json").read_text())
    rules = {identity(r["source"]): r for r in reverse["replace"]}
    for entry in APPROVAL["reverse_bindings"]:
        if (
            digest(rules.get(identity(entry["source"]))) != entry["binding_sha256"]
            or proofs["uvek-2025-cutoff", identity(entry["source"])]["inventory_sha256"]
            != entry["source_inventory_sha256"]
            or proofs["ecoinvent-3.12-cutoff", identity(entry["target"])]["inventory_sha256"]
            != entry["target_inventory_sha256"]
        ):
            raise ValueError("PET-waste independently reviewed reverse supplier changed.")
    if any(identity(r) in rules for r in APPROVAL["unsupported_reverse_sources"]):
        raise ValueError("A new reverse PET-treatment correspondence needs independent review.")


def forward_decision(rule, observations, inventories, binding, original_review):
    """Compile only complete exact profiles, retaining all other profiles as blocked."""
    row = SOURCES[identity(rule["source"])]
    decision = composition_decision(
        rule,
        {(label, sign) for label, sign in observations if label.endswith("-cutoff")},
        [(r["label"], r["inventory_sha256"]) for r in inventories],
        binding,
        deepcopy(original_review),
        approval_row=row,
        rationale=LIMITATIONS,
        review_path=REVIEW_PATH,
        namespace="pet-waste-market",
        mapping_method="reviewed_pet_waste_market_service",
        profile_reason="Only enumerated complete cut-off profiles are approved; positive steam and uninspected profiles stay unresolved.",
    )
    pending = {p["label"]: p["reason"] for p in row["pending_profiles"]}
    for p in decision["review"]["profiles"]:
        if p["review"]["status"] == "unresolved":
            p["review"]["reason"] = pending[
                "-".join(p["source_profile"][f] for f in ("family", "version", "system_model"))
            ]
    decision["source_observations"] = [list(pair) for pair in sorted(observations)]
    return decision
