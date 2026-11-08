"""Retain steel-waste treatment and freight around explicitly approved recovery handoffs."""

import hashlib
import json
import pickle
import sqlite3
from copy import deepcopy
from pathlib import Path

from brightpath import TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules

try:
    from review_uvek_market_compositions import composition_decision, validate_composition_bindings
except ModuleNotFoundError:
    from scripts.review_uvek_market_compositions import composition_decision, validate_composition_bindings

FIELDS = ("name", "reference product", "location", "unit")
ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_steel_residue_market_review.json"
APPROVAL = json.loads((ROOT / REVIEW_PATH).read_text())
SOURCES = {tuple(r["source"][f] for f in FIELDS): r for r in APPROVAL["rules"]}
RULES_SHA256 = "004abab66c537d928e7f842b3c7e250097ed1965ca4adf8c00ab3b997e52dc42"
VERSIONS = {"3.7", "3.8", "3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12"}
LIMITATIONS = APPROVAL["limitations"]
if (
    APPROVAL["authorized_system_model"] != "cutoff"
    or APPROVAL["source_count"] != 11
    or len(SOURCES) != 11
    or APPROVAL["source_profile_count"] != 88
    or APPROVAL["approved_source_profile_count"] != 68
    or APPROVAL["pending_source_profile_count"] != 20
    or APPROVAL["inventory_count"] != 717
):
    raise ValueError("Steel-residue market scope changed; repeat the scientific review.")
EVIDENCE_SCOPE = {}
for row in APPROVAL["inventories"]:
    profile = tuple(row["label"].split("-"))
    if profile not in {("ecoinvent", v, "cutoff") for v in VERSIONS} | {("uvek", "2025", "cutoff")}:
        raise ValueError("Unreviewed steel-residue evidence profile.")
    EVIDENCE_SCOPE.setdefault(profile, set()).add(tuple(row["source"][f] for f in FIELDS))


def identity(row):
    return tuple(row[f] for f in FIELDS)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def transport(name, location):
    return dict(zip(FIELDS, (name, name, location, "ton kilometer"), strict=True))


ROAD = transport("Transport, freight, lorry, fleet average", "RER")
DIESEL = transport("Transport, freight, rail, diesel, with particle filter", "CH")
ELECTRIC = transport("Transport, freight, rail, electricity with shunting", "CH")
BARGE = transport("Transport, barge", "RER")


def inventory_record(source, blobs, comment, product_information):
    """Fingerprint actual full vectors and descriptions without publishing licensed inventories."""
    (reference,) = [e for e in map(pickle.loads, blobs) if e["type"] == "production"]
    if reference["amount"] not in (-1, 1) or tuple(reference["input"]) != tuple(reference["output"]):
        raise ValueError("Steel-residue reference changed; repeat the scientific review.")
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
                raise ValueError("Steel-residue exact patch scope changed; repeat the scientific review.")
    return records


def validate_inventories(inventories, observations, native):
    """Require all original references, full vectors, descriptions and native service roles."""
    expected = {
        json.dumps({**r, "source": list(identity(r["source"]))}, sort_keys=True) for r in APPROVAL["inventories"]
    }
    actual = {json.dumps({**r, "source": list(s)}, sort_keys=True) for s, rows in inventories.items() for r in rows}
    if actual != expected or len(expected) != 717 or sum(map(len, inventories.values())) != 717:
        raise ValueError("Steel-residue inventory or metadata changed; repeat the scientific review.")
    for source, row in SOURCES.items():
        wanted = {(p["label"], -1) for p in [*row["profiles"], *row["pending_profiles"]]}
        observed = {
            (label, sign) for label, sign in observations["ecoinvent"].get(source, set()) if label.endswith("-cutoff")
        }
        if observed != wanted or {(r["label"], r["reference_amount"]) for r in inventories[source]} != wanted:
            raise ValueError("Steel-residue exact source profile changed; repeat the scientific review.")
    for role in APPROVAL["native_roles"]:
        source = identity(role["source"])
        if (
            native.get(source, {}).get("native_flow_type") != "PRODUCT_FLOW"
            or native[source].get("native_reference_is_input") is not False
            or observations["uvek"].get(source) != {("uvek-2025-cutoff", 1)}
        ):
            raise ValueError("Steel-residue full freight supplier role changed; repeat the scientific review.")


def validate_bindings(resource):
    """Freeze exact omitted handoffs, retained full treatments and independently reviewed freight."""
    validate_composition_bindings(resource, approval=APPROVAL, label="Steel-residue")
    shared = ROOT / "brightpath/data/export/uvek_residual_landfill_review.json"
    if hashlib.sha256(shared.read_bytes()).hexdigest() != APPROVAL["shared_landfill_review_sha256"]:
        raise ValueError("Steel-residue shared landfill review changed; repeat the scientific review.")
    if (
        digest(APPROVAL["rules"]) != RULES_SHA256
        or digest(APPROVAL["local_recipes"]) != "a649c9a891a754f6d3c46e8d384d71b8873db2a73f140fa3269b2633e134e07f"
    ):
        raise ValueError("Steel-residue quantities or omission boundary changed; repeat the scientific review.")
    catalog = json.loads((ROOT / "brightpath/data/export/reference_catalogs/ecoinvent__3.10__cutoff.json").read_text())
    by_uuid = {r["flow_id"]: r for r in catalog["biosphere"]}
    for definition in APPROVAL["canonical_biosphere_definitions"]:
        if {k: by_uuid.get(definition["flow_id"], {}).get(k) for k in definition} != definition:
            raise ValueError("Steel-residue canonical flow definition changed; repeat the scientific review.")
    proofs = {(r["label"], identity(r["source"])): r for r in APPROVAL["inventories"]}
    choices = {(r["label"], identity(r["source"])): r for r in APPROVAL["freight_choices"]}
    local = APPROVAL["local_recipes"]
    selected = {}
    used, local_used = set(), set()
    for row in SOURCES.values():
        for profile in row["profiles"]:
            for omission in profile["omissions"]:
                label = omission["authorization_label"]
                if label not in selected:
                    selected[label] = {
                        identity(r["source"]): r
                        for r in select_profile_sign_rules(resource, TechnosphereProfile(*label.split("-")))["delete"]
                    }
                rule = selected[label].get(tuple(omission["source"]))
                if (
                    digest(rule) != omission["approved_rule_sha256"]
                    or not rule.get("approved_omission")
                    or omission["amount"] >= 0
                    or omission["source_inventory_sha256"]
                    != proofs[profile["label"], tuple(omission["source"])]["inventory_sha256"]
                    or omission["upstream_inventory_sha256"]
                    != proofs[profile["label"], tuple(omission["upstream_source"])]["inventory_sha256"]
                ):
                    raise ValueError("Steel-residue exact omission evidence changed; repeat the scientific review.")
            expected = []
            for entry in profile["freight_choices"]:
                key = profile["label"], tuple(entry["source"])
                choice = choices[key]
                if (
                    digest(choice) != entry["choice_sha256"]
                    or choice["source_inventory_sha256"] != proofs[key]["inventory_sha256"]
                    or choice["conversion_factor"] != 1
                    or entry["amount"] <= 0
                ):
                    raise ValueError("Steel-residue freight evidence changed; repeat the scientific review.")
                kind, source = choice["kind"], key[1]
                if kind in {"road", "barge"}:
                    target = ROAD if kind == "road" else BARGE
                    if ("lorry" if kind == "road" else "barge") not in source[0] or choice["target"] != target:
                        raise ValueError("Unreviewed steel-residue freight function.")
                    expected.append({**target, "type": "technosphere", "amount": entry["amount"]})
                else:
                    rid = choice["target"].get("recipe_ref")
                    rail = []
                    for member in choice["members"]:
                        traction, provider = member["traction"], tuple(member["source"])
                        if (
                            traction not in {"diesel", "electric"}
                            or traction not in provider[0]
                            or "steam" in provider[0]
                            or member["amount"] <= 0
                            or member["inventory_sha256"] != proofs[profile["label"], provider]["inventory_sha256"]
                        ):
                            raise ValueError("Steel-residue rail traction changed; repeat the scientific review.")
                        rail.append(
                            {
                                **(DIESEL if traction == "diesel" else ELECTRIC),
                                "type": "technosphere",
                                "amount": member["amount"],
                            }
                        )
                    if (
                        kind != "rail"
                        or rid not in local
                        or local[rid]["inputs"] != rail
                        or abs(sum(e["amount"] for e in rail) - 1) > 1e-12
                    ):
                        raise ValueError("Steel-residue rail weights changed; repeat the scientific review.")
                    expected.append({"recipe": rid, "amount": entry["amount"]})
                    local_used.add(rid)
                used.add(key)
            if row["kind"] == "market":
                actual = [
                    e for e in profile["inputs"] if e.get("unit") == "ton kilometer" or e.get("recipe") in local_used
                ]
                if actual != expected or any(e.get("type") == "biosphere" for e in profile["inputs"]):
                    raise ValueError("Steel-residue retained freight or market boundary changed.")
    if used != set(choices):
        raise ValueError("Steel-residue unused or missing freight choices.")
    reverse = json.loads((ROOT / "brightpath/data/migrations/uvek/uvek-2025-to-ecoinvent-3.12-cutoff.json").read_text())
    rules = {identity(r["source"]): r for r in reverse["replace"]}
    if {identity(r["source"]) for r in APPROVAL["reverse_bindings"]} != {
        identity(t) for t in (ROAD, DIESEL, ELECTRIC, BARGE)
    }:
        raise ValueError("Steel-residue independent reverse scope changed.")
    for entry in APPROVAL["reverse_bindings"]:
        if (
            digest(rules.get(identity(entry["source"]))) != entry["binding_sha256"]
            or entry["source_inventory_sha256"]
            != proofs["uvek-2025-cutoff", identity(entry["source"])]["inventory_sha256"]
            or entry["target_inventory_sha256"]
            != proofs["ecoinvent-3.12-cutoff", identity(entry["target"])]["inventory_sha256"]
        ):
            raise ValueError("Steel-residue reverse supplier changed; repeat the independent scientific review.")


def forward_decision(rule, observations, inventories, binding, original_review):
    """Compile the retained inventory, with explicit warnings for exact approved omissions."""
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
        namespace="steel-residue-landfill" if row["kind"] == "landfill" else "steel-residue-market",
        mapping_method="reviewed_steel_residue_service",
        profile_reason="Only exact inspected cut-off profiles are approved; unresolved clinker/steam boundaries remain pending.",
    )
    pending = {p["label"]: p["reason"] for p in row["pending_profiles"]}
    for profile in decision["review"]["profiles"]:
        if profile["review"]["status"] == "unresolved":
            label = "-".join(profile["source_profile"][f] for f in ("family", "version", "system_model"))
            profile["review"]["reason"] = pending[label]
    # Include precisely the reviewed nested landfill/rail graph, preserving existing recipe IDs.
    needed = {e["recipe"] for recipe in decision["recipes"].values() for e in recipe["inputs"] if "recipe" in e}
    while needed:
        rid = needed.pop()
        if rid in decision["recipes"]:
            continue
        recipe = deepcopy(APPROVAL["local_recipes"][rid])
        decision["recipes"][rid] = recipe
        needed.update(e["recipe"] for e in recipe["inputs"] if "recipe" in e)
    decision["source_observations"] = [list(pair) for pair in sorted(observations)]
    return decision
