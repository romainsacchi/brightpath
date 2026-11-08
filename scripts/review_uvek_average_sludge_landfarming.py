"""Retain average wet-sludge landfarming, all physical releases and complete delivery services."""

import hashlib
import json
import math
import pickle
import sqlite3
from copy import deepcopy
from pathlib import Path

try:
    from review_uvek_market_compositions import composition_decision, validate_composition_bindings
except ModuleNotFoundError:
    from scripts.review_uvek_market_compositions import (
        composition_decision,
        validate_composition_bindings,
    )

FIELDS = ("name", "reference product", "location", "unit")
ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_average_sludge_landfarming_review.json"
APPROVAL = json.loads((ROOT / REVIEW_PATH).read_text())
REVIEW_SHA256 = "81703d0ce16ee7a9b12df8964304cb0c4147cacc1deb020d79f0aff4542d8bfb"
SOURCES = {tuple(r["source"][f] for f in FIELDS): r for r in APPROVAL["rules"]}
VERSIONS = {"3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12"}
LIMITATIONS = APPROVAL["limitations"]
PRODUCT = "sewage sludge, 97% water, WWT, WW, average"
EXPECTED_SOURCES = {
    (name, PRODUCT, location, "kilogram")
    for name in ("market for " + PRODUCT, "treatment of " + PRODUCT + ", landfarming")
    for location in ("CA-QC", "CH", "Europe without Switzerland", "RoW")
}
EVIDENCE_SCOPE = {}
for row in APPROVAL["inventories"]:
    profile = tuple(row["label"].split("-"))
    if profile not in {("ecoinvent", v, "cutoff") for v in VERSIONS} | {("uvek", "2025", "cutoff")}:
        raise ValueError("Unreviewed average-sludge evidence profile.")
    EVIDENCE_SCOPE.setdefault(profile, set()).add(tuple(row["source"][f] for f in FIELDS))


def identity(row):
    return tuple(row[f] for f in FIELDS)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def inventory_record(source, blobs, comment, product_information):
    """Pin full actual vectors and descriptions without distributing raw licensed inventories."""
    (reference,) = [e for e in map(pickle.loads, blobs) if e["type"] == "production"]
    if reference["amount"] not in (-1, 1) or tuple(reference["input"]) != tuple(reference["output"]):
        raise ValueError("Average-sludge landfarming evidence reference changed; repeat the scientific review.")
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
                raise ValueError("Average-sludge landfarming exact patch scope changed; repeat the scientific review.")
    return records


def validate_review():
    """Refuse changed scope, incomplete field inventories or unreviewed omissions."""
    if (
        digest(APPROVAL) != REVIEW_SHA256
        or set(SOURCES) != EXPECTED_SOURCES
        or APPROVAL["authorized_system_model"] != "cutoff"
        or (APPROVAL["source_count"], APPROVAL["source_profile_count"], APPROVAL["inventory_count"]) != (8, 48, 221)
        or APPROVAL["source_identified_existing_composed_parent_profiles"] != 0
        or len(APPROVAL["canonical_biosphere_definitions"]) != 45
    ):
        raise ValueError("Average-sludge scope changed; repeat the scientific review.")
    wanted = {f"ecoinvent-{v}-cutoff" for v in VERSIONS}
    for row in SOURCES.values():
        if len(row["profiles"]) != 6 or {p["label"] for p in row["profiles"]} != wanted or row["pending_profiles"]:
            raise ValueError("Average-sludge exact profiles changed; repeat the scientific review.")
        for p in row["profiles"]:
            if p["source_sign"] != -1:
                raise ValueError("Average-sludge waste reference changed; repeat the scientific review.")
            if row["kind"] != "treatment":
                continue
            tech = [e for e in p["inputs"] if e["type"] == "technosphere"]
            bio = [e for e in p["inputs"] if e["type"] == "biosphere"]
            indicators = [e for e in bio if e.get("approved_omission_if_unavailable")]
            expected_bio = 45 if p["label"] in {"ecoinvent-3.9-cutoff", "ecoinvent-3.9.1-cutoff"} else 44
            if (
                len(tech) != 1
                or len(bio) != expected_bio
                or len(indicators) != 1
                or identity(tech[0])
                != ("Slurry spreading, by vacuum tanker", "Slurry spreading, by vacuum tanker", "CH", "cubic meter")
                or tech[0]["amount"] != 0.00097087
                or indicators[0]["name"] != "Non-hazardous waste disposed"
                or indicators[0]["categories"] != ["inventory indicator", "waste"]
                or indicators[0]["unit"] != "kilogram"
                or indicators[0]["amount"] != 1
                or len(p["inputs"]) != expected_bio + 1
            ):
                raise ValueError(
                    "Average-sludge physical or mechanical boundary changed; repeat the scientific review."
                )
            water = [e for e in bio if e["name"] == "Water"]
            if (
                len(water) != 2
                or any(e["unit"] != "cubic meter" for e in water)
                or not math.isclose(sum(e["amount"] for e in water) * 1000, 0.97, rel_tol=1e-14)
            ):
                raise ValueError("Average-sludge wet-water balance changed; repeat the scientific review.")


def validate_inventories(inventories, observations, native):
    """Require every full treatment/market, mechanical, rail and heavy-road inventory."""
    validate_review()
    expected = {
        json.dumps({**r, "source": list(identity(r["source"]))}, sort_keys=True) for r in APPROVAL["inventories"]
    }
    actual = {json.dumps({**r, "source": list(s)}, sort_keys=True) for s, rows in inventories.items() for r in rows}
    if actual != expected or len(expected) != 221 or sum(map(len, inventories.values())) != 221:
        raise ValueError("Average-sludge full inventory changed; repeat the scientific review.")
    for source, row in SOURCES.items():
        wanted = {(p["label"], -1) for p in row["profiles"]}
        observed = {
            (label, sign) for label, sign in observations["ecoinvent"].get(source, set()) if label.endswith("-cutoff")
        }
        if observed != wanted or {(r["label"], r["reference_amount"]) for r in inventories[source]} != wanted:
            raise ValueError("Average-sludge source reference changed; repeat the scientific review.")
    for role in APPROVAL["native_roles"]:
        source = identity(role["source"])
        if observations["uvek"].get(source) != {("uvek-2025-cutoff", 1)} or any(
            native.get(source, {}).get(f) != role[f]
            for f in ("native_flow_type", "native_reference_is_input", "flow_property_name", "unit_name")
        ):
            raise ValueError("Average-sludge native product role changed; repeat the scientific review.")


def validate_bindings(resource):
    """Freeze canonical emissions, complete scoped freight and the independent reverse service."""
    validate_review()
    validate_composition_bindings(resource, approval=APPROVAL, label="Average-sludge landfarming")
    for path, expected in APPROVAL["shared_scientific_review_sha256"].items():
        if hashlib.sha256((ROOT / path).read_bytes()).hexdigest() != expected:
            raise ValueError("Shared average-sludge service review changed; repeat the scientific review.")
    catalog = json.loads((ROOT / "brightpath/data/export/reference_catalogs/ecoinvent__3.10__cutoff.json").read_text())
    definitions = {r["flow_id"]: r for r in catalog["biosphere"]}
    for flow in APPROVAL["canonical_biosphere_definitions"]:
        if {f: definitions.get(flow["flow_id"], {}).get(f) for f in flow} != flow:
            raise ValueError("Average-sludge canonical release changed; repeat the scientific review.")
    proofs = {(r["label"], identity(r["source"])): r for r in APPROVAL["inventories"]}
    road = (
        "Transport, freight, lorry, fleet average",
        "Transport, freight, lorry, fleet average",
        "RER",
        "ton kilometer",
    )
    choices = {(r["label"], identity(r["source"])): r for r in APPROVAL["freight_choices"]}
    if len(choices) != 30 or len(APPROVAL["freight_uses"]) != 42:
        raise ValueError("Average-sludge delivery scope changed; repeat the scientific review.")
    for node, choice in choices.items():
        if choice["source_inventory_sha256"] != proofs[node]["inventory_sha256"] or choice["conversion_factor"] != 1:
            raise ValueError("Average-sludge freight reference changed; repeat the scientific review.")
        if choice["kind"] == "complete_heavy_road":
            if identity(choice["target"]) != road:
                raise ValueError("Average-sludge full heavy fleet changed; repeat the scientific review.")
        else:
            body = resource["recipes"].get(choice["target"]["recipe_ref"], {})
            expected = {
                "Transport, freight, rail, electricity with shunting": 0.958767540376505,
                "Transport, freight, rail, diesel, with particle filter": 0.0412324596234946,
            }
            inputs = body.get("inputs", [])
            if (
                len(inputs) != 2
                or {e["name"]: e["amount"] for e in inputs} != expected
                or any(e["location"] != "CH" or e["unit"] != "ton kilometer" for e in inputs)
            ):
                raise ValueError("Average-sludge Swiss rail shares changed; repeat the scientific review.")
    reverse = json.loads((ROOT / "brightpath/data/migrations/uvek/uvek-2025-to-ecoinvent-3.12-cutoff.json").read_text())
    entry = APPROVAL["reverse_binding"]
    rule = next((r for r in reverse["replace"] if r["source"] == entry["source"]), None)
    if (
        digest(rule) != entry["binding_sha256"]
        or entry["source_inventory_sha256"] != proofs["uvek-2025-cutoff", identity(entry["source"])]["inventory_sha256"]
        or entry["target_inventory_sha256"]
        != proofs["ecoinvent-3.12-cutoff", identity(entry["target"])]["inventory_sha256"]
    ):
        raise ValueError("Average-sludge independent reverse spreading changed; repeat the scientific review.")


def forward_decision(rule, observations, inventories, binding, original_review):
    """Replace only the source-exact nonempty treatment or complete delivery inventory."""
    validate_review()
    approval = SOURCES[identity(rule["source"])]
    prior = approval["previous_flat_sign_decision"]
    if binding != prior["binding"]:
        raise ValueError("Average-sludge prior supplier changed; repeat the scientific review.")
    if rule.get("conversion_factor", 1) == -1:
        if (
            prior["original_factor"] != 1
            or prior["factor"] != -1
            or prior["review"] != rule.get("reference_flow_sign_review")
            or prior["review"].get("status") != "verified"
            or prior["review"].get("source_sign") != -1
            or prior["review"].get("target_sign") != 1
        ):
            raise ValueError("Average-sludge prior sign changed; repeat the scientific review.")
        rule = deepcopy(rule)
        rule.pop("conversion_factor")
    decision = composition_decision(
        rule,
        {(label, sign) for label, sign in observations if label.endswith("-cutoff")},
        [(r["label"], r["inventory_sha256"]) for r in inventories],
        binding,
        deepcopy(original_review),
        approval_row=approval,
        rationale=LIMITATIONS,
        review_path=REVIEW_PATH,
        namespace="average-sludge-landfarming",
        mapping_method="reviewed_average_sludge_landfarming_service",
        profile_reason="Only the 48 independently inspected cut-off treatment/market profiles are approved.",
    )
    decision["source_observations"] = [list(pair) for pair in sorted(observations)]
    return decision
