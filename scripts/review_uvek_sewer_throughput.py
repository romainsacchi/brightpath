"""Compile complete sewer services normalized per cubic metre of wastewater conveyed."""

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
REVIEW_PATH = "brightpath/data/export/uvek_sewer_throughput_review.json"
APPROVAL = json.loads((ROOT / REVIEW_PATH).read_text())
SOURCES = {tuple(row["source"][f] for f in FIELDS): row for row in APPROVAL["rules"]}
VERSIONS = {"3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12"}
if (
    len(SOURCES) != 4
    or APPROVAL["authorized_system_model"] != "cutoff"
    or APPROVAL["source_count"] != 4
    or APPROVAL["source_profile_count"] != 24
    or APPROVAL["inventory_count"] != 449
    or sum(len(row["profiles"]) for row in SOURCES.values()) != 24
    or any(row["pending_profiles"] for row in SOURCES.values())
    or {s[0] for s in SOURCES}
    != {
        "market for sewer grid, rural",
        "market for sewer grid, urban",
        "sewer grid construction, rural",
        "sewer grid construction, urban",
    }
    or any(s[2:] != ("GLO", "cubic meter") for s in SOURCES)
):
    raise ValueError("Sewer-throughput scope changed; repeat the scientific review.")
LIMITATIONS = APPROVAL["limitations"]
EVIDENCE_SCOPE = {}
for row in APPROVAL["inventories"]:
    profile = tuple(row["label"].split("-"))
    if profile not in {("ecoinvent", v, "cutoff") for v in VERSIONS} | {("uvek", "2025", "cutoff")}:
        raise ValueError("Unreviewed sewer-throughput evidence profile.")
    EVIDENCE_SCOPE.setdefault(profile, set()).add(tuple(row["source"][f] for f in FIELDS))


def identity(row):
    return tuple(row[f] for f in FIELDS)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def inventory_record(source, blobs, comment, product_information):
    """Pin complete original vectors and descriptions without distributing source inventories."""
    (reference,) = [e for e in map(pickle.loads, blobs) if e["type"] == "production"]
    if reference["amount"] not in (-1, 1) or tuple(reference["input"]) != tuple(reference["output"]):
        raise ValueError("Sewer-throughput evidence reference changed; repeat the scientific review.")
    return {
        "source": list(source),
        "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
        "activity_comment_sha256": hashlib.sha256(str(comment or "").encode()).hexdigest(),
        "product_information_sha256": hashlib.sha256(str(product_information or "").encode()).hexdigest(),
        "reference_amount": reference["amount"],
    }


def collect_extra_inventories(brightway_root):
    """Read only the original exact patch inventories, including its actual waste submix."""
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
                raise ValueError("Sewer-throughput patch scope changed; repeat the scientific review.")
    return records


def validate_inventories(inventories, observations, native):
    """Freeze all actual source/supplier vectors, reference magnitudes and native roles."""
    expected = {
        json.dumps({**r, "source": list(identity(r["source"]))}, sort_keys=True) for r in APPROVAL["inventories"]
    }
    actual = {json.dumps({**r, "source": list(s)}, sort_keys=True) for s, rows in inventories.items() for r in rows}
    if actual != expected or len(expected) != 449 or sum(map(len, inventories.values())) != 449:
        raise ValueError("Sewer-throughput inventory or descriptions changed; repeat the scientific review.")
    for source, row in SOURCES.items():
        wanted = {(p["label"], 1) for p in row["profiles"]}
        observed = {
            (label, sign) for label, sign in observations["ecoinvent"].get(source, set()) if label.endswith("-cutoff")
        }
        if wanted != observed or {(r["label"], r["reference_amount"]) for r in inventories[source]} != wanted:
            raise ValueError("Sewer-throughput exact reference changed; repeat the scientific review.")
    if (
        len(APPROVAL["native_roles"]) != 21
        or sum(r["native_flow_type"] == "WASTE_FLOW" for r in APPROVAL["native_roles"]) != 4
    ):
        raise ValueError("Sewer-throughput native supplier scope changed.")
    for row in APPROVAL["native_roles"]:
        source = identity(row["source"])
        if (
            native.get(source, {}).get("native_flow_type") != row["native_flow_type"]
            or native[source].get("native_reference_is_input") is not row["native_reference_is_input"]
            or observations["uvek"].get(source) != {("uvek-2025-cutoff", 1)}
        ):
            raise ValueError("Sewer-throughput native supplier role changed; repeat the scientific review.")


def validate_bindings(resource):
    """Freeze component choices, exact patch composition, water conversions and independent inverses."""
    validate_composition_bindings(resource, approval=APPROVAL, label="Sewer-throughput")
    proofs = {(r["label"], identity(r["source"])): r for r in APPROVAL["inventories"]}
    local = APPROVAL["local_recipes"]
    if len(local) != 16 or len(APPROVAL["local_market_proofs"]) != 4:
        raise ValueError("Sewer-throughput local recipe scope changed.")
    for row in APPROVAL["local_market_proofs"]:
        source = identity(row["source"])
        if (
            row["label"] not in {"ecoinvent-3.9.1-cutoff", "ecoinvent-3.10.1-cutoff"}
            or source[0] != "market group for waste polyethylene"
            or source[2] not in {"RER", "Europe without Switzerland"}
            or row["source_sign"] != -1
            or row["inventory_sha256"] != proofs[row["label"], source]["inventory_sha256"]
            or digest(local.get(row["recipe_ref"])) != row["recipe_sha256"]
        ):
            raise ValueError("Sewer-throughput exact patch submix changed; repeat the scientific review.")
    for source, row in SOURCES.items():
        for p in row["profiles"]:
            if p["source_sign"] != 1 or p["inventory_sha256"] != proofs[p["label"], source]["inventory_sha256"]:
                raise ValueError("Sewer-throughput profile changed.")
            if source[0].startswith("market for"):
                (entry,) = p["inputs"]
                recipe = local[entry["recipe"]]
                if entry["amount"] != 1 or recipe["unit"] != "cubic meter" or not recipe["inputs"]:
                    raise ValueError("Sewer-throughput market reference or full construction changed.")
            else:
                flows = [e for e in p["inputs"] if e.get("type") == "biosphere"]
                if (
                    len(flows) != 2
                    or {tuple(e["categories"]) for e in flows} != {("air",), ("water",)}
                    or any(e["name"] != "Water" or e["unit"] != "cubic meter" or e["amount"] <= 0 for e in flows)
                ):
                    raise ValueError("Sewer-throughput direct releases changed; no omission is approved.")
    bio = json.loads(
        (ROOT / "brightpath/data/migrations/uvek/ecoinvent-3.10-biosphere-to-uvek-2026-biosphere.json").read_text()
    )

    def bio_key(row):
        return row["name"], tuple(row["categories"]), row["unit"]

    bio_rules = {bio_key(r["source"]): r for r in bio["replace"]}
    if len(APPROVAL["biosphere_bindings"]) != 2:
        raise ValueError("Sewer-throughput water correspondence scope changed.")
    for entry in APPROVAL["biosphere_bindings"]:
        rule = bio_rules.get(bio_key(entry["source"]))
        if digest(rule) != entry["binding_sha256"] or rule["conversion_factor"] != 1000:
            raise ValueError("Sewer-throughput water correspondence changed; repeat the scientific review.")
    reverse = json.loads((ROOT / "brightpath/data/migrations/uvek/uvek-2025-to-ecoinvent-3.12-cutoff.json").read_text())
    reverse_rules = {identity(r["source"]): r for r in reverse["replace"]}
    unsupported = {identity(row["source"]) for row in APPROVAL["unsupported_reverse_suppliers"]}
    if len(unsupported) != 6 or unsupported != {
        identity(row["source"]) for row in APPROVAL["native_roles"] if identity(row["source"]) not in reverse_rules
    }:
        raise ValueError("Sewer-throughput reverse coverage changed; repeat the independent review.")
    if len(APPROVAL["reverse_bindings"]) != 15:
        raise ValueError("Sewer-throughput independent reverse scope changed.")
    for row in APPROVAL["reverse_bindings"]:
        source, target = identity(row["source"]), identity(row["target"])
        if (
            digest(reverse_rules.get(source)) != row["binding_sha256"]
            or row["source_inventory_sha256"] != proofs["uvek-2025-cutoff", source]["inventory_sha256"]
            or row["target_inventory_sha256"] != proofs["ecoinvent-3.12-cutoff", target]["inventory_sha256"]
        ):
            raise ValueError("Sewer-throughput independent reverse supplier changed; repeat its review.")


def forward_decision(rule, observations, inventories, binding, original_review):
    """Compile exact positive-throughput services without converting or approximating sewer length."""
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
        namespace="sewer-throughput",
        mapping_method="reviewed_sewer_throughput_service",
        profile_reason="Only exact inspected positive cubic-metre sewer-throughput profiles are approved.",
    )
    local = APPROVAL["local_recipes"]
    needed = {e["recipe"] for r in decision["recipes"].values() for e in r["inputs"] if "recipe" in e}
    while needed:
        rid = needed.pop()
        if rid not in local or rid in decision["recipes"]:
            continue
        recipe = deepcopy(local[rid])
        decision["recipes"][rid] = recipe
        needed.update(e["recipe"] for e in recipe["inputs"] if "recipe" in e)
    decision["source_observations"] = [list(pair) for pair in sorted(observations)]
    return decision
