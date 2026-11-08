"""Distinguish bulk cathode copper supply from a lithium-ion battery cathode."""

import hashlib
import json
import pickle
import sqlite3
from copy import deepcopy
from pathlib import Path

FIELDS = ("name", "reference product", "location", "unit")
ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_copper_cathode_review.json"
APPROVAL = json.loads((ROOT / REVIEW_PATH).read_text())
SOURCE = ("market for copper, cathode", "copper, cathode", "GLO", "kilogram")
TARGET = ("Copper, at regional storage", "Copper, at regional storage", "RER", "kilogram")
PREVIOUS = ("Cathode", "Cathode", "GLO", "kilogram")
VERSIONS = {"3.6", "3.7", "3.8", "3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12"}
LIMITATIONS = (
    "Reviewed cut-off copper supply proxy: preserve one kg of bulk cathode copper using the complete "
    "UVEK Copper, at regional storage process. The former Cathode target supplies a lithium-ion battery "
    "electrode containing NMC material and aluminium, not bulk copper metal. Both reviewed copper "
    "suppliers have positive one-kg product references. Apply factor +1 to preserve signed caller mass, "
    "uncertainty, formulas, foreground suppliers and separate emissions. The global source market is "
    "approximated by UVEK's European storage supply based on German 1994 primary/secondary copper and "
    "import/logistics data. Retain the complete target, including concentrate production, separately "
    "modelled refining, scrap allocation and transport; do not add extra transport or copper production. "
    "This is a screening commodity proxy: grade/purity, supplier mix, vintage, recycling allocation, "
    "manufacturing, freight and LCIA equivalence are not established. The native secondary-copper "
    "documentation calls for a more detailed analysis when secondary copper materially affects results. "
    "No purity, yield or concentrate-mass normalization is inferred. This is a nonempty metal supply, "
    "not an allocation-only stock, treatment service, recovery credit or battery component. Only the "
    "nine inspected cut-off profiles are reviewed. Consequential conversion remains unsupported. "
    "The separately authored UVEK-to-ecoinvent 3.12 cut-off copper-supply proxy is retained unchanged; "
    "no inverse is inferred from this forward decision."
)
EVIDENCE_SCOPE = {}
for row in APPROVAL["inventories"]:
    profile = tuple(row["label"].split("-"))
    EVIDENCE_SCOPE.setdefault(profile, set()).add(tuple(row["source"][f] for f in FIELDS))


def identity(row):
    return tuple(row[f] for f in FIELDS)


def inventory_record(source, blobs, comment, product_information):
    """Retain complete inventory/metadata hashes and the exact positive unit reference."""
    rows = [pickle.loads(b) for b in blobs]
    references = [r for r in rows if r["type"] == "production"]
    if (
        len(references) != 1
        or references[0]["amount"] != 1
        or isinstance(references[0]["amount"], bool)
        or tuple(references[0]["input"]) != tuple(references[0]["output"])
    ):
        raise ValueError("Copper-supply positive unit reference changed; repeat the scientific review.")
    return {
        "source": list(source),
        "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
        "activity_comment_sha256": hashlib.sha256(str(comment or "").encode()).hexdigest(),
        "product_information_sha256": hashlib.sha256(str(product_information or "").encode()).hexdigest(),
        "reference_amount": 1,
        "total_exchanges": len(rows),
    }


def collect_extra_inventories(brightway_root):
    """Read both explicitly reviewed original patch releases without series inheritance."""
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
                raise ValueError("Copper-supply patch coverage changed; repeat the scientific review.")
    return result


def validate_inventories(inventories, observations, native):
    """Require all nine source vectors and all fifteen native process boundaries."""
    (row,) = APPROVAL["rules"]
    if (
        APPROVAL["authorized_system_model"] != "cutoff"
        or (identity(row["source"]), identity(row["target"]), identity(row["previous_target"]))
        != (SOURCE, TARGET, PREVIOUS)
        or {r["label"] for r in row["profiles"]} != {f"ecoinvent-{v}-cutoff" for v in VERSIONS}
    ):
        raise ValueError("Copper-supply authorization scope changed; repeat the scientific review.")
    expected = {
        json.dumps({**r, "source": list(identity(r["source"]))}, sort_keys=True) for r in APPROVAL["inventories"]
    }
    actual = {json.dumps({**r, "source": list(s)}, sort_keys=True) for s, rows in inventories.items() for r in rows}
    if actual != expected or len(expected) != 24 or sum(map(len, inventories.values())) != 24:
        raise ValueError("Copper-supply inventory or metadata changed; repeat the scientific review.")
    wanted = {(f"ecoinvent-{v}-cutoff", 1) for v in VERSIONS}
    if {
        (label, sign) for label, sign in observations["ecoinvent"].get(SOURCE, set()) if label.endswith("-cutoff")
    } != wanted:
        raise ValueError("Copper-supply source reference coverage changed; repeat the scientific review.")
    for role in APPROVAL["native_roles"]:
        source = identity(role["source"])
        metadata = native.get(source, {})
        if observations["uvek"].get(source) != {("uvek-2025-cutoff", 1)} or any(
            metadata.get(field) != role[field]
            for field in ("native_flow_type", "native_reference_is_input", "flow_property_name", "unit_name")
        ):
            raise ValueError("Copper-supply native role changed; repeat the scientific review.")
    reverse = APPROVAL["independent_reverse"]
    if hashlib.sha256((ROOT / reverse["resource_path"]).read_bytes()).hexdigest() != reverse["resource_sha256"]:
        raise ValueError("Independent reverse copper correspondence changed; repeat the scientific review.")


def approved_copper_rule(rule):
    """Correct only the inspected commodity identity; reject unrelated mass or target drift."""
    if identity(rule["source"]) != SOURCE:
        return rule
    if identity(rule["target"]) not in (PREVIOUS, TARGET) or rule.get("conversion_factor", 1) != 1:
        raise ValueError("Copper-supply target or mass factor changed; repeat the scientific review.")
    return {**rule, "target": dict(zip(FIELDS, TARGET, strict=True))}


def forward_decision(rule, observations, uvek_signs, native, binding, previous, original_review):
    """Record a positive product-to-product proxy without reversing any caller quantity."""
    if (
        identity(rule["source"]) != SOURCE
        or identity(rule["target"]) != TARGET
        or rule.get("conversion_factor", 1) != 1
        or {(label, sign) for label, sign in observations if label.endswith("-cutoff")}
        != {(f"ecoinvent-{v}-cutoff", 1) for v in VERSIONS}
        or uvek_signs != {1}
        or native.get("native_flow_type") != "PRODUCT_FLOW"
        or native.get("native_reference_is_input") is not False
    ):
        raise ValueError("Copper-supply correspondence changed; repeat the scientific review.")
    return {
        "binding": binding,
        "original_factor": 1,
        "factor": 1,
        "source_observations": [list(pair) for pair in sorted(observations)],
        "uvek_reference_sign": 1,
        "uvek_native_flow_type": "PRODUCT_FLOW",
        "original_target": dict(zip(FIELDS, PREVIOUS, strict=True)),
        "original_review": deepcopy(previous.get("original_review", original_review)),
        "original_information_loss": previous.get(
            "original_information_loss", rule.get("technosphere_information_loss")
        ),
        "supplier_proxy_review": {
            "rationale": "Auto-approved under the standing independently inspected whole-function, mass-unit and geographic-proxy authorization. "
            + LIMITATIONS,
            "limitations": [LIMITATIONS],
            "evidence": [REVIEW_PATH],
        },
        "review": {
            "status": "verified",
            "source_sign": 1,
            "target_sign": 1,
            "reason": LIMITATIONS,
            "evidence": [REVIEW_PATH],
        },
    }
