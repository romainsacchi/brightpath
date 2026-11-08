"""Preserve complete packed-rock-wool supply and original delivery freight without substituting glass wool."""

import hashlib
import json
import pickle
import sqlite3
from pathlib import Path

try:
    from review_uvek_market_compositions import composition_decision, validate_composition_bindings
    from review_uvek_whole_freight import (
        apply_explicit_reverse,
        explicit_reverse_decision,
        explicit_reverse_rule,
        inventory_record,
        project_explicit_reverse,
    )
except ModuleNotFoundError:
    from scripts.review_uvek_market_compositions import composition_decision, validate_composition_bindings
    from scripts.review_uvek_whole_freight import (
        apply_explicit_reverse,
        explicit_reverse_decision,
        explicit_reverse_rule,
        inventory_record,
        project_explicit_reverse,
    )

FIELDS = ("name", "reference product", "location", "unit")
ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_packed_wool_review.json"
APPROVAL = json.loads((ROOT / REVIEW_PATH).read_text())
REVIEW_SHA256 = "58559a16566bb6bd515b3d86b99fe338f518410e98c9025a60575ca05c384a59"
SOURCE = ("market for stone wool, packed", "stone wool, packed", "GLO", "kilogram")
TARGET = ("Rock wool, packed, at plant", "Rock wool, packed, at plant", "CH", "kilogram")
BARE = ("Rock wool, at plant", "Rock wool, at plant", "CH", "kilogram")
EVIDENCE_SCOPE = {}
for row in APPROVAL["inventories"]:
    EVIDENCE_SCOPE.setdefault(tuple(row["label"].split("-")), set()).add(tuple(row["source"][f] for f in FIELDS))
LIMITATIONS = (
    "Reviewed complete packed stone/rock-wool supply per kg. The former global-market target was glass "
    "wool; use the independently inspected complete CH packed-rock-wool factory for each original CH/RoW "
    "producer share. Retain both shares separately and every original road, light-commercial, rail and "
    "barge delivery-freight coefficient. Both packed factories consume one kg of their own unpacked "
    "stone/rock wool and the inspected LDPE packing quantity. Their packed/unpacked inventories split "
    "manufacture between linked modules: use the complete packed factory, not unpacked wool alone, "
    "and add no second wool, packaging or factory process. The described 100 kg/m3 is a packing assumption, "
    "not a volume, thermal-performance or payload conversion. Positive one-kg product references require "
    "factor +1, preserving caller signed kg, uncertainty, formulas, foreground suppliers and separate "
    "emissions. Producer geography, vintage, energy, materials, binder, infrastructure, allocation and "
    "insulation/LCIA equivalence remain unestablished. Freight uses separately reviewed complete matching "
    "vehicle classes and modes; country/fuel/load/vintage/duty mixes differ. Retain all market delivery "
    "freight without normalization or another delivery leg. Internal market uncertainty/formulas are "
    "not reconstructed. This is a nonempty packed mineral-wool product, not an allocation-only stock. "
    "Only nine exact inspected cut-off profiles are approved; no consequential or missing-profile "
    "fallback is permitted. The independently reviewed reverse native at-plant product maps to the "
    "Swiss packed factory, not this delivered global market; no inverse market reconstruction is inferred."
)


def identity(row):
    return tuple(row[f] for f in FIELDS)


def collect_extra_inventories(brightway_root):
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
                raise ValueError("Packed-wool original patch coverage changed; repeat the scientific review.")
    return result


def validate_inventories(inventories, observations, native):
    (rule,) = APPROVAL["rules"]
    expected = {
        json.dumps({**r, "source": list(identity(r["source"]))}, sort_keys=True) for r in APPROVAL["inventories"]
    }
    actual = {json.dumps({**r, "source": list(s)}, sort_keys=True) for s, rows in inventories.items() for r in rows}
    if (
        hashlib.sha256(json.dumps(APPROVAL, sort_keys=True).encode()).hexdigest() != REVIEW_SHA256
        or APPROVAL["authorized_system_model"] != "cutoff"
        or identity(rule["source"]) != SOURCE
        or APPROVAL["source_profile_count"] != 9
        or actual != expected
        or len(expected) != 47
        or sum(map(len, inventories.values())) != 47
    ):
        raise ValueError("Packed-wool inventory or review scope changed; repeat the scientific review.")
    observed = {
        (label, sign) for label, sign in observations["ecoinvent"].get(SOURCE, set()) if label.endswith("-cutoff")
    }
    if observed != {(p["label"], 1) for p in rule["profiles"]}:
        raise ValueError("Packed-wool positive kg reference changed; repeat the scientific review.")
    for role in APPROVAL["native_roles"]:
        source = identity(role["source"])
        if (
            source not in (TARGET, BARE)
            or observations["uvek"].get(source) != {("uvek-2025-cutoff", 1)}
            or any(
                native.get(source, {}).get(f) != role[f]
                for f in ("native_flow_type", "native_reference_is_input", "flow_property_name", "unit_name")
            )
        ):
            raise ValueError("Packed-wool native material role changed; repeat the scientific review.")


def validate_bindings(resource):
    validate_composition_bindings(
        resource, approval={"rules": APPROVAL["rules"], "recipe_dependencies": {}}, label="Packed-wool"
    )


def forward_decision(rule, observations, inventories, binding, original_review):
    return composition_decision(
        rule,
        {(label, sign) for label, sign in observations if label.endswith("-cutoff")},
        [(r["label"], r["inventory_sha256"]) for r in inventories],
        binding,
        original_review,
        approval_row=APPROVAL["rules"][0],
        rationale=LIMITATIONS,
        review_path=REVIEW_PATH,
        namespace="packed-wool-market",
        mapping_method="reviewed_complete_packed_wool_market",
        profile_reason="Select an exact inspected complete packed-wool supply profile; no material or delivery-freight fallback is approved.",
    )


REVERSE_METHOD = "reviewed_complete_packed_wool_reverse"
REVERSE_LIMITATIONS = (
    "Independently inspected complete packed-rock-wool factory screening proxy: preserve native CH "
    "at-plant packed product per one kg using ecoinvent 3.12 cut-off CH stone-wool packed production. "
    "Both exact positive-reference inventories consume one kg of their own unpacked wool and the "
    "matched LDPE packing quantity. Their linked packed/unpacked modules include the split manufacturing "
    "boundary. Do not use a glass-wool process, a bare-wool process alone or the delivered global market, "
    "add market freight, duplicate packaging, normalize density or convert thermal performance. "
    "Energy, raw materials, binder, assets, vintage, background and LCIA/insulation equivalence differ "
    "and remain unestablished. Factor +1 preserves signed kg, uncertainty, formulas, foreground suppliers "
    "and separate emissions. This direction is independently inspected, not an inversion of the "
    "two-producer forward market. Consequential conversion remains unsupported."
)


def reverse_addition():
    row = APPROVAL["reverse"]
    if identity(row["source"]) != TARGET or identity(row["target"]) != (
        "stone wool production, packed",
        "stone wool, packed",
        "CH",
        "kilogram",
    ):
        raise ValueError("Independent packed-wool reverse boundary changed; repeat the scientific review.")
    proofs = {(r["label"], identity(r["source"])): r for r in APPROVAL["inventories"]}
    for label, source, field in (
        ("uvek-2025-cutoff", TARGET, "source_inventory_sha256"),
        ("ecoinvent-3.12-cutoff", identity(row["target"]), "target_inventory_sha256"),
    ):
        proof = proofs[label, source]
        if proof["reference_amount"] != 1 or proof["inventory_sha256"] != row[field]:
            raise ValueError("Independent packed-wool reverse inventory changed; repeat the scientific review.")
    return explicit_reverse_rule(row, REVIEW_PATH, REVERSE_METHOD, REVERSE_LIMITATIONS)


def legacy_reverse_projection(resource):
    rule, reviews = reverse_addition()
    return project_explicit_reverse(resource, rule, reviews, REVERSE_METHOD)


def apply_reverse(resource, review_rows, sources, targets):
    rule, reviews = reverse_addition()
    apply_explicit_reverse(resource, review_rows, sources, targets, rule, reviews, REVERSE_METHOD)


def reverse_decision(rule, observations, uvek_signs, native, binding):
    return explicit_reverse_decision(rule, observations, uvek_signs, native, binding, reverse_addition()[0])
