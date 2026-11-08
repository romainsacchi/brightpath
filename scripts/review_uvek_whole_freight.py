"""Review complete freight classes and preserve signed tonne-kilometres through their parent DAG."""

import hashlib
import json
import pickle
import sqlite3
from copy import deepcopy
from pathlib import Path

from brightpath import TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.migrations.schema import intern_review, rule_identifier

FIELDS = ("name", "reference product", "location", "unit")
ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_whole_freight_review.json"
APPROVAL = json.loads((ROOT / REVIEW_PATH).read_text())
REVIEW_SHA256 = "73751d418e15b987569c2df7dae8f1662124845fa97affa4d09e8c732215754d"
SOURCES = {tuple(r["source"][f] for f in FIELDS): r for r in APPROVAL["rules"]}
EXPECTED_KINDS = {
    (f"market group for {product}", product, "GLO", "ton kilometer"): kind
    for product, kind in (
        ("transport, freight train", "rail"),
        ("transport, freight, inland waterways, barge", "barge"),
        ("transport, freight, light commercial vehicle", "light_road"),
        ("transport, freight, light commercial vehicle, fleet average", "light_road"),
        ("transport, freight, lorry, diesel, unspecified", "heavy_road"),
        ("transport, freight, lorry, unspecified", "heavy_road"),
    )
}
VERSIONS = {"3.6", "3.7", "3.8", "3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12"}
TARGETS = {
    "light_road": ("Transport, freight, lorry, 3.5t gross weight, fleet average", "CH"),
    "heavy_road": ("Transport, freight, lorry, fleet average", "RER"),
    "rail": ("Transport, freight, rail", "RER"),
    "barge": ("Transport, barge", "RER"),
}
LIMITATIONS = (
    "Reviewed complete freight screening proxy per tonne-kilometre. Preserve the supplied freight mode "
    "and vehicle class: light-commercial freight uses the complete CH 3.5t light-duty fleet, heavy "
    "unspecified diesel freight uses the complete RER heavy-lorry diesel fleet across weight classes "
    "and delivery cycles, and generic rail and inland-barge freight use the complete RER services. "
    "Do not replace all heavy freight by a 3.5t vehicle or a single 18t long-haul vehicle, replace "
    "light-commercial freight by the heavy fleet, or replace full rail transport by train operation. "
    "The inspected target services include vehicle production, maintenance, disposal and road/track "
    "infrastructure through their linked inventories; add no second vehicle, operation or infrastructure. "
    "Positive source and target one-tkm references require factor +1: preserve signed quantities, "
    "uncertainty, formulas, foreground suppliers and separate emissions without absolute values, payload "
    "normalization, passenger-km conversion or kilometre scaling. These are whole-fleet geographic "
    "proxies: source country, vehicle, load, fuel, duty, age and emissions mixtures are not reconstructed. "
    "The CH light-duty mix includes diesel, hybrid, gas and battery-electric vehicles and replaces a "
    "source generic petrol/diesel light-commercial fleet. The RER heavy-diesel class weights use Swiss "
    "2020 shares. Generic global rail supply can include steam; its steam/diesel/electricity shares "
    "are approximated by the RER fleet. No individual steam-transport correspondence is created. "
    "Vintage, technology, allocation, assets, background and LCIA equivalence remain unestablished. "
    "Only enumerated independently inspected cut-off profiles are approved; unknown profiles and "
    "consequential routes remain unsupported. Reverse correspondences require independent inspection."
)
EVIDENCE_SCOPE = {}
for row in APPROVAL["inventories"]:
    EVIDENCE_SCOPE.setdefault(tuple(row["label"].split("-")), set()).add(tuple(row["source"][f] for f in FIELDS))


def identity(row):
    return tuple(row[f] for f in FIELDS)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def inventory_record(source, blobs, comment, product_information):
    """Pin complete source, native lifecycle and parent inventories without publishing their vectors."""
    rows = [pickle.loads(b) for b in blobs]
    refs = [r for r in rows if r["type"] == "production"]
    if (
        len(refs) != 1
        or isinstance(refs[0]["amount"], bool)
        or refs[0]["amount"] not in (-1, 1)
        or tuple(refs[0]["input"]) != tuple(refs[0]["output"])
    ):
        raise ValueError("Whole-freight unit reference changed; repeat the scientific review.")
    return {
        "source": list(source),
        "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
        "activity_comment_sha256": hashlib.sha256(str(comment or "").encode()).hexdigest(),
        "product_information_sha256": hashlib.sha256(str(product_information or "").encode()).hexdigest(),
        "reference_amount": int(refs[0]["amount"]),
        "total_exchanges": len(rows),
    }


def collect_extra_inventories(brightway_root):
    """Read the exact two inspected patch databases without profile inheritance."""
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
                raise ValueError("Whole-freight original patch scope changed; repeat the scientific review.")
    return result


def validate_inventories(inventories, observations, native):
    """Require the independently inspected fleet, vehicle lifecycle and entire parent closure."""
    if (
        digest(APPROVAL) != REVIEW_SHA256
        or APPROVAL["authorized_system_model"] != "cutoff"
        or {s: row["kind"] for s, row in SOURCES.items()} != EXPECTED_KINDS
        or sum(len(r["profiles"]) for r in SOURCES.values()) != 32
        or APPROVAL["inventory_count"] != 2111
    ):
        raise ValueError("Whole-freight approval scope changed; repeat the scientific review.")
    expected = {
        json.dumps({**r, "source": list(identity(r["source"]))}, sort_keys=True) for r in APPROVAL["inventories"]
    }
    actual = {json.dumps({**r, "source": list(s)}, sort_keys=True) for s, rows in inventories.items() for r in rows}
    if actual != expected or len(expected) != 2111 or sum(map(len, inventories.values())) != 2111:
        raise ValueError("Whole-freight inventory or metadata changed; repeat the scientific review.")
    for source, row in SOURCES.items():
        name, location = TARGETS[row["kind"]]
        if row["target"] != dict(zip(FIELDS, (name, name, location, "ton kilometer"), strict=True)):
            raise ValueError("Whole-freight mode/class changed; repeat the scientific review.")
        observed = {
            (label, sign) for label, sign in observations["ecoinvent"].get(source, set()) if label.endswith("-cutoff")
        }
        if observed != {(p["label"], 1) for p in row["profiles"]}:
            raise ValueError("Whole-freight positive tkm reference scope changed; repeat the scientific review.")
    for role in APPROVAL["native_roles"]:
        source = identity(role["source"])
        if observations["uvek"].get(source) != {("uvek-2025-cutoff", 1)} or any(
            native.get(source, {}).get(f) != role[f]
            for f in ("native_flow_type", "native_reference_is_input", "flow_property_name", "unit_name")
        ):
            raise ValueError("Whole-freight native tkm product role changed; repeat the scientific review.")


def validate_bindings(resource):
    """Freeze the fully propagated cut-off DAG; reject a partially published primitive correction."""
    rows = APPROVAL.get("parent_rule_bindings", [])
    if len(rows) != 687 or APPROVAL.get("publication_status") != "ready":
        raise ValueError("Whole-freight parent propagation is incomplete; repeat the scientific review.")
    selected = {}
    for row in rows:
        label = row["label"]
        if label not in selected:
            chosen = select_profile_sign_rules(resource, TechnosphereProfile(*label.split("-")))
            selected[label] = {identity(r["source"]): r for r in chosen["replace"]}
        current = selected[label].get(identity(row["source"]))
        if digest(current) != row["binding_sha256"]:
            # A later source-bound stock review changes only this parent's material
            # input. Its replacement rule, physical recipe and empty-stock body
            # have independent exact guards; all other freight parents stay frozen.
            try:
                from review_uvek_metalliferous_stock import accepts_revised_parent_binding
            except ModuleNotFoundError:
                from scripts.review_uvek_metalliferous_stock import accepts_revised_parent_binding
            if not accepts_revised_parent_binding(resource, row, current):
                raise ValueError("Whole-freight parent binding changed; repeat the scientific review.")


def approved_freight_rule(rule):
    """Project only the exact reviewed whole-function identity without changing its tkm factor."""
    row = SOURCES.get(identity(rule["source"]))
    if row is None:
        return rule
    name, location = TARGETS[EXPECTED_KINDS[identity(rule["source"])]]
    if row["kind"] != EXPECTED_KINDS[identity(rule["source"])] or row["target"] != dict(
        zip(FIELDS, (name, name, location, "ton kilometer"), strict=True)
    ):
        raise ValueError("Whole-freight mode/class changed; repeat the scientific review.")
    if rule["target"] not in (row["previous_target"], row["target"]) or rule.get("conversion_factor", 1) != 1:
        raise ValueError("Whole-freight target or tkm factor changed; repeat the scientific review.")
    return {**rule, "target": deepcopy(row["target"])}


def forward_decision(rule, observations, uvek_signs, native, binding, previous, original_review):
    """Select only the exact inspected positive product profiles; never infer an inverse or a payload factor."""
    row = SOURCES[identity(rule["source"])]
    observed = {(label, sign) for label, sign in observations if label.endswith("-cutoff")}
    if (
        rule["target"] != row["target"]
        or rule.get("conversion_factor", 1) != 1
        or observed != {(p["label"], 1) for p in row["profiles"]}
        or uvek_signs != {1}
        or native.get("native_flow_type") != "PRODUCT_FLOW"
        or native.get("native_reference_is_input") is not False
    ):
        raise ValueError("Whole-freight correspondence changed; repeat the scientific review.")
    evidence = [REVIEW_PATH]
    return {
        "binding": binding,
        "original_target": deepcopy(row["previous_target"]),
        "original_factor": 1,
        "factor": 1,
        "source_observations": [list(p) for p in sorted(observations)],
        "uvek_reference_sign": 1,
        "uvek_native_flow_type": "PRODUCT_FLOW",
        "original_review": deepcopy(previous.get("original_review", original_review)),
        "original_information_loss": previous.get(
            "original_information_loss", rule.get("technosphere_information_loss")
        ),
        "supplier_proxy_review": {
            "rationale": "Auto-approved under independently inspected matching whole-function/tkm and geographic service-proxy authorization. "
            + LIMITATIONS,
            "limitations": [LIMITATIONS],
            "evidence": evidence,
        },
        "review": {
            "status": "unresolved",
            "reason": "Select only an exact independently inspected whole-freight source profile.",
            "evidence": evidence,
            "profiles": [
                {
                    "source_profile": dict(
                        zip(("family", "version", "system_model"), p["label"].split("-"), strict=True)
                    ),
                    "conversion_factor": 1,
                    "review": {
                        "status": "verified",
                        "source_sign": 1,
                        "target_sign": 1,
                        "reason": LIMITATIONS,
                        "evidence": [*evidence, f"{REVIEW_PATH}#{p['label']}/{p['inventory_sha256']}"],
                    },
                }
                for p in row["profiles"]
            ],
        },
    }


def explicit_reverse_rule(row, review_path, method, limitations):
    """Author only an independently inspected equal-unit positive product correspondence."""
    if (
        row["source"]["unit"] != row["target"]["unit"]
        or isinstance(row["conversion_factor"], bool)
        or row["conversion_factor"] != 1
    ):
        raise ValueError("Independent reverse reference basis changed; repeat the scientific review.")
    review = {
        "rationale": limitations,
        "limitations": [limitations],
        "evidence": [review_path + "#independent-reverse"],
    }
    reviews = {}
    reference = intern_review({"reviews": reviews}, review)
    return {
        "id": rule_identifier("uvek-2025-to-ecoinvent-3.12-cutoff", row["source"]),
        "source": deepcopy(row["source"]),
        "target": deepcopy(row["target"]),
        "conversion_factor": 1,
        "classification": "proxy",
        "confidence": 1,
        "mapping_method": method,
        "review_ref": reference,
        "technosphere_information_loss": limitations,
        "reference_flow_sign_review": {
            "status": "verified",
            "source_sign": 1,
            "target_sign": 1,
            "reason": limitations,
            "evidence": review["evidence"],
        },
    }, reviews


def project_explicit_reverse(resource, expected, reviews, method):
    """Validate an independent addition before removing it for frozen legacy comparisons."""
    present = []
    for rule in resource["replace"]:
        if rule["source"] != expected["source"]:
            if rule.get("mapping_method") == method:
                raise ValueError("Unreviewed independent reverse source; repeat the scientific review.")
            continue
        if rule != expected or resource["reviews"].get(rule["review_ref"]) != reviews[expected["review_ref"]]:
            raise ValueError("Independent reverse correspondence changed; repeat the scientific review.")
        present.append(rule)
    if len(present) > 1:
        raise ValueError("Duplicate independent reverse correspondence.")
    result = deepcopy(resource)
    if present:
        result["replace"] = [r for r in result["replace"] if r["id"] != expected["id"]]
        if not any(r.get("review_ref") == expected["review_ref"] for r in result["replace"]):
            result["reviews"].pop(expected["review_ref"])
        result["coverage"]["mapped_identities"] -= 1
        result["coverage"]["unresolved_identities"] += 1
        result["coverage"]["dispositions"]["proxy"] -= 1
        result["coverage"]["dispositions"]["unresolved"] += 1
    return result


def apply_explicit_reverse(resource, review_rows, sources, targets, expected, reviews, method):
    """Add the separately reviewed direction only when both exact catalogs contain its endpoints."""
    from collections import Counter

    project_explicit_reverse(resource, expected, reviews, method)
    if identity(expected["source"]) not in sources or identity(expected["target"]) not in targets:
        raise ValueError("Independent reverse endpoint missing from its exact catalog.")
    if not any(r["id"] == expected["id"] for r in resource["replace"]):
        resource["replace"].append(deepcopy(expected))
    resource["replace"].sort(key=lambda r: identity(r["source"]))
    resource["reviews"].update(reviews)
    for i, row in enumerate(review_rows):
        if row["source"] == expected["source"]:
            review_rows[i] = {"source": deepcopy(expected["source"]), "rule_id": expected["id"], "disposition": "proxy"}
    resource["coverage"].update(
        mapped_identities=len(resource["replace"]) + len(resource["delete"]),
        unresolved_identities=len(sources) - len(resource["replace"]) - len(resource["delete"]),
        dispositions=dict(Counter(r["disposition"] for r in review_rows)),
    )


REVERSE_METHOD = "reviewed_complete_light_commercial_reverse"
REVERSE_LIMITATIONS = (
    "Independently inspected CH light-commercial freight screening proxy per one tkm. Match the complete "
    "UVEK 3.5t light-duty fleet to the complete ecoinvent 3.12 cut-off Swiss light-commercial fleet market. "
    "Both exact inventories have positive one-tkm references. Preserve signed demand with factor +1, "
    "uncertainty, formulas, foreground suppliers and separate emissions; do not use a heavy-lorry fleet, "
    "passenger-km, vehicle-km, vehicle lifetime or payload conversion. Vehicle production, maintenance, "
    "disposal and roads remain in the selected complete service. Native diesel/hybrid/gas/battery-electric "
    "shares differ from the target petrol/diesel light-commercial mix and vehicle assumptions. Vintage, "
    "load, energy, emissions, assets and LCIA equivalence are not established. No individual fuel-specific "
    "operator or original country/technology share is reconstructed. This direction is independently "
    "inspected, not an inverse of the global-to-Swiss forward proxy; consequential routes remain unsupported."
)


def light_reverse_addition():
    row = APPROVAL["light_road_reverse"]
    name, location = TARGETS["light_road"]
    expected_source = (name, name, location, "ton kilometer")
    expected_target = (
        "market for transport, freight, light commercial vehicle, fleet average",
        "transport, freight, light commercial vehicle, fleet average",
        "CH",
        "ton kilometer",
    )
    if identity(row["source"]) != expected_source or identity(row["target"]) != expected_target:
        raise ValueError("Independent reverse light-duty class changed; repeat the scientific review.")
    proofs = {(r["label"], identity(r["source"])): r for r in APPROVAL["inventories"]}
    for label, source, field in (
        ("uvek-2025-cutoff", expected_source, "source_inventory_sha256"),
        ("ecoinvent-3.12-cutoff", expected_target, "target_inventory_sha256"),
    ):
        proof = proofs[label, source]
        if proof["reference_amount"] != 1 or proof["inventory_sha256"] != row[field]:
            raise ValueError("Independent reverse light-duty inventory changed; repeat the scientific review.")
    return explicit_reverse_rule(row, REVIEW_PATH, REVERSE_METHOD, REVERSE_LIMITATIONS)


def legacy_reverse_projection(resource):
    rule, reviews = light_reverse_addition()
    return project_explicit_reverse(resource, rule, reviews, REVERSE_METHOD)


def apply_reverse(resource, review_rows, sources, targets):
    rule, reviews = light_reverse_addition()
    apply_explicit_reverse(resource, review_rows, sources, targets, rule, reviews, REVERSE_METHOD)


def explicit_reverse_decision(rule, observations, uvek_signs, native, binding, expected):
    """Keep an independently authored positive reverse pairing replayable in the evidence ledger."""
    if (
        rule != expected
        or observations != {("ecoinvent-3.12-cutoff", 1)}
        or uvek_signs != {1}
        or native.get("native_flow_type") != "PRODUCT_FLOW"
        or native.get("native_reference_is_input") is not False
    ):
        raise ValueError("Independent reverse inventory/reference changed; repeat the scientific review.")
    return {
        "binding": binding,
        "original_factor": 1,
        "factor": 1,
        "source_observations": [["ecoinvent-3.12-cutoff", 1]],
        "uvek_reference_sign": 1,
        "uvek_native_flow_type": "PRODUCT_FLOW",
        "review": deepcopy(expected["reference_flow_sign_review"]),
    }


def reverse_decision(rule, observations, uvek_signs, native, binding):
    return explicit_reverse_decision(rule, observations, uvek_signs, native, binding, light_reverse_addition()[0])
