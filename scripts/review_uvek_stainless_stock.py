"""Match hot-rolled stainless stock to material, rather than a rolling service."""

import hashlib
import json
from copy import deepcopy
from pathlib import Path

FIELDS = ("name", "reference product", "location", "unit")
REVIEW_PATH = "brightpath/data/export/uvek_stainless_stock_review.json"
APPROVAL = json.loads((Path(__file__).resolve().parents[1] / REVIEW_PATH).read_text())
SOURCES = {tuple(row["source"][field] for field in FIELDS): row for row in APPROVAL["rules"]}
TARGET_NAME = "Chromium steel 18/8, at plant"
TARGET = dict(zip(FIELDS, (TARGET_NAME, TARGET_NAME, "RER", "kilogram"), strict=True))
EXPECTED = {
    (name, "steel, chromium steel 18/8, hot rolled", location, "kilogram")
    for name, location in (
        ("market for steel, chromium steel 18/8, hot rolled", "GLO"),
        ("steel production, chromium steel 18/8, hot rolled", "RoW"),
    )
}
if (
    set(SOURCES) != EXPECTED
    or APPROVAL["authorized_system_model"] != "cutoff"
    or APPROVAL["source_count"] != 2
    or APPROVAL["source_profile_count"] != 14
    or APPROVAL["inventory_count"] != 23
    or any(row["target"] != TARGET for row in SOURCES.values())
):
    raise ValueError("Hot-rolled stainless-stock review scope changed; repeat the scientific review.")
LIMITATIONS = (
    "Reviewed cut-off hot-rolled 18/8 stainless material proxy. Source markets supply the inspected "
    "hot-rolled RER/RoW producers; each producer uses one kg of 18/8 stock and one kg of hot-rolling "
    "service. UVEK's RER material includes 0.63 kg converter stock, 0.37 kg electric stock and one kg "
    "of Hot rolling, steel per kg product. This is the full hot-rolled material, not the former "
    "Sheet rolling, chromium steel cold-rolling service, whose 0.0516 kg stock input covers rolling "
    "losses. Preserve incoming signed kg with factor +1; no separate rolling input is added. "
    "The RER average stock replaces source GLO/RoW supply, freight, production and technology mixes; "
    "geographic, vintage and background differences remain, and equal LCIA scores are not established. "
    "Preserve caller uncertainty, formulas, foreground suppliers and separate emissions. No treatment, "
    "recovery credit, omission, allocation reconstruction or inverse is inferred. Cross-system-model "
    "conversion remains unsupported; the existing reverse material rule is checked independently."
)
EVIDENCE_SCOPE = {}
for row in APPROVAL["inventories"]:
    profile = tuple(row["label"].split("-"))
    if profile[2] != "cutoff":
        raise ValueError("Stainless-stock evidence must remain cut-off only.")
    EVIDENCE_SCOPE.setdefault(profile, set()).add(tuple(row["source"][f] for f in FIELDS))


def inventory_record(source, blobs, comment, product_information):
    """Retain sparse fingerprints of inventories and their functional descriptions."""
    return {
        "source": list(source),
        "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
        "activity_comment_sha256": hashlib.sha256(str(comment or "").encode()).hexdigest(),
        "product_information_sha256": hashlib.sha256(str(product_information or "").encode()).hexdigest(),
    }


def validate_inventories(inventories, observations, native):
    """Bind all source producers, the market, stock and former rolling service."""
    expected = {
        (
            row["label"],
            tuple(row["source"][f] for f in FIELDS),
            *(row[k] for k in ("inventory_sha256", "activity_comment_sha256", "product_information_sha256")),
        )
        for row in APPROVAL["inventories"]
    }
    actual = {
        (
            row["label"],
            source,
            *(row[k] for k in ("inventory_sha256", "activity_comment_sha256", "product_information_sha256")),
        )
        for source, rows in inventories.items()
        for row in rows
    }
    if actual != expected or len(expected) != 23 or sum(map(len, inventories.values())) != 23:
        raise ValueError("Stainless-stock inventory or description changed; repeat the scientific review.")
    for profile, sources in EVIDENCE_SCOPE.items():
        family, _version, _model = profile
        for source in sources:
            observed = observations[family].get(source, set())
            if ("-".join(profile), 1) not in observed:
                raise ValueError("Stainless-stock evidence reference changed; repeat the scientific review.")
            if family == "uvek":
                metadata = native.get(source, {})
                if (
                    observed != {("uvek-2025-cutoff", 1)}
                    or metadata.get("native_flow_type") != "PRODUCT_FLOW"
                    or metadata.get("native_reference_is_input") is not False
                ):
                    raise ValueError("Stainless-stock or rolling native role changed; repeat the scientific review.")
    for source, row in SOURCES.items():
        if {
            (label, sign) for label, sign in observations["ecoinvent"].get(source, set()) if label.endswith("-cutoff")
        } != {(p["label"], 1) for p in row["profiles"]}:
            raise ValueError("Stainless-stock source profile reference changed; repeat the scientific review.")


def approved_stock_rule(rule):
    """Project only the two reviewed material-for-service corrections."""
    row = SOURCES.get(tuple(rule["source"][f] for f in FIELDS))
    if row is None:
        return rule
    if rule["target"] not in (row["previous_target"], TARGET) or rule.get("conversion_factor", 1) != 1:
        raise ValueError("Stainless-stock target or kg basis changed; repeat the scientific review.")
    return {**rule, "target": deepcopy(TARGET)}


def stock_decision(rule, observations, uvek_signs, native, binding, previous, original_review):
    """Keep positive material coordinates and disclose the geographic stock proxy."""
    row = SOURCES[tuple(rule["source"][f] for f in FIELDS)]
    if (
        rule["target"] != TARGET
        or rule.get("conversion_factor", 1) != 1
        or {(label, sign) for label, sign in observations if label.endswith("-cutoff")}
        != {(p["label"], 1) for p in row["profiles"]}
        or uvek_signs != {1}
        or native.get("native_flow_type") != "PRODUCT_FLOW"
        or native.get("native_reference_is_input") is not False
    ):
        raise ValueError("Stainless-stock correspondence changed; repeat the scientific review.")
    review = {
        "rationale": "Auto-approved under standing independently inspected matching-material, function, kg and geographic-proxy approvals. "
        + LIMITATIONS,
        "limitations": [LIMITATIONS],
        "evidence": [REVIEW_PATH],
    }
    return {
        "binding": binding,
        "original_factor": 1,
        "factor": 1,
        "source_observations": [list(pair) for pair in sorted(observations)],
        "uvek_reference_sign": 1,
        "uvek_native_flow_type": "PRODUCT_FLOW",
        "original_target": deepcopy(row["previous_target"]),
        "original_review": deepcopy(previous.get("original_review", original_review)),
        "original_information_loss": previous.get(
            "original_information_loss", rule.get("technosphere_information_loss")
        ),
        "supplier_proxy_review": review,
        "review": {
            "status": "verified",
            "source_sign": 1,
            "target_sign": 1,
            "reason": LIMITATIONS,
            "evidence": [REVIEW_PATH],
        },
    }
