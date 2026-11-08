"""Match inspected sewer infrastructure by capacity class and physical length."""

import hashlib
import json
from copy import deepcopy
from pathlib import Path

FIELDS = ("name", "reference product", "location", "unit")
REVIEW_PATH = "brightpath/data/export/uvek_sewer_grid_review.json"
APPROVAL = json.loads((Path(__file__).resolve().parents[1] / REVIEW_PATH).read_text())
SOURCES = {tuple(row["source"][field] for field in FIELDS): row for row in APPROVAL["rules"]}
CAPACITY_CLASSES = {
    "1.6E8l/year, 6 km": (5, 806, 6.13),
    "1E9l/year, 30 km": (4, 5321, 30.3),
    "5E9l/year, 110 km": (3, 24864, 109.4),
    "1.1E10l/year, 242 km": (2, 71133, 242),
    "4.7E10l/year, 583 km": (1, 233225, 583),
}
expected_sources = {
    (name + capacity, "sewer grid, " + capacity, location, "kilometer")
    for capacity in CAPACITY_CLASSES
    for name, location in (
        ("market for sewer grid, ", "GLO"),
        ("sewer grid construction, ", "CH"),
        ("sewer grid construction, ", "RoW"),
    )
}
if set(SOURCES) != expected_sources or APPROVAL["authorized_system_model"] != "cutoff":
    raise ValueError("Sewer-grid identity or system-model scope changed; repeat the scientific review.")
for source, row in SOURCES.items():
    capacity = source[1].removeprefix("sewer grid, ")
    number, pce, length = CAPACITY_CLASSES[capacity]
    name = f"Sewer grid, class {number}"
    if (row["capacity_class"], row["person_equivalents"], row["network_length_km"]) != (number, pce, length) or row[
        "target"
    ] != dict(zip(FIELDS, (name, name, "CH", "kilometer"), strict=True)):
        raise ValueError("Sewer-grid capacity correspondence changed; repeat the scientific review.")
LIMITATIONS = (
    "Reviewed cut-off sewer-infrastructure proxy: match the documented wastewater-plant capacity class, "
    "person equivalents, network length and 100-year lifetime including renovation after 70 years. "
    "Both source and target supply one kilometre of physical sewer infrastructure with positive reference "
    "production. Preserve incoming length and sign with factor +1; do not scale by the total network length "
    "or by the annual wastewater throughput. Source title lengths can be rounded; the inspected descriptions "
    "establish the matching classes. This is construction infrastructure, not wastewater treatment. "
    "UVEK's Swiss construction, materials, energy, end-of-life, water and emission inventories replace those "
    "in ecoinvent. Source markets and RoW construction use the CH proxy; geographic supply mixes are not "
    "reconstructed. Technology, vintage, background and LCIA equivalence are not established. Preserve "
    "caller uncertainty, formulas, foreground suppliers and separate emissions. No extra infrastructure "
    "or sewage-treatment demand is added, and no volumetric conversion or omission is approved. "
    "Cross-system-model conversion remains unsupported; reverse correspondences are inspected independently."
)
EVIDENCE_SCOPE = {}
for row in SOURCES.values():
    for profile in row["profiles"]:
        key = tuple(profile["label"].split("-"))
        assert key[2] == "cutoff"
        EVIDENCE_SCOPE.setdefault(key, set()).add(tuple(row["source"][f] for f in FIELDS))
    EVIDENCE_SCOPE.setdefault(("uvek", "2025", "cutoff"), set()).add(tuple(row["target"][f] for f in FIELDS))
if (
    len(SOURCES) != APPROVAL["source_count"]
    or sum(len(row["profiles"]) for row in SOURCES.values()) != APPROVAL["source_profile_count"]
):
    raise ValueError("Sewer-grid review coverage changed.")


def inventory_record(source, blobs, comment, product_information):
    """Retain fingerprints only, without publishing licensed inventories."""
    return {
        "source": list(source),
        "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
        "activity_comment_sha256": hashlib.sha256(str(comment or "").encode()).hexdigest(),
        "product_information_sha256": hashlib.sha256(str(product_information or "").encode()).hexdigest(),
    }


def validate_inventories(inventories, observations, native):
    """Bind every inspected source profile and each physical UVEK grid."""
    expected = {
        (
            row["label"],
            tuple(row["source"][f] for f in FIELDS),
            row["inventory_sha256"],
            row["activity_comment_sha256"],
            row["product_information_sha256"],
        )
        for row in APPROVAL["inventories"]
    }
    actual = {
        (
            row["label"],
            source,
            row["inventory_sha256"],
            row["activity_comment_sha256"],
            row["product_information_sha256"],
        )
        for source, records in inventories.items()
        for row in records
    }
    if (
        actual != expected
        or len(expected) != APPROVAL["inventory_count"]
        or sum(len(rows) for rows in inventories.values()) != len(expected)
    ):
        raise ValueError("Sewer-grid inventory or metadata changed; repeat the scientific review.")
    for source, row in SOURCES.items():
        cf = {
            (label, sign) for label, sign in observations["ecoinvent"].get(source, set()) if label.endswith("-cutoff")
        }
        if cf != {(p["label"], 1) for p in row["profiles"]}:
            raise ValueError("Sewer-grid source reference changed; repeat the scientific review.")
        target = tuple(row["target"][f] for f in FIELDS)
        metadata = native.get(target, {})
        if (
            observations["uvek"].get(target) != {("uvek-2025-cutoff", 1)}
            or metadata.get("native_flow_type") != "PRODUCT_FLOW"
            or metadata.get("native_reference_is_input") is not False
        ):
            raise ValueError("Sewer-grid target reference or native role changed; repeat the scientific review.")


def approved_grid_rule(rule):
    """Project a reviewed class correction while rejecting unrelated target drift."""
    row = SOURCES.get(tuple(rule["source"][f] for f in FIELDS))
    if row is None:
        return rule
    if rule["target"] not in (row["previous_target"], row["target"]) or rule.get("conversion_factor", 1) != 1:
        raise ValueError("Sewer-grid target or length factor changed; repeat the scientific review.")
    return {**rule, "target": deepcopy(row["target"])}


def grid_decision(rule, observations, uvek_signs, native, binding, previous, original_review):
    """Retain existing positive coordinates and disclose the construction proxy."""
    row = SOURCES[tuple(rule["source"][f] for f in FIELDS)]
    cf = {(label, sign) for label, sign in observations if label.endswith("-cutoff")}
    if (
        rule["target"] != row["target"]
        or rule.get("conversion_factor", 1) != 1
        or cf != {(p["label"], 1) for p in row["profiles"]}
        or uvek_signs != {1}
        or native.get("native_flow_type") != "PRODUCT_FLOW"
        or native.get("native_reference_is_input") is not False
    ):
        raise ValueError("Sewer-grid correspondence changed; repeat the scientific review.")
    review = {
        "rationale": "Auto-approved under the standing independently inspected matching-function, unit and geographic-proxy authorization. "
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
