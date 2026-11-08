"""Independently reviewed modern sweet-gas flaring with an exact physical reference conversion."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review, rule_identifier

FIELDS = ("name", "reference product", "location", "unit")
REVIEW_PATH = "brightpath/data/export/uvek_natural_gas_flare_review.json"
APPROVAL = json.loads((Path(__file__).resolve().parents[1] / REVIEW_PATH).read_text())
TARGET = tuple(APPROVAL["target"]["target"][f] for f in FIELDS)
REVERSE = APPROVAL["reverse"]
REVERSE_METHOD = "reviewed_natural_gas_flare_reverse"
VOLUME_PER_MJ = APPROVAL["volume_per_mj"]
LIMITATIONS = (
    "Independently inspected modern sweet-natural-gas production-flaring proxy: one source MJ represents "
    "1/36 m3 of the UVEK flare. Apply factor -1/36, including the reference-coordinate inversion; do not "
    "replace flaring with a gas turbine or purchased-fuel production. Both inventories follow the modern "
    "Meili-2021 approach: flaring includes resource withdrawal. Modern source comments assign CH4, NOx "
    "and SO2 to main production activities; neither inspected flare contains these flows. Full parent "
    "production inventories are not claimed equivalent. The source withdraws 1/36 standard m3 of natural "
    "gas per MJ; UVEK withdraws "
    "one standard m3 and emits 36 MJ waste heat per reference m3. Neither flare has technosphere suppliers, "
    "infrastructure, fuel-production inputs or recovered-energy credits. The exact market supplies -1 MJ "
    "of its corresponding flare without other active exchanges. UVEK replaces seven/eight modern source "
    "biosphere exchanges with nine: emission coefficients and pollutant coverage differ; source oxygen "
    "balancing is absent from UVEK, and UVEK adds nitrous oxide and waste heat. CO2 and NMVOC values also "
    "differ slightly. Norwegian observations, global extrapolation, time and volume-reference conventions "
    "are screening approximations; LCIA equivalence is not established or quantified. Preserve caller "
    "uncertainty, formulas, foreground suppliers, production and separate emissions; assess overlaps with "
    "separately represented production emissions independently. Only exact inspected ecoinvent 3.9--3.12 "
    "profiles are approved. Older sweet-gas profiles retain CH4/NOx in the flare and remain pending; all "
    "sour-gas profiles, patch versions and uninspected contexts remain outside this approval."
)
REVERSE_LIMITATIONS = (
    "Independently inspected reverse sweet-natural-gas production-flaring proxy: one UVEK m3 corresponds "
    "to 36 MJ of ecoinvent 3.12 cut-off flaring. Apply factor -36 to convert the positive UVEK service "
    "reference to the negative ecoinvent waste-treatment reference; this is neither fuel production nor "
    "a reversed guess from a forward rule. Both exact inventories retain one standard m3 of gas-resource "
    "withdrawal per source m3. Modern ecoinvent comments assign CH4, NOx and SO2 to main production; "
    "neither inspected flare contains these flows. Full parent production inventories are not claimed "
    "equivalent. Neither has "
    "technosphere suppliers, flare infrastructure or energy credits. Ecoinvent substitutes eight biosphere "
    "exchanges for UVEK's nine: it adds oxygen balancing and does not retain UVEK's nitrous oxide or waste "
    "heat; CO2/NMVOC coefficients, observation periods and geographic extrapolations differ. Standard-volume "
    "representation and LCIA equivalence are not independently reconstructed or quantified. Preserve caller "
    "signed quantities, supported uncertainty, formulas, foreground suppliers, production and separate "
    "emissions without taking absolute values. Older sweet-gas and all sour-gas references remain excluded."
)
expected_labels = {
    f"ecoinvent-{v}-{m}"
    for v in ("3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12")
    for m in ("cutoff", "consequential")
}
for row in APPROVAL["rules"]:
    if {p["label"] for p in row["profiles"]} != expected_labels or any(
        (p["status"] == "verified") != (p["label"].split("-")[1] in {"3.9", "3.10", "3.11", "3.12"})
        or p["status"] not in {"verified", "unresolved"}
        for p in row["profiles"]
    ):
        raise ValueError("Natural-gas flare approved/pending boundaries changed; repeat the scientific review.")
if (
    sum(p["status"] == "verified" for row in APPROVAL["rules"] for p in row["profiles"])
    != APPROVAL["approved_source_profile_count"]
    or APPROVAL["approved_source_profile_count"] != 16
    or APPROVAL["pending_source_profile_count"] != 12
):
    raise ValueError("Natural-gas flare approved/pending profile counts changed.")
FLARES = {}
for row in APPROVAL["rules"]:
    source = tuple(row["source"][f] for f in FIELDS)
    if source in FLARES or source[1] != "waste natural gas, sweet" or source[3] != "megajoule":
        raise ValueError("Duplicate or unsupported sweet-gas flare scope.")
    FLARES[source] = {
        "target": TARGET,
        "previous_target": tuple(row["previous_target"][f] for f in FIELDS),
        "source_versions": ("3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12"),
        "native_product_service": True,
        "reference_function": "sweet-natural-gas production-flaring service",
        "rationale": (
            "Auto-approved under the independently inspected matching-material/function/reference-basis "
            "service-proxy authorization. Modern source comments and the independently inspected UVEK "
            "flare support the matching operating boundary; their gas-resource and energy references "
            "independently establish 36 MJ/m3. " + LIMITATIONS
        ),
        "limitations": LIMITATIONS,
        "inventory_evidence": [
            f"{REVIEW_PATH}: 28 exact source inventory and comment fingerprints, including 16 approved modern "
            "and 12 explicitly pending older profiles; independent UVEK reference and boundary inspection."
        ],
    }
if (
    len(FLARES) != APPROVAL["source_count"]
    or sum(len(r["profiles"]) for r in APPROVAL["rules"]) != APPROVAL["source_profile_count"]
    or VOLUME_PER_MJ != 1 / 36
    or APPROVAL["target"]["gas_resource_m3"] != 1
    or APPROVAL["target"]["waste_heat_mj"] != 36
    or APPROVAL["target"]["native_flow_type"] != "PRODUCT_FLOW"
    or APPROVAL["target"]["native_reference_is_input"] is not False
    or REVERSE["conversion_factor"] != -36
):
    raise ValueError("Natural-gas flare scope or physical reference basis changed.")

EVIDENCE_SCOPE = {
    **{
        ("ecoinvent", version, model): set(FLARES)
        for version in FLARES[next(iter(FLARES))]["source_versions"]
        for model in ("cutoff", "consequential")
    },
    ("uvek", "2025", "cutoff"): {TARGET},
}


def inventory_record(source, blobs, comment):
    """Bind trusted local inventory bytes and documented emission boundaries."""
    return {
        "source": list(source),
        "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
        "activity_comment_sha256": hashlib.sha256(str(comment or "").encode()).hexdigest(),
    }


def validate_inventories(inventories):
    """Reject missing, changed or additional inspected source/target inventories."""
    for row in APPROVAL["rules"]:
        source = tuple(row["source"][f] for f in FIELDS)
        expected = sorted((p["label"], p["inventory_sha256"], p["activity_comment_sha256"]) for p in row["profiles"])
        actual = sorted(
            (p["label"], p["inventory_sha256"], p["activity_comment_sha256"]) for p in inventories.get(source, [])
        )
        if actual != expected:
            raise ValueError("Natural-gas flare source inventory/boundary changed; repeat the independent review.")
    target = APPROVAL["target"]
    if inventories.get(TARGET) != [
        {
            "label": "uvek-2025-cutoff",
            **{k: target[k] for k in ("inventory_sha256", "activity_comment_sha256")},
            "source": list(TARGET),
        }
    ]:
        raise ValueError("Natural-gas flare UVEK inventory/boundary changed; repeat the independent review.")


def forward_profiles(source, observations, inventories, evidence):
    """Approve only the modern boundary and keep every older counterpart explicit."""
    validate_inventories(inventories)
    row = next(r for r in APPROVAL["rules"] if tuple(r["source"][f] for f in FIELDS) == source)
    if sorted(observations) != [(p["label"], -1) for p in sorted(row["profiles"], key=lambda p: p["label"])]:
        raise ValueError("Natural-gas flare reference/profile coverage changed; repeat the review.")
    entries = []
    for p in row["profiles"]:
        approved = p["status"] == "verified"
        entry = {
            "source_profile": dict(zip(("family", "version", "system_model"), p["label"].split("-"), strict=True)),
            "review": {
                "status": "verified" if approved else "unresolved",
                "source_sign": -1,
                "target_sign": 1,
                "reason": (
                    LIMITATIONS
                    if approved
                    else "Older sweet-gas flares retain methane and nitrogen oxides inside this activity. No omission or transfer of those emissions to main production has been approved."
                ),
                "evidence": [
                    *evidence,
                    f"{REVIEW_PATH}#{p['label']}/{p['inventory_sha256']}/{p['activity_comment_sha256']}",
                ],
            },
        }
        if approved:
            entry["conversion_factor"] = -VOLUME_PER_MJ
        entries.append(entry)
    return entries


def reverse_reference_review():
    return {
        "status": "verified",
        "source_sign": 1,
        "target_sign": -1,
        "reason": REVERSE_LIMITATIONS,
        "evidence": [REVIEW_PATH],
    }


def reverse_rule():
    review = {"rationale": REVERSE_LIMITATIONS, "limitations": [REVERSE_LIMITATIONS], "evidence": [REVIEW_PATH]}
    rule = {
        "id": rule_identifier("uvek-2025-to-ecoinvent-3.12-cutoff", REVERSE["source"]),
        "source": deepcopy(REVERSE["source"]),
        "target": deepcopy(REVERSE["target"]),
        "conversion_factor": -36,
        "confidence": 1,
        "classification": "proxy",
        "mapping_method": REVERSE_METHOD,
        "technosphere_information_loss": REVERSE_LIMITATIONS,
        "review_ref": intern_review({}, review),
        "reference_flow_sign_review": reverse_reference_review(),
    }
    return rule, review


def reverse_decision(rule, observations, inventories, native_row, uvek_signs, binding):
    """Independently require the exact reverse volume, waste reference and modern boundary."""
    validate_inventories(inventories)
    expected, _ = reverse_rule()
    if (
        rule != expected
        or observations != {("ecoinvent-3.12-cutoff", -1)}
        or uvek_signs != {1}
        or native_row["native_flow_type"] != "PRODUCT_FLOW"
        or native_row["native_reference_is_input"] is not False
    ):
        raise ValueError("Reverse natural-gas flare correspondence changed; repeat the independent review.")
    return {
        "binding": binding,
        "original_factor": -36,
        "factor": -36,
        "source_observations": [list(r) for r in sorted(observations)],
        "uvek_reference_sign": 1,
        "uvek_native_flow_type": "PRODUCT_FLOW",
        "review": reverse_reference_review(),
    }


def apply_reverse_flare(resource, review_rows, sources, targets):
    """Publish the independently reviewed reverse service without inferring any inverse."""
    expected, review = reverse_rule()
    source = tuple(expected["source"][f] for f in FIELDS)
    target = tuple(expected["target"][f] for f in FIELDS)
    if source not in sources or target not in targets:
        raise ValueError("Reverse natural-gas flare endpoint catalog changed.")
    existing = next((r for r in resource["replace"] if r["source"] == expected["source"]), None)
    if existing is not None and existing != expected:
        raise ValueError("Conflicting reverse natural-gas flare rule.")
    if existing is None:
        resource["replace"].append(expected)
        resource["replace"].sort(key=lambda r: tuple(r["source"][f] for f in FIELDS))
    assert intern_review(resource, review) == expected["review_ref"]
    rows = [r for r in review_rows if r["source"] == expected["source"]]
    if len(rows) != 1:
        raise ValueError("Reverse natural-gas flare catalog review coverage changed.")
    rows[0].clear()
    rows[0].update(source=deepcopy(expected["source"]), disposition="proxy", rule_id=expected["id"])
    resource["coverage"].update(
        mapped_identities=len(resource["replace"]) + len(resource["delete"]),
        unresolved_identities=len(sources) - len(resource["replace"]) - len(resource["delete"]),
        dispositions=dict(Counter(r["disposition"] for r in review_rows)),
    )


def legacy_projection(resource):
    """Validate and remove this later batch only for the frozen publisher comparison."""
    expected, review = reverse_rule()
    existing = next((r for r in resource["replace"] if r["source"] == expected["source"]), None)
    if existing is None:
        return resource
    if existing != expected or resource["reviews"].get(expected["review_ref"]) != review:
        raise ValueError("Stale independently reviewed natural-gas flare reverse rule.")
    projected = deepcopy(resource)
    projected["replace"] = [r for r in projected["replace"] if r["id"] != expected["id"]]
    projected["reviews"].pop(expected["review_ref"])
    coverage = projected["coverage"]
    coverage["mapped_identities"] -= 1
    coverage["unresolved_identities"] += 1
    coverage["dispositions"]["proxy"] -= 1
    coverage["dispositions"]["unresolved"] += 1
    return projected
