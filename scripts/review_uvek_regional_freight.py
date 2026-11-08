"""Review regional heavy-road markets and retain the exact Swiss rail traction mix."""

import hashlib
import json
from copy import deepcopy
from pathlib import Path

try:
    from review_uvek_market_compositions import composition_decision
    from review_uvek_whole_freight import inventory_record as inventory_record
except ModuleNotFoundError:
    from scripts.review_uvek_market_compositions import composition_decision
    from scripts.review_uvek_whole_freight import inventory_record as inventory_record

from brightpath import TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules

FIELDS = ("name", "reference product", "location", "unit")
ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_regional_freight_review.json"
APPROVAL = json.loads((ROOT / REVIEW_PATH).read_text())
REVIEW_SHA256 = "0b87f5c21e22193c8e204139db83df3dffafa7214c4d7de8746d099717b24f04"
LIMITATIONS = APPROVAL["limitations"]
SOURCES = {tuple(r["source"][f] for f in FIELDS): r for r in APPROVAL["rules"]}
EXPECTED_SOURCES = {
    (
        "market for transport, freight, lorry, unspecified",
        "transport, freight, lorry, diesel, unspecified",
        loc,
        "ton kilometer",
    )
    for loc in ("BR", "ZA", "RoW", "RER")
} | {
    (
        "market for transport, freight, train, fleet average",
        "transport, freight, train, fleet average",
        "CH",
        "ton kilometer",
    )
}
EVIDENCE_SCOPE = {}
for row in APPROVAL["inventories"]:
    EVIDENCE_SCOPE.setdefault(tuple(row["label"].split("-")), set()).add(tuple(row["source"][f] for f in FIELDS))


def identity(row):
    return tuple(row[f] for f in FIELDS)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def validate_review():
    """Require independently inspected unit services, never a similar name or inherited profile."""
    if (
        digest(APPROVAL) != REVIEW_SHA256
        or set(SOURCES) != EXPECTED_SOURCES
        or APPROVAL["authorized_system_model"] != "cutoff"
        or APPROVAL["inventory_count"] != 470
        or APPROVAL["propagation"]["parent_profile_count"] != 424
    ):
        raise ValueError("Regional-freight approval changed; repeat the scientific review.")
    for source, row in SOURCES.items():
        if {p["label"] for p in row["profiles"]} != {"ecoinvent-3.11-cutoff", "ecoinvent-3.12-cutoff"} or len(
            row["profiles"]
        ) != 2:
            raise ValueError("Regional-freight exact profile scope changed; repeat the scientific review.")
        if source[2] == "CH":
            for profile in row["profiles"]:
                inputs = profile["inputs"]
                expected = {
                    "Transport, freight, rail, electricity with shunting": 0.958767540376505,
                    "Transport, freight, rail, diesel, with particle filter": 0.0412324596234946,
                }
                if (
                    len(inputs) != 2
                    or {e["name"]: e["amount"] for e in inputs} != expected
                    or any(
                        e["location"] != "CH"
                        or e["unit"] != "ton kilometer"
                        or e["type"] != "technosphere"
                        or e["reference product"] != e["name"]
                        for e in inputs
                    )
                ):
                    raise ValueError("Swiss rail traction coefficients changed; repeat the scientific review.")
        elif row["target"] != dict(
            zip(
                FIELDS,
                (
                    "Transport, freight, lorry, fleet average",
                    "Transport, freight, lorry, fleet average",
                    "RER",
                    "ton kilometer",
                ),
                strict=True,
            )
        ):
            raise ValueError("Regional heavy-road class changed; repeat the scientific review.")


def approved_freight_rule(rule):
    """Correct only the enumerated heavy-road primitive; rail is a separate exact-profile recipe."""
    row = SOURCES.get(identity(rule["source"]))
    if row is None or row["kind"] == "rail":
        return rule
    validate_review()
    if rule["target"] not in (row["previous_target"], row["target"]) or rule.get("conversion_factor", 1) != 1:
        raise ValueError("Regional-road target or tonne-kilometre factor changed; repeat the scientific review.")
    return {**rule, "target": deepcopy(row["target"])}


def validate_inventories(inventories, observations, native):
    """Pin all complete parent/source/native vectors and positive native product roles."""
    validate_review()
    expected = {
        json.dumps({**r, "source": list(identity(r["source"]))}, sort_keys=True) for r in APPROVAL["inventories"]
    }
    actual = {json.dumps({**r, "source": list(s)}, sort_keys=True) for s, rows in inventories.items() for r in rows}
    if actual != expected or len(expected) != 470 or sum(map(len, inventories.values())) != 470:
        raise ValueError("Regional-freight full inventory changed; repeat the scientific review.")
    for source, row in SOURCES.items():
        observed = {
            (label, sign) for label, sign in observations["ecoinvent"].get(source, set()) if label.endswith("-cutoff")
        }
        if observed != {(p["label"], 1) for p in row["profiles"]}:
            raise ValueError("Regional-freight positive source reference changed; repeat the scientific review.")
    for role in APPROVAL["native_roles"]:
        source = identity(role["source"])
        if observations["uvek"].get(source) != {("uvek-2025-cutoff", 1)} or any(
            native.get(source, {}).get(f) != role[f]
            for f in ("native_flow_type", "native_reference_is_input", "flow_property_name", "unit_name")
        ):
            raise ValueError("Regional-freight native tkm product role changed; repeat the scientific review.")


def validate_bindings(resource):
    """Refuse partial propagation or silently altered independently scoped custom services."""
    validate_review()
    # Digester material markets now use the independently reviewed allocation
    # stock and preserve these exact freight suppliers under their new author.
    try:
        from review_uvek_digester_stock import SOURCES as superseded
    except ModuleNotFoundError:
        from scripts.review_uvek_digester_stock import SOURCES as superseded

    by_label = {}
    for row in APPROVAL["parent_rule_bindings"]:
        if identity(row["source"]) in superseded:
            continue
        label = row["label"]
        if label not in by_label:
            chosen = select_profile_sign_rules(resource, TechnosphereProfile(*label.split("-")))
            by_label[label] = {identity(r["source"]): r for r in chosen["replace"]}
        if digest(by_label[label].get(identity(row["source"]))) != row["binding_sha256"]:
            raise ValueError("Regional-freight parent propagation changed; repeat the scientific review.")
    for row in APPROVAL["parent_profiles"]:
        if identity(row["source"]) in superseded:
            continue
        if digest(resource["recipes"].get(row["target_recipe"])) != row["target_sha256"]:
            raise ValueError("Regional-freight parent recipe body changed; repeat the scientific review.")
    for rid, expected in APPROVAL["protected_custom_recipe_bindings"].items():
        if digest(resource["recipes"].get(rid)) != expected:
            raise ValueError("Independently reviewed freight vector changed; repeat the scientific review.")


def forward_decision(rule, observations, inventories, binding, original_review, previous=None):
    """Keep exact signed tkm through whole road services or the two-input Swiss rail helper."""
    validate_review()
    previous = previous or {}
    source = identity(rule["source"])
    row = SOURCES[source]
    observed = {(label, sign) for label, sign in observations if label.endswith("-cutoff")}
    if observed != {(p["label"], 1) for p in row["profiles"]}:
        raise ValueError("Regional-freight source profile changed; repeat the scientific review.")
    if row["kind"] == "rail":
        return composition_decision(
            rule,
            observed,
            [(r["label"], r["inventory_sha256"]) for r in inventories],
            binding,
            original_review,
            approval_row=row,
            rationale=LIMITATIONS,
            review_path=REVIEW_PATH,
            namespace="regional-freight-rail",
            mapping_method="reviewed_regional_freight_rail_mix",
            profile_reason="Only the two independently inspected Swiss rail-market profiles are approved; no route or profile fallback.",
        )
    if rule["target"] != row["target"] or rule.get("conversion_factor", 1) != 1:
        raise ValueError("Regional-road source or target changed; repeat the scientific review.")
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
        "supplier_proxy_review": {"rationale": LIMITATIONS, "limitations": [LIMITATIONS], "evidence": [REVIEW_PATH]},
        "review": {
            "status": "unresolved",
            "reason": "Select only an exact inspected regional heavy-road profile.",
            "evidence": [REVIEW_PATH],
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
                        "evidence": [REVIEW_PATH, f"{REVIEW_PATH}#{p['label']}/{p['inventory_sha256']}"],
                    },
                }
                for p in row["profiles"]
            ],
        },
    }


def collect_extra_inventories(brightway_root):
    """No patch-profile inheritance: this cohort is confined to two core releases."""
    return []
