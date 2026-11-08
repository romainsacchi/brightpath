"""Independent same-region ash-disposal correspondences and reference-sign checks."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

FIELDS = ("name", "reference product", "location", "unit")
REVIEW_PATH = "brightpath/data/export/uvek_reverse_ash_treatment_review.json"
APPROVAL = json.loads((Path(__file__).resolve().parents[1] / REVIEW_PATH).read_text())
METHOD = "reviewed_ash_treatment_reverse"
EVIDENCE_SCOPE = {
    ("uvek", "2025", "cutoff"): {tuple(r["source"][f] for f in FIELDS) for r in APPROVAL["rules"]},
    ("ecoinvent", "3.12", "cutoff"): {tuple(r["target"][f] for f in FIELDS) for r in APPROVAL["rules"]},
}
LIMITATIONS = (
    "Independently reviewed reverse ash-treatment proxy: preserve one kg of dry hard-coal ash sent to "
    "residual-material landfill, or dry lignite ash sent to opencast refill, using the matching ecoinvent "
    "3.12 cut-off service for the same region and route. UVEK has positive Brightway production with a "
    "native waste-input reference; ecoinvent has negative waste-treatment production. Apply factor -1 to "
    "the caller exchange and its uncertainty/formula, without taking an absolute value. The regional "
    "endpoints agree, but underlying Swiss infrastructure/background supply in UVEK, disposal controls, "
    "ash composition, metal/leachate releases and long-term fate models may differ. UVEK specifies 0% "
    "water; the inspected ecoinvent target documentation records H2O as unavailable, so identical moisture "
    "is not established. Preserve the kg quantity without moisture normalization. No LCIA equivalence "
    "or moisture-based rescaling is claimed. Preserve foreground suppliers, production, separately listed "
    "emissions and complete original-exchange change records. This is independently inspected service "
    "evidence, not a reversal of a similarity candidate or forward mapping."
)


def identity(record):
    return tuple(record.get(f) for f in FIELDS)


def additions():
    rules, reviews = [], {}
    review = {
        "rationale": LIMITATIONS,
        "evidence": [
            f"{REVIEW_PATH}: independent source/target fingerprints, native waste role, reference signs, regions and disposal boundaries."
        ],
    }
    review_ref = intern_review({"reviews": reviews}, review)
    for row in APPROVAL["rules"]:
        if (
            (row["source_reference_sign"], row["target_reference_sign"], row["conversion_factor"]) != (1, -1, -1)
            or row["source_native_flow_type"] != "WASTE_FLOW"
            or row["source_native_reference_is_input"] is not True
            or row["source"]["location"] != row["target"]["location"]
            or row["source"]["unit"] != "kilogram"
            or row["target"]["unit"] != "kilogram"
        ):
            raise ValueError("Reverse ash-treatment reference basis changed; repeat the independent review.")
        digest = hashlib.sha256(json.dumps(identity(row["source"])).encode()).hexdigest()[:16]
        rules.append(
            {
                "id": f"uvek-ei312-{digest}",
                "source": deepcopy(row["source"]),
                "target": deepcopy(row["target"]),
                "conversion_factor": -1,
                "classification": "proxy",
                "mapping_method": METHOD,
                "review_ref": review_ref,
            }
        )
    return rules, reviews


def legacy_ash_projection(resource):
    """Validate this whole review batch before excluding it from older baseline checks."""
    rules, reviews = additions()
    expected = {identity(r["source"]): r for r in rules}
    present = set()
    for rule in resource["replace"]:
        key = identity(rule["source"])
        if key not in expected:
            if rule.get("mapping_method") == METHOD:
                raise ValueError("Unapproved reverse ash source outside the independently reviewed scope.")
            continue
        normalized = {k: v for k, v in rule.items() if k != "reference_flow_sign_review"}
        if normalized != expected[key] or resource["reviews"].get(rule["review_ref"]) != reviews[rule["review_ref"]]:
            raise ValueError("Reviewed reverse ash correspondence changed; repeat the independent review.")
        present.add(key)
    if present and present != expected.keys():
        raise ValueError("Reviewed reverse ash batch is incomplete.")
    return {**resource, "replace": [r for r in resource["replace"] if identity(r["source"]) not in present]}


def apply_reverse_ash(resource, review, source_identities, target_identities):
    legacy_ash_projection(resource)
    rules, reviews = additions()
    if any(
        identity(r["source"]) not in source_identities or identity(r["target"]) not in target_identities for r in rules
    ):
        raise ValueError("Reviewed reverse ash endpoint is missing from the exact catalog.")
    keys = {identity(r["source"]) for r in rules}
    resource["replace"] = [r for r in resource["replace"] if identity(r["source"]) not in keys] + deepcopy(rules)
    resource["replace"].sort(key=lambda r: identity(r["source"]))
    resource["reviews"].update(reviews)
    updates = {
        identity(r["source"]): {"source": r["source"], "rule_id": r["id"], "disposition": "proxy"} for r in rules
    }
    review[:] = [deepcopy(updates.get(identity(r["source"]), r)) for r in review]
    resource["coverage"].update(
        mapped_identities=len(resource["replace"]) + len(resource["delete"]),
        unresolved_identities=len(source_identities) - len(resource["replace"]) - len(resource["delete"]),
        dispositions=dict(Counter(r["disposition"] for r in review)),
    )


def validate_reverse_ash_inventories(inventories):
    for row in APPROVAL["rules"]:
        if inventories.get(identity(row["source"])) != [
            ("uvek-2025-cutoff", row["source_inventory_sha256"])
        ] or inventories.get(identity(row["target"])) != [("ecoinvent-3.12-cutoff", row["target_inventory_sha256"])]:
            raise ValueError("Reverse ash source/target inventory changed; repeat the independent review.")
