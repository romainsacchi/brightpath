"""Physical cut-off coproducts and markets; retain separate consequential nutrient omissions."""

import hashlib
import json
from collections import defaultdict
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

try:
    from review_uvek_market_compositions import composition_decision, validate_composition_bindings
except ModuleNotFoundError:
    from scripts.review_uvek_market_compositions import composition_decision, validate_composition_bindings

FIELDS = ("name", "reference product", "location", "unit")
REVIEW_PATH = "brightpath/data/export/uvek_cutoff_feed_product_review.json"
APPROVAL = json.loads((Path(__file__).resolve().parents[1] / REVIEW_PATH).read_text())
PRODUCTS = {tuple(r["source"][f] for f in FIELDS): r for r in APPROVAL["rules"]}
MARKETS = {tuple(r["source"][f] for f in FIELDS): r for r in APPROVAL["markets"]}
EVIDENCE_SCOPE = defaultdict(set)
for row in APPROVAL["inventories"]:
    EVIDENCE_SCOPE[tuple(row["label"].split("-"))].add(tuple(row["source"][f] for f in FIELDS))
if (
    len(PRODUCTS) != APPROVAL["source_count"]
    or len(PRODUCTS) != 16
    or len(MARKETS) != APPROVAL["market_count"]
    or len(MARKETS) != 5
    or len(APPROVAL["inventories"]) != APPROVAL["inventory_count"]
    or APPROVAL["inventory_count"] != 176
    or len(APPROVAL["targets"]) != 8
):
    raise ValueError("Cut-off coproduct review scope changed; repeat the independent review.")


INFO_IDENTITIES = {
    tuple(row["source"][f] for f in FIELDS) for row in APPROVAL["inventories"] if "product_information_sha256" in row
}


def inventory_record(source, blobs, comment, product_information=None):
    record = {
        "source": list(source),
        "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
        "activity_comment_sha256": hashlib.sha256(str(comment or "").encode()).hexdigest(),
    }
    if tuple(source) in INFO_IDENTITIES:
        record["product_information_sha256"] = hashlib.sha256(str(product_information or "").encode()).hexdigest()
    return record


def validate_inventories(inventories, observations, native):
    expected = defaultdict(list)
    for row in APPROVAL["inventories"]:
        source = tuple(row["source"][f] for f in FIELDS)
        expected[source].append(
            (
                row["label"],
                row["inventory_sha256"],
                row["activity_comment_sha256"],
                row.get("product_information_sha256"),
            )
        )
    actual = {
        key: sorted(
            (r["label"], r["inventory_sha256"], r["activity_comment_sha256"], r.get("product_information_sha256"))
            for r in rows
        )
        for key, rows in inventories.items()
    }
    if actual != {key: sorted(rows) for key, rows in expected.items()}:
        raise ValueError("Cut-off coproduct inventory or boundary comment changed; repeat the review.")
    for row in APPROVAL["targets"]:
        target = tuple(row["target"][f] for f in FIELDS)
        entry = native.get(target, {})
        if (
            observations["uvek"].get(target) != {("uvek-2025-cutoff", 1)}
            or entry.get("native_flow_type") != "PRODUCT_FLOW"
            or entry.get("native_reference_is_input") is not False
        ):
            raise ValueError("Cut-off coproduct target is not the inspected positive product output.")


def product_review(row):
    return {
        "rationale": (
            "Standing independently inspected matching-product and physical-quantity approval: "
            f"{row['source']['name']} / {row['source']['reference product']} / {row['source']['location']}. "
            + row.get("limitations", APPROVAL["limitations"])
        ),
        "limitations": [row.get("limitations", APPROVAL["limitations"])],
        "evidence": [REVIEW_PATH],
    }


def reference_review(row):
    return {
        "status": "unresolved",
        "reason": "Select only an independently inspected exact cut-off physical-product profile.",
        "evidence": [REVIEW_PATH],
        "profiles": [
            {
                "source_profile": dict(zip(("family", "version", "system_model"), p["label"].split("-"), strict=True)),
                "conversion_factor": 1,
                "review": {
                    "status": "verified",
                    "source_sign": 1,
                    "target_sign": 1,
                    "reason": row.get("limitations", APPROVAL["limitations"]),
                    "evidence": [f"{REVIEW_PATH}#{p['label']}/{p['inventory_sha256']}/{p['activity_comment_sha256']}"],
                },
            }
            for p in row["profiles"]
        ],
    }


def compiled_product_rule(row):
    return {
        **deepcopy(row["original_rule"]),
        "target": deepcopy(row["target"]),
        "review_ref": intern_review({}, product_review(row)),
        "technosphere_information_loss": row.get("limitations", APPROVAL["limitations"]),
        "reference_flow_sign_review": reference_review(row),
    }


def approved_product_rule(rule):
    row = PRODUCTS.get(tuple(rule["source"][f] for f in FIELDS))
    if row is None:
        return rule
    if rule not in (row["original_rule"], compiled_product_rule(row)):
        raise ValueError("Cut-off coproduct supplier metadata changed; repeat the independent review.")
    return {**rule, "target": deepcopy(row["target"])}


def product_decision(rule, observations, binding):
    row = PRODUCTS[tuple(rule["source"][f] for f in FIELDS)]
    if (
        sorted(observations) != [(p["label"], 1) for p in row["profiles"]]
        or rule["target"] != row["target"]
        or rule.get("conversion_factor", 1) != 1
    ):
        raise ValueError("Cut-off coproduct source references changed; repeat the independent review.")
    return {
        "binding": binding,
        "original_target": deepcopy(row["previous_target"]),
        "original_factor": 1,
        "factor": 1,
        "source_observations": [list(r) for r in sorted(observations)],
        "uvek_reference_sign": 1,
        "uvek_native_flow_type": "PRODUCT_FLOW",
        "original_review": deepcopy(row["original_review"]),
        "original_information_loss": row["original_rule"].get("technosphere_information_loss"),
        "supplier_proxy_review": product_review(row),
        "review": reference_review(row),
    }


def validate_market_bindings(resource):
    projected = deepcopy(resource)
    for rule in projected["replace"]:
        source = tuple(rule["source"][f] for f in FIELDS)
        if source in PRODUCTS:
            approved_product_rule(rule)
            rule.clear()
            rule.update(compiled_product_rule(PRODUCTS[source]))
            intern_review(projected, product_review(PRODUCTS[source]))
    validate_composition_bindings(
        projected,
        approval={"rules": list(MARKETS.values()), "recipe_dependencies": {}},
        label="Physical cut-off coproduct market",
    )


def market_decision(rule, observations, inventories, binding, original_review, omission_review):
    source = tuple(rule["source"][f] for f in FIELDS)
    row = MARKETS[source]
    decision = composition_decision(
        rule,
        observations,
        [(r["label"], r["inventory_sha256"]) for r in inventories],
        binding,
        original_review,
        approval_row=row,
        rationale=row["review_rationale"],
        review_path=REVIEW_PATH,
        namespace="cutoff-feed-market",
        mapping_method="reviewed_cutoff_feed_market_recipe",
        profile_reason="Physical cut-off supply and consequential nutritional omissions have separate exact-profile approvals.",
    )
    omissions = {
        tuple(e["source_profile"][k] for k in ("family", "version", "system_model")): e
        for e in row["original_rule"]["reference_flow_sign_review"]["profiles"]
        if e.get("operation") == "delete"
    }
    if len(omissions) != 7:
        raise ValueError("Existing consequential nutrient omissions changed; repeat the omission review.")
    for index, entry in enumerate(decision["review"]["profiles"]):
        profile = entry["source_profile"]
        if profile["system_model"] != "consequential":
            continue
        key = tuple(profile[k] for k in ("family", "version", "system_model"))
        original = omissions[key]
        if original["review_ref"] != intern_review({}, omission_review):
            raise ValueError("Existing consequential nutrient omission evidence changed.")
        decision["review"]["profiles"][index] = deepcopy(original)
    decision["profile_omission_review"] = deepcopy(omission_review)
    return decision


REVERSE_METHOD = "reviewed_cutoff_feed_product_reverse"
REVERSE_PRODUCTS = {tuple(r["source"][f] for f in FIELDS): r for r in APPROVAL["reverse_rules"]}
REVERSE_LIMITATIONS = (
    "Independently inspected reverse physical-coproduct screening proxy: preserve one kg and its incoming "
    "sign through the corresponding ecoinvent 3.12 cut-off allocated DDGS feedstock, grass-fibre process or "
    "sweet-sorghum grain product. Both exact inventories have single positive one-kg product references. "
    "Five correspondences retain the named region; Swiss potato DDGS uses the inspected RoW potato-DDGS "
    "process because the catalog has no Swiss supplier. Economic allocation, agricultural inputs, drying "
    "and process energy, technology, infrastructure, freight, water/emissions and background models differ. "
    "UVEK DDGS is stated per wet-basis kg; an identical moisture fraction to ecoinvent's 90% dry matter is "
    "not independently quantified. Do not invent moisture, nutrient or market-mix reconstruction. Sorghum "
    "grain documents 9.1% moisture on both sides. No exact nutritional, insulation or LCIA equivalence is "
    "established. These explicit directional selections were independently inspected; they do not invert "
    "the many-to-one forward supplier map, reconstruct original market mixes or undo nutritional omissions. "
    "Preserve caller signed quantities, supported uncertainty/formulas, foreground suppliers and separate "
    "emissions. Only UVEK 2025 to ecoinvent 3.12 cut-off is authored; UVEK 2026 uses its explicit release bridge."
)
if len(REVERSE_PRODUCTS) != APPROVAL["reverse_count"] or APPROVAL["reverse_count"] != 8:
    raise ValueError("Independent reverse coproduct scope changed; repeat the review.")


def reverse_additions():
    from brightpath.migrations.schema import rule_identifier

    rules, reviews = [], {}
    for row in APPROVAL["reverse_rules"]:
        rationale = row.get("limitations", REVERSE_LIMITATIONS) + " " + row["source"]["name"] + ": " + row["geography"]
        review = {
            "rationale": rationale,
            "limitations": [rationale],
            "evidence": [REVIEW_PATH + "#independent-reverse"],
        }
        reference = intern_review({"reviews": reviews}, review)
        rules.append(
            {
                "id": rule_identifier("uvek-2025-to-ecoinvent-3.12-cutoff", row["source"]),
                "source": deepcopy(row["source"]),
                "target": deepcopy(row["target"]),
                "conversion_factor": 1,
                "classification": "proxy",
                "confidence": 1,
                "mapping_method": REVERSE_METHOD,
                "review_ref": reference,
                "technosphere_information_loss": rationale,
                "reference_flow_sign_review": {
                    "status": "verified",
                    "source_sign": 1,
                    "target_sign": 1,
                    "reason": rationale,
                    "evidence": review["evidence"],
                },
            }
        )
    return rules, reviews


def reverse_decision(rule, observations, uvek_signs, native_row, binding):
    expected = next(r for r in reverse_additions()[0] if r["source"] == rule["source"])
    row = REVERSE_PRODUCTS[tuple(rule["source"][f] for f in FIELDS)]
    source_evidence = next(
        r for r in APPROVAL["inventories"] if r["source"] == row["source"] and r["label"] == "uvek-2025-cutoff"
    )
    target_evidence = next(
        r for r in APPROVAL["inventories"] if r["source"] == row["target"] and r["label"] == "ecoinvent-3.12-cutoff"
    )
    if (
        rule != expected
        or observations != {("ecoinvent-3.12-cutoff", 1)}
        or uvek_signs != {1}
        or native_row.get("native_flow_type") != "PRODUCT_FLOW"
        or native_row.get("native_reference_is_input") is not False
        or row["conversion_factor"] != 1
        or row["source_sign"] != 1
        or row["target_sign"] != 1
        or source_evidence["inventory_sha256"] != row["source_inventory_sha256"]
        or target_evidence["inventory_sha256"] != row["target_inventory_sha256"]
        or source_evidence["activity_comment_sha256"] != row["source_comment_sha256"]
        or target_evidence["activity_comment_sha256"] != row["target_comment_sha256"]
    ):
        raise ValueError("Independent reverse coproduct inventory/reference changed; repeat the review.")
    return {
        "binding": binding,
        "original_factor": 1,
        "factor": 1,
        "source_observations": [["ecoinvent-3.12-cutoff", 1]],
        "uvek_reference_sign": 1,
        "uvek_native_flow_type": "PRODUCT_FLOW",
        "review": deepcopy(expected["reference_flow_sign_review"]),
    }


def legacy_reverse_projection(resource):
    expected_rules, reviews = reverse_additions()
    expected = {tuple(r["source"][f] for f in FIELDS): r for r in expected_rules}
    present = [r for r in resource["replace"] if tuple(r["source"][f] for f in FIELDS) in expected]
    if not present:
        return resource
    cumulative, allowed = set(), set()
    for batch in APPROVAL["reverse_batches"]:
        cumulative.update(tuple(source[f] for f in FIELDS) for source in batch)
        allowed.add(frozenset(cumulative))
    present_sources = frozenset(tuple(r["source"][f] for f in FIELDS) for r in present)
    present_reviews = {r["review_ref"] for r in present}
    if (
        cumulative != expected.keys()
        or present_sources not in allowed
        or any(r != expected[tuple(r["source"][f] for f in FIELDS)] for r in present)
        or any(resource["reviews"].get(k) != reviews[k] for k in present_reviews)
    ):
        raise ValueError("Stale independent reverse coproduct batch; repeat the review.")
    projected = deepcopy(resource)
    ids = {r["id"] for r in present}
    projected["replace"] = [r for r in projected["replace"] if r["id"] not in ids]
    for reference in present_reviews:
        projected["reviews"].pop(reference)
    coverage = projected["coverage"]
    coverage["mapped_identities"] -= len(present)
    coverage["unresolved_identities"] += len(present)
    coverage["dispositions"]["proxy"] -= len(present)
    coverage["dispositions"]["unresolved"] += len(present)
    return projected


def apply_reverse_products(resource, review_rows, sources, targets):
    from collections import Counter

    legacy_reverse_projection(resource)
    rules, reviews = reverse_additions()
    for rule in rules:
        if (
            tuple(rule["source"][f] for f in FIELDS) not in sources
            or tuple(rule["target"][f] for f in FIELDS) not in targets
        ):
            raise ValueError("Independent reverse coproduct endpoint missing from exact catalog.")
        existing = next((r for r in resource["replace"] if r["source"] == rule["source"]), None)
        if existing is not None and existing != rule:
            raise ValueError("Conflicting reverse coproduct correspondence.")
        if existing is None:
            resource["replace"].append(deepcopy(rule))
        rows = [r for r in review_rows if r["source"] == rule["source"]]
        if len(rows) != 1:
            raise ValueError("Independent reverse coproduct catalog review coverage changed.")
        rows[0].clear()
        rows[0].update(source=deepcopy(rule["source"]), disposition="proxy", rule_id=rule["id"])
    resource["replace"].sort(key=lambda r: tuple(r["source"][f] for f in FIELDS))
    resource["reviews"].update(deepcopy(reviews))
    resource["coverage"].update(
        mapped_identities=len(resource["replace"]) + len(resource["delete"]),
        unresolved_identities=len(sources) - len(resource["replace"]) - len(resource["delete"]),
        dispositions=dict(Counter(r["disposition"] for r in review_rows)),
    )
