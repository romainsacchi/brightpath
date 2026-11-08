"""Physical cut-off slag products retain their zero-allocation raw-supply boundary."""

import hashlib
import json
from collections import Counter, defaultdict
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

try:
    from review_uvek_market_compositions import composition_decision, composition_recipe, validate_composition_bindings
except ModuleNotFoundError:
    from scripts.review_uvek_market_compositions import (
        composition_decision,
        composition_recipe,
        validate_composition_bindings,
    )

FIELDS = ("name", "reference product", "location", "unit")
REVIEW_PATH = "brightpath/data/export/uvek_cutoff_slag_product_review.json"
APPROVAL = json.loads((Path(__file__).resolve().parents[1] / REVIEW_PATH).read_text())
PRODUCTS = {tuple(row["source"][f] for f in FIELDS): row for row in APPROVAL["rules"]}
EVIDENCE_SCOPE = defaultdict(set)
for row in APPROVAL["inventories"]:
    EVIDENCE_SCOPE[tuple(row["label"].split("-"))].add(tuple(row["source"][f] for f in FIELDS))
REVERSE_METHOD = "reviewed_cutoff_slag_operating_product_reverse"
FORWARD_LIMITATIONS = (
    "Independently inspected physical cut-off ground-granulated blast-furnace-slag product proxy: supply "
    "one kg of the UVEK no-burdens operating product plus the exact source quantity of its already reviewed "
    "cut-off raw-slag market helper. The raw material is a positive-reference, production-only Recycled "
    "Content stock with zero upstream production burdens; its original freight remains represented. "
    "UVEK's with-burdens variant instead adds economically allocated slag production and extra haulage, "
    "which must not replace the source cut-off boundary. No burdens means no upstream raw-slag allocation, "
    "not zero processing impacts. Granulation/grinding, heat and electricity, transport, plant geography, "
    "moisture, granulometry, water, emissions, infrastructure and disposal remain technology proxies. "
    "Internal component uncertainty and formulas are not reconstructed. Preserve caller signs, supported "
    "uncertainty/formulas, foreground suppliers, production and separate emissions. Score/export every "
    "returned helper. No treatment reference, constrained SCM credit, strength equivalence, allocation "
    "reconstruction or new omission is approved. Only observed exact cut-off profiles are covered; no "
    "unknown patch or consequential fallback is inferred."
)
REVERSE_LIMITATIONS = (
    "Independently inspected reverse no-burdens GGBFS operating-product proxy: represent one kg of UVEK "
    "RER ground granulated slag by the ecoinvent 3.12 cut-off US ground-granulated-slag product and subtract "
    "its exact 1.0011225250000004 kg positive-reference US raw-slag market input. That market contains "
    "zero-production-burden cut-off stock plus freight; removing it retains the operating process while "
    "excluding upstream raw-slag supply and haulage, matching UVEK's inspected no-burdens boundary. "
    "This is an explicit RER-to-US technology/geography proxy. Natural-gas units, energy mix, direct "
    "emissions, water, infrastructure, disposal, process yield, moisture, granulometry and background "
    "models differ; equal material performance or LCIA scores are not established. Preserve incoming kg, "
    "signs, supported uncertainty/formulas, foreground suppliers and separate emissions. Include all "
    "returned helpers in scoring/export. Do not infer a waste-treatment or SCM-credit inverse, rebuild "
    "original supplier geography, or map the economically allocated with-burdens variant to a zero-"
    "allocation cut-off product."
)


def identity(record):
    return tuple(record.get(f) for f in FIELDS)


def inventory_record(source, blobs, comment, product_information):
    return {
        "source": list(source),
        "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
        "activity_comment_sha256": hashlib.sha256(str(comment or "").encode()).hexdigest(),
        "product_information_sha256": hashlib.sha256(str(product_information or "").encode()).hexdigest(),
    }


def validate_inventories(inventories, observations, native):
    """Bind the physical product, received stock and two UVEK allocation variants."""
    expected = defaultdict(list)
    for row in APPROVAL["inventories"]:
        source = identity(row["source"])
        expected[source].append(
            tuple(
                row[k] for k in ("label", "inventory_sha256", "activity_comment_sha256", "product_information_sha256")
            )
        )
        family = row["label"].split("-")[0]
        signs = {sign for label, sign in observations[family].get(source, set()) if label == row["label"]}
        if signs != {1}:
            raise ValueError("Cut-off slag product or raw-stock reference changed; repeat the independent review.")
        if family == "uvek" and (
            native.get(source, {}).get("native_flow_type") != "PRODUCT_FLOW"
            or native.get(source, {}).get("native_reference_is_input") is not False
        ):
            raise ValueError("UVEK slag product boundary changed; repeat the independent review.")
    actual = {
        source: sorted(
            tuple(
                row[k] for k in ("label", "inventory_sha256", "activity_comment_sha256", "product_information_sha256")
            )
            for row in rows
        )
        for source, rows in inventories.items()
    }
    if actual != {source: sorted(rows) for source, rows in expected.items()}:
        raise ValueError("Cut-off slag inventory or boundary metadata changed; repeat the independent review.")


def validate_bindings(resource):
    validate_composition_bindings(resource, approval=APPROVAL, label="Cut-off slag physical-product")


def product_recipe(source, profile):
    return composition_recipe(
        source, profile, rationale=FORWARD_LIMITATIONS, review_path=REVIEW_PATH, namespace="cutoff-slag-product"
    )


def product_decision(rule, observations, inventories, binding, original_review):
    return composition_decision(
        rule,
        observations,
        inventories,
        binding,
        original_review,
        approval_row=PRODUCTS[identity(rule["source"])],
        rationale=FORWARD_LIMITATIONS,
        review_path=REVIEW_PATH,
        namespace="cutoff-slag-product",
        mapping_method="reviewed_cutoff_slag_product",
        profile_reason="Select an independently inspected physical cut-off GGBFS profile; no treatment, SCM-credit or patch fallback is approved.",
    )


def reverse_addition():
    row = APPROVAL["reverse"]
    review = {"rationale": REVERSE_LIMITATIONS, "evidence": [REVIEW_PATH]}
    reviews = {}
    review_ref = intern_review({"reviews": reviews}, review)
    digest = hashlib.sha256(json.dumps([identity(row["source"]), row["inputs"]], sort_keys=True).encode()).hexdigest()[
        :20
    ]
    recipe_id = "reverse-cutoff-slag-operating-" + digest
    rule = {
        "id": "uvek-ei312-cutoff-slag-" + digest,
        "source": deepcopy(row["source"]),
        "target": {"recipe_ref": recipe_id},
        "conversion_factor": 1,
        "classification": "proxy",
        "mapping_method": REVERSE_METHOD,
        "review_ref": review_ref,
    }
    recipe = {
        **deepcopy(row["source"]),
        "name": row["source"]["name"] + ", reconstructed from US cut-off operation",
        "location": "US",
        "inputs": deepcopy(row["inputs"]),
        "rationale": REVERSE_LIMITATIONS,
        "evidence": [REVIEW_PATH],
    }
    return rule, {recipe_id: recipe}, reviews


def legacy_reverse_projection(resource):
    """Validate the independent addition before protecting the frozen publisher baseline."""
    expected, recipes, reviews = reverse_addition()
    present = []
    for rule in resource["replace"]:
        if identity(rule["source"]) != identity(expected["source"]):
            if rule.get("mapping_method") == REVERSE_METHOD:
                raise ValueError("Unapproved reverse slag source outside the independent scope.")
            continue
        normalized = {k: v for k, v in rule.items() if k != "reference_flow_sign_review"}
        if normalized != expected or resource["reviews"].get(rule.get("review_ref")) != reviews[expected["review_ref"]]:
            raise ValueError("Reverse slag product correspondence changed; repeat the independent review.")
        present.append(rule)
    if len(present) > 1 or (present and any(resource["recipes"].get(k) != v for k, v in recipes.items())):
        raise ValueError("Reverse slag operating reconstruction changed; repeat the independent review.")
    if not present and any(key in resource["recipes"] for key in recipes):
        raise ValueError("An unbound reverse slag recipe cannot bypass its exact correspondence.")
    return {
        **resource,
        "replace": [r for r in resource["replace"] if r not in present],
        "recipes": {k: v for k, v in resource["recipes"].items() if k not in recipes},
    }


def apply_reverse_product(resource, review, source_identities, target_identities):
    legacy_reverse_projection(resource)
    rule, recipes, reviews = reverse_addition()
    if identity(rule["source"]) not in source_identities or any(
        identity(e) not in target_identities for recipe in recipes.values() for e in recipe["inputs"]
    ):
        raise ValueError("Reverse slag source or operating dependency is missing from its exact catalog.")
    resource["replace"] = [r for r in resource["replace"] if identity(r["source"]) != identity(rule["source"])] + [rule]
    resource["replace"].sort(key=lambda r: identity(r["source"]))
    resource["recipes"].update(recipes)
    resource["reviews"].update(reviews)
    for index, row in enumerate(review):
        if identity(row["source"]) == identity(rule["source"]):
            review[index] = {"source": deepcopy(rule["source"]), "rule_id": rule["id"], "disposition": "recipe"}
    resource["coverage"].update(
        mapped_identities=len(resource["replace"]) + len(resource["delete"]),
        unresolved_identities=len(source_identities) - len(resource["replace"]) - len(resource["delete"]),
        dispositions=dict(Counter(r["disposition"] for r in review)),
    )
