"""Correct sodium metal and an independently documented KCl nutrient basis."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_heat_salts_review.json"
REVIEW_SHA256 = "3e5aba4af887970b65684f7346fe6b19dab2b3bf6239d9e240b14154da698ca8"


def digest(value):
    """Bind the complete rule/helper to its independently inspected evidence."""
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


def compiled_salt_rule(row, authorization):
    """Compile only the two corrections; upstream qualifications retain their rules."""
    rule = deepcopy(row["previous_rule"])
    if row["action"] == "qualify_existing_rule_without_change":
        return rule, None
    if row["action"] not in {"source_electrolysis_helper", "documented_K2O_basis_conversion"}:
        raise ValueError("Unknown salt-reference qualification action.")
    reason = " ".join(row["limitations"])
    shared = {"rationale": authorization, "limitations": row["limitations"], "evidence": [REVIEW_PATH]}
    rule.update(
        target=deepcopy(row["target"]),
        conversion_factor=row["conversion_factor"],
        classification="proxy",
        confidence=1.0,
        mapping_method="reviewed_heat_" + row["action"],
        report_correspondence=True,
        technosphere_information_loss=reason,
        review_ref=intern_review({}, shared),
        reference_flow_sign_review={
            "status": "unresolved",
            "reason": "Only the independently inspected exact source profile is qualified.",
            "evidence": [REVIEW_PATH],
            "profiles": [
                {
                    "source_profile": row["source_profile"],
                    "conversion_factor": row["conversion_factor"],
                    "review": {
                        "status": "verified",
                        "source_sign": 1,
                        "target_sign": 1,
                        "reason": reason,
                        "evidence": [REVIEW_PATH],
                    },
                }
            ],
        },
    )
    return rule, shared


def apply_heat_salts_review(resource, root=ROOT):
    """Refuse changed references, source allocation, chemistry or helper dependencies."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Heat salts evidence changed; repeat its scientific review.")
    review = json.loads(raw)
    catalog = root / "brightpath/data/export/reference_catalogs/ecoinvent__3.10__cutoff.json"
    if hashlib.sha256(catalog.read_bytes()).hexdigest() != review["canonical_catalog_sha256"]:
        raise ValueError("Heat salts canonical biosphere definitions changed.")
    indexed = {r["id"]: r for r in resource["replace"]}
    pending = []
    for row in review["records"]:
        compiled, shared = compiled_salt_rule(row, review["authorization"])
        current = indexed.get(row["rule_id"])
        if (
            digest(row["previous_rule"]) != row["previous_rule_sha256"]
            or current not in (row["previous_rule"], compiled)
            or row["previous_rule"]["source"] != row["source"]
            or row["source_profile"] not in resource["source_profiles"]
            or row["source_profile"]["system_model"] != "cutoff"
            or row["source_reference_amount"] != 1
            or row["target_reference_amount"] != 1
        ):
            raise ValueError("Heat salts identity, reference or rule changed.")
        if row["action"] == "documented_K2O_basis_conversion":
            if (
                row["conversion_factor"]
                != row["declared_source_K2O_fraction"] / row["declared_native_K2O_reference_kg"]
            ):
                raise ValueError("Heat KCl documented nutrient basis changed.")
        elif row["conversion_factor"] != 1:
            raise ValueError("Heat sodium or upstream-service reference basis changed.")
        if "existing_recipe_sha256" in row:
            if digest(resource["recipes"][row["selected_recipe_ref"]]) != row["existing_recipe_sha256"]:
                raise ValueError("Heat salts chemical-heat helper changed.")
        recipe = row.get("recipe")
        if recipe is not None:
            if (
                len(recipe["inputs"]) != row["source_input_count"]
                or row["source_input_count"] != 9
                or len([e for e in recipe["inputs"] if e.get("type") == "biosphere"]) != 5
                or row["source_technosphere_input_count"] != 4
                or row["source_biosphere_input_count"] != 5
                or resource.get("recipes", {}).get(row["recipe_ref"]) not in (None, recipe)
            ):
                raise ValueError("Heat sodium source inventory or helper changed.")
        pending.append((current, compiled, shared, row))
    for current, compiled, shared, row in pending:
        if "recipe" in row:
            resource["recipes"][row["recipe_ref"]] = deepcopy(row["recipe"])
        if shared is not None:
            intern_review(resource, shared)
        current.clear()
        current.update(compiled)
    resource["coverage"]["mapping_methods"] = dict(
        sorted(Counter(r["mapping_method"] for r in resource["replace"]).items())
    )
    resource["coverage"]["mean_confidence"] = round(
        sum(r["confidence"] for r in resource["replace"]) / len(resource["replace"]), 6
    )
    dispositions = Counter(
        "recipe" if "recipe_ref" in r["target"] else r.get("classification", "unclassified")
        for r in resource["replace"]
    )
    dispositions["omitted"] = len(resource.get("delete", []))
    if resource["coverage"].get("unresolved_identities"):
        dispositions["unresolved"] = resource["coverage"]["unresolved_identities"]
    resource["coverage"]["dispositions"] = dict(dispositions)
