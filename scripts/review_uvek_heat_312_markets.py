"""Qualify three delivered or processing-service markets for exact heat dependencies."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_heat_312_markets_review.json"
REVIEW_SHA256 = "8079772089b1702b8595d1ffcf1ce29c00e96cc682e14385a01890de71dbb2dd"


def digest(value):
    """Bind whole rules and source-derived helper contents to the scientific review."""
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


def read_review(root=ROOT):
    """Reject changed evidence instead of broadening profile approvals implicitly."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Heat3.12 closure evidence changed; repeat its scientific review.")
    return json.loads(raw)


def shared_closure_review(row):
    """Retain complete-proxy and delivery differences in portable warnings."""
    return {"rationale": row["limitations"][0], "limitations": row["limitations"], "evidence": [REVIEW_PATH]}


def compiled_closure_rule(row):
    """Qualify independently inspected complete delivered or processing-service markets."""
    rule = deepcopy(row["previous_rule"])
    if row["resource_action"] != "reconstruct_market":
        raise ValueError("Unknown heat3.12 market action.")
    reason = " ".join(row["limitations"])
    rule.update(
        target=deepcopy(row["target"]),
        conversion_factor=row["conversion_factor"],
        classification="proxy",
        mapping_method="reviewed_heat_312_market_composition",
        report_correspondence=True,
        review_ref=intern_review({}, shared_closure_review(row)),
        technosphere_information_loss=reason,
        reference_flow_sign_review={
            "status": "unresolved",
            "reason": "Only independently inspected exact cut-off profiles are qualified.",
            "evidence": [REVIEW_PATH],
            "profiles": [
                {
                    "source_profile": deepcopy(row["source_profile"]),
                    "conversion_factor": row["conversion_factor"],
                    "review": {
                        "status": "verified",
                        "source_sign": row["source_reference_amount"],
                        "target_sign": 1,
                        "reason": reason,
                        "evidence": [REVIEW_PATH],
                    },
                }
            ],
        },
    )
    return rule


def apply_heat_312_markets_review(resource, root=ROOT):
    """Validate every rule and dependency before mutating any part of the resource."""
    review = read_review(root)
    indexed = {r["id"]: r for r in resource["replace"]}
    pending = []
    for row in review["records"]:
        previous = row["previous_rule"]
        current = indexed.get(row["rule_id"])
        compiled = compiled_closure_rule(row)
        if (
            digest(previous) != row["previous_rule_sha256"]
            or current not in (previous, compiled)
            or previous["source"] != row["source"]
            or previous.get("conversion_factor", 1) != row["conversion_factor"]
            or row["source_reference_amount"] != row["conversion_factor"]
            or row["target_reference_amount"] != 1
            or row["source_profile"] != {"family": "ecoinvent", "version": "3.12", "system_model": "cutoff"}
            or row["source_profile"] not in resource["source_profiles"]
        ):
            raise ValueError("Heat3.12 closure identity, reference or earlier rule changed.")
        if row["resource_action"] != "reconstruct_market":
            raise ValueError("Unknown heat3.12 market action.")
        if row["resource_action"] == "reconstruct_market":
            if row["target"].get("recipe_ref") not in row["new_recipes"] or row["renormalized"] is not False:
                raise ValueError("Heat3.12 delivery helper or original producer shares changed.")
            for recipe in row["new_recipes"].values():
                bindings = [
                    b
                    for b in row["retained_input_bindings"]
                    if b["source_activity"]["location"] == recipe["location"]
                    and recipe["name"].startswith(b["source_activity"]["name"] + ", source market preserved,")
                ]
                if (
                    recipe["unit"] != row["source"]["unit"]
                    or recipe["inputs"] != [b["target_input"] for b in bindings]
                    or any(
                        e["amount"] <= 0 or e.get("type", "technosphere") != "technosphere" for e in recipe["inputs"]
                    )
                ):
                    raise ValueError("Heat3.12 retained delivery quantities or uncertainty changed.")
        for key, recipe in row["new_recipes"].items():
            if resource["recipes"].get(key) not in (None, recipe) or recipe.get("source_input_omissions"):
                raise ValueError("Heat3.12 helper changed or gained an omission.")
        pending.append((current, compiled, row))
    for current, compiled, row in pending:
        resource["recipes"].update(deepcopy(row["new_recipes"]))
        intern_review(resource, shared_closure_review(row))
        current.clear()
        current.update(compiled)
    resource["coverage"]["mapping_methods"] = dict(
        sorted(Counter(r["mapping_method"] for r in resource["replace"]).items())
    )
    dispositions = Counter(
        "recipe" if "recipe_ref" in r["target"] else r.get("classification", "unclassified")
        for r in resource["replace"]
    )
    dispositions["omitted"] = len(resource.get("delete", []))
    if resource["coverage"].get("unresolved_identities"):
        dispositions["unresolved"] = resource["coverage"]["unresolved_identities"]
    resource["coverage"]["dispositions"] = dict(dispositions)
