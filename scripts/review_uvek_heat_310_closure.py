"""Qualify primary-metal proxies and reconstruct literal condensate fate shares."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_heat_310_closure_review.json"
REVIEW_SHA256 = "4843438cf642091f696cdfbbbc1e4b7c2653d635a8c0f96e2fdeac0dcaf08a33"


def digest(value):
    """Bind complete rules and component relocation evidence."""
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


def shared_closure_review(row):
    """Keep scientific differences visible in portable migration provenance."""
    return {"rationale": row["limitations"][0], "limitations": row["limitations"], "evidence": [REVIEW_PATH]}


def compiled_closure_rule(row):
    """Retain qualified aluminium; restrict corrections to the inspected profile."""
    rule = deepcopy(row["previous_rule"])
    if row["action"] == "qualify_existing":
        return rule
    if row["action"] not in {"correct_native_boundary", "reconstruct_market"}:
        raise ValueError("Unknown heat closure qualification action.")
    reason = " ".join(row["limitations"])
    rule.update(
        target=deepcopy(row["target"]),
        conversion_factor=row["conversion_factor"],
        classification="proxy",
        mapping_method="reviewed_heat_" + row["action"],
        report_correspondence=True,
        review_ref=intern_review({}, shared_closure_review(row)),
        technosphere_information_loss=reason,
        reference_flow_sign_review={
            "status": "unresolved",
            "reason": "Only the independently inspected exact source profile is qualified.",
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


def apply_heat_310_closure_review(resource, root=ROOT):
    """Check the entire batch before mutation, including all relocated sewer inputs."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Heat closure evidence changed; repeat its scientific review.")
    review = json.loads(raw)
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
            or row["source_profile"] != {"family": "ecoinvent", "version": "3.10", "system_model": "cutoff"}
            or row["source_profile"] not in resource["source_profiles"]
        ):
            raise ValueError("Heat closure identity, sign or earlier rule changed.")
        if row["action"] == "reconstruct_market":
            treatment = row["new_recipes"][row["treatment_recipe_ref"]]
            collection = row["new_recipes"][row["collection_recipe_ref"]]
            market = row["new_recipes"][row["market_recipe_ref"]]
            sewer = row["native_sewer_relocation"]["removed_embedded_exchange"]
            if (
                row["conversion_factor"] != -1
                or row["target"] != {"recipe_ref": row["market_recipe_ref"]}
                or digest(treatment["inputs"]) != row["native_treatment_input_sha256"]
                or len(treatment["inputs"]) != 76
                or any(e["type"] == "technosphere" and e.get("name") == sewer["name"] for e in treatment["inputs"])
                or collection["inputs"] != [sewer]
                or sewer["amount"] != row["native_sewer_relocation"]["allocation_rate_km_per_m3"]
                or sewer["uncertainty type"] != 0
                or len(market["inputs"]) != 20
                or [e for e in market["inputs"] if e.get("type") == "biosphere"]
                != row["retained_untreated_biosphere_inputs"]
                or [e for e in treatment["inputs"] if e.get("type") == "biosphere"]
                != row["native_treatment_biosphere_inputs"]
                or market["inputs"] != [binding["target_input"] for binding in row["retained_source_bindings"]]
            ):
                raise ValueError("Heat condensate fates, sewer relocation or treatment copy changed.")
        elif row["new_recipes"] or row["source"]["unit"] != row["target"]["unit"]:
            raise ValueError("Heat metal reference unit or helper boundary changed.")
        for key, recipe in row["new_recipes"].items():
            if resource["recipes"].get(key) not in (None, recipe) or recipe.get("source_input_omissions"):
                raise ValueError("Heat condensate recipe changed or gained an omission.")
        pending.append((current, compiled, row))
    for current, compiled, row in pending:
        resource["recipes"].update(deepcopy(row["new_recipes"]))
        if row["action"] != "qualify_existing":
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
