"""Qualify four complete native service proxies for inspected heat dependencies."""

import hashlib
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_heat_38_services_review.json"
REVIEW_SHA256 = "04d7574358a2e7f212725d66847a6665f60d84e890b251c04c54147321f0fdaf"
METHOD = "reviewed_complete_service_proxy"


def digest(value):
    """Bind complete reviewed rules, including their previous reference decisions."""
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


def read_review(root=ROOT):
    """Refuse scientific evidence changes instead of refreshing fingerprints blindly."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Heat service evidence changed; repeat its scientific review.")
    return json.loads(raw)


def shared_service_review(row):
    """Retain material ambiguities and changed inventories in portable warnings."""
    return {"rationale": row["limitations"][0], "limitations": row["limitations"], "evidence": [REVIEW_PATH]}


def compiled_service_rule(row):
    """Bind only independently inspected profiles to the reviewed complete supplier."""
    rule = deepcopy(row["previous_rule"])
    reason = " ".join(row["limitations"])
    rule.update(
        target=deepcopy(row["target"]),
        conversion_factor=row["conversion_factor"],
        classification="proxy",
        mapping_method=METHOD,
        report_correspondence=True,
        review_ref=intern_review({}, shared_service_review(row)),
        technosphere_information_loss=reason,
        reference_flow_sign_review={
            "status": "unresolved",
            "reason": "Only independently inspected cut-off profiles are qualified.",
            "evidence": [REVIEW_PATH],
            "profiles": [
                {
                    "source_profile": deepcopy(p["source_profile"]),
                    "conversion_factor": row["conversion_factor"],
                    "review": {
                        "status": "verified",
                        "source_sign": p["source_reference_amount"],
                        "target_sign": 1,
                        "reason": reason,
                        "evidence": [REVIEW_PATH],
                    },
                }
                for p in row["profiles"]
            ],
        },
    )
    return rule


def apply_heat_38_services_review(resource, root=ROOT):
    """Validate all profiles before changing rules; native inventories remain unchanged."""
    review = read_review(root)
    indexed = {r["id"]: r for r in resource["replace"]}
    pending = []
    for row in review["records"]:
        previous = row["previous_rule"]
        compiled = compiled_service_rule(row)
        current = indexed.get(row["rule_id"])
        if (
            digest(previous) != row["previous_rule_sha256"]
            or current not in (previous, compiled)
            or previous["source"] != row["source"]
            or previous.get("conversion_factor", 1) != row["conversion_factor"]
            or row["conversion_factor"] not in (-1, 1)
            or row["source"]["unit"] != row["target"]["unit"]
            or set(row["native_references"]) != {"2025", "2026"}
            or any(
                n["identity"] != row["target"] or n["reference_amount"] != 1 for n in row["native_references"].values()
            )
        ):
            raise ValueError("Heat service identity, reference or earlier rule changed.")
        for p in row["profiles"]:
            if (
                p["source_profile"] not in resource["source_profiles"]
                or p["source_profile"]["family"] != "ecoinvent"
                or p["source_profile"]["system_model"] != "cutoff"
                or p["identity"] != row["source"]
                or p["source_reference_amount"] != row["conversion_factor"]
                or p["target_reference_amount"] != 1
            ):
                raise ValueError("Heat service source profile or coordinate changed.")
        pending.append((current, compiled, row))
    for current, compiled, row in pending:
        intern_review(resource, shared_service_review(row))
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
