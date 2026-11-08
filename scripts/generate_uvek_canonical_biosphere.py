"""Build canonical biosphere correspondences from publisher identity, never name similarity."""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter, defaultdict
from pathlib import Path

from brightpath.migrations.schema import rule_identifier, validate_migration_resource

VERSIONS = ("3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12")
REVIEW_PATH = "brightpath/data/export/uvek_canonical_biosphere_identity_review.json"
SYNONYM_RULES_SHA256 = "5c4365f5308cd820fddb18c3993bb75c0f2044f1834dc49ed6ef1670da35d4ef"


def identity(row):
    return {"name": row["name"], "categories": list(row["categories"]), "unit": row["unit"]}


def key(row):
    return row["name"], tuple(row["categories"]), row["unit"]


def _synonyms(root, targets):
    path = root / REVIEW_PATH
    review = json.loads(path.read_text(encoding="utf-8"))
    digest = hashlib.sha256(json.dumps(review["rules"], sort_keys=True).encode()).hexdigest()
    if digest != SYNONYM_RULES_SHA256 or review["target_profile"] != {"family": "ecoinvent", "version": "3.10"}:
        raise ValueError("Canonical chemical identity review changed; repeat the scientific review.")
    result = {}
    for row in review["rules"]:
        source, target = row["source"], row["target"]

        def normalized(name):
            return " ".join(re.findall(r"[a-z0-9]+", name.casefold()))

        if (
            source["unit"] != target["unit"]
            or source["categories"] != target["categories"]
            or row["source_cas"] != row["target_cas"]
            or row["publisher_unit"] != "kg"
            or row["publisher_compartment"] != "water"
            or row["publisher_subcompartment"] != "ground-"
            or normalized(source["name"]) not in {normalized(n) for n in row["publisher_synonyms"]}
            or targets.get(target["flow_id"]) != target
        ):
            raise ValueError("Canonical publisher synonym identity, unit or receiving context changed.")
        for profile in row["source_profiles"]:
            result[key(source), profile["version"]] = row
    return result


def build_canonical_biosphere_resource(catalog_directory: Path) -> dict:
    """Retain same-UUID/unit/context flows and two independently reviewed exact synonyms.

    The profile-specific evidence is necessary when a label has denoted different
    publisher UUIDs across releases. Missing or incompatible flows remain explicit
    unresolved entries; neither a chemical nor a compartment is inferred.
    """
    if __package__:
        from scripts.generate_uvek_migration_resources import _resource_metadata
    else:
        from generate_uvek_migration_resources import _resource_metadata

    target_path = catalog_directory / "ecoinvent__3.10__cutoff.json"
    payload = json.loads(target_path.read_text(encoding="utf-8"))
    targets = {row["flow_id"]: row for row in payload["biosphere"]}
    if len(targets) != len(payload["biosphere"]):
        raise ValueError("Canonical target flow IDs are not unique.")
    evidence = defaultdict(lambda: defaultdict(set))
    hashes = {}
    for version in VERSIONS:
        for model in ("cutoff", "consequential"):
            path = catalog_directory / f"ecoinvent__{version}__{model}.json"
            hashes[path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
            for row in json.loads(path.read_text(encoding="utf-8"))["biosphere"]:
                evidence[key(row)][version].add(row["flow_id"])
    root = Path(__file__).resolve().parents[1]
    synonyms = _synonyms(root, targets)
    resource = _resource_metadata(
        name="ecoinvent-3.x-to-ecoinvent-3.10-biosphere-for-uvek-2025",
        description=(
            "Canonical ecoinvent 3.10 biosphere compatibility for UVEK: exact publisher UUID, unit and "
            "receiving context, or explicitly reviewed same-CAS publisher synonyms. Source-version "
            "decisions are mandatory. Unsupported flows remain unresolved, without chemical or compartment inference."
        ),
        axis="biosphere",
        source={"family": "ecoinvent", "versions": list(VERSIONS)},
        target={"family": "ecoinvent", "version": "3.10"},
    )
    resource["canonical_biosphere_identity_version"] = 1
    resource["catalog_sha256"] = hashes
    resource["unresolved"] = []
    resource["identity_review"] = REVIEW_PATH
    for source, profiles in sorted(evidence.items()):
        source_dict = {"name": source[0], "categories": list(source[1]), "unit": source[2]}
        decisions = []
        for version, uuids in sorted(profiles.items()):
            profile = {"family": "ecoinvent", "version": version}
            valid = [targets[u] for u in uuids if u in targets and key(targets[u])[1:] == source[1:]]
            decision = {"source_profile": profile, "source_flow_ids": sorted(uuids)}
            if len(uuids) == len(valid) == 1:
                decision.update(
                    status="verified",
                    identity_kind="publisher_uuid",
                    target={**identity(valid[0]), "flow_id": valid[0]["flow_id"]},
                )
            elif (source, version) in synonyms and uuids == {synonyms[source, version]["source"]["flow_id"]}:
                row = synonyms[source, version]
                decision.update(
                    status="verified",
                    identity_kind="reviewed_publisher_synonym",
                    target=row["target"],
                    source_cas=row["source_cas"],
                    target_cas=row["target_cas"],
                    evidence=REVIEW_PATH,
                )
            else:
                reason = (
                    "Publisher UUID has a different physical unit or receiving context in the target."
                    if any(u in targets for u in uuids)
                    else "No identical publisher UUID is present in the target, and no exact synonym is reviewed."
                )
                if len(uuids) > 1:
                    reason = "The source name/context/unit denotes multiple publisher flow IDs in this release."
                decision.update(status="unresolved", reason=reason)
            decisions.append(decision)
        approved = [d for d in decisions if d["status"] == "verified"]
        rid = rule_identifier(resource["name"], source_dict)
        if not approved:
            resource["unresolved"].append({"id": rid, "source": source_dict, "profiles": decisions})
            continue
        distinct = {json.dumps(d["target"], sort_keys=True) for d in approved}
        method = "publisher_flow_identity_profile" if len(distinct) > 1 else "publisher_flow_identity"
        if len(distinct) == 1 and identity(approved[0]["target"]) == source_dict:
            method = "identity"
        resource["replace"].append(
            {
                "id": rid,
                "source": source_dict,
                "target": approved[0]["target"],
                "mapping_method": method,
                "confidence": 1.0,
                "biosphere_identity_review": {
                    "status": "profile_dependent" if len(distinct) > 1 else "verified",
                    "reason": "Select the exact source-release publisher identity; preserve mass and full receiving context.",
                    "profiles": decisions,
                },
            }
        )
    methods = Counter(r["mapping_method"] for r in resource["replace"])
    resource["coverage"] = {
        "source_identities": len(evidence),
        "target_identities": len(payload["biosphere"]),
        "mapped_identities": len(resource["replace"]),
        "unresolved_identities": len(resource["unresolved"]),
        "mapping_methods": dict(sorted(methods.items())),
        "mean_confidence": 1.0,
    }
    validate_migration_resource(resource)
    return resource
