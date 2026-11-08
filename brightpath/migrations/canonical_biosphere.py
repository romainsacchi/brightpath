"""Exact source-release selection for the UVEK canonical ecoinvent biosphere bridge."""

from __future__ import annotations

from collections import Counter
from copy import deepcopy

from brightpath.exceptions import MigrationError
from brightpath.models import Issue
from brightpath.units import normalize_unit

MARKER = "canonical_biosphere_identity_version"
REVIEW = "biosphere_identity_review"


def _key(row):
    return row.get("name"), tuple(row.get("categories", ())), normalize_unit(row.get("unit"))


def _checked_target(source, decision, default):
    """Recheck each executable decision even when the low-level caller owns the resource."""
    target = decision.get("target", default)
    uuids = decision.get("source_flow_ids", [])
    if (
        decision.get("status") != "verified"
        or len(uuids) != 1
        or not target.get("name")
        or not target.get("flow_id")
        or tuple(target.get("categories", ())) != tuple(source.get("categories", ()))
        or target.get("unit") != source.get("unit")
    ):
        raise MigrationError("Canonical flow identity changes a physical unit or receiving context.")
    kind = decision.get("identity_kind")
    if kind == "publisher_uuid":
        if uuids != [target["flow_id"]]:
            raise MigrationError("Publisher UUID correspondence has different source and target IDs.")
    elif kind == "reviewed_publisher_synonym":
        if (
            not decision.get("source_cas")
            or decision["source_cas"] != decision.get("target_cas")
            or not decision.get("evidence")
        ):
            raise MigrationError("Publisher synonym correspondence needs matching chemical identity evidence.")
    else:
        raise MigrationError("Unreviewed canonical chemical correspondence.")
    return target


def validate_canonical_biosphere_resource(resource: dict) -> None:
    """Check publisher identities and exact version scopes before using the bridge."""
    if MARKER not in resource:
        return
    if (
        type(resource[MARKER]) is not int
        or resource[MARKER] != 1
        or resource.get("axis") != "biosphere"
        or resource.get("target_profile") != {"family": "ecoinvent", "version": "3.10"}
        or resource.get("delete")
        or resource.get("disaggregate")
    ):
        raise MigrationError("Invalid canonical biosphere identity resource.")
    allowed = {(p["family"], p["version"]) for p in resource["source_profiles"]}
    seen = set()
    for mapped, rows in ((True, resource["replace"]), (False, resource.get("unresolved", []))):
        for row in rows:
            source = row["source"]
            identity = _key(source)
            if not all(identity) or identity in seen:
                raise MigrationError("Canonical biosphere source identities are incomplete or duplicated.")
            seen.add(identity)
            review = row.get(REVIEW, {}) if mapped else row
            profiles = review.get("profiles")
            if not isinstance(profiles, list) or not profiles:
                raise MigrationError("Canonical biosphere decisions need exact source-release evidence.")
            versions, targets = set(), set()
            for decision in profiles:
                profile = decision.get("source_profile", {})
                scope = profile.get("family"), profile.get("version")
                uuids = decision.get("source_flow_ids")
                if (
                    scope not in allowed
                    or scope in versions
                    or not isinstance(uuids, list)
                    or not uuids
                    or any(not isinstance(u, str) or not u for u in uuids)
                    or len(uuids) != len(set(uuids))
                ):
                    raise MigrationError("Canonical biosphere source release or publisher IDs changed.")
                versions.add(scope)
                if decision.get("status") == "unresolved":
                    if not decision.get("reason") or "target" in decision:
                        raise MigrationError("An unresolved physical flow cannot carry an inferred target.")
                    continue
                target = _checked_target(source, decision, row.get("target", {}))
                targets.add((*_key(target), target["flow_id"]))
            if mapped:
                if (
                    not targets
                    or (*_key(row["target"]), row["target"].get("flow_id")) not in targets
                    or review.get("status") != ("profile_dependent" if len(targets) > 1 else "verified")
                    or row.get("conversion_factor", 1) != 1
                    or row.get("confidence") != 1
                    or row.get("mapping_method")
                    not in {"identity", "publisher_flow_identity", "publisher_flow_identity_profile"}
                ):
                    raise MigrationError("Canonical biosphere default target or profile decision is inconsistent.")
            elif targets:
                raise MigrationError("Unresolved canonical flow has an executable profile target.")
    coverage = resource["coverage"]
    if (
        len(seen) != coverage["source_identities"]
        or len(resource.get("unresolved", [])) != coverage["unresolved_identities"]
    ):
        raise MigrationError("Canonical biosphere unresolved coverage does not match its source inventory.")


def select_canonical_biosphere_rules(resource: dict, source_version: str) -> dict:
    """Return independently owned rules for one inspected biosphere release.

    The source profile, not an optional exchange UUID, distinguishes reused
    labels such as manganese. Unsupported physical flows have no replacement.
    """
    if MARKER not in resource:
        return resource
    selected, unresolved = [], []
    for mapped, rows in ((True, resource["replace"]), (False, resource.get("unresolved", []))):
        for row in rows:
            profiles = row[REVIEW]["profiles"] if mapped else row["profiles"]
            decisions = [
                p for p in profiles if p["source_profile"] == {"family": "ecoinvent", "version": source_version}
            ]
            if len(decisions) > 1:
                raise MigrationError("Duplicate canonical source-release decisions.")
            if not decisions:
                continue
            decision = decisions[0]
            if decision["status"] == "verified":
                target = deepcopy(_checked_target(row["source"], decision, row.get("target", {})))
                selected.append(
                    {
                        **row,
                        "target": target,
                        "mapping_method": (
                            "identity" if _key(row["source"]) == _key(target) else "publisher_flow_identity"
                        ),
                        REVIEW: {**row[REVIEW], "status": "verified", "profiles": [deepcopy(decision)]},
                    }
                )
            else:
                unresolved.append(
                    {"id": row["id"], "source": deepcopy(row["source"]), "profiles": [deepcopy(decision)]}
                )
    if not selected and not unresolved:
        raise MigrationError(f"No inspected canonical biosphere identity decisions for release {source_version}.")
    result = {
        **resource,
        "source_profiles": [{"family": "ecoinvent", "version": source_version}],
        "replace": selected,
        "unresolved": unresolved,
        "coverage": {
            **resource["coverage"],
            "source_identities": len(selected) + len(unresolved),
            "mapped_identities": len(selected),
            "unresolved_identities": len(unresolved),
            "mapping_methods": dict(Counter(r["mapping_method"] for r in selected)),
        },
    }
    return result


def unresolved_canonical_biosphere_issues(data: list[dict], resource: dict) -> list[Issue]:
    """Reject uninspected physical flows without deleting or guessing any exchange."""
    if MARKER not in resource:
        return []
    supported = {_key(r["source"]) for r in resource["replace"]}
    reasons = {_key(r["source"]): r["profiles"][0]["reason"] for r in resource["unresolved"]}
    issues = []
    for di, dataset in enumerate(data):
        for ei, exchange in enumerate(dataset.get("exchanges", [])):
            if exchange.get("type") != "biosphere" or _key(exchange) in supported:
                continue
            reason = reasons.get(
                _key(exchange), "No publisher identity is inspected for this flow in the declared source release."
            )
            issues.append(
                Issue(
                    severity="error",
                    code="migration_biosphere_identity_unresolved",
                    message=reason + " The physical exchange is retained and the conversion is rolled back.",
                    path=f"activity[{di}].exchanges[{ei}]",
                )
            )
    return issues
