from __future__ import annotations

import re
from collections import Counter
from typing import Iterable

from brightpath.catalogs import load_background_catalog
from brightpath.models import InventoryDocument, Issue, ValidationReport
from brightpath.utils import inspect_brightway_inventory

_CONTEXT_PATTERN = re.compile(r"^(?P<path>activity\[\d+\](?:\.exchanges\[\d+\])?):\s*(?P<message>.+)$")


def validate_brightway_inventory(
    document: InventoryDocument,
    *,
    check_background_links: bool = True,
    additional_foreground_targets: Iterable[tuple[str, str, str, str]] = (),
) -> ValidationReport:
    """Validate canonical inventory structure and optional background links."""

    profile = document.background_profile.normalized()
    report = ValidationReport(profile=profile)
    data = document.data

    errors, warnings = inspect_brightway_inventory(
        data,
        require_simapro_category=False,
        validate_units=False,
    )
    report.issues.extend(_messages_to_issues(errors, severity="error", code="inventory_structure"))
    report.issues.extend(
        _messages_to_issues(
            warnings,
            severity="warning",
            code="inventory_plausibility",
        )
    )
    report.issues.extend(_duplicate_identity_issues(data))
    report.issues.extend(_production_identity_issues(data))

    if not check_background_links:
        return report
    if not profile.is_complete:
        report.issues.append(
            Issue(
                severity="error",
                code="background_profile_incomplete",
                message=(
                    "Background profile must define family, version, and system model "
                    "before link validation or migration."
                ),
            )
        )
        return report
    if profile.family not in {"ecoinvent", "uvek"}:
        report.issues.append(
            Issue(
                severity="error",
                code="background_family_unsupported",
                message=f"Unsupported background family: {profile.family!r}.",
            )
        )
        return report

    try:
        catalog = load_background_catalog(profile)
    except FileNotFoundError:
        report.issues.append(
            Issue(
                severity="error",
                code="background_catalog_missing",
                message=f"No reference catalog is available for {profile.label()}.",
                suggested_fix="Install or generate the exact target reference catalog.",
            )
        )
        return report

    foreground = {_technosphere_key(activity) for activity in data if all(_technosphere_key(activity))}
    foreground.update(
        tuple(str(part or "").strip() for part in target)
        for target in additional_foreground_targets
        if len(target) == 4
    )

    for activity_index, activity in enumerate(data):
        for exchange_index, exchange in enumerate(activity.get("exchanges", [])):
            path = f"activity[{activity_index}].exchanges[{exchange_index}]"
            if exchange.get("type") == "technosphere":
                key = _technosphere_key(exchange)
                if all(key) and key not in foreground and key not in catalog.technosphere:
                    report.issues.append(
                        Issue(
                            severity="error",
                            code="unknown_technosphere_target",
                            message=(
                                "Technosphere exchange does not match an inventory dataset or "
                                f"the {profile.label()} reference catalog: {' | '.join(key)}."
                            ),
                            path=path,
                        )
                    )
            elif exchange.get("type") == "biosphere":
                key = (
                    str(exchange.get("name") or ""),
                    tuple(str(item) for item in exchange.get("categories", ())),
                    str(exchange.get("unit") or ""),
                )
                if all((key[0], key[1], key[2])) and key not in catalog.biosphere:
                    report.issues.append(
                        Issue(
                            severity="error",
                            code="unknown_biosphere_flow",
                            message=(
                                "Biosphere exchange does not match the selected background "
                                f"reference catalog: {key[0]} | {'::'.join(key[1])} | {key[2]}."
                            ),
                            path=path,
                        )
                    )

    return report


def _messages_to_issues(messages: list[str], *, severity: str, code: str) -> list[Issue]:
    issues = []
    for message in messages:
        match = _CONTEXT_PATTERN.match(message)
        issues.append(
            Issue(
                severity=severity,
                code=code,
                message=message,
                path=match.group("path") if match else "",
            )
        )
    return issues


def _duplicate_identity_issues(data: list[dict]) -> list[Issue]:
    identities = [
        (
            str(activity.get("name") or ""),
            str(activity.get("reference product") or ""),
            str(activity.get("location") or ""),
        )
        for activity in data
    ]
    counts = Counter(identities)
    return [
        Issue(
            severity="error",
            code="duplicate_dataset_identity",
            message=f"Dataset identity must be unique: {identity!r}.",
            path=repr(identity),
        )
        for identity, count in counts.items()
        if count > 1
    ]


def _production_identity_issues(data: list[dict]) -> list[Issue]:
    issues = []
    for activity_index, activity in enumerate(data):
        production = [
            exchange
            for exchange in activity.get("exchanges", [])
            if isinstance(exchange, dict) and exchange.get("type") == "production"
        ]
        if len(production) != 1:
            continue
        activity_identity = _technosphere_key(activity)
        production_identity = _technosphere_key(production[0])
        if activity_identity != production_identity:
            issues.append(
                Issue(
                    severity="error",
                    code="production_identity_mismatch",
                    message=(
                        "Production exchange identity must match its activity by name, "
                        "reference product, location, and unit."
                    ),
                    path=f"activity[{activity_index}]",
                    suggested_fix="Call normalize() or correct the production exchange fields.",
                )
            )
    return issues


def _technosphere_key(value: dict) -> tuple[str, str, str, str]:
    return (
        str(value.get("name") or ""),
        str(value.get("reference product") or value.get("product") or ""),
        str(value.get("location") or ""),
        str(value.get("unit") or ""),
    )
