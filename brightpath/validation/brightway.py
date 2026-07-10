from __future__ import annotations

import re
from collections import Counter
from typing import Iterable

from brightpath.background import (
    CatalogProvider,
    catalog_provider_from_environment,
    validate_background_links,
)
from brightpath.models import InventoryDocument, Issue, ValidationReport
from brightpath.utils import inspect_brightway_inventory

_CONTEXT_PATTERN = re.compile(r"^(?P<path>activity\[\d+\](?:\.exchanges\[\d+\])?):\s*(?P<message>.+)$")


def validate_brightway_inventory(
    document: InventoryDocument,
    *,
    check_background_links: bool = True,
    additional_foreground_targets: Iterable[tuple[str, str, str, str]] = (),
    catalog_provider: CatalogProvider | None = None,
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
    provider = catalog_provider or catalog_provider_from_environment()
    background = validate_background_links(
        data,
        document.context.background,
        provider,
        foreground_technosphere_targets=additional_foreground_targets,
    )
    report.issues.extend(_background_issues(background.issues))

    return report


def _background_issues(issues) -> list[Issue]:
    code_aliases = {
        "background.technosphere_link_unresolved": "unknown_technosphere_target",
        "background.biosphere_link_unresolved": "unknown_biosphere_flow",
        "background.technosphere_catalog_missing": "background_catalog_missing",
        "background.biosphere_catalog_missing": "background_catalog_missing",
    }
    return [
        Issue(
            severity=issue.severity.value,
            code=code_aliases.get(issue.code, issue.code),
            message=issue.message,
            path=issue.path.replace("datasets[", "activity[", 1),
            suggested_fix=issue.suggested_fix,
        )
        for issue in issues
    ]


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
