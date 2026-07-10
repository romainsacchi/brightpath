"""Read-only validation of canonical inventory background links."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from brightpath.background.catalogs import (
    BiosphereCatalog,
    BiosphereIdentity,
    CatalogIntegrityError,
    CatalogNotFoundError,
    CatalogProvider,
    TechnosphereCatalog,
    TechnosphereIdentity,
)
from brightpath.core.context import BackgroundContext, BiosphereProfile, TechnosphereProfile
from brightpath.core.reports import Issue, Severity, StageKind, StageReport

_STAGE = StageKind.BACKGROUND_VALIDATION


@dataclass(frozen=True)
class _Link:
    path: str
    identity: tuple


def validate_background_links(
    inventory: list[dict],
    context: BackgroundContext,
    provider: CatalogProvider,
    *,
    foreground_technosphere_targets: Iterable[TechnosphereIdentity] = (),
) -> StageReport:
    """Validate technosphere and biosphere links against exact catalogs.

    Dataset identities in *inventory* are treated as foreground technosphere
    targets. Additional targets can be supplied for inventories that are
    validated as one part of a larger foreground system. Technosphere and
    biosphere catalogs are loaded independently from the injected *provider*.

    The function never modifies *inventory* and reports link or catalog
    failures instead of raising them. Invalid service arguments still raise
    :class:`TypeError`, as they indicate a caller contract error rather than an
    invalid inventory link.

    :param inventory: Canonical inventory dictionaries to inspect.
    :param context: Exact technosphere and biosphere profiles to validate.
    :param provider: Explicit source of exact background catalogs.
    :param foreground_technosphere_targets: Additional four-field foreground
        identities in ``(name, reference product, location, unit)`` order.
    :return: An immutable background-validation stage report.
    """

    _validate_arguments(inventory, context, provider)
    foreground = set()
    for dataset in inventory:
        identity = _technosphere_identity(dataset)
        if all(identity):
            foreground.add(identity)
    foreground.update(identity for identity in _foreground_targets(foreground_technosphere_targets) if all(identity))

    technosphere_links, biosphere_links = _collect_links(inventory)
    technosphere_issues, technosphere_metrics = _validate_technosphere(
        technosphere_links,
        foreground,
        context.technosphere,
        provider,
    )
    biosphere_issues, biosphere_metrics = _validate_biosphere(
        biosphere_links,
        context.biosphere,
        provider,
    )

    return StageReport(
        stage=_STAGE,
        label="background links",
        issues=tuple(technosphere_issues + biosphere_issues),
        metrics={
            "context": {
                "technosphere": _technosphere_profile_details(context.technosphere),
                "biosphere": _biosphere_profile_details(context.biosphere),
            },
            "technosphere": technosphere_metrics,
            "biosphere": biosphere_metrics,
        },
    )


def _validate_arguments(inventory: object, context: object, provider: object) -> None:
    if not isinstance(inventory, list) or any(not isinstance(dataset, dict) for dataset in inventory):
        raise TypeError("inventory must be a list of dictionaries.")
    if not isinstance(context, BackgroundContext):
        raise TypeError("context must be a BackgroundContext.")
    if not isinstance(provider, CatalogProvider):
        raise TypeError("provider must implement CatalogProvider.")


def _foreground_targets(targets: Iterable[TechnosphereIdentity]) -> set[TechnosphereIdentity]:
    normalized = set()
    try:
        iterator = iter(targets)
    except TypeError as error:
        raise TypeError("foreground_technosphere_targets must be an iterable of four-field identities.") from error
    for target in iterator:
        if isinstance(target, (str, bytes)) or not isinstance(target, Sequence) or len(target) != 4:
            raise TypeError("foreground_technosphere_targets must contain four-field identities.")
        normalized.add(tuple(str(part or "") for part in target))
    return normalized


def _collect_links(inventory: list[dict]) -> tuple[list[_Link], list[_Link]]:
    technosphere = []
    biosphere = []
    for dataset_index, dataset in enumerate(inventory):
        exchanges = dataset.get("exchanges", ())
        if not isinstance(exchanges, Sequence) or isinstance(exchanges, (str, bytes, bytearray)):
            continue
        for exchange_index, exchange in enumerate(exchanges):
            if not isinstance(exchange, Mapping):
                continue
            path = f"datasets[{dataset_index}].exchanges[{exchange_index}]"
            if exchange.get("type") == "technosphere":
                technosphere.append(_Link(path, _technosphere_identity(exchange)))
            elif exchange.get("type") == "biosphere":
                biosphere.append(_Link(path, _biosphere_identity(exchange)))
    return technosphere, biosphere


def _validate_technosphere(
    links: list[_Link],
    foreground: set[TechnosphereIdentity],
    profile: TechnosphereProfile,
    provider: CatalogProvider,
) -> tuple[list[Issue], dict[str, Any]]:
    foreground_links = [link for link in links if link.identity in foreground]
    catalog_candidates = [link for link in links if link.identity not in foreground]
    issues: list[Issue] = []
    catalog: TechnosphereCatalog | None = None
    catalog_status = "not_required"

    if catalog_candidates:
        try:
            catalog = provider.load_technosphere(profile)
            if catalog.profile != profile:
                raise CatalogIntegrityError(
                    f"Provider returned {catalog.profile.label()} for requested profile {profile.label()}."
                )
            catalog_status = "loaded"
        except CatalogNotFoundError as error:
            catalog = None
            catalog_status = "missing"
            issues.append(_catalog_issue("technosphere", "missing", profile, str(error)))
        except CatalogIntegrityError as error:
            catalog = None
            catalog_status = "invalid"
            issues.append(_catalog_issue("technosphere", "invalid", profile, str(error)))

    catalog_links: list[_Link] = []
    unresolved: list[_Link] = []
    if catalog is not None:
        catalog_links = [link for link in catalog_candidates if link.identity in catalog.identities]
        unresolved = [link for link in catalog_candidates if link.identity not in catalog.identities]
        issues.extend(_unresolved_technosphere_issue(link, profile) for link in unresolved)

    resolved_count = len(foreground_links) + len(catalog_links)
    unchecked_count = len(catalog_candidates) if catalog is None else 0
    metrics = _link_metrics(
        total=len(links),
        resolved=resolved_count,
        unresolved=len(unresolved),
        unchecked=unchecked_count,
    )
    metrics.update(
        {
            "foreground_links": len(foreground_links),
            "catalog_links": len(catalog_links),
            "catalog": _catalog_metrics(catalog_status, catalog),
        }
    )
    return issues, metrics


def _validate_biosphere(
    links: list[_Link],
    profile: BiosphereProfile,
    provider: CatalogProvider,
) -> tuple[list[Issue], dict[str, Any]]:
    issues: list[Issue] = []
    catalog: BiosphereCatalog | None = None
    catalog_status = "not_required"

    if links:
        try:
            catalog = provider.load_biosphere(profile)
            if catalog.profile != profile:
                raise CatalogIntegrityError(
                    f"Provider returned {catalog.profile.label()} for requested profile {profile.label()}."
                )
            catalog_status = "loaded"
        except CatalogNotFoundError as error:
            catalog = None
            catalog_status = "missing"
            issues.append(_catalog_issue("biosphere", "missing", profile, str(error)))
        except CatalogIntegrityError as error:
            catalog = None
            catalog_status = "invalid"
            issues.append(_catalog_issue("biosphere", "invalid", profile, str(error)))

    resolved: list[_Link] = []
    unresolved: list[_Link] = []
    if catalog is not None:
        resolved = [link for link in links if link.identity in catalog.identities]
        unresolved = [link for link in links if link.identity not in catalog.identities]
        issues.extend(_unresolved_biosphere_issue(link, profile) for link in unresolved)

    metrics = _link_metrics(
        total=len(links),
        resolved=len(resolved),
        unresolved=len(unresolved),
        unchecked=len(links) if catalog is None else 0,
    )
    metrics["catalog"] = _catalog_metrics(catalog_status, catalog)
    return issues, metrics


def _link_metrics(*, total: int, resolved: int, unresolved: int, unchecked: int) -> dict[str, Any]:
    denominator = total or 1
    return {
        "total_links": total,
        "resolved_links": resolved,
        "unresolved_links": unresolved,
        "unchecked_links": unchecked,
        "coverage": resolved / denominator if total else 1.0,
        "validation_coverage": (resolved + unresolved) / denominator if total else 1.0,
    }


def _catalog_metrics(status: str, catalog: TechnosphereCatalog | BiosphereCatalog | None) -> dict[str, Any]:
    return {
        "status": status,
        "identity_count": len(catalog.identities) if catalog is not None else 0,
        "digest": catalog.digest if catalog is not None else "",
        "schema_version": catalog.schema_version if catalog is not None else None,
        "source": catalog.source if catalog is not None else "",
    }


def _catalog_issue(
    axis: str,
    condition: str,
    profile: TechnosphereProfile | BiosphereProfile,
    reason: str,
) -> Issue:
    details = (
        _technosphere_profile_details(profile)
        if isinstance(profile, TechnosphereProfile)
        else _biosphere_profile_details(profile)
    )
    details["reason"] = reason
    return Issue(
        severity=Severity.ERROR,
        code=f"background.{axis}_catalog_{condition}",
        message=f"The exact {axis} catalog for {profile.label()} is {condition}.",
        stage=_STAGE,
        path=f"background.{axis}",
        details=details,
        suggested_fix=f"Install or repair the exact {axis} catalog before validating these links.",
    )


def _unresolved_technosphere_issue(link: _Link, profile: TechnosphereProfile) -> Issue:
    name, reference_product, location, unit = link.identity
    return Issue(
        severity=Severity.ERROR,
        code="background.technosphere_link_unresolved",
        message=(
            "Technosphere exchange does not match a foreground dataset or the exact " f"{profile.label()} catalog."
        ),
        stage=_STAGE,
        path=link.path,
        details={
            "identity": {
                "name": name,
                "reference_product": reference_product,
                "location": location,
                "unit": unit,
            },
            "profile": _technosphere_profile_details(profile),
        },
        suggested_fix="Correct the exchange identity or select the matching technosphere profile.",
    )


def _unresolved_biosphere_issue(link: _Link, profile: BiosphereProfile) -> Issue:
    name, categories, unit = link.identity
    return Issue(
        severity=Severity.ERROR,
        code="background.biosphere_link_unresolved",
        message=f"Biosphere exchange does not match the exact {profile.label()} catalog.",
        stage=_STAGE,
        path=link.path,
        details={
            "identity": {"name": name, "categories": categories, "unit": unit},
            "profile": _biosphere_profile_details(profile),
        },
        suggested_fix="Correct the exchange identity or select the matching biosphere profile.",
    )


def _technosphere_identity(value: Mapping) -> TechnosphereIdentity:
    return (
        str(value.get("name") or ""),
        str(value.get("reference product") or value.get("product") or ""),
        str(value.get("location") or ""),
        str(value.get("unit") or ""),
    )


def _biosphere_identity(value: Mapping) -> BiosphereIdentity:
    raw_categories = value.get("categories", ())
    if isinstance(raw_categories, str):
        categories = (raw_categories,)
    elif isinstance(raw_categories, Sequence):
        categories = tuple(str(category) for category in raw_categories)
    else:
        categories = ()
    return str(value.get("name") or ""), categories, str(value.get("unit") or "")


def _technosphere_profile_details(profile: TechnosphereProfile) -> dict[str, str]:
    return {
        "family": profile.family,
        "version": profile.version,
        "system_model": profile.system_model,
    }


def _biosphere_profile_details(profile: BiosphereProfile) -> dict[str, str]:
    return {"family": profile.family, "version": profile.version}
