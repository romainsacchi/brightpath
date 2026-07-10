from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from . import DATA_DIR
from .background.catalogs import CatalogIntegrityError, catalog_provider_from_environment
from .core.context import BiosphereProfile, TechnosphereProfile
from .models import BackgroundProfile, default_biosphere_profile


@dataclass(frozen=True)
class BackgroundCatalog:
    """Exact technosphere and biosphere identities for one profile."""

    profile: BackgroundProfile
    technosphere: frozenset[tuple[str, str, str, str]]
    biosphere: frozenset[tuple[str, tuple[str, ...], str]]


def catalog_directory() -> Path:
    """Return the active reference-catalog directory.

    ``BRIGHTPATH_REFERENCE_DIR`` overrides the packaged directory when set.
    """

    configured = (os.getenv("BRIGHTPATH_REFERENCE_DIR") or "").strip()
    if configured:
        return Path(configured).expanduser()
    return DATA_DIR / "export" / "reference_catalogs"


def catalog_filename(profile: BackgroundProfile) -> str:
    """Return the canonical JSON filename for *profile*."""

    normalized = profile.normalized()
    return (
        f"{normalized.family or 'unknown'}"
        f"__{normalized.version or 'unknown'}"
        f"__{normalized.system_model or 'unknown'}.json"
    )


def catalog_path(profile: BackgroundProfile) -> Path:
    """Return the expected path for *profile* in the active directory."""

    return catalog_directory() / catalog_filename(profile)


def collect_technosphere_catalog_entries(
    inventory_data: list[dict],
) -> frozenset[tuple[str, str, str, str]]:
    """Collect dataset identities suitable for a technosphere catalog."""

    return frozenset(
        (
            str(activity.get("name") or ""),
            str(activity.get("reference product") or ""),
            str(activity.get("location") or ""),
            str(activity.get("unit") or ""),
        )
        for activity in inventory_data
    )


def collect_biosphere_catalog_entries(
    inventory_data: list[dict],
) -> frozenset[tuple[str, tuple[str, ...], str]]:
    """Collect biosphere identities from canonical inventory data."""

    return frozenset(
        (
            str(exchange.get("name") or ""),
            tuple(str(item) for item in exchange.get("categories", ())),
            str(exchange.get("unit") or ""),
        )
        for activity in inventory_data
        for exchange in activity.get("exchanges", [])
        if exchange.get("type") == "biosphere"
    )


def write_background_catalog(
    profile: BackgroundProfile,
    *,
    technosphere: Iterable[tuple[str, str, str, str]],
    biosphere: Iterable[tuple[str, tuple[str, ...], str]],
    output_dir: Path | None = None,
) -> Path:
    """Write a reference-catalog JSON file and return its absolute path."""

    normalized = profile.normalized()
    directory = output_dir or catalog_directory()
    directory.mkdir(parents=True, exist_ok=True)

    payload = {
        "profile": {
            "family": normalized.family,
            "version": normalized.version,
            "system_model": normalized.system_model,
        },
        "technosphere": [
            {
                "name": name,
                "reference_product": reference_product,
                "location": location,
                "unit": unit,
            }
            for name, reference_product, location, unit in sorted(set(technosphere))
        ],
        "biosphere": [
            {
                "name": name,
                "categories": list(categories),
                "unit": unit,
            }
            for name, categories, unit in sorted(set(biosphere))
        ],
    }

    path = (directory / catalog_filename(normalized)).resolve()
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def available_catalog_profiles(*, family: str = "") -> list[BackgroundProfile]:
    """List legacy combined profiles exposed by the active provider stack.

    This compatibility projection is derived from exact technosphere profiles.
    New code should query :class:`~brightpath.background.CatalogProvider`
    directly so technosphere and biosphere availability remain independent.
    """

    selected_family = BackgroundProfile(family=family).normalized().family if family else ""
    provider = catalog_provider_from_environment()
    technosphere_profiles = provider.technosphere_profiles()
    if (os.getenv("BRIGHTPATH_REFERENCE_DIR") or "").strip():
        directory = catalog_directory()
        technosphere_profiles = tuple(
            profile
            for profile in technosphere_profiles
            if (directory / catalog_filename(BackgroundProfile.from_technosphere_profile(profile))).is_file()
        )
    profiles = (BackgroundProfile.from_technosphere_profile(profile) for profile in technosphere_profiles)
    return [profile for profile in profiles if not selected_family or profile.family == selected_family]


def load_background_catalog(profile: BackgroundProfile) -> BackgroundCatalog:
    """Load exact catalog axes and combine them at the legacy boundary.

    A :class:`BackgroundProfile` cannot express an independent biosphere, so
    this compatibility API uses the documented legacy biosphere default. New
    callers that need different axes should use
    :class:`~brightpath.background.CatalogProvider` directly. In particular,
    UVEK 2025 resolves to the ecoinvent 3.10 biosphere catalog.

    :raises FileNotFoundError: If the corresponding catalog is unavailable.
    :raises brightpath.background.CatalogIntegrityError: If a provider returns
        a catalog for a different exact profile or a resource fails integrity
        validation.
    """

    normalized = profile.normalized()
    technosphere_profile = normalized.to_technosphere_profile()
    biosphere_profile = default_biosphere_profile(technosphere_profile)
    provider = catalog_provider_from_environment()
    technosphere = provider.load_technosphere(technosphere_profile)
    biosphere = provider.load_biosphere(biosphere_profile)

    _require_exact_profile("technosphere", technosphere.profile, technosphere_profile)
    _require_exact_profile("biosphere", biosphere.profile, biosphere_profile)
    return BackgroundCatalog(
        profile=normalized,
        technosphere=technosphere.identities,
        biosphere=biosphere.identities,
    )


def _require_exact_profile(
    axis: str,
    actual: TechnosphereProfile | BiosphereProfile,
    expected: TechnosphereProfile | BiosphereProfile,
) -> None:
    if actual != expected:
        raise CatalogIntegrityError(
            f"Provider returned {actual.label()} for requested {axis} profile {expected.label()}."
        )
