from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from . import DATA_DIR
from .models import BackgroundProfile


@dataclass(frozen=True)
class BackgroundCatalog:
    profile: BackgroundProfile
    technosphere: frozenset[tuple[str, str, str, str]]
    biosphere: frozenset[tuple[str, tuple[str, ...], str]]


def catalog_directory() -> Path:
    configured = (os.getenv("BRIGHTPATH_REFERENCE_DIR") or "").strip()
    if configured:
        return Path(configured).expanduser()
    return DATA_DIR / "export" / "reference_catalogs"


def catalog_filename(profile: BackgroundProfile) -> str:
    normalized = profile.normalized()
    return (
        f"{normalized.family or 'unknown'}"
        f"__{normalized.version or 'unknown'}"
        f"__{normalized.system_model or 'unknown'}.json"
    )


def catalog_path(profile: BackgroundProfile) -> Path:
    return catalog_directory() / catalog_filename(profile)


def collect_technosphere_catalog_entries(
    inventory_data: list[dict],
) -> frozenset[tuple[str, str, str, str]]:
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
    directory = catalog_directory()
    if not directory.is_dir():
        return []

    profiles: list[BackgroundProfile] = []
    for path in sorted(directory.glob("*.json")):
        parts = path.stem.split("__")
        if len(parts) != 3:
            continue
        profile = BackgroundProfile(
            family=parts[0],
            version=parts[1],
            system_model=parts[2],
        ).normalized()
        if family and profile.family != family.strip().lower():
            continue
        profiles.append(profile)
    return profiles


def load_background_catalog(profile: BackgroundProfile) -> BackgroundCatalog:
    normalized = profile.normalized()
    path = catalog_path(normalized)
    if not path.is_file():
        raise FileNotFoundError(f"Background catalog is missing for {normalized}: {path}")

    payload = json.loads(path.read_text(encoding="utf-8"))
    loaded_profile = BackgroundProfile(
        family=payload.get("profile", {}).get("family", normalized.family),
        version=payload.get("profile", {}).get("version", normalized.version),
        system_model=payload.get("profile", {}).get("system_model", normalized.system_model),
    ).normalized()

    technosphere = frozenset(
        (
            str(row["name"]),
            str(row["reference_product"]),
            str(row["location"]),
            str(row["unit"]),
        )
        for row in payload.get("technosphere", [])
    )
    biosphere = frozenset(
        (
            str(row["name"]),
            tuple(str(item) for item in row["categories"]),
            str(row["unit"]),
        )
        for row in payload.get("biosphere", [])
    )
    return BackgroundCatalog(
        profile=loaded_profile,
        technosphere=technosphere,
        biosphere=biosphere,
    )
