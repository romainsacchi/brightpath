"""Process-category resolution for openLCA JSON-LD exports."""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass

from brightpath.formats.openlca_references import OpenLCAReferenceCatalog
from brightpath.models import BackgroundProfile
from brightpath.profiles.simapro_categories import (
    SimaProCategoryCatalog,
    SimaProCategoryReference,
    normalize_simapro_category,
    resolve_simapro_category,
)
from brightpath.units import normalize_unit

_REFERENCE_TAXONOMY_PROFILE = BackgroundProfile("ecoinvent", "3.9.1", "cutoff")
_UNCATEGORIZED_FOREGROUND = "foreground/Uncategorized"


@dataclass(frozen=True)
class OpenLCAProcessCategoryResolution:
    """Auditable result of resolving one openLCA process category."""

    category: str
    method: str
    confidence: float
    candidates: tuple[str, ...] = ()


@dataclass(frozen=True)
class OpenLCAProcessCategoryCatalog:
    """Native openLCA category observations prepared for category inference."""

    inference_catalog: SimaProCategoryCatalog
    native_by_canonical: dict[str, str]
    fallback_category: str


def build_openlca_process_category_catalog(
    reference_catalog: OpenLCAReferenceCatalog | None,
) -> OpenLCAProcessCategoryCatalog | None:
    """Build an inference view over exact process categories in a target database."""

    if reference_catalog is None:
        return None

    observations: Counter[tuple[str, str, str, str]] = Counter()
    native_counts: dict[str, Counter[str]] = {}
    for identity, reference in reference_catalog.technosphere.items():
        process_name, reference_product, _location, unit = identity
        canonical = _canonical_category(reference.category)
        if not canonical:
            continue
        role = _process_role(process_name)
        observations[(reference_product, str(normalize_unit(unit)), role, canonical)] += 1
        native_counts.setdefault(canonical, Counter())[reference.category] += 1

    references = tuple(
        SimaProCategoryReference(
            reference_product=reference_product,
            unit=unit,
            process_role=role,
            category=category,
            dataset_count=count,
        )
        for (reference_product, unit, role, category), count in sorted(observations.items())
    )
    native_by_canonical = {
        canonical: sorted(counts.items(), key=lambda item: (-item[1], item[0].casefold()))[0][0]
        for canonical, counts in native_counts.items()
    }
    fallback = native_by_canonical.get(_canonical_category("material/Others/unspecified"))
    if fallback is None:
        fallback = _UNCATEGORIZED_FOREGROUND
    return OpenLCAProcessCategoryCatalog(
        inference_catalog=SimaProCategoryCatalog(
            profile=_REFERENCE_TAXONOMY_PROFILE,
            source_version="target-openlca",
            references=references,
            categories=frozenset(native_by_canonical),
            digest="",
            source=reference_catalog.source,
        ),
        native_by_canonical=native_by_canonical,
        fallback_category=fallback,
    )


def resolve_openlca_process_category(
    dataset: Mapping,
    *,
    current_category: object = "",
    catalog: OpenLCAProcessCategoryCatalog | None = None,
) -> OpenLCAProcessCategoryResolution:
    """Resolve an openLCA category without mutating *dataset*.

    Explicit BrightPath and existing openLCA categories take precedence. A
    supplied SimaPro production category is translated to the exact target
    hierarchy when possible. Otherwise, the existing product, unit, and
    process-role resolver operates over category observations extracted from
    the target openLCA database. Ambiguous or unknown datasets use an existing
    target fallback instead of being left at the process-tree root.
    """

    explicit = _category_path(dataset.get("openlca category"))
    if explicit:
        return OpenLCAProcessCategoryResolution(explicit, "explicit", 1.0)

    existing = str(current_category or "").strip()
    if existing:
        return OpenLCAProcessCategoryResolution(existing, "existing", 1.0)

    production = next(
        (
            exchange
            for exchange in dataset.get("exchanges", ())
            if isinstance(exchange, Mapping) and exchange.get("type") == "production"
        ),
        {},
    )
    supplied_simapro = production.get("simapro category") if isinstance(production, Mapping) else ""
    if supplied_simapro:
        try:
            category = normalize_simapro_category(supplied_simapro)
        except ValueError:
            category = _category_path(supplied_simapro)
        native = _native_category(category, catalog)
        if native:
            return OpenLCAProcessCategoryResolution(native, "simapro_category", 1.0)
        if category and catalog is None:
            return OpenLCAProcessCategoryResolution(category, "simapro_category", 1.0)

    if catalog is None:
        return OpenLCAProcessCategoryResolution(
            category=_UNCATEGORIZED_FOREGROUND,
            method="foreground_fallback",
            confidence=0.0,
        )

    resolution = resolve_simapro_category(
        dataset,
        profile=_REFERENCE_TAXONOMY_PROFILE,
        catalog=catalog.inference_catalog,
    )
    native = _native_category(resolution.category, catalog)
    if native:
        return OpenLCAProcessCategoryResolution(
            category=native,
            method=f"target_{resolution.method}",
            confidence=resolution.confidence,
            candidates=resolution.candidates,
        )
    return OpenLCAProcessCategoryResolution(
        category=catalog.fallback_category,
        method="target_fallback",
        confidence=0.0,
        candidates=resolution.candidates,
    )


def _category_path(value: object) -> str:
    parts = [part.strip() for part in str(value or "").split("/") if part.strip()]
    return "/".join(parts)


def _canonical_category(value: object) -> str:
    parts = [part.strip().casefold() for part in re.split(r"[/\\]", str(value or "")) if part.strip()]
    return "/".join(parts)


def _native_category(
    category: object,
    catalog: OpenLCAProcessCategoryCatalog | None,
) -> str:
    if not category or catalog is None:
        return ""
    return catalog.native_by_canonical.get(_canonical_category(category), "")


def _process_role(name: str) -> str:
    normalized = name.casefold().strip()
    return "market" if normalized.startswith(("market for ", "market group for ")) else "transformation"
