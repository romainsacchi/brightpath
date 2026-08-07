"""Resolve foreground datasets to categories observed in SimaPro references."""

from __future__ import annotations

import csv
import hashlib
import json
import re
import unicodedata
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from difflib import SequenceMatcher
from enum import Enum
from functools import lru_cache
from pathlib import Path

from brightpath import DATA_DIR
from brightpath.models import BackgroundProfile
from brightpath.units import normalize_unit

_CATEGORY_DIRECTORY = DATA_DIR / "export" / "simapro_categories"
_MANIFEST_FILENAME = "RESOURCE_MANIFEST.json"
_SUPPORTED_CATEGORY_TYPES = frozenset(
    {
        "material",
        "energy",
        "transport",
        "processing",
        "use",
        "waste treatment",
        "waste scenario",
    }
)
_CATEGORY_TYPE_ALIASES = {"materials": "material"}
_RESOURCE_BY_PROFILE = {
    ("ecoinvent", "3.9", "cutoff"): "ecoinvent__3.9.1__cutoff.csv",
    ("ecoinvent", "3.9.1", "cutoff"): "ecoinvent__3.9.1__cutoff.csv",
}
_MARKET_PREFIXES = ("market for ", "market group for ")
_ROLE_COMPONENTS = frozenset({"market", "transformation"})
_TOKEN_STOPWORDS = frozenset(
    {
        "a",
        "an",
        "and",
        "at",
        "by",
        "for",
        "from",
        "in",
        "of",
        "on",
        "or",
        "the",
        "to",
        "with",
    }
)


class SimaProCategoryMode(str, Enum):
    """How the SimaPro writer handles foreground production categories."""

    PRESERVE = "preserve"
    INFER_EXISTING = "infer_existing"


@dataclass(frozen=True)
class SimaProCategoryReference:
    """One aggregated product-to-category observation from a SimaPro export."""

    reference_product: str
    unit: str
    process_role: str
    category: str
    dataset_count: int


@dataclass(frozen=True)
class SimaProCategoryCatalog:
    """Versioned SimaPro category observations for one background profile."""

    profile: BackgroundProfile
    source_version: str
    references: tuple[SimaProCategoryReference, ...]
    categories: frozenset[str]
    digest: str
    source: str


@dataclass(frozen=True)
class SimaProCategoryResolution:
    """Auditable outcome of category preservation or inference."""

    original_category: str
    category: str
    confidence: float
    method: str
    candidates: tuple[str, ...] = ()
    reason: str = ""

    @property
    def changed(self) -> bool:
        """Whether inference selected a different category."""

        return bool(self.category and self.category != self.original_category)


def coerce_simapro_category_mode(value: SimaProCategoryMode | str) -> SimaProCategoryMode:
    """Return a validated :class:`SimaProCategoryMode`."""

    try:
        return value if isinstance(value, SimaProCategoryMode) else SimaProCategoryMode(value)
    except (TypeError, ValueError) as error:
        allowed = ", ".join(mode.value for mode in SimaProCategoryMode)
        raise ValueError(f"category_mode must be one of: {allowed}.") from error


def normalize_simapro_category(value: object) -> str:
    """Return a canonical slash-separated SimaPro category path."""

    parts = [part.strip() for part in re.split(r"[/\\]", str(value or "")) if part.strip()]
    if not parts:
        return ""
    category_type = _CATEGORY_TYPE_ALIASES.get(parts[0].lower(), parts[0].lower())
    if category_type not in _SUPPORTED_CATEGORY_TYPES:
        supported = ", ".join(sorted(_SUPPORTED_CATEGORY_TYPES))
        raise ValueError(f"Unsupported SimaPro category type {parts[0]!r}; expected one of: {supported}.")
    return "/".join((category_type, *parts[1:]))


def split_simapro_category(value: object) -> tuple[str, str]:
    """Return the category type and SimaPro backslash-separated subcategory."""

    normalized = normalize_simapro_category(value)
    if not normalized:
        raise ValueError("SimaPro category must not be empty.")
    category_type, *parts = normalized.split("/")
    return category_type, "\\".join(parts)


@lru_cache(maxsize=None)
def load_simapro_category_catalog(profile: BackgroundProfile) -> SimaProCategoryCatalog:
    """Load and integrity-check the packaged catalog for an exact supported profile."""

    normalized = profile.normalized()
    key = (normalized.family, normalized.version, normalized.system_model)
    try:
        filename = _RESOURCE_BY_PROFILE[key]
    except KeyError as error:
        raise FileNotFoundError(f"No SimaPro category catalog is available for {normalized.label()}.") from error

    manifest_path = _CATEGORY_DIRECTORY / _MANIFEST_FILENAME
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"Could not load SimaPro category manifest {manifest_path}.") from error
    if manifest.get("schema_version") != 1:
        raise ValueError(f"Unsupported SimaPro category manifest schema in {manifest_path}.")
    resource = next((item for item in manifest.get("resources", ()) if item.get("file") == filename), None)
    if not isinstance(resource, dict):
        raise ValueError(f"SimaPro category manifest does not describe {filename}.")
    if resource.get("schema_version") != 1:
        raise ValueError(f"Unsupported SimaPro category resource schema for {filename}.")

    path = _CATEGORY_DIRECTORY / filename
    payload = path.read_bytes()
    digest = hashlib.sha256(payload).hexdigest()
    if digest != resource.get("sha256") or len(payload) != resource.get("size"):
        raise ValueError(f"SimaPro category resource integrity check failed for {path}.")

    references = _read_category_references(path)
    if len(references) != resource.get("reference_rows"):
        raise ValueError(f"SimaPro category resource row count does not match its manifest for {path}.")
    categories = frozenset(reference.category for reference in references)
    if len(categories) != resource.get("categories"):
        raise ValueError(f"SimaPro category resource category count does not match its manifest for {path}.")

    source_profile = resource.get("source_profile") or {}
    if source_profile != {"family": "ecoinvent", "version": "3.9.1", "system_model": "cutoff"}:
        raise ValueError(f"SimaPro category resource {filename} has an unexpected source profile.")
    return SimaProCategoryCatalog(
        profile=normalized,
        source_version=str(source_profile.get("version") or normalized.version),
        references=references,
        categories=categories,
        digest=digest,
        source=str(path.resolve()),
    )


def resolve_simapro_category(
    activity: Mapping,
    *,
    profile: BackgroundProfile,
    current_category: object = "",
    catalog: SimaProCategoryCatalog | None = None,
) -> SimaProCategoryResolution:
    """Resolve an activity to an observed category without mutating it.

    An already observed category is preserved. Otherwise exact product, unit,
    and process-role observations are preferred. Fuzzy matching is used only
    when several similar reference products agree on a category hierarchy.
    """

    original = normalize_simapro_category(current_category) if current_category else ""
    try:
        reference = catalog or load_simapro_category_catalog(profile)
    except FileNotFoundError as error:
        return SimaProCategoryResolution(
            original_category=original,
            category=original,
            confidence=0.0,
            method="catalog_unavailable",
            reason=str(error),
        )
    if original and original in reference.categories:
        return SimaProCategoryResolution(original, original, 1.0, "existing")

    production = next(
        (
            exchange
            for exchange in (activity.get("exchanges") or ())
            if isinstance(exchange, Mapping) and exchange.get("type") == "production"
        ),
        {},
    )
    product = str(
        activity.get("reference product")
        or activity.get("product")
        or production.get("reference product")
        or production.get("product")
        or ""
    ).strip()
    unit = str(normalize_unit(str(activity.get("unit") or production.get("unit") or "").strip()))
    role = _process_role(str(activity.get("name") or production.get("name") or ""))
    if not product:
        return SimaProCategoryResolution(
            original,
            original,
            0.0,
            "unresolved",
            reason="The activity has no reference product for category inference.",
        )

    exact = [
        item
        for item in reference.references
        if _search_key(item.reference_product) == _search_key(product)
        and item.process_role == role
        and (not unit or item.unit == unit)
    ]
    if not exact:
        exact = [
            item
            for item in reference.references
            if _search_key(item.reference_product) == _search_key(product) and item.process_role == role
        ]
    exact_choice = _dominant_category(exact)
    if exact_choice is not None:
        category, confidence, candidates = exact_choice
        return SimaProCategoryResolution(original, category, confidence, "exact_product", candidates)

    fuzzy_choice = _fuzzy_consensus(product, unit, role, reference.references, reference.categories)
    if fuzzy_choice is not None:
        category, confidence, candidates = fuzzy_choice
        return SimaProCategoryResolution(original, category, confidence, "fuzzy_hierarchy", candidates)

    candidates = tuple(category for category, _count in _rank_categories(exact)[:3])
    return SimaProCategoryResolution(
        original,
        original,
        0.0,
        "unresolved",
        candidates,
        "No sufficiently specific, unambiguous category was found in the reference catalog.",
    )


def _read_category_references(path: Path) -> tuple[SimaProCategoryReference, ...]:
    expected = {"reference_product", "unit", "process_role", "category", "dataset_count"}
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if set(reader.fieldnames or ()) != expected:
            raise ValueError(f"Unexpected SimaPro category resource columns in {path}.")
        rows = []
        for row in reader:
            category = normalize_simapro_category(row["category"])
            count = int(row["dataset_count"])
            if count < 1:
                raise ValueError(f"Invalid dataset_count in SimaPro category resource {path}.")
            rows.append(
                SimaProCategoryReference(
                    reference_product=row["reference_product"].strip(),
                    unit=str(normalize_unit(row["unit"].strip())),
                    process_role=row["process_role"].strip(),
                    category=category,
                    dataset_count=count,
                )
            )
    return tuple(rows)


def _process_role(name: str) -> str:
    normalized = name.casefold().strip()
    return "market" if normalized.startswith(_MARKET_PREFIXES) else "transformation"


def _dominant_category(
    references: Iterable[SimaProCategoryReference],
) -> tuple[str, float, tuple[str, ...]] | None:
    ranked = _rank_categories(references)
    if not ranked:
        return None
    total = sum(count for _category, count in ranked)
    top_category, top_count = ranked[0]
    second_count = ranked[1][1] if len(ranked) > 1 else 0
    share = top_count / total
    if len(ranked) == 1:
        confidence = 1.0
    elif share >= 0.8 and top_count >= 2 * second_count:
        confidence = min(0.99, 0.8 + 0.19 * share)
    else:
        return None
    return top_category, confidence, tuple(category for category, _count in ranked[:3])


def _rank_categories(references: Iterable[SimaProCategoryReference]) -> list[tuple[str, int]]:
    counts = Counter()
    for reference in references:
        counts[reference.category] += reference.dataset_count
    return sorted(counts.items(), key=lambda item: (-item[1], item[0].casefold()))


def _fuzzy_consensus(
    product: str,
    unit: str,
    role: str,
    references: Iterable[SimaProCategoryReference],
    categories: frozenset[str],
) -> tuple[str, float, tuple[str, ...]] | None:
    query_tokens = _tokens(product)
    if len(query_tokens) < 2:
        return None

    by_product: dict[str, list[SimaProCategoryReference]] = defaultdict(list)
    for reference in references:
        if reference.process_role == role and (not unit or reference.unit == unit):
            by_product[reference.reference_product].append(reference)
    if not by_product and unit:
        for reference in references:
            if reference.process_role == role:
                by_product[reference.reference_product].append(reference)

    scored = []
    for candidate, rows in by_product.items():
        candidate_tokens = _tokens(candidate)
        shared = query_tokens.intersection(candidate_tokens)
        if len(shared) < 2:
            continue
        dice = 2 * len(shared) / (len(query_tokens) + len(candidate_tokens))
        sequence = SequenceMatcher(None, _search_key(product), _search_key(candidate)).ratio()
        score = 0.65 * dice + 0.35 * sequence
        if score >= 0.55:
            scored.append((score, candidate, rows))
    if not scored:
        return None

    scored.sort(key=lambda item: (-item[0], item[1].casefold()))
    best = scored[0][0]
    selected = [item for item in scored if item[0] >= max(0.55, best - 0.14)][:12]
    candidate_categories = []
    for score, _candidate, rows in selected:
        for row in rows:
            candidate_categories.extend([row.category] * max(1, round(score * row.dataset_count)))
    prefix = _common_semantic_prefix(candidate_categories)
    if len(prefix) < 3:
        return None

    category = _category_for_role(prefix, role, categories)
    if category is None:
        return None
    support = sum(1 for value in candidate_categories if _has_prefix(value, prefix)) / len(candidate_categories)
    if support < 0.85:
        return None
    confidence = min(0.94, 0.55 * best + 0.35 * support + 0.05 * min(len(prefix), 4))
    if confidence < 0.78:
        return None
    candidates = tuple(candidate for _score, candidate, _rows in selected[:5])
    return category, confidence, candidates


def _common_semantic_prefix(categories: Iterable[str]) -> tuple[str, ...]:
    paths = []
    for category in categories:
        parts = tuple(part for part in category.split("/") if part.casefold() not in _ROLE_COMPONENTS)
        if parts:
            paths.append(parts)
    if not paths:
        return ()
    prefix = list(paths[0])
    for path in paths[1:]:
        length = 0
        for left, right in zip(prefix, path, strict=False):
            if left.casefold() != right.casefold():
                break
            length += 1
        prefix = prefix[:length]
        if not prefix:
            break
    return tuple(prefix)


def _category_for_role(prefix: tuple[str, ...], role: str, categories: frozenset[str]) -> str | None:
    direct = "/".join((*prefix, "Market" if role == "market" else "Transformation"))
    if direct in categories:
        return direct

    prefix_text = "/".join(prefix) + "/"
    role_component = "market" if role == "market" else "transformation"
    candidates = [
        category
        for category in categories
        if category.startswith(prefix_text)
        and role_component in {part.casefold() for part in category.split("/")[len(prefix) :]}
    ]
    if not candidates:
        return None
    return min(candidates, key=lambda category: (len(category.split("/")), category.casefold()))


def _has_prefix(category: str, prefix: tuple[str, ...]) -> bool:
    parts = tuple(part for part in category.split("/") if part.casefold() not in _ROLE_COMPONENTS)
    return tuple(part.casefold() for part in parts[: len(prefix)]) == tuple(part.casefold() for part in prefix)


def _tokens(value: str) -> frozenset[str]:
    return frozenset(token for token in _search_key(value).split() if token not in _TOKEN_STOPWORDS and len(token) > 1)


def _search_key(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value.casefold())
    return " ".join(re.findall(r"[a-z0-9]+", normalized))
