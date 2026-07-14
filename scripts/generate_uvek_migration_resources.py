"""Generate heuristic ecoinvent-to-UVEK 2025 migration resources.

The generated crosswalks are compatibility aids, not scientific equivalence
claims. Curated legacy correspondences take precedence; remaining identities
are matched deterministically to the closest compatible packaged target.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

import yaml
from bw2io.units import normalize_units as normalize_unit

ECOINVENT_VERSIONS = ("3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12")
ECOINVENT_SYSTEM_MODELS = ("cutoff", "consequential")
UVEK_VERSION = "2025"
UVEK_SYSTEM_MODEL = "cutoff"

_TOKEN = re.compile(r"[a-z0-9]+")
_STOP_WORDS = {
    "a",
    "an",
    "and",
    "as",
    "at",
    "by",
    "for",
    "from",
    "in",
    "into",
    "market",
    "of",
    "on",
    "or",
    "process",
    "processing",
    "production",
    "service",
    "the",
    "to",
    "treatment",
    "with",
}
_LOCATION_PRIORITY = ("CH", "RER", "GLO", "RoW")
_UNIT_FALLBACKS = {
    "litre": ("cubic meter", 0.001),
    "guest night": ("unit", 1.0),
    "kilogram day": ("kilogram", 1.0),
    "person kilometer": ("person-kilometer", 1.0),
}


@dataclass(frozen=True)
class TechnosphereIdentity:
    name: str
    reference_product: str
    location: str
    unit: str

    @classmethod
    def from_row(cls, row: dict) -> "TechnosphereIdentity":
        return cls(
            str(row["name"]),
            str(row["reference_product"]),
            str(row["location"]),
            str(row["unit"]),
        )

    def as_rule_identity(self) -> dict:
        return {
            "name": self.name,
            "reference product": self.reference_product,
            "location": self.location,
            "unit": self.unit,
        }


@dataclass(frozen=True)
class BiosphereIdentity:
    name: str
    categories: tuple[str, ...]
    unit: str

    @classmethod
    def from_row(cls, row: dict) -> "BiosphereIdentity":
        return cls(str(row["name"]), tuple(str(value) for value in row["categories"]), str(row["unit"]))

    def as_rule_identity(self) -> dict:
        return {
            "name": self.name,
            "categories": list(self.categories),
            "unit": self.unit,
        }


def _normalized_text(value: str) -> str:
    return " ".join(_TOKEN.findall(value.lower()))


def _tokens(*values: str) -> frozenset[str]:
    return frozenset(
        token
        for value in values
        for token in _TOKEN.findall(value.lower())
        if token not in _STOP_WORDS and len(token) > 1
    )


def _character_ngrams(value: str, size: int = 3) -> frozenset[str]:
    normalized = f"  {_normalized_text(value)}  "
    return frozenset(normalized[index : index + size] for index in range(max(1, len(normalized) - size + 1)))


def _set_similarity(left: frozenset[str], right: frozenset[str]) -> float:
    union = left | right
    return len(left & right) / len(union) if union else 0.0


class _TechnosphereMatcher:
    def __init__(
        self,
        targets: Iterable[TechnosphereIdentity],
        curated: dict[TechnosphereIdentity, TechnosphereIdentity],
        conversion_factors: dict[str, float],
    ) -> None:
        self.targets = tuple(sorted(set(targets), key=_technosphere_key))
        self.target_set = frozenset(self.targets)
        self.curated = curated
        self.conversion_factors = conversion_factors
        self.terms = tuple(_tokens(target.name, target.reference_product) for target in self.targets)
        self.names = tuple(_normalized_text(target.name) for target in self.targets)
        self.products = tuple(_normalized_text(target.reference_product) for target in self.targets)
        self.name_grams = tuple(_character_ngrams(target.name) for target in self.targets)
        self.product_grams = tuple(_character_ngrams(target.reference_product) for target in self.targets)
        self.term_frequency = Counter(token for terms in self.terms for token in terms)
        self.by_unit: dict[str, list[int]] = defaultdict(list)
        self.by_token: dict[tuple[str, str], list[int]] = defaultdict(list)
        for index, (target, terms) in enumerate(zip(self.targets, self.terms, strict=True)):
            unit = normalize_unit(target.unit)
            self.by_unit[unit].append(index)
            for token in terms:
                self.by_token[(unit, token)].append(index)

    def match(self, source: TechnosphereIdentity) -> tuple[TechnosphereIdentity, str, float, float | None]:
        curated = self.curated.get(source)
        if curated in self.target_set:
            if normalize_unit(source.unit) == normalize_unit(curated.unit):
                return curated, "curated_legacy", 1.0, None
            curated_factor = self.conversion_factors.get(source.name)
            if curated_factor is not None:
                return curated, "curated_legacy", 1.0, curated_factor

        source_unit = normalize_unit(source.unit)
        target_unit, conversion_factor = self._target_unit(source_unit)
        candidates = self._candidates(source, target_unit)
        source_terms = _tokens(source.name, source.reference_product)
        source_name_grams = _character_ngrams(source.name)
        source_product_grams = _character_ngrams(source.reference_product)

        def rank(index: int) -> tuple[float, int, int]:
            target = self.targets[index]
            target_terms = self.terms[index]
            union = source_terms | target_terms
            token_score = len(source_terms & target_terms) / len(union) if union else 0.0
            name_score = _set_similarity(source_name_grams, self.name_grams[index])
            product_score = _set_similarity(source_product_grams, self.product_grams[index])
            location_bonus = 0.08 if source.location == target.location else 0.0
            if target.location in _LOCATION_PRIORITY:
                location_bonus += (len(_LOCATION_PRIORITY) - _LOCATION_PRIORITY.index(target.location)) * 0.005
            score = 0.42 * product_score + 0.30 * name_score + 0.20 * token_score + location_bonus
            location_rank = -_LOCATION_PRIORITY.index(target.location) if target.location in _LOCATION_PRIORITY else -9
            return score, location_rank, -index

        selected = max(candidates, key=rank)
        score = min(1.0, rank(selected)[0])
        return self.targets[selected], "heuristic_similarity", round(score, 6), conversion_factor

    def _target_unit(self, source_unit: str) -> tuple[str, float | None]:
        if source_unit in self.by_unit:
            return source_unit, None
        fallback = _UNIT_FALLBACKS.get(source_unit)
        if fallback and normalize_unit(fallback[0]) in self.by_unit:
            return normalize_unit(fallback[0]), fallback[1]
        raise ValueError(f"UVEK 2025 has no target-unit fallback for ecoinvent unit {source_unit!r}.")

    @staticmethod
    def _conversion_factor(source_unit: str, target_unit: str) -> float | None:
        normalized_source = normalize_unit(source_unit)
        normalized_target = normalize_unit(target_unit)
        if normalized_source == normalized_target:
            return None
        fallback = _UNIT_FALLBACKS.get(normalized_source)
        if fallback and normalize_unit(fallback[0]) == normalized_target:
            return fallback[1]
        raise ValueError(f"Missing conversion factor from {source_unit!r} to {target_unit!r}.")

    def _candidates(self, source: TechnosphereIdentity, target_unit: str) -> tuple[int, ...]:
        terms = _tokens(source.name, source.reference_product)
        indexed = [self.by_token[(target_unit, token)] for token in terms if self.by_token[(target_unit, token)]]
        indexed.sort(key=len)
        candidates: set[int] = set()
        for values in indexed[:5]:
            candidates.update(values)
            if len(candidates) >= 500:
                break
        if not candidates:
            candidates.update(self.by_unit[target_unit])
        if len(candidates) <= 60:
            return tuple(sorted(candidates))

        source_name = _normalized_text(source.name)
        source_product = _normalized_text(source.reference_product)

        def cheap_rank(index: int) -> tuple[float, int]:
            shared = terms & self.terms[index]
            token_weight = sum(1.0 / math.log2(2 + self.term_frequency[token]) for token in shared)
            substring_bonus = (
                1.0 if source_product in self.products[index] or self.products[index] in source_product else 0.0
            )
            location_bonus = 0.5 if source.location == self.targets[index].location else 0.0
            name_bonus = 0.25 if source_name in self.names[index] or self.names[index] in source_name else 0.0
            return token_weight + substring_bonus + location_bonus + name_bonus, -index

        return tuple(sorted(candidates, key=cheap_rank, reverse=True)[:60])


class _BiosphereMatcher:
    def __init__(self, targets: Iterable[BiosphereIdentity]) -> None:
        self.targets = tuple(sorted(set(targets), key=_biosphere_key))
        self.target_set = frozenset(self.targets)
        self.terms = tuple(_tokens(target.name, *target.categories) for target in self.targets)
        self.names = tuple(_normalized_text(target.name) for target in self.targets)
        self.categories = tuple(_normalized_text(" ".join(target.categories)) for target in self.targets)
        self.name_grams = tuple(_character_ngrams(target.name) for target in self.targets)
        self.category_grams = tuple(_character_ngrams(" ".join(target.categories)) for target in self.targets)
        self.term_frequency = Counter(token for terms in self.terms for token in terms)
        self.by_unit: dict[str, list[int]] = defaultdict(list)
        self.by_name_unit: dict[tuple[str, str], list[int]] = defaultdict(list)
        self.by_token: dict[tuple[str, str], list[int]] = defaultdict(list)
        for index, (target, terms) in enumerate(zip(self.targets, self.terms, strict=True)):
            unit = normalize_unit(target.unit)
            self.by_unit[unit].append(index)
            self.by_name_unit[(unit, target.name)].append(index)
            for token in terms:
                self.by_token[(unit, token)].append(index)

    def match(self, source: BiosphereIdentity) -> tuple[BiosphereIdentity, str, float]:
        if source in self.target_set:
            return source, "identity", 1.0
        unit = normalize_unit(source.unit)
        if unit not in self.by_unit:
            raise ValueError(f"ecoinvent 3.10 biosphere has no target with unit {source.unit!r}.")
        exact_name = self.by_name_unit.get((unit, source.name), ())
        candidates = tuple(exact_name) or self._candidates(source, unit)
        source_terms = _tokens(source.name, *source.categories)
        source_name_grams = _character_ngrams(source.name)
        source_category_grams = _character_ngrams(" ".join(source.categories))

        def rank(index: int) -> tuple[float, int]:
            target = self.targets[index]
            target_terms = self.terms[index]
            union = source_terms | target_terms
            token_score = len(source_terms & target_terms) / len(union) if union else 0.0
            name_score = _set_similarity(source_name_grams, self.name_grams[index])
            category_score = _set_similarity(source_category_grams, self.category_grams[index])
            compartment_bonus = (
                0.10
                if source.categories and target.categories and source.categories[0] == target.categories[0]
                else 0.0
            )
            return 0.55 * name_score + 0.20 * category_score + 0.15 * token_score + compartment_bonus, -index

        selected = max(candidates, key=rank)
        return self.targets[selected], "heuristic_similarity", round(min(1.0, rank(selected)[0]), 6)

    def _candidates(self, source: BiosphereIdentity, unit: str) -> tuple[int, ...]:
        terms = _tokens(source.name, *source.categories)
        indexed = [self.by_token[(unit, token)] for token in terms if self.by_token[(unit, token)]]
        indexed.sort(key=len)
        candidates: set[int] = set()
        for values in indexed[:5]:
            candidates.update(values)
            if len(candidates) >= 500:
                break
        if not candidates:
            candidates.update(self.by_unit[unit])
        if len(candidates) <= 60:
            return tuple(sorted(candidates))

        source_name = _normalized_text(source.name)
        source_categories = _normalized_text(" ".join(source.categories))

        def cheap_rank(index: int) -> tuple[float, int]:
            shared = terms & self.terms[index]
            token_weight = sum(1.0 / math.log2(2 + self.term_frequency[token]) for token in shared)
            name_bonus = 1.0 if source_name == self.names[index] else 0.0
            category_bonus = 0.5 if source_categories == self.categories[index] else 0.0
            return token_weight + name_bonus + category_bonus, -index

        return tuple(sorted(candidates, key=cheap_rank, reverse=True)[:60])


def _load_catalog(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Catalog {path} must contain an object.")
    return payload


def _load_curated_mapping(path: Path) -> dict[TechnosphereIdentity, TechnosphereIdentity]:
    result = {}
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            source = TechnosphereIdentity(row["name"], row["reference product"], row["location"], row["unit"])
            target = TechnosphereIdentity(
                row["UVEK name"],
                row["UVEK ref prod"],
                row["UVEK location"],
                row["UVEK unit"],
            )
            result[source] = target
    return result


def _load_conversion_factors(path: Path) -> dict[str, float]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    result = {}
    for name, details in payload.items():
        factor = details.get("factor") if isinstance(details, dict) else None
        if isinstance(factor, (int, float)) and not isinstance(factor, bool) and math.isfinite(float(factor)):
            result[str(name)] = float(factor)
    return result


def _resource_metadata(*, name: str, description: str, axis: str, source: dict, target: dict) -> dict:
    return {
        "schema_version": 1,
        "status": "active",
        "name": name,
        "description": description,
        "axis": axis,
        "quality": "heuristic",
        "source_profile": source,
        "target_profile": target,
        "methodology": {
            "curated_precedence": "brightpath/data/export/ecoinvent_to_uvek_mapping.csv",
            "fallback": "deterministic token and string similarity with unit-compatible targets",
            "scientific_equivalence": False,
        },
        "replace": [],
        "disaggregate": [],
        "delete": [],
    }


def build_resources(catalog_directory: Path, curated_path: Path, conversion_factors_path: Path) -> tuple[dict, dict]:
    uvek_payload = _load_catalog(catalog_directory / "uvek__2025__cutoff.json")
    uvek_targets = {TechnosphereIdentity.from_row(row) for row in uvek_payload["technosphere"]}
    technosphere_sources: set[TechnosphereIdentity] = set()
    biosphere_sources: set[BiosphereIdentity] = set()
    source_catalogs = []
    for version in ECOINVENT_VERSIONS:
        for system_model in ECOINVENT_SYSTEM_MODELS:
            filename = f"ecoinvent__{version}__{system_model}.json"
            payload = _load_catalog(catalog_directory / filename)
            source_catalogs.append(filename)
            technosphere_sources.update(TechnosphereIdentity.from_row(row) for row in payload["technosphere"])
            biosphere_sources.update(BiosphereIdentity.from_row(row) for row in payload["biosphere"])

    biosphere_310 = _load_catalog(catalog_directory / "ecoinvent__3.10__cutoff.json")
    biosphere_targets = {BiosphereIdentity.from_row(row) for row in biosphere_310["biosphere"]}
    technosphere_matcher = _TechnosphereMatcher(
        uvek_targets,
        _load_curated_mapping(curated_path),
        _load_conversion_factors(conversion_factors_path),
    )
    biosphere_matcher = _BiosphereMatcher(biosphere_targets)

    technosphere = _resource_metadata(
        name="ecoinvent-3.x-to-uvek-2025-technosphere",
        description=(
            "Heuristic compatibility mapping from packaged ecoinvent 3.x technosphere identities to existing "
            "UVEK 2025 activities. It is intended to enable conversion workflows and is not an equivalence claim."
        ),
        axis="technosphere",
        source={
            "family": "ecoinvent",
            "versions": list(ECOINVENT_VERSIONS),
            "system_models": list(ECOINVENT_SYSTEM_MODELS),
        },
        target={"family": "uvek", "version": UVEK_VERSION, "system_model": UVEK_SYSTEM_MODEL},
    )
    technosphere["source_catalogs"] = source_catalogs
    method_counts: Counter[str] = Counter()
    confidence_total = 0.0
    for source in sorted(technosphere_sources, key=_technosphere_key):
        target, method, confidence, conversion_factor = technosphere_matcher.match(source)
        rule = {
            "source": source.as_rule_identity(),
            "target": target.as_rule_identity(),
            "mapping_method": method,
            "confidence": confidence,
        }
        if conversion_factor is not None:
            rule["conversion_factor"] = conversion_factor
        technosphere["replace"].append(rule)
        method_counts[method] += 1
        confidence_total += confidence
    technosphere["coverage"] = {
        "source_identities": len(technosphere_sources),
        "target_identities": len(uvek_targets),
        "mapped_identities": len(technosphere["replace"]),
        "mapping_methods": dict(sorted(method_counts.items())),
        "mean_confidence": round(confidence_total / len(technosphere_sources), 6),
    }

    biosphere = _resource_metadata(
        name="ecoinvent-3.x-to-ecoinvent-3.10-biosphere-for-uvek-2025",
        description=(
            "Heuristic compatibility mapping from packaged ecoinvent 3.x biosphere identities to the "
            "ecoinvent 3.10 biosphere used with UVEK 2025."
        ),
        axis="biosphere",
        source={"family": "ecoinvent", "versions": list(ECOINVENT_VERSIONS)},
        target={"family": "ecoinvent", "version": "3.10"},
    )
    method_counts = Counter()
    confidence_total = 0.0
    for source in sorted(biosphere_sources, key=_biosphere_key):
        target, method, confidence = biosphere_matcher.match(source)
        biosphere["replace"].append(
            {
                "source": source.as_rule_identity(),
                "target": target.as_rule_identity(),
                "mapping_method": method,
                "confidence": confidence,
            }
        )
        method_counts[method] += 1
        confidence_total += confidence
    biosphere["coverage"] = {
        "source_identities": len(biosphere_sources),
        "target_identities": len(biosphere_targets),
        "mapped_identities": len(biosphere["replace"]),
        "mapping_methods": dict(sorted(method_counts.items())),
        "mean_confidence": round(confidence_total / len(biosphere_sources), 6),
    }
    return technosphere, biosphere


def _technosphere_key(identity: TechnosphereIdentity) -> tuple[str, str, str, str]:
    return identity.name, identity.reference_product, identity.location, identity.unit


def _biosphere_key(identity: BiosphereIdentity) -> tuple[str, tuple[str, ...], str]:
    return identity.name, identity.categories, identity.unit


def _write_resource(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--catalog-directory",
        type=Path,
        default=Path("brightpath/data/export/reference_catalogs"),
    )
    parser.add_argument(
        "--curated-mapping",
        type=Path,
        default=Path("brightpath/data/export/ecoinvent_to_uvek_mapping.csv"),
    )
    parser.add_argument(
        "--conversion-factors",
        type=Path,
        default=Path("brightpath/data/export/uvek_conversion_factors.yaml"),
    )
    parser.add_argument(
        "--output-directory",
        type=Path,
        default=Path("brightpath/data/migrations/uvek"),
    )
    args = parser.parse_args(argv)
    technosphere, biosphere = build_resources(
        args.catalog_directory,
        args.curated_mapping,
        args.conversion_factors,
    )
    _write_resource(args.output_directory / "ecoinvent-to-uvek-2025.json", technosphere)
    _write_resource(args.output_directory / "ecoinvent-to-ecoinvent-3.10-biosphere.json", biosphere)
    print(
        f"Wrote {len(technosphere['replace'])} technosphere and "
        f"{len(biosphere['replace'])} biosphere mapping rules."
    )


if __name__ == "__main__":
    main()
