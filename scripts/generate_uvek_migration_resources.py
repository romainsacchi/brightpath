"""Generate heuristic ecoinvent-to-UVEK 2025 migration resources.

The generated crosswalks are compatibility aids, not scientific equivalence
claims. Curated legacy correspondences take precedence; remaining identities
are matched deterministically to the closest compatible packaged target.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

import yaml
from bw2io.units import normalize_units as normalize_unit

from brightpath.migrations.schema import (
    MIGRATION_SCHEMA,
    MIGRATION_SCHEMA_VERSION,
    dumps_migration_resource,
    intern_review,
    normalize_migration_resource,
    profile_list,
    rule_identifier,
    validate_migration_resource,
)

if __package__ is None:
    from review_uvek_reference_signs import apply_packaged_sign_review
    from scope_uvek_system_models import cutoff_uvek_resource
else:
    from scripts.review_uvek_reference_signs import apply_packaged_sign_review
    from scripts.scope_uvek_system_models import cutoff_uvek_resource

ECOINVENT_VERSIONS = ("3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12")
ECOINVENT_SYSTEM_MODELS = ("cutoff",)
# Frozen scientific fingerprints still include the retired model. These inputs
# are replayed only for historical authoring, before the publication projection.
_HISTORICAL_REVIEW_SYSTEM_MODELS = ("cutoff", "consequential")
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
        indexed = [
            self.by_token[(target_unit, token)] for token in sorted(terms) if self.by_token[(target_unit, token)]
        ]
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
            token_weight = sum(1.0 / math.log2(2 + self.term_frequency[token]) for token in sorted(shared))
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
        indexed = [self.by_token[(unit, token)] for token in sorted(terms) if self.by_token[(unit, token)]]
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
            token_weight = sum(1.0 / math.log2(2 + self.term_frequency[token]) for token in sorted(shared))
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
        "schema": MIGRATION_SCHEMA,
        "schema_version": MIGRATION_SCHEMA_VERSION,
        "status": "active",
        "name": name,
        "description": description,
        "axis": axis,
        "quality": "heuristic",
        "source_profiles": profile_list(source),
        "target_profile": target,
        "reverse_policy": "explicit_only",
        "recipes": {},
        "reviews": {},
        "coverage": {},
        "replace": [],
        "disaggregate": [],
        "delete": [],
    }


def apply_heuristic_candidate_baselines(resource: dict, payload: dict, targets: set[tuple]) -> None:
    """Restore frozen authoring inputs without granting any runtime review or approval.

    These inputs precede the unchanged reviewed overrides and bound sign audit.
    They retain historical candidate identities after deterministic shortlist fixes.
    """
    fields = ("name", "reference product", "location", "unit")
    required = {"id", "source", "target", "mapping_method", "confidence"}
    if (
        payload.get("schema_version") != 1
        or payload.get("status") != "authoring_baseline_only"
        or payload.get("resource") != resource["name"]
        or not isinstance(payload.get("candidates"), list)
    ):
        raise ValueError("Invalid heuristic authoring baseline scope.")
    indexed = {rule["id"]: rule for rule in resource["replace"]}
    seen, pending = set(), []
    for row in payload["candidates"]:
        candidate = row["candidate"]
        if set(candidate) != required or any(
            set(candidate[role]) != set(fields)
            or any(not isinstance(candidate[role][f], str) or not candidate[role][f] for f in fields)
            for role in ("source", "target")
        ):
            raise ValueError("A heuristic authoring baseline contains only an exact candidate and score.")
        digest = hashlib.sha256(json.dumps(candidate, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        binding = hashlib.sha256(
            json.dumps([candidate["source"], candidate["target"]], sort_keys=True, ensure_ascii=False).encode()
        ).hexdigest()
        current = indexed.get(candidate["id"])
        confidence = candidate["confidence"]
        if (
            candidate["id"] in seen
            or candidate["id"] != rule_identifier(resource["name"], candidate["source"])
            or digest != row.get("candidate_sha256")
            or binding != row.get("reference_binding_sha256")
            or current is None
            or current["source"] != candidate["source"]
            or current.get("mapping_method") != "heuristic_similarity"
            or candidate["mapping_method"] != "heuristic_similarity"
            or set(current) - required - {"conversion_factor"}
            or current.get("conversion_factor", 1) != 1
            or normalize_unit(candidate["source"]["unit"]) != normalize_unit(candidate["target"]["unit"])
            or tuple(candidate["target"][f] for f in fields) not in targets
            or isinstance(confidence, bool)
            or not isinstance(confidence, (int, float))
            or not math.isfinite(confidence)
            or not 0 <= confidence <= 1
        ):
            raise ValueError("Stale, reviewed, incompatible or duplicate heuristic authoring baseline.")
        seen.add(candidate["id"])
        pending.append((current, candidate))
    for current, candidate in pending:
        current.update(target=dict(candidate["target"]), confidence=candidate["confidence"])


def apply_reviewed_overrides(resource: dict, overrides: dict, targets: set[tuple]) -> None:
    """Apply audited exact-identity decisions, rejecting stale or invalid inputs."""

    fields = ("name", "reference product", "location", "unit")

    def identity(record):
        return tuple(record[field] for field in fields)

    indexed = {identity(rule["source"]): rule for rule in resource["replace"]}
    seen = set()
    pending = []
    for decision in overrides["decisions"]:
        source = identity(decision["source"])
        target = identity(decision["target"])
        if source in seen or source not in indexed or target not in targets:
            raise ValueError(f"Invalid or duplicate reviewed UVEK identity: {source}")
        seen.add(source)
        current = indexed[source]
        if identity(current["target"]) not in (identity(decision["previous_target"]), target):
            raise ValueError(f"Reviewed UVEK decision conflicts with regenerated target: {source}")
        normalized_source_unit = normalize_unit(source[3])
        normalized_target_unit = normalize_unit(target[3])
        passenger_distance_alias = {normalized_source_unit, normalized_target_unit} == {
            "person kilometer",
            "person-kilometer",
        }
        if normalized_source_unit != normalized_target_unit and not passenger_distance_alias:
            raise ValueError(f"Reviewed UVEK decision changes units: {source}")
        confidence = decision["confidence"]
        if isinstance(confidence, bool) or not isinstance(confidence, (int, float)) or not 0 <= confidence <= 1:
            raise ValueError(f"Invalid reviewed confidence: {source}")
        pending.append((current, decision, passenger_distance_alias))
    for current, decision, passenger_distance_alias in pending:
        current.update(
            target=decision["target"],
            mapping_method=decision["mapping_method"].removeprefix("clic_"),
            confidence=decision["confidence"],
        )
        # Detailed comparison history stays in the decision ledger. Runtime
        # reviews explain the selected mapping and its limitations only.
        provenance = {
            "rationale": decision["reason"],
            "limitations": decision.get("limitations", []),
        }
        current["review_ref"] = intern_review(resource, provenance)
        current.pop("review_provenance", None)
        if passenger_distance_alias:
            current["conversion_factor"] = 1.0
        elif "reference_flow_sign_review" not in current:
            current.pop("conversion_factor", None)
    resource["coverage"]["mapping_methods"] = dict(
        sorted(Counter(rule["mapping_method"] for rule in resource["replace"]).items())
    )
    resource["coverage"]["mean_confidence"] = round(
        sum(rule["confidence"] for rule in resource["replace"]) / len(resource["replace"]), 6
    )
    referenced = {
        rule["review_ref"]
        for operation in ("replace", "disaggregate", "delete")
        for rule in resource.get(operation, [])
        if "review_ref" in rule
    }
    referenced.update(
        entry["review_ref"]
        for rule in resource["replace"]
        for entry in rule.get("reference_flow_sign_review", {}).get("profiles", [])
        if "review_ref" in entry
    )
    resource["reviews"] = {key: value for key, value in resource.get("reviews", {}).items() if key in referenced}


def _build_historical_review_resources(
    catalog_directory: Path, curated_path: Path, conversion_factors_path: Path
) -> tuple[dict, dict]:
    """Replay frozen historical evidence; this output is not an executable route resource."""
    uvek_payload = _load_catalog(catalog_directory / "uvek__2025__cutoff.json")
    uvek_targets = {TechnosphereIdentity.from_row(row) for row in uvek_payload["technosphere"]}
    technosphere_sources: set[TechnosphereIdentity] = set()
    biosphere_sources: set[BiosphereIdentity] = set()
    for version in ECOINVENT_VERSIONS:
        for system_model in _HISTORICAL_REVIEW_SYSTEM_MODELS:
            filename = f"ecoinvent__{version}__{system_model}.json"
            payload = _load_catalog(catalog_directory / filename)
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
            "system_models": list(_HISTORICAL_REVIEW_SYSTEM_MODELS),
        },
        target={"family": "uvek", "version": UVEK_VERSION, "system_model": UVEK_SYSTEM_MODEL},
    )
    method_counts: Counter[str] = Counter()
    confidence_total = 0.0
    for source in sorted(technosphere_sources, key=_technosphere_key):
        target, method, confidence, conversion_factor = technosphere_matcher.match(source)
        rule = {
            "id": rule_identifier(technosphere["name"], source.as_rule_identity()),
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
        "unresolved_identities": 0,
        "mapping_methods": dict(sorted(method_counts.items())),
        "mean_confidence": round(confidence_total / len(technosphere_sources), 6),
    }
    baseline_path = (
        Path(__file__).resolve().parents[1] / "brightpath/data/export/uvek_heuristic_candidate_baselines.json"
    )
    apply_heuristic_candidate_baselines(
        technosphere,
        json.loads(baseline_path.read_text(encoding="utf-8")),
        {_technosphere_key(target) for target in uvek_targets},
    )
    overrides_path = Path(__file__).resolve().parents[1] / "brightpath/data/export/uvek_reviewed_overrides.json"
    apply_reviewed_overrides(
        technosphere,
        json.loads(overrides_path.read_text(encoding="utf-8")),
        {_technosphere_key(target) for target in uvek_targets},
    )

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
                "id": rule_identifier(biosphere["name"], source.as_rule_identity()),
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
        "unresolved_identities": 0,
        "mapping_methods": dict(sorted(method_counts.items())),
        "mean_confidence": round(confidence_total / len(biosphere_sources), 6),
    }
    apply_packaged_sign_review(technosphere, Path(__file__).resolve().parents[1])
    validate_migration_resource(technosphere)
    validate_migration_resource(biosphere)
    return technosphere, biosphere


def apply_pilot_reviews(technosphere: dict) -> None:
    """Replay inspected pilot corrections independently of system-model projection."""
    if __package__:
        from scripts.review_uvek_pilot_materials import (
            apply_cogen_heat_exchanger_review,
            apply_cdr_small_biomethane_heat_review,
            apply_european_delivered_water_review,
            apply_pilot_material_review,
            apply_pilot_paint_market_review,
            apply_pilot_silver_market_review,
        )
        from scripts.review_uvek_pilot_incinerators import apply_pilot_incinerator_review
        from scripts.review_uvek_pilot_landfills import apply_pilot_landfill_review
        from scripts.review_uvek_pilot_generator import apply_pilot_generator_fuel_review
        from scripts.review_uvek_pilot_grid_converter import apply_pilot_grid_converter_review
        from scripts.review_uvek_pilot_copper_supply import apply_pilot_copper_supply_review
        from scripts.review_uvek_pilot_wastewater import apply_pilot_wastewater_review
        from scripts.review_uvek_pilot_wood_steel_freight import apply_pilot_wood_steel_freight_review
        from scripts.review_uvek_pilot_secondary_aluminium import apply_pilot_secondary_aluminium_review
        from scripts.review_uvek_pilot_inert_landfills import apply_pilot_inert_landfill_review
        from scripts.review_uvek_pilot_low_alloyed_cast_stock import apply_pilot_low_alloyed_cast_stock_review
        from scripts.review_uvek_pilot_sanitary_inert import apply_pilot_sanitary_inert_review
        from scripts.review_uvek_pilot_inert_compositions import apply_pilot_inert_composition_review
        from scripts.review_uvek_pilot_european_scrap_steel import apply_pilot_european_scrap_steel_review
        from scripts.review_uvek_pilot_uncontrolled_markets import apply_pilot_uncontrolled_market_review
        from scripts.review_uvek_pilot_teg_supply import apply_pilot_teg_supply_review
        from scripts.review_uvek_pilot_pe_collection import apply_pilot_pe_collection_review
        from scripts.review_uvek_pilot_purge_handover import apply_pilot_purge_handover_review
        from scripts.review_uvek_pilot_kcl_site import apply_pilot_kcl_site_review
        from scripts.review_uvek_pilot_nitrate_goods import apply_pilot_nitrate_goods_review
        from scripts.review_uvek_pilot_nitrate_pe_country import apply_pilot_nitrate_pe_country_review
        from scripts.review_uvek_pilot_nitrate_pe_row import apply_pilot_nitrate_pe_row_review
        from scripts.review_uvek_pilot_nitrate_cleanfill import apply_pilot_nitrate_cleanfill_review
        from scripts.review_uvek_pilot_nitrate_wool import apply_pilot_nitrate_wool_review
        from scripts.review_uvek_pilot_nitrate_wool_europe import apply_pilot_nitrate_wool_europe_review
        from scripts.review_uvek_pilot_nitrate_inert39 import apply_pilot_nitrate_inert39_review
        from scripts.review_uvek_pilot_nitrate_ps import apply_pilot_nitrate_ps_review
        from scripts.review_uvek_pilot_nitrate_scrap import apply_pilot_nitrate_scrap_review
        from scripts.review_uvek_pilot_nitrate_hdpe import apply_pilot_nitrate_hdpe_review
        from scripts.review_uvek_pilot_nitrate_foam import apply_pilot_nitrate_foam_review
        from scripts.review_uvek_pilot_nitrate_sand import apply_pilot_nitrate_sand_review
        from scripts.review_uvek_pilot_nitrate_glass import apply_pilot_nitrate_glass_review
        from scripts.review_uvek_pilot_nitrate_water import apply_pilot_nitrate_water_review
        from scripts.review_uvek_pilot_nitrate_paint import apply_pilot_nitrate_paint_review
        from scripts.review_uvek_pilot_nitrate_concrete_delivery import apply_pilot_nitrate_concrete_delivery_review
        from scripts.review_uvek_pilot_fae_routes import apply_pilot_fae_routes_review
        from scripts.review_uvek_pilot_european_glass import apply_pilot_european_glass_review
        from scripts.review_uvek_pilot_polymer_routes import apply_pilot_polymer_routes_review
        from scripts.review_uvek_pilot_hazardous_paint import apply_pilot_hazardous_paint_review
        from scripts.review_uvek_pilot_wastewater_routes import apply_pilot_wastewater_routes_review
        from scripts.review_uvek_pilot_steel_row_routes import apply_pilot_steel_row_routes_review
        from scripts.review_uvek_pilot_ng_heat_market import apply_pilot_ng_heat_market_review
        from scripts.review_uvek_pilot_mixed_heat_market import apply_pilot_mixed_heat_market_review
        from scripts.review_uvek_pilot_nitrate_market import apply_pilot_nitrate_market_review
        from scripts.review_uvek_deferred_naocl import apply_deferred_naocl_review
        from scripts.review_uvek_deferred_concrete import apply_deferred_concrete_review
        from scripts.review_uvek_deferred_xanthate import apply_deferred_xanthate_review
        from scripts.review_uvek_deferred_wafer import apply_deferred_wafer_review
        from scripts.review_uvek_deferred_acrylic import apply_deferred_acrylic_review
        from scripts.review_uvek_partial_paperboard import apply_partial_paperboard_review
        from scripts.review_uvek_deferred_titanium import apply_deferred_titanium_review
        from scripts.review_uvek_heat_hydrogen import apply_heat_hydrogen_review
        from scripts.review_uvek_heat_dependencies import apply_heat_dependency_review
        from scripts.review_uvek_heat_profiles import apply_heat_profile_review
        from scripts.review_uvek_heat_compressed_air import apply_heat_compressed_air_review
        from scripts.review_uvek_heat_salts import apply_heat_salts_review
        from scripts.review_uvek_heat_310_utilities import apply_heat_310_utilities_review
        from scripts.review_uvek_heat_310_waste import apply_heat_310_waste_review
        from scripts.review_uvek_heat_310_routes import apply_heat_310_routes_review
        from scripts.review_uvek_heat_310_closure import apply_heat_310_closure_review
        from scripts.review_uvek_heat_38_profiles import apply_heat_38_profiles_review
        from scripts.review_uvek_heat_38_corrections import apply_heat_38_corrections_review
        from scripts.review_uvek_heat_38_services import apply_heat_38_services_review
        from scripts.review_uvek_heat_38_closure import apply_heat_38_closure_review
        from scripts.review_uvek_heat_312_markets import apply_heat_312_markets_review
        from scripts.review_uvek_cement_heat_market import apply_cement_heat_market_review
        from scripts.review_uvek_cement_treatment_39 import apply_cement_treatment_39_review
        from scripts.review_uvek_fuels_312_dependencies import apply_fuels_312_dependencies_review
        from scripts.review_uvek_fuels_39_endpoints import apply_fuels_39_endpoint_review
        from scripts.review_uvek_fuels_39_silica import apply_fuels_39_silica_review
        from scripts.review_uvek_fuels_39_molybdenum import apply_fuels_39_molybdenum_review
        from scripts.review_uvek_fuels_39_carbon import apply_fuels_39_carbon_review
        from scripts.review_uvek_fuels_39_material_markets import apply_fuels_39_material_markets_review
        from scripts.review_uvek_fuels_39_oxide_services import apply_fuels_39_oxide_services_review
        from scripts.review_uvek_fuels_39_heat_recycling import apply_fuels_39_heat_recycling_review
        from scripts.review_uvek_fuels_39_final_markets import apply_fuels_39_final_markets_review
        from scripts.review_uvek_fuels_38_services import apply_fuels_38_services_review
        from scripts.review_uvek_fuels_38_nutrients import apply_fuels_38_nutrients_review
        from scripts.review_uvek_packaging_hdpe import apply_packaging_hdpe_review
        from scripts.review_uvek_fuels_38_lime import apply_fuels_38_lime_review
        from scripts.review_uvek_fuels_38_compost import apply_fuels_38_compost_review
        from scripts.review_uvek_fuels_38_ph_agent import apply_fuels_38_ph_agent_review
        from scripts.review_uvek_fuels_38_crops import apply_fuels_38_crops_review
        from scripts.review_uvek_fuels_38_chemical_heat import apply_fuels_38_chemical_heat_review
        from scripts.review_uvek_fuels_38_water_profile import apply_fuels_38_water_profile_review
        from scripts.review_uvek_fuels_38_delivered_materials import apply_fuels_38_delivered_materials_review
        from scripts.review_uvek_fuels_38_cast_profiles import apply_fuels_38_cast_profiles_review
        from scripts.review_uvek_fuels_38_gas_wood import apply_fuels_38_gas_wood_review
        from scripts.review_uvek_fuels_37_services import apply_fuels_37_services_review
        from scripts.review_uvek_fuels_37_oils_lto import apply_fuels_37_oils_lto_review
        from scripts.review_uvek_fuels_311_remaining import apply_fuels_311_remaining_review
        from scripts.review_uvek_fuels_37_market_profiles import apply_fuels_37_market_profiles_review
        from scripts.review_uvek_fuels_37_sand_hour import apply_fuels_37_sand_hour_review
        from scripts.review_uvek_fuels_37_waste_oil import apply_fuels_37_waste_oil_review
        from scripts.review_uvek_wool_europe_direct_grid import apply_wool_europe_direct_grid_review
        from scripts.review_uvek_packaging_38_complete import apply_packaging_38_complete_review
        from scripts.review_uvek_deferred_delivered_gas import apply_deferred_delivered_gas_review
        from scripts.review_uvek_deferred_waste_markets import apply_deferred_waste_markets_review
        from scripts.review_uvek_deferred_sorting import apply_deferred_sorting_review
        from scripts.review_uvek_deferred_materials import apply_deferred_materials_review
        from scripts.review_uvek_pilot_legacy_components import apply_pilot_legacy_components_review
    else:
        from review_uvek_pilot_materials import (
            apply_cogen_heat_exchanger_review,
            apply_cdr_small_biomethane_heat_review,
            apply_european_delivered_water_review,
            apply_pilot_material_review,
            apply_pilot_paint_market_review,
            apply_pilot_silver_market_review,
        )
        from review_uvek_pilot_incinerators import apply_pilot_incinerator_review
        from review_uvek_pilot_landfills import apply_pilot_landfill_review
        from review_uvek_pilot_generator import apply_pilot_generator_fuel_review
        from review_uvek_pilot_grid_converter import apply_pilot_grid_converter_review
        from review_uvek_pilot_copper_supply import apply_pilot_copper_supply_review
        from review_uvek_pilot_wastewater import apply_pilot_wastewater_review
        from review_uvek_pilot_wood_steel_freight import apply_pilot_wood_steel_freight_review
        from review_uvek_pilot_secondary_aluminium import apply_pilot_secondary_aluminium_review
        from review_uvek_pilot_inert_landfills import apply_pilot_inert_landfill_review
        from review_uvek_pilot_low_alloyed_cast_stock import apply_pilot_low_alloyed_cast_stock_review
        from review_uvek_pilot_sanitary_inert import apply_pilot_sanitary_inert_review
        from review_uvek_pilot_inert_compositions import apply_pilot_inert_composition_review
        from review_uvek_pilot_european_scrap_steel import apply_pilot_european_scrap_steel_review
        from review_uvek_pilot_uncontrolled_markets import apply_pilot_uncontrolled_market_review
        from review_uvek_pilot_teg_supply import apply_pilot_teg_supply_review
        from review_uvek_pilot_pe_collection import apply_pilot_pe_collection_review
        from review_uvek_pilot_purge_handover import apply_pilot_purge_handover_review
        from review_uvek_pilot_kcl_site import apply_pilot_kcl_site_review
        from review_uvek_pilot_nitrate_goods import apply_pilot_nitrate_goods_review
        from review_uvek_pilot_nitrate_pe_country import apply_pilot_nitrate_pe_country_review
        from review_uvek_pilot_nitrate_pe_row import apply_pilot_nitrate_pe_row_review
        from review_uvek_pilot_nitrate_cleanfill import apply_pilot_nitrate_cleanfill_review
        from review_uvek_pilot_nitrate_wool import apply_pilot_nitrate_wool_review
        from review_uvek_pilot_nitrate_wool_europe import apply_pilot_nitrate_wool_europe_review
        from review_uvek_pilot_nitrate_inert39 import apply_pilot_nitrate_inert39_review
        from review_uvek_pilot_nitrate_ps import apply_pilot_nitrate_ps_review
        from review_uvek_pilot_nitrate_scrap import apply_pilot_nitrate_scrap_review
        from review_uvek_pilot_nitrate_hdpe import apply_pilot_nitrate_hdpe_review
        from review_uvek_pilot_nitrate_foam import apply_pilot_nitrate_foam_review
        from review_uvek_pilot_nitrate_sand import apply_pilot_nitrate_sand_review
        from review_uvek_pilot_nitrate_glass import apply_pilot_nitrate_glass_review
        from review_uvek_pilot_nitrate_water import apply_pilot_nitrate_water_review
        from review_uvek_pilot_nitrate_paint import apply_pilot_nitrate_paint_review
        from review_uvek_pilot_nitrate_concrete_delivery import apply_pilot_nitrate_concrete_delivery_review
        from review_uvek_pilot_fae_routes import apply_pilot_fae_routes_review
        from review_uvek_pilot_european_glass import apply_pilot_european_glass_review
        from review_uvek_pilot_polymer_routes import apply_pilot_polymer_routes_review
        from review_uvek_pilot_hazardous_paint import apply_pilot_hazardous_paint_review
        from review_uvek_pilot_wastewater_routes import apply_pilot_wastewater_routes_review
        from review_uvek_pilot_steel_row_routes import apply_pilot_steel_row_routes_review
        from review_uvek_pilot_ng_heat_market import apply_pilot_ng_heat_market_review
        from review_uvek_pilot_mixed_heat_market import apply_pilot_mixed_heat_market_review
        from review_uvek_pilot_nitrate_market import apply_pilot_nitrate_market_review
        from review_uvek_deferred_naocl import apply_deferred_naocl_review
        from review_uvek_deferred_concrete import apply_deferred_concrete_review
        from review_uvek_deferred_xanthate import apply_deferred_xanthate_review
        from review_uvek_deferred_wafer import apply_deferred_wafer_review
        from review_uvek_deferred_acrylic import apply_deferred_acrylic_review
        from review_uvek_partial_paperboard import apply_partial_paperboard_review
        from review_uvek_deferred_titanium import apply_deferred_titanium_review
        from review_uvek_heat_hydrogen import apply_heat_hydrogen_review
        from review_uvek_heat_dependencies import apply_heat_dependency_review
        from review_uvek_heat_profiles import apply_heat_profile_review
        from review_uvek_heat_compressed_air import apply_heat_compressed_air_review
        from review_uvek_heat_salts import apply_heat_salts_review
        from review_uvek_heat_310_utilities import apply_heat_310_utilities_review
        from review_uvek_heat_310_waste import apply_heat_310_waste_review
        from review_uvek_heat_310_routes import apply_heat_310_routes_review
        from review_uvek_heat_310_closure import apply_heat_310_closure_review
        from review_uvek_heat_38_profiles import apply_heat_38_profiles_review
        from review_uvek_heat_38_corrections import apply_heat_38_corrections_review
        from review_uvek_heat_38_services import apply_heat_38_services_review
        from review_uvek_heat_38_closure import apply_heat_38_closure_review
        from review_uvek_heat_312_markets import apply_heat_312_markets_review
        from review_uvek_cement_heat_market import apply_cement_heat_market_review
        from review_uvek_cement_treatment_39 import apply_cement_treatment_39_review
        from review_uvek_fuels_312_dependencies import apply_fuels_312_dependencies_review
        from review_uvek_fuels_39_endpoints import apply_fuels_39_endpoint_review
        from review_uvek_fuels_39_silica import apply_fuels_39_silica_review
        from review_uvek_fuels_39_molybdenum import apply_fuels_39_molybdenum_review
        from review_uvek_fuels_39_carbon import apply_fuels_39_carbon_review
        from review_uvek_fuels_39_material_markets import apply_fuels_39_material_markets_review
        from review_uvek_fuels_39_oxide_services import apply_fuels_39_oxide_services_review
        from review_uvek_fuels_39_heat_recycling import apply_fuels_39_heat_recycling_review
        from review_uvek_fuels_39_final_markets import apply_fuels_39_final_markets_review
        from review_uvek_fuels_38_services import apply_fuels_38_services_review
        from review_uvek_fuels_38_nutrients import apply_fuels_38_nutrients_review
        from review_uvek_packaging_hdpe import apply_packaging_hdpe_review
        from review_uvek_fuels_38_lime import apply_fuels_38_lime_review
        from review_uvek_fuels_38_compost import apply_fuels_38_compost_review
        from review_uvek_fuels_38_ph_agent import apply_fuels_38_ph_agent_review
        from review_uvek_fuels_38_crops import apply_fuels_38_crops_review
        from review_uvek_fuels_38_chemical_heat import apply_fuels_38_chemical_heat_review
        from review_uvek_fuels_38_water_profile import apply_fuels_38_water_profile_review
        from review_uvek_fuels_38_delivered_materials import apply_fuels_38_delivered_materials_review
        from review_uvek_fuels_38_cast_profiles import apply_fuels_38_cast_profiles_review
        from review_uvek_fuels_38_gas_wood import apply_fuels_38_gas_wood_review
        from review_uvek_fuels_37_services import apply_fuels_37_services_review
        from review_uvek_fuels_37_oils_lto import apply_fuels_37_oils_lto_review
        from review_uvek_fuels_311_remaining import apply_fuels_311_remaining_review
        from review_uvek_fuels_37_market_profiles import apply_fuels_37_market_profiles_review
        from review_uvek_fuels_37_sand_hour import apply_fuels_37_sand_hour_review
        from review_uvek_fuels_37_waste_oil import apply_fuels_37_waste_oil_review
        from review_uvek_wool_europe_direct_grid import apply_wool_europe_direct_grid_review
        from review_uvek_packaging_38_complete import apply_packaging_38_complete_review
        from review_uvek_deferred_delivered_gas import apply_deferred_delivered_gas_review
        from review_uvek_deferred_waste_markets import apply_deferred_waste_markets_review
        from review_uvek_deferred_sorting import apply_deferred_sorting_review
        from review_uvek_deferred_materials import apply_deferred_materials_review
        from review_uvek_pilot_legacy_components import apply_pilot_legacy_components_review

    apply_pilot_material_review(technosphere)
    apply_pilot_incinerator_review(technosphere)
    apply_pilot_paint_market_review(technosphere)
    apply_pilot_silver_market_review(technosphere)
    apply_cogen_heat_exchanger_review(technosphere)
    apply_cdr_small_biomethane_heat_review(technosphere)
    apply_pilot_landfill_review(technosphere)
    apply_pilot_generator_fuel_review(technosphere)
    apply_pilot_grid_converter_review(technosphere)
    apply_pilot_copper_supply_review(technosphere)
    apply_pilot_wastewater_review(technosphere)
    apply_pilot_wood_steel_freight_review(technosphere)
    apply_pilot_secondary_aluminium_review(technosphere)
    apply_pilot_inert_landfill_review(technosphere)
    apply_pilot_low_alloyed_cast_stock_review(technosphere)
    apply_pilot_sanitary_inert_review(technosphere)
    apply_pilot_inert_composition_review(technosphere)
    apply_pilot_european_scrap_steel_review(technosphere)
    apply_pilot_uncontrolled_market_review(technosphere)
    apply_pilot_teg_supply_review(technosphere)
    apply_pilot_pe_collection_review(technosphere)
    apply_pilot_purge_handover_review(technosphere)
    apply_pilot_kcl_site_review(technosphere)
    apply_pilot_nitrate_goods_review(technosphere)
    apply_pilot_nitrate_pe_country_review(technosphere)
    apply_pilot_nitrate_pe_row_review(technosphere)
    apply_pilot_nitrate_cleanfill_review(technosphere)
    apply_pilot_nitrate_wool_review(technosphere)
    apply_pilot_nitrate_wool_europe_review(technosphere)
    apply_pilot_nitrate_inert39_review(technosphere)
    apply_pilot_nitrate_ps_review(technosphere)
    apply_pilot_nitrate_scrap_review(technosphere)
    apply_pilot_nitrate_hdpe_review(technosphere)
    apply_pilot_nitrate_foam_review(technosphere)
    apply_pilot_nitrate_sand_review(technosphere)
    apply_pilot_nitrate_glass_review(technosphere)
    apply_pilot_nitrate_water_review(technosphere)
    apply_pilot_nitrate_paint_review(technosphere)
    apply_pilot_nitrate_concrete_delivery_review(technosphere)
    apply_pilot_fae_routes_review(technosphere)
    apply_pilot_european_glass_review(technosphere)
    apply_pilot_polymer_routes_review(technosphere)
    apply_pilot_hazardous_paint_review(technosphere)
    apply_pilot_wastewater_routes_review(technosphere)
    apply_pilot_steel_row_routes_review(technosphere)
    apply_pilot_ng_heat_market_review(technosphere)
    apply_pilot_mixed_heat_market_review(technosphere)
    apply_pilot_nitrate_market_review(technosphere)
    apply_pilot_legacy_components_review(technosphere)
    apply_deferred_materials_review(technosphere)
    apply_deferred_sorting_review(technosphere)
    apply_deferred_waste_markets_review(technosphere)
    apply_deferred_delivered_gas_review(technosphere)
    apply_deferred_naocl_review(technosphere)
    apply_deferred_concrete_review(technosphere)
    apply_deferred_xanthate_review(technosphere)
    apply_deferred_wafer_review(technosphere)
    apply_deferred_acrylic_review(technosphere)
    apply_partial_paperboard_review(technosphere)
    apply_deferred_titanium_review(technosphere)
    apply_heat_hydrogen_review(technosphere)
    apply_heat_dependency_review(technosphere)
    apply_heat_profile_review(technosphere)
    apply_heat_compressed_air_review(technosphere)
    apply_heat_salts_review(technosphere)
    apply_heat_310_utilities_review(technosphere)
    apply_heat_310_waste_review(technosphere)
    apply_heat_310_routes_review(technosphere)
    apply_heat_310_closure_review(technosphere)
    apply_heat_38_profiles_review(technosphere)
    apply_heat_38_corrections_review(technosphere)
    apply_heat_38_services_review(technosphere)
    apply_heat_38_closure_review(technosphere)
    apply_heat_312_markets_review(technosphere)
    apply_cement_heat_market_review(technosphere)
    apply_cement_treatment_39_review(technosphere)
    apply_fuels_312_dependencies_review(technosphere)
    apply_fuels_39_endpoint_review(technosphere)
    apply_fuels_39_silica_review(technosphere)
    apply_fuels_39_molybdenum_review(technosphere)
    apply_fuels_39_carbon_review(technosphere)
    apply_fuels_39_material_markets_review(technosphere)
    apply_fuels_39_oxide_services_review(technosphere)
    apply_fuels_39_heat_recycling_review(technosphere)
    apply_fuels_39_final_markets_review(technosphere)
    apply_fuels_38_services_review(technosphere)
    apply_fuels_38_nutrients_review(technosphere)
    apply_packaging_hdpe_review(technosphere)
    apply_fuels_38_lime_review(technosphere)
    apply_fuels_38_compost_review(technosphere)
    apply_fuels_38_ph_agent_review(technosphere)
    apply_fuels_38_crops_review(technosphere)
    apply_fuels_38_chemical_heat_review(technosphere)
    apply_fuels_38_water_profile_review(technosphere)
    apply_fuels_38_delivered_materials_review(technosphere)
    apply_fuels_38_cast_profiles_review(technosphere)
    apply_fuels_38_gas_wood_review(technosphere)
    apply_fuels_37_services_review(technosphere)
    apply_fuels_37_oils_lto_review(technosphere)
    apply_fuels_311_remaining_review(technosphere)
    apply_fuels_37_market_profiles_review(technosphere)
    apply_fuels_37_sand_hour_review(technosphere)
    apply_fuels_37_waste_oil_review(technosphere)
    apply_wool_europe_direct_grid_review(technosphere)
    apply_packaging_38_complete_review(technosphere)
    if __package__:
        from scripts.review_uvek_geothermal_36_grids import apply_geothermal_36_grid_review
    else:
        from review_uvek_geothermal_36_grids import apply_geothermal_36_grid_review

    apply_geothermal_36_grid_review(technosphere)
    if __package__:
        from scripts.review_uvek_geothermal_36_markets import apply_geothermal_36_market_review
    else:
        from review_uvek_geothermal_36_markets import apply_geothermal_36_market_review

    apply_geothermal_36_market_review(technosphere)
    if __package__:
        from scripts.review_uvek_steel_39_first import apply_steel_39_first_review
    else:
        from review_uvek_steel_39_first import apply_steel_39_first_review

    apply_steel_39_first_review(technosphere)
    if __package__:
        from scripts.review_uvek_steel_39_air import apply_steel_39_air_review
    else:
        from review_uvek_steel_39_air import apply_steel_39_air_review

    apply_steel_39_air_review(technosphere)
    if __package__:
        from scripts.review_uvek_steel_39_inert import apply_steel_39_inert_review
    else:
        from review_uvek_steel_39_inert import apply_steel_39_inert_review

    apply_steel_39_inert_review(technosphere)
    if __package__:
        from scripts.review_uvek_steel_39_products import apply_steel_39_product_corrections
    else:
        from review_uvek_steel_39_products import apply_steel_39_product_corrections

    apply_steel_39_product_corrections(technosphere)
    if __package__:
        from scripts.review_uvek_steel_39_coal_sign import apply_steel_39_coal_sign_review
    else:
        from review_uvek_steel_39_coal_sign import apply_steel_39_coal_sign_review

    apply_steel_39_coal_sign_review(technosphere)
    if __package__:
        from scripts.review_uvek_steel_39_regional_gas import apply_steel_39_regional_gas_review
    else:
        from review_uvek_steel_39_regional_gas import apply_steel_39_regional_gas_review

    apply_steel_39_regional_gas_review(technosphere)
    if __package__:
        from scripts.review_uvek_steel_39_ng_heat import apply_steel_39_ng_heat_review
    else:
        from review_uvek_steel_39_ng_heat import apply_steel_39_ng_heat_review

    apply_steel_39_ng_heat_review(technosphere)
    if __package__:
        from scripts.review_uvek_steel_39_small_heat import apply_steel_39_small_heat_review
    else:
        from review_uvek_steel_39_small_heat import apply_steel_39_small_heat_review

    apply_steel_39_small_heat_review(technosphere)
    if __package__:
        from scripts.review_uvek_steel_39_zero_heat import apply_steel_39_zero_heat_review
    else:
        from review_uvek_steel_39_zero_heat import apply_steel_39_zero_heat_review

    apply_steel_39_zero_heat_review(technosphere)
    if __package__:
        from scripts.review_uvek_steel_39_process_heat import apply_steel_39_process_heat_review
    else:
        from review_uvek_steel_39_process_heat import apply_steel_39_process_heat_review

    apply_steel_39_process_heat_review(technosphere)
    if __package__:
        from scripts.review_uvek_steel_39_wastewater import apply_steel_39_wastewater_review
    else:
        from review_uvek_steel_39_wastewater import apply_steel_39_wastewater_review

    apply_steel_39_wastewater_review(technosphere)
    if __package__:
        from scripts.review_uvek_steel_39_first_residue import apply_steel_39_first_residue_review
    else:
        from review_uvek_steel_39_first_residue import apply_steel_39_first_residue_review

    apply_steel_39_first_residue_review(technosphere)
    if __package__:
        from scripts.review_uvek_steel_39_mill_scale_direct import apply_steel_39_mill_scale_direct_review
    else:
        from review_uvek_steel_39_mill_scale_direct import apply_steel_39_mill_scale_direct_review

    apply_steel_39_mill_scale_direct_review(technosphere)
    if __package__:
        from scripts.review_uvek_steel_39_remaining_residue import apply_steel_39_remaining_residue_review
    else:
        from review_uvek_steel_39_remaining_residue import apply_steel_39_remaining_residue_review

    apply_steel_39_remaining_residue_review(technosphere)
    if __package__:
        from scripts.review_uvek_ferrosilicon_voltage import apply_ferrosilicon_voltage_review
    else:
        from review_uvek_ferrosilicon_voltage import apply_ferrosilicon_voltage_review

    apply_ferrosilicon_voltage_review(technosphere)
    if __package__:
        from scripts.review_uvek_ferrosilicon_gas import apply_ferrosilicon_gas_review
    else:
        from review_uvek_ferrosilicon_gas import apply_ferrosilicon_gas_review

    apply_ferrosilicon_gas_review(technosphere)
    if __package__:
        from scripts.review_uvek_ferrosilicon_stored_fuels import apply_ferrosilicon_stored_fuels_review
    else:
        from review_uvek_ferrosilicon_stored_fuels import apply_ferrosilicon_stored_fuels_review

    apply_ferrosilicon_stored_fuels_review(technosphere)
    if __package__:
        from scripts.review_uvek_ferrosilicon_last_supplies import apply_ferrosilicon_last_supplies_review
    else:
        from review_uvek_ferrosilicon_last_supplies import apply_ferrosilicon_last_supplies_review

    apply_ferrosilicon_last_supplies_review(technosphere)
    if __package__:
        from scripts.review_uvek_ferrosilicon_market import apply_ferrosilicon_market_review
    else:
        from review_uvek_ferrosilicon_market import apply_ferrosilicon_market_review

    apply_ferrosilicon_market_review(technosphere)
    if __package__:
        from scripts.review_uvek_pulp_heat_energy import apply_pulp_heat_energy_review
    else:
        from review_uvek_pulp_heat_energy import apply_pulp_heat_energy_review

    apply_pulp_heat_energy_review(technosphere)
    if __package__:
        from scripts.review_uvek_pulp_heat_dry_markets import apply_pulp_heat_dry_markets_review
    else:
        from review_uvek_pulp_heat_dry_markets import apply_pulp_heat_dry_markets_review

    apply_pulp_heat_dry_markets_review(technosphere)
    if __package__:
        from scripts.review_uvek_pulp_heat_solution_markets import apply_pulp_heat_solution_markets_review
    else:
        from review_uvek_pulp_heat_solution_markets import apply_pulp_heat_solution_markets_review

    apply_pulp_heat_solution_markets_review(technosphere)
    if __package__:
        from scripts.review_uvek_pulp_heat_urea import apply_pulp_heat_urea_review
    else:
        from review_uvek_pulp_heat_urea import apply_pulp_heat_urea_review

    apply_pulp_heat_urea_review(technosphere)
    if __package__:
        from scripts.review_uvek_pulp_heat_auxiliary_markets import apply_pulp_heat_auxiliary_markets_review
    else:
        from review_uvek_pulp_heat_auxiliary_markets import apply_pulp_heat_auxiliary_markets_review

    apply_pulp_heat_auxiliary_markets_review(technosphere)
    if __package__:
        from scripts.review_uvek_pulp_heat_freight import apply_pulp_heat_freight_review
    else:
        from review_uvek_pulp_heat_freight import apply_pulp_heat_freight_review

    apply_pulp_heat_freight_review(technosphere)
    if __package__:
        from scripts.review_uvek_pulp_heat_wood_markets import apply_pulp_heat_wood_markets_review
    else:
        from review_uvek_pulp_heat_wood_markets import apply_pulp_heat_wood_markets_review

    apply_pulp_heat_wood_markets_review(technosphere)
    if __package__:
        from scripts.review_uvek_pulp_heat_bark_market import apply_pulp_heat_bark_market_review
    else:
        from review_uvek_pulp_heat_bark_market import apply_pulp_heat_bark_market_review

    apply_pulp_heat_bark_market_review(technosphere)
    if __package__:
        from scripts.review_uvek_pulp_heat_limestone import apply_pulp_heat_limestone_review
    else:
        from review_uvek_pulp_heat_limestone import apply_pulp_heat_limestone_review

    apply_pulp_heat_limestone_review(technosphere)
    if __package__:
        from scripts.review_uvek_pulp_heat_biomethanol_market import apply_pulp_heat_biomethanol_market_review
    else:
        from review_uvek_pulp_heat_biomethanol_market import apply_pulp_heat_biomethanol_market_review

    apply_pulp_heat_biomethanol_market_review(technosphere)
    if __package__:
        from scripts.review_uvek_pcc_allocated_carbonate import apply_pcc_allocated_carbonate_review
    else:
        from review_uvek_pcc_allocated_carbonate import apply_pcc_allocated_carbonate_review

    apply_pcc_allocated_carbonate_review(technosphere)
    if __package__:
        from scripts.review_uvek_pulp_heat_ether_ethanol_rer import apply_pulp_heat_ether_ethanol_rer_review
    else:
        from review_uvek_pulp_heat_ether_ethanol_rer import apply_pulp_heat_ether_ethanol_rer_review

    apply_pulp_heat_ether_ethanol_rer_review(technosphere)
    if __package__:
        from scripts.review_uvek_pulp_heat_ethanol_row import apply_pulp_heat_ethanol_row_review
    else:
        from review_uvek_pulp_heat_ethanol_row import apply_pulp_heat_ethanol_row_review

    apply_pulp_heat_ethanol_row_review(technosphere)
    if __package__:
        from scripts.review_uvek_pcc_water_row import apply_pcc_water_row_review
    else:
        from review_uvek_pcc_water_row import apply_pcc_water_row_review

    apply_pcc_water_row_review(technosphere)
    if __package__:
        from scripts.review_uvek_pcc_last_four import apply_pcc_last_four_review
    else:
        from review_uvek_pcc_last_four import apply_pcc_last_four_review

    apply_pcc_last_four_review(technosphere)
    if __package__:
        from scripts.review_uvek_pcc_market import apply_pcc_market_review
    else:
        from review_uvek_pcc_market import apply_pcc_market_review

    apply_pcc_market_review(technosphere)
    if __package__:
        from scripts.review_uvek_pulp_heat_route_markets import apply_pulp_heat_route_markets_review
    else:
        from review_uvek_pulp_heat_route_markets import apply_pulp_heat_route_markets_review

    apply_pulp_heat_route_markets_review(technosphere)
    if __package__:
        from scripts.review_uvek_pulp_heat_allyl_markets import apply_pulp_heat_allyl_markets_review
    else:
        from review_uvek_pulp_heat_allyl_markets import apply_pulp_heat_allyl_markets_review

    apply_pulp_heat_allyl_markets_review(technosphere)
    if __package__:
        from scripts.review_uvek_pulp_heat_so2_sulfite import apply_pulp_heat_so2_sulfite_review
    else:
        from review_uvek_pulp_heat_so2_sulfite import apply_pulp_heat_so2_sulfite_review

    apply_pulp_heat_so2_sulfite_review(technosphere)
    if __package__:
        from scripts.review_uvek_native_nongas_heat_proxy import apply_native_nongas_heat_proxy_review
    else:
        from review_uvek_native_nongas_heat_proxy import apply_native_nongas_heat_proxy_review

    apply_native_nongas_heat_proxy_review(technosphere)
    if __package__:
        from scripts.review_uvek_remaining_production_heat import apply_remaining_production_heat_review
    else:
        from review_uvek_remaining_production_heat import apply_remaining_production_heat_review

    apply_remaining_production_heat_review(technosphere)
    if __package__:
        from scripts.review_uvek_mining_38_material_suppliers import apply_mining_38_material_suppliers_review
    else:
        from review_uvek_mining_38_material_suppliers import apply_mining_38_material_suppliers_review

    apply_mining_38_material_suppliers_review(technosphere)
    if __package__:
        from scripts.review_uvek_mining_38_high_slag import apply_mining_38_high_slag_review
    else:
        from review_uvek_mining_38_high_slag import apply_mining_38_high_slag_review

    apply_mining_38_high_slag_review(technosphere)
    if __package__:
        from scripts.review_uvek_mining_38_polyester_fibre import apply_mining_38_polyester_fibre_review
    else:
        from review_uvek_mining_38_polyester_fibre import apply_mining_38_polyester_fibre_review

    apply_mining_38_polyester_fibre_review(technosphere)
    if __package__:
        from scripts.review_uvek_mining_38_pam_public import apply_mining_38_pam_public_review
    else:
        from review_uvek_mining_38_pam_public import apply_mining_38_pam_public_review

    apply_mining_38_pam_public_review(technosphere)
    if __package__:
        from scripts.review_uvek_mining_38_msw_portfolio import apply_mining_38_msw_portfolio_review
    else:
        from review_uvek_mining_38_msw_portfolio import apply_mining_38_msw_portfolio_review

    apply_mining_38_msw_portfolio_review(technosphere)
    apply_european_delivered_water_review(technosphere)
    if __package__:
        from scripts.review_uvek_mdea_312 import apply_mdea_312_review
    else:
        from review_uvek_mdea_312 import apply_mdea_312_review

    apply_mdea_312_review(technosphere)
    if __package__:
        from scripts.review_uvek_battery_first_materials_39 import apply_battery_first_materials_39_review
    else:
        from review_uvek_battery_first_materials_39 import apply_battery_first_materials_39_review

    apply_battery_first_materials_39_review(technosphere)
    if __package__:
        from scripts.review_uvek_battery_fluoride_39 import apply_battery_fluoride_39_review
    else:
        from review_uvek_battery_fluoride_39 import apply_battery_fluoride_39_review

    apply_battery_fluoride_39_review(technosphere)
    if __package__:
        from scripts.review_uvek_battery_native_materials_39 import apply_battery_native_materials_39_review
    else:
        from review_uvek_battery_native_materials_39 import apply_battery_native_materials_39_review

    apply_battery_native_materials_39_review(technosphere)
    if __package__:
        from scripts.review_uvek_generic_cable_39 import apply_generic_cable_39_review
    else:
        from review_uvek_generic_cable_39 import apply_generic_cable_39_review

    apply_generic_cable_39_review(technosphere)
    if __package__:
        from scripts.review_uvek_euro_v_freight_39 import apply_euro_v_freight_39_review
    else:
        from review_uvek_euro_v_freight_39 import apply_euro_v_freight_39_review

    apply_euro_v_freight_39_review(technosphere)
    if __package__:
        from scripts.review_uvek_lcd_module_39 import apply_lcd_module_39_review
    else:
        from review_uvek_lcd_module_39 import apply_lcd_module_39_review

    apply_lcd_module_39_review(technosphere)
    if __package__:
        from scripts.review_uvek_pvf_treatment_39 import apply_pvf_treatment_39_review
    else:
        from review_uvek_pvf_treatment_39 import apply_pvf_treatment_39_review

    apply_pvf_treatment_39_review(technosphere)
    if __package__:
        from scripts.review_uvek_unsorted_iron_scrap_39 import apply_unsorted_iron_scrap_39_review
    else:
        from review_uvek_unsorted_iron_scrap_39 import apply_unsorted_iron_scrap_39_review

    apply_unsorted_iron_scrap_39_review(technosphere)
    if __package__:
        from scripts.review_uvek_methane_sulfonic_acid_39 import apply_methane_sulfonic_acid_39_review
    else:
        from review_uvek_methane_sulfonic_acid_39 import apply_methane_sulfonic_acid_39_review

    apply_methane_sulfonic_acid_39_review(technosphere)
    if __package__:
        from scripts.review_uvek_ilmenite_mine_39 import apply_ilmenite_mine_39_review
    else:
        from review_uvek_ilmenite_mine_39 import apply_ilmenite_mine_39_review

    apply_ilmenite_mine_39_review(technosphere)
    if __package__:
        from scripts.review_uvek_petrol_row_39 import apply_petrol_row_39_review
    else:
        from review_uvek_petrol_row_39 import apply_petrol_row_39_review

    apply_petrol_row_39_review(technosphere)
    if __package__:
        from scripts.review_uvek_ilmenite_recultivation_39 import apply_ilmenite_recultivation_39_review
    else:
        from review_uvek_ilmenite_recultivation_39 import apply_ilmenite_recultivation_39_review

    apply_ilmenite_recultivation_39_review(technosphere)
    if __package__:
        from scripts.review_uvek_natural_gas_heat_groups_39 import apply_natural_gas_heat_groups_39_review
    else:
        from review_uvek_natural_gas_heat_groups_39 import apply_natural_gas_heat_groups_39_review

    apply_natural_gas_heat_groups_39_review(technosphere)
    if __package__:
        from scripts.review_uvek_eaf_dust_market_39 import apply_eaf_dust_market_39_review
    else:
        from review_uvek_eaf_dust_market_39 import apply_eaf_dust_market_39_review

    apply_eaf_dust_market_39_review(technosphere)
    if __package__:
        from scripts.review_uvek_eaf_slag_market_39 import apply_eaf_slag_market_39_review
    else:
        from review_uvek_eaf_slag_market_39 import apply_eaf_slag_market_39_review

    apply_eaf_slag_market_39_review(technosphere)
    if __package__:
        from scripts.review_uvek_scrap_delivery_39 import apply_scrap_delivery_39_review
    else:
        from review_uvek_scrap_delivery_39 import apply_scrap_delivery_39_review

    apply_scrap_delivery_39_review(technosphere)
    if __package__:
        from scripts.review_uvek_sodium_sulfate_39 import apply_sodium_sulfate_39_review
    else:
        from review_uvek_sodium_sulfate_39 import apply_sodium_sulfate_39_review

    apply_sodium_sulfate_39_review(technosphere)
    if __package__:
        from scripts.review_uvek_ammonium_sulfate_39 import apply_ammonium_sulfate_39_review
    else:
        from review_uvek_ammonium_sulfate_39 import apply_ammonium_sulfate_39_review

    apply_ammonium_sulfate_39_review(technosphere)
    if __package__:
        from scripts.review_uvek_argon_liquid_39 import apply_argon_liquid_39_review
    else:
        from review_uvek_argon_liquid_39 import apply_argon_liquid_39_review

    apply_argon_liquid_39_review(technosphere)
    if __package__:
        from scripts.review_uvek_secondary_metallurgy_slag_39 import apply_secondary_metallurgy_slag_39_review
    else:
        from review_uvek_secondary_metallurgy_slag_39 import apply_secondary_metallurgy_slag_39_review

    apply_secondary_metallurgy_slag_39_review(technosphere)
    if __package__:
        from scripts.review_uvek_electricity_grids_39 import apply_electricity_grids_39_review
    else:
        from review_uvek_electricity_grids_39 import apply_electricity_grids_39_review

    apply_electricity_grids_39_review(technosphere)
    if __package__:
        from scripts.review_uvek_ferrochromium_39 import apply_ferrochromium_39_review
    else:
        from review_uvek_ferrochromium_39 import apply_ferrochromium_39_review

    apply_ferrochromium_39_review(technosphere)
    if __package__:
        from scripts.review_uvek_propane_machine_39 import apply_propane_machine_39_review
    else:
        from review_uvek_propane_machine_39 import apply_propane_machine_39_review

    apply_propane_machine_39_review(technosphere)
    if __package__:
        from scripts.review_uvek_plastic_processing_39 import apply_plastic_processing_39_review
    else:
        from review_uvek_plastic_processing_39 import apply_plastic_processing_39_review

    apply_plastic_processing_39_review(technosphere)
    if __package__:
        from scripts.review_uvek_gfrp_handlayup_39 import apply_gfrp_handlayup_39_review
    else:
        from review_uvek_gfrp_handlayup_39 import apply_gfrp_handlayup_39_review

    apply_gfrp_handlayup_39_review(technosphere)
    if __package__:
        from scripts.review_uvek_lmo_cell_39 import apply_lmo_cell_39_review
    else:
        from review_uvek_lmo_cell_39 import apply_lmo_cell_39_review

    apply_lmo_cell_39_review(technosphere)
    if __package__:
        from scripts.review_uvek_nylon6_39 import apply_nylon6_39_review
    else:
        from review_uvek_nylon6_39 import apply_nylon6_39_review

    apply_nylon6_39_review(technosphere)
    if __package__:
        from scripts.review_uvek_converter_steel_39 import apply_converter_steel_39_review
    else:
        from review_uvek_converter_steel_39 import apply_converter_steel_39_review

    apply_converter_steel_39_review(technosphere)
    if __package__:
        from scripts.review_uvek_pet_market_39 import apply_pet_market_39_review
    else:
        from review_uvek_pet_market_39 import apply_pet_market_39_review

    apply_pet_market_39_review(technosphere)
    if __package__:
        from scripts.review_uvek_cmc_latex_markets_39 import apply_cmc_latex_markets_39_review
    else:
        from review_uvek_cmc_latex_markets_39 import apply_cmc_latex_markets_39_review

    apply_cmc_latex_markets_39_review(technosphere)
    if __package__:
        from scripts.review_uvek_acetoacetic_acid_39 import apply_acetoacetic_acid_39_review
    else:
        from review_uvek_acetoacetic_acid_39 import apply_acetoacetic_acid_39_review

    apply_acetoacetic_acid_39_review(technosphere)
    if __package__:
        from scripts.review_uvek_mixed_butene_39 import apply_mixed_butene_39_review
    else:
        from review_uvek_mixed_butene_39 import apply_mixed_butene_39_review

    apply_mixed_butene_39_review(technosphere)
    if __package__:
        from scripts.review_uvek_hydrogen_borohydride_39 import apply_hydrogen_borohydride_39_review
    else:
        from review_uvek_hydrogen_borohydride_39 import apply_hydrogen_borohydride_39_review

    apply_hydrogen_borohydride_39_review(technosphere)
    if __package__:
        from scripts.archive_uvek_unreferenced_reviews import archive_unreferenced_reviews
    else:
        from archive_uvek_unreferenced_reviews import archive_unreferenced_reviews

    archive_unreferenced_reviews(technosphere)


def build_resources(catalog_directory: Path, curated_path: Path, conversion_factors_path: Path) -> tuple[dict, dict]:
    """Generate executable mappings only between compatible system models."""
    if __package__:
        from scripts.generate_uvek_canonical_biosphere import build_canonical_biosphere_resource
    else:
        from generate_uvek_canonical_biosphere import build_canonical_biosphere_resource

    historical, _ = _build_historical_review_resources(catalog_directory, curated_path, conversion_factors_path)
    technosphere = cutoff_uvek_resource(historical, catalog_directory)
    apply_pilot_reviews(technosphere)
    validate_migration_resource(technosphere)
    return technosphere, build_canonical_biosphere_resource(catalog_directory)


def _technosphere_key(identity: TechnosphereIdentity) -> tuple[str, str, str, str]:
    return identity.name, identity.reference_product, identity.location, identity.unit


def _biosphere_key(identity: BiosphereIdentity) -> tuple[str, tuple[str, ...], str]:
    return identity.name, identity.categories, identity.unit


def _write_resource(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(dumps_migration_resource(normalize_migration_resource(payload)), encoding="utf-8")


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
    from scripts.review_uvek_quizalofop_synonym import build_extended_biosphere_resource

    _write_resource(
        args.output_directory / "ecoinvent-to-ecoinvent-3.10-biosphere-v2.json",
        build_extended_biosphere_resource(biosphere),
    )
    print(
        f"Wrote {len(technosphere['replace'])} technosphere and "
        f"{len(biosphere['replace'])} biosphere mapping rules."
    )


if __name__ == "__main__":
    main()
