"""Structured metadata contract for the supported ecoSpold1 process subset.

The contract is intentionally independent of free-form dataset comments.  It
can be populated from metadata preserved by the ecoSpold1 reader or from an
explicit YAML/JSON export profile.
"""

from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Sequence

import yaml
from jsonschema import Draft202012Validator

ECOSPOLD1_METADATA_SCHEMA_VERSION = 1


def _text(value: object) -> str:
    return str(value or "").strip()


def _integer(value: object, default: int = 0) -> int:
    if value in (None, ""):
        return default
    if isinstance(value, bool):
        raise TypeError("Boolean values are not valid integer metadata.")
    return int(value)


def _boolean(value: object, default: bool = False) -> bool:
    if value in (None, ""):
        return default
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes"}:
        return True
    if normalized in {"0", "false", "no"}:
        return False
    raise ValueError(f"Expected a boolean value, got {value!r}.")


def _mapping(value: object, path: str) -> Mapping[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise TypeError(f"{path} must be a mapping.")
    return value


def _date_text(value: object) -> str:
    text = _text(value)
    if not text:
        return ""
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as error:
        raise ValueError(f"Expected an ISO date (YYYY-MM-DD), got {text!r}.") from error


@dataclass(frozen=True)
class EcoSpold1Person:
    """A person referenced by ecoSpold1 administrative metadata."""

    name: str = ""
    address: str = ""
    company_code: str = ""
    country_code: str = ""
    telephone: str = ""
    telefax: str = ""
    email: str = ""
    number: int = 0

    @classmethod
    def from_mapping(cls, value: object, path: str = "person") -> "EcoSpold1Person":
        data = _mapping(value, path)
        return cls(
            name=_text(data.get("name")),
            address=_text(data.get("address")),
            company_code=_text(data.get("company_code", data.get("companyCode"))),
            country_code=_text(data.get("country_code", data.get("countryCode"))).upper(),
            telephone=_text(data.get("telephone")),
            telefax=_text(data.get("telefax")),
            email=_text(data.get("email")),
            number=_integer(data.get("number")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {key: value for key, value in asdict(self).items() if value not in ("", 0)}


@dataclass(frozen=True)
class EcoSpold1Source:
    """Bibliographic or communication source required by ecoSpold1."""

    first_author: str = ""
    year: int = 0
    title: str = ""
    place_of_publication: str = ""
    source_type: int = 0
    additional_authors: str = ""
    page_numbers: str = ""
    name_of_editors: str = ""
    title_of_anthology: str = ""
    publisher: str = ""
    journal: str = ""
    volume_number: int = 0
    issue_number: str = ""
    text: str = ""
    number: int = 0

    @classmethod
    def from_mapping(cls, value: object, path: str = "source") -> "EcoSpold1Source":
        data = _mapping(value, path)
        return cls(
            first_author=_text(data.get("first_author", data.get("firstAuthor"))),
            year=_integer(data.get("year")),
            title=_text(data.get("title")),
            place_of_publication=_text(data.get("place_of_publication", data.get("placeOfPublications"))),
            source_type=_integer(data.get("source_type", data.get("sourceType"))),
            additional_authors=_text(data.get("additional_authors", data.get("additionalAuthors"))),
            page_numbers=_text(data.get("page_numbers", data.get("pageNumbers"))),
            name_of_editors=_text(data.get("name_of_editors", data.get("nameOfEditors"))),
            title_of_anthology=_text(data.get("title_of_anthology", data.get("titleOfAnthology"))),
            publisher=_text(data.get("publisher")),
            journal=_text(data.get("journal")),
            volume_number=_integer(data.get("volume_number", data.get("volumeNo"))),
            issue_number=_text(data.get("issue_number", data.get("issueNo"))),
            text=_text(data.get("text")),
            number=_integer(data.get("number")),
        )

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        return {key: value for key, value in result.items() if value not in ("", 0) or key == "source_type"}


@dataclass(frozen=True)
class EcoSpold1TimePeriod:
    """Temporal coverage of one process dataset."""

    start_date: str = ""
    end_date: str = ""
    data_valid_for_entire_period: bool = True
    text: str = ""

    @classmethod
    def from_mapping(cls, value: object, path: str = "time_period") -> "EcoSpold1TimePeriod":
        data = _mapping(value, path)
        return cls(
            start_date=_date_text(data.get("start_date", data.get("startDate"))),
            end_date=_date_text(data.get("end_date", data.get("endDate"))),
            data_valid_for_entire_period=_boolean(
                data.get("data_valid_for_entire_period", data.get("dataValidForEntirePeriod")),
                True,
            ),
            text=_text(data.get("text")),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class EcoSpold1Classification:
    """English classification rendered on the reference function."""

    category: str = ""
    subcategory: str = ""

    @classmethod
    def from_mapping(cls, value: object, path: str = "classification") -> "EcoSpold1Classification":
        data = _mapping(value, path)
        return cls(
            category=_text(data.get("category")),
            subcategory=_text(data.get("subcategory", data.get("sub_category"))),
        )

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


@dataclass(frozen=True)
class EcoSpold1DatasetMetadata:
    """Resolved metadata for one exported ecoSpold1 process dataset."""

    classification: EcoSpold1Classification = field(default_factory=EcoSpold1Classification)
    time_period: EcoSpold1TimePeriod = field(default_factory=EcoSpold1TimePeriod)
    sources: tuple[EcoSpold1Source, ...] = ()
    data_entry_person: EcoSpold1Person = field(default_factory=EcoSpold1Person)
    data_generator_person: EcoSpold1Person = field(default_factory=EcoSpold1Person)
    copyright: bool = False
    data_published_in: int = 0
    access_restricted_to: int = 0
    quality_network: int = 1
    technology: str = ""
    geography_text: str = ""
    dataset_version: str = "1.0"
    internal_version: str = "1.0"
    raw: Mapping[str, Any] = field(default_factory=dict, repr=False)

    @classmethod
    def from_mapping(cls, value: object, path: str = "metadata") -> "EcoSpold1DatasetMetadata":
        data = _mapping(value, path)
        sources_value = data.get("sources")
        if sources_value is None and data.get("source") is not None:
            sources_value = [data["source"]]
        if sources_value is None:
            sources: tuple[EcoSpold1Source, ...] = ()
        elif isinstance(sources_value, Sequence) and not isinstance(sources_value, (str, bytes, bytearray)):
            sources = tuple(
                EcoSpold1Source.from_mapping(item, f"{path}.sources[{index}]")
                for index, item in enumerate(sources_value)
            )
        else:
            raise TypeError(f"{path}.sources must be a sequence of mappings.")
        return cls(
            classification=EcoSpold1Classification.from_mapping(data.get("classification"), f"{path}.classification"),
            time_period=EcoSpold1TimePeriod.from_mapping(data.get("time_period"), f"{path}.time_period"),
            sources=sources,
            data_entry_person=EcoSpold1Person.from_mapping(data.get("data_entry_person"), f"{path}.data_entry_person"),
            data_generator_person=EcoSpold1Person.from_mapping(
                data.get("data_generator_person"), f"{path}.data_generator_person"
            ),
            copyright=_boolean(data.get("copyright"), False),
            data_published_in=_integer(data.get("data_published_in")),
            access_restricted_to=_integer(data.get("access_restricted_to")),
            quality_network=_integer(data.get("quality_network"), 1),
            technology=_text(data.get("technology")),
            geography_text=_text(data.get("geography_text")),
            dataset_version=_text(data.get("dataset_version")) or "1.0",
            internal_version=_text(data.get("internal_version")) or "1.0",
            raw=deepcopy(data.get("raw", {})),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "classification": self.classification.to_dict(),
            "time_period": self.time_period.to_dict(),
            "sources": [source.to_dict() for source in self.sources],
            "data_entry_person": self.data_entry_person.to_dict(),
            "data_generator_person": self.data_generator_person.to_dict(),
            "copyright": self.copyright,
            "data_published_in": self.data_published_in,
            "access_restricted_to": self.access_restricted_to,
            "quality_network": self.quality_network,
            "technology": self.technology,
            "geography_text": self.geography_text,
            "dataset_version": self.dataset_version,
            "internal_version": self.internal_version,
            "raw": deepcopy(dict(self.raw)),
        }

    def missing_required(self) -> tuple[str, ...]:
        """Return stable relative paths for semantically required values."""

        missing = []
        required = {
            "classification.category": self.classification.category,
            "classification.subcategory": self.classification.subcategory,
            "time_period.start_date": self.time_period.start_date,
            "time_period.end_date": self.time_period.end_date,
            "data_entry_person.name": self.data_entry_person.name,
            "data_entry_person.address": self.data_entry_person.address,
            "data_entry_person.company_code": self.data_entry_person.company_code,
            "data_entry_person.country_code": self.data_entry_person.country_code,
            "data_generator_person.name": self.data_generator_person.name,
            "data_generator_person.address": self.data_generator_person.address,
            "data_generator_person.company_code": self.data_generator_person.company_code,
            "data_generator_person.country_code": self.data_generator_person.country_code,
        }
        missing.extend(path for path, value in required.items() if not value)
        if not self.sources:
            missing.append("sources")
        else:
            for index, source in enumerate(self.sources):
                for name in ("first_author", "year", "title", "place_of_publication"):
                    if not getattr(source, name):
                        missing.append(f"sources[{index}].{name}")
        return tuple(missing)


@dataclass(frozen=True)
class EcoSpold1DatasetOverride:
    """Metadata override selected by code or exact process identity."""

    selector: Mapping[str, str]
    metadata: Mapping[str, Any]

    def __post_init__(self) -> None:
        selector = {str(key): _text(value) for key, value in dict(self.selector).items() if _text(value)}
        allowed = {"code", "name", "reference_product", "location", "unit"}
        unknown = sorted(set(selector) - allowed)
        if unknown:
            raise ValueError(f"Unknown ecoSpold1 dataset selector fields: {', '.join(unknown)}.")
        if not selector:
            raise ValueError("An ecoSpold1 dataset override selector must not be empty.")
        object.__setattr__(self, "selector", selector)
        object.__setattr__(self, "metadata", deepcopy(dict(self.metadata)))

    def matches(self, dataset: Mapping[str, Any]) -> bool:
        values = {
            "code": _text(dataset.get("code")),
            "name": _text(dataset.get("name")),
            "reference_product": _text(dataset.get("reference product", dataset.get("product"))),
            "location": _text(dataset.get("location")),
            "unit": _text(dataset.get("unit")),
        }
        return all(values[key] == value for key, value in self.selector.items())


@dataclass(frozen=True)
class EcoSpold1ExportProfile:
    """Document-level defaults plus exact per-dataset metadata overrides."""

    defaults: Mapping[str, Any] = field(default_factory=dict)
    datasets: tuple[EcoSpold1DatasetOverride, ...] = ()
    schema_version: int = ECOSPOLD1_METADATA_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECOSPOLD1_METADATA_SCHEMA_VERSION:
            raise ValueError(
                f"Unsupported ecoSpold1 metadata schema version {self.schema_version!r}; "
                f"expected {ECOSPOLD1_METADATA_SCHEMA_VERSION}."
            )
        object.__setattr__(self, "defaults", deepcopy(dict(self.defaults)))
        object.__setattr__(self, "datasets", tuple(self.datasets))

    @classmethod
    def from_mapping(cls, value: object) -> "EcoSpold1ExportProfile":
        data = _normalize_yaml_dates(_mapping(value, "ecoSpold1 export profile"))
        _validate_profile_payload(data)
        raw_overrides = data.get("datasets", ())
        if not isinstance(raw_overrides, Sequence) or isinstance(raw_overrides, (str, bytes, bytearray)):
            raise TypeError("ecoSpold1 export profile datasets must be a sequence.")
        overrides = []
        for index, item in enumerate(raw_overrides):
            entry = _mapping(item, f"datasets[{index}]")
            overrides.append(
                EcoSpold1DatasetOverride(
                    selector=_mapping(entry.get("selector"), f"datasets[{index}].selector"),
                    metadata=_mapping(entry.get("metadata"), f"datasets[{index}].metadata"),
                )
            )
        return cls(
            defaults=_mapping(data.get("defaults"), "defaults"),
            datasets=tuple(overrides),
            schema_version=_integer(data.get("schema_version"), ECOSPOLD1_METADATA_SCHEMA_VERSION),
        )

    @classmethod
    def from_file(cls, path: str | Path) -> "EcoSpold1ExportProfile":
        source = Path(path)
        with source.open("r", encoding="utf-8") as handle:
            payload = yaml.safe_load(handle)
        return cls.from_mapping(payload or {})

    def metadata_for(self, dataset: Mapping[str, Any]) -> EcoSpold1DatasetMetadata:
        embedded = _mapping(dataset.get("ecospold1"), "dataset.ecospold1")
        selected = [override for override in self.datasets if override.matches(dataset)]
        if len(selected) > 1:
            raise ValueError(
                "Multiple ecoSpold1 metadata overrides match dataset {!r}.".format(_text(dataset.get("name")))
            )
        merged = _deep_merge(self.defaults, embedded)
        if selected:
            merged = _deep_merge(merged, selected[0].metadata)
        return EcoSpold1DatasetMetadata.from_mapping(merged)


def coerce_ecospold1_profile(value: object | None) -> EcoSpold1ExportProfile:
    """Coerce a profile instance, mapping, path, or ``None``."""

    if value is None:
        return EcoSpold1ExportProfile()
    if isinstance(value, EcoSpold1ExportProfile):
        return value
    if isinstance(value, (str, Path)):
        return EcoSpold1ExportProfile.from_file(value)
    return EcoSpold1ExportProfile.from_mapping(value)


def _deep_merge(base: Mapping[str, Any], override: Mapping[str, Any]) -> dict[str, Any]:
    result = deepcopy(dict(base))
    for key, value in override.items():
        if isinstance(value, Mapping) and isinstance(result.get(key), Mapping):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = deepcopy(value)
    return result


def _normalize_yaml_dates(value: Any) -> Any:
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _normalize_yaml_dates(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_yaml_dates(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_normalize_yaml_dates(item) for item in value)
    return deepcopy(value)


def _validate_profile_payload(payload: Mapping[str, Any]) -> None:
    schema_path = Path(__file__).resolve().parents[1] / "data" / "schemas" / "ecospold1-export-metadata-v1.schema.json"
    with schema_path.open("r", encoding="utf-8") as handle:
        schema = json.load(handle)
    errors = sorted(Draft202012Validator(schema).iter_errors(payload), key=lambda error: list(error.path))
    if errors:
        first = errors[0]
        location = ".".join(str(part) for part in first.path) or "profile"
        raise ValueError(f"{location}: {first.message}")
