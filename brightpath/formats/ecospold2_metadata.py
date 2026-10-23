"""Structured metadata and UUID references for ecoSpold2 exchange packages."""

from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Sequence
from uuid import UUID

import yaml
from jsonschema import Draft202012Validator

from brightpath.background import catalog_provider_from_environment
from brightpath.core.context import InventoryContext

ECOSPOLD2_METADATA_SCHEMA_VERSION = 1
ECOSPOLD2_REFERENCE_SCHEMA_VERSION = 1


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


def _uuid_text(value: object, *, path: str, required: bool = False) -> str:
    text = _text(value)
    if not text:
        if required:
            raise ValueError(f"{path} must be a UUID.")
        return ""
    try:
        return str(UUID(text))
    except (ValueError, AttributeError) as error:
        raise ValueError(f"{path} must be a UUID, got {text!r}.") from error


@dataclass(frozen=True)
class EcoSpold2Person:
    """A person referenced from ecoSpold2 administrative information."""

    name: str = ""
    email: str = ""
    id: str = ""
    company_id: str = ""
    company_code: str = ""

    @classmethod
    def from_mapping(cls, value: object, path: str) -> "EcoSpold2Person":
        data = _mapping(value, path)
        return cls(
            name=_text(data.get("name", data.get("personName"))),
            email=_text(data.get("email", data.get("personEmail"))),
            id=_uuid_text(data.get("id", data.get("personId")), path=f"{path}.id"),
            company_id=_uuid_text(data.get("company_id", data.get("companyId")), path=f"{path}.company_id"),
            company_code=_text(data.get("company_code", data.get("companyCode"))),
        )

    def to_dict(self) -> dict[str, Any]:
        return {key: value for key, value in asdict(self).items() if value}


@dataclass(frozen=True)
class EcoSpold2TimePeriod:
    """Temporal coverage of one ecoSpold2 activity."""

    start_date: str = ""
    end_date: str = ""
    data_valid_for_entire_period: bool = True
    comment: str = ""

    @classmethod
    def from_mapping(cls, value: object, path: str = "time_period") -> "EcoSpold2TimePeriod":
        data = _mapping(value, path)
        return cls(
            start_date=_date_text(data.get("start_date", data.get("startDate"))),
            end_date=_date_text(data.get("end_date", data.get("endDate"))),
            data_valid_for_entire_period=_boolean(
                data.get("data_valid_for_entire_period", data.get("isDataValidForEntirePeriod")), True
            ),
            comment=_text(data.get("comment")),
        )


@dataclass(frozen=True)
class EcoSpold2MacroEconomicScenario:
    """Macro-economic scenario required by the ecoSpold2 activity schema."""

    name: str = ""
    id: str = ""
    comment: str = ""

    @classmethod
    def from_mapping(cls, value: object, path: str = "macroeconomic_scenario") -> "EcoSpold2MacroEconomicScenario":
        data = _mapping(value, path)
        return cls(
            name=_text(data.get("name")),
            id=_uuid_text(data.get("id", data.get("macroEconomicScenarioId")), path=f"{path}.id"),
            comment=_text(data.get("comment")),
        )


@dataclass(frozen=True)
class EcoSpold2FileAttributes:
    """Version and context fields attached to an ecoSpold2 file."""

    major_release: int = 1
    minor_release: int = 0
    major_revision: int = 1
    minor_revision: int = 0
    default_language: str = "en"
    context_id: str = ""
    context_name: str = ""

    @classmethod
    def from_mapping(cls, value: object, path: str = "file_attributes") -> "EcoSpold2FileAttributes":
        data = _mapping(value, path)
        return cls(
            major_release=_integer(data.get("major_release", data.get("majorRelease")), 1),
            minor_release=_integer(data.get("minor_release", data.get("minorRelease")), 0),
            major_revision=_integer(data.get("major_revision", data.get("majorRevision")), 1),
            minor_revision=_integer(data.get("minor_revision", data.get("minorRevision")), 0),
            default_language=_text(data.get("default_language", data.get("defaultLanguage"))) or "en",
            context_id=_uuid_text(data.get("context_id", data.get("contextId")), path=f"{path}.context_id"),
            context_name=_text(data.get("context_name", data.get("contextName"))),
        )


@dataclass(frozen=True)
class EcoSpold2DatasetMetadata:
    """Resolved metadata for one exported ecoSpold2 activity dataset."""

    time_period: EcoSpold2TimePeriod = field(default_factory=EcoSpold2TimePeriod)
    macroeconomic_scenario: EcoSpold2MacroEconomicScenario = field(default_factory=EcoSpold2MacroEconomicScenario)
    data_entry_person: EcoSpold2Person = field(default_factory=EcoSpold2Person)
    data_generator_person: EcoSpold2Person = field(default_factory=EcoSpold2Person)
    file_attributes: EcoSpold2FileAttributes = field(default_factory=EcoSpold2FileAttributes)
    activity_id: str = ""
    activity_name_id: str = ""
    geography_id: str = ""
    activity_type: int = 1
    special_activity_type: int = 0
    technology_level: int = 3
    technology_comment: str = ""
    copyright: bool = False
    data_published_in: int = 0
    published_source_id: str = ""
    published_source_context_id: str = ""
    published_source_year: str = ""
    published_source_first_author: str = ""
    access_restricted_to: int = 0
    classifications: tuple[Mapping[str, Any], ...] = ()
    raw: Mapping[str, Any] = field(default_factory=dict, repr=False)

    @classmethod
    def from_mapping(cls, value: object, path: str = "metadata") -> "EcoSpold2DatasetMetadata":
        data = _mapping(value, path)
        classifications = data.get("classifications", ())
        if not isinstance(classifications, Sequence) or isinstance(classifications, (str, bytes, bytearray)):
            raise TypeError(f"{path}.classifications must be a sequence.")
        return cls(
            time_period=EcoSpold2TimePeriod.from_mapping(data.get("time_period"), f"{path}.time_period"),
            macroeconomic_scenario=EcoSpold2MacroEconomicScenario.from_mapping(
                data.get("macroeconomic_scenario"), f"{path}.macroeconomic_scenario"
            ),
            data_entry_person=EcoSpold2Person.from_mapping(data.get("data_entry_person"), f"{path}.data_entry_person"),
            data_generator_person=EcoSpold2Person.from_mapping(
                data.get("data_generator_person"), f"{path}.data_generator_person"
            ),
            file_attributes=EcoSpold2FileAttributes.from_mapping(
                data.get("file_attributes"), f"{path}.file_attributes"
            ),
            activity_id=_uuid_text(data.get("activity_id"), path=f"{path}.activity_id"),
            activity_name_id=_uuid_text(data.get("activity_name_id"), path=f"{path}.activity_name_id"),
            geography_id=_uuid_text(data.get("geography_id"), path=f"{path}.geography_id"),
            activity_type=_integer(data.get("activity_type"), 1),
            special_activity_type=_integer(data.get("special_activity_type"), 0),
            technology_level=_integer(data.get("technology_level"), 3),
            technology_comment=_text(data.get("technology_comment")),
            copyright=_boolean(data.get("copyright"), False),
            data_published_in=_integer(data.get("data_published_in"), 0),
            published_source_id=_uuid_text(data.get("published_source_id"), path=f"{path}.published_source_id"),
            published_source_context_id=_uuid_text(
                data.get("published_source_context_id"), path=f"{path}.published_source_context_id"
            ),
            published_source_year=_text(data.get("published_source_year")),
            published_source_first_author=_text(data.get("published_source_first_author")),
            access_restricted_to=_integer(data.get("access_restricted_to"), 0),
            classifications=tuple(
                deepcopy(dict(_mapping(item, f"{path}.classifications"))) for item in classifications
            ),
            raw=deepcopy(dict(_mapping(data.get("raw"), f"{path}.raw"))),
        )

    def missing_required(self) -> tuple[str, ...]:
        required = {
            "time_period.start_date": self.time_period.start_date,
            "time_period.end_date": self.time_period.end_date,
            "macroeconomic_scenario.name": self.macroeconomic_scenario.name,
            "data_entry_person.name": self.data_entry_person.name,
            "data_entry_person.email": self.data_entry_person.email,
            "data_generator_person.name": self.data_generator_person.name,
            "data_generator_person.email": self.data_generator_person.email,
        }
        if self.data_published_in == 2:
            required["published_source_id"] = self.published_source_id
        return tuple(path for path, value in required.items() if not value)

    def to_dict(self) -> dict[str, Any]:
        return {
            "time_period": asdict(self.time_period),
            "macroeconomic_scenario": asdict(self.macroeconomic_scenario),
            "data_entry_person": self.data_entry_person.to_dict(),
            "data_generator_person": self.data_generator_person.to_dict(),
            "file_attributes": asdict(self.file_attributes),
            "activity_id": self.activity_id,
            "activity_name_id": self.activity_name_id,
            "geography_id": self.geography_id,
            "activity_type": self.activity_type,
            "special_activity_type": self.special_activity_type,
            "technology_level": self.technology_level,
            "technology_comment": self.technology_comment,
            "copyright": self.copyright,
            "data_published_in": self.data_published_in,
            "published_source_id": self.published_source_id,
            "published_source_context_id": self.published_source_context_id,
            "published_source_year": self.published_source_year,
            "published_source_first_author": self.published_source_first_author,
            "access_restricted_to": self.access_restricted_to,
            "classifications": [deepcopy(dict(item)) for item in self.classifications],
            "raw": deepcopy(dict(self.raw)),
        }


@dataclass(frozen=True)
class EcoSpold2DatasetOverride:
    """Metadata override selected by code or exact process identity."""

    selector: Mapping[str, str]
    metadata: Mapping[str, Any]

    def __post_init__(self) -> None:
        selector = {str(key): _text(value) for key, value in dict(self.selector).items() if _text(value)}
        allowed = {"code", "name", "reference_product", "location", "unit"}
        unknown = sorted(set(selector) - allowed)
        if unknown:
            raise ValueError(f"Unknown ecoSpold2 dataset selector fields: {', '.join(unknown)}.")
        if not selector:
            raise ValueError("An ecoSpold2 dataset override selector must not be empty.")
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
class EcoSpold2ExportProfile:
    """Document defaults plus exact per-dataset ecoSpold2 overrides."""

    defaults: Mapping[str, Any] = field(default_factory=dict)
    datasets: tuple[EcoSpold2DatasetOverride, ...] = ()
    schema_version: int = ECOSPOLD2_METADATA_SCHEMA_VERSION

    @classmethod
    def from_mapping(cls, value: object) -> "EcoSpold2ExportProfile":
        data = _normalize_yaml_dates(_mapping(value, "ecoSpold2 export profile"))
        _validate_payload(data, "ecospold2-export-metadata-v1.schema.json")
        raw_overrides = data.get("datasets", ())
        overrides = []
        for index, item in enumerate(raw_overrides):
            entry = _mapping(item, f"datasets[{index}]")
            overrides.append(
                EcoSpold2DatasetOverride(
                    selector=_mapping(entry.get("selector"), f"datasets[{index}].selector"),
                    metadata=_mapping(entry.get("metadata"), f"datasets[{index}].metadata"),
                )
            )
        return cls(
            defaults=deepcopy(dict(_mapping(data.get("defaults"), "defaults"))),
            datasets=tuple(overrides),
            schema_version=_integer(data.get("schema_version"), ECOSPOLD2_METADATA_SCHEMA_VERSION),
        )

    @classmethod
    def from_file(cls, path: str | Path) -> "EcoSpold2ExportProfile":
        return cls.from_mapping(_load_yaml_or_json(path))

    def metadata_for(self, dataset: Mapping[str, Any]) -> EcoSpold2DatasetMetadata:
        embedded = _mapping(dataset.get("ecospold2"), "dataset.ecospold2")
        selected = [override for override in self.datasets if override.matches(dataset)]
        if len(selected) > 1:
            raise ValueError(f"Multiple ecoSpold2 metadata overrides match dataset {_text(dataset.get('name'))!r}.")
        merged = _deep_merge(self.defaults, embedded)
        if selected:
            merged = _deep_merge(merged, selected[0].metadata)
        return EcoSpold2DatasetMetadata.from_mapping(merged)


TechnosphereIdentity = tuple[str, str, str, str]
BiosphereIdentity = tuple[str, tuple[str, ...], str]


@dataclass(frozen=True)
class EcoSpold2ReferenceCatalog:
    """Exact identity-to-UUID references used during ecoSpold2 export."""

    technosphere: Mapping[TechnosphereIdentity, tuple[str, str]] = field(default_factory=dict)
    biosphere: Mapping[BiosphereIdentity, str] = field(default_factory=dict)
    units: Mapping[str, str] = field(default_factory=dict)
    geographies: Mapping[str, str] = field(default_factory=dict)
    schema_version: int = ECOSPOLD2_REFERENCE_SCHEMA_VERSION

    @classmethod
    def from_mapping(cls, value: object) -> "EcoSpold2ReferenceCatalog":
        data = _normalize_yaml_dates(_mapping(value, "ecoSpold2 reference catalog"))
        _validate_payload(data, "ecospold2-reference-catalog-v1.schema.json")
        technosphere = {}
        for index, row in enumerate(data.get("technosphere", ())):
            identity = _mapping(row.get("identity"), f"technosphere[{index}].identity")
            key = (
                _text(identity.get("name")),
                _text(identity.get("reference_product")),
                _text(identity.get("location")),
                _text(identity.get("unit")),
            )
            if key in technosphere:
                raise ValueError(f"Duplicate ecoSpold2 technosphere reference identity {key!r}.")
            technosphere[key] = (
                _uuid_text(row.get("activity_id"), path=f"technosphere[{index}].activity_id", required=True),
                _uuid_text(row.get("flow_id"), path=f"technosphere[{index}].flow_id", required=True),
            )
        biosphere = {}
        for index, row in enumerate(data.get("biosphere", ())):
            identity = _mapping(row.get("identity"), f"biosphere[{index}].identity")
            key = (
                _text(identity.get("name")),
                tuple(_text(item) for item in identity.get("categories", ())),
                _text(identity.get("unit")),
            )
            if key in biosphere:
                raise ValueError(f"Duplicate ecoSpold2 biosphere reference identity {key!r}.")
            biosphere[key] = _uuid_text(row.get("flow_id"), path=f"biosphere[{index}].flow_id", required=True)
        units = _reference_map(data.get("units", ()), "name", "id", "units")
        geographies = _reference_map(data.get("geographies", ()), "shortname", "id", "geographies")
        return cls(
            technosphere=technosphere,
            biosphere=biosphere,
            units=units,
            geographies=geographies,
            schema_version=_integer(data.get("schema_version"), ECOSPOLD2_REFERENCE_SCHEMA_VERSION),
        )

    @classmethod
    def from_file(cls, path: str | Path) -> "EcoSpold2ReferenceCatalog":
        return cls.from_mapping(_load_yaml_or_json(path))


def coerce_ecospold2_profile(value: object | None) -> EcoSpold2ExportProfile:
    if value is None:
        return EcoSpold2ExportProfile()
    if isinstance(value, EcoSpold2ExportProfile):
        return value
    if isinstance(value, (str, Path)):
        return EcoSpold2ExportProfile.from_file(value)
    return EcoSpold2ExportProfile.from_mapping(value)


def coerce_ecospold2_references(value: object | None) -> EcoSpold2ReferenceCatalog:
    if value is None:
        return EcoSpold2ReferenceCatalog()
    if isinstance(value, EcoSpold2ReferenceCatalog):
        return value
    if isinstance(value, (str, Path)):
        return EcoSpold2ReferenceCatalog.from_file(value)
    return EcoSpold2ReferenceCatalog.from_mapping(value)


def packaged_ecospold2_references(context: InventoryContext) -> EcoSpold2ReferenceCatalog:
    """Load exact packaged UUID references for an inventory background context."""

    if not isinstance(context, InventoryContext):
        raise TypeError("context must be an InventoryContext.")
    provider = catalog_provider_from_environment()
    technosphere = provider.load_technosphere(context.background.technosphere)
    biosphere = provider.load_biosphere(context.background.biosphere)
    return EcoSpold2ReferenceCatalog(
        technosphere=technosphere.references,
        biosphere=biosphere.references,
    )


def _reference_map(rows: object, key_name: str, id_name: str, path: str) -> dict[str, str]:
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray)):
        raise TypeError(f"{path} must be a sequence.")
    result = {}
    for index, raw in enumerate(rows):
        row = _mapping(raw, f"{path}[{index}]")
        key = _text(row.get(key_name))
        if key in result:
            raise ValueError(f"Duplicate ecoSpold2 reference {path}.{key!r}.")
        result[key] = _uuid_text(row.get(id_name), path=f"{path}[{index}].{id_name}", required=True)
    return result


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


def _load_yaml_or_json(path: str | Path) -> Mapping[str, Any]:
    source = Path(path)
    with source.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    return _mapping(payload or {}, str(source))


def _validate_payload(payload: Mapping[str, Any], schema_name: str) -> None:
    schema_path = Path(__file__).resolve().parents[1] / "data" / "schemas" / schema_name
    with schema_path.open("r", encoding="utf-8") as handle:
        schema = json.load(handle)
    errors = sorted(Draft202012Validator(schema).iter_errors(payload), key=lambda error: list(error.path))
    if errors:
        first = errors[0]
        location = ".".join(str(part) for part in first.path) or "profile"
        raise ValueError(f"{location}: {first.message}")
