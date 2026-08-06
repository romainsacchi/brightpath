"""Zipped openLCA JSON-LD syntax codec.

BrightPath v1 supports process-centric openLCA JSON-LD packages stored as one
ZIP archive with the ``olca-schema.json`` manifest at the root. The codec maps
processes into BrightPath's legacy dataset dictionaries while preserving
openLCA-specific fields in the ``openlca`` extension namespace.
"""

from __future__ import annotations

import importlib
import json
import uuid
import zipfile
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from brightpath.core.context import BackgroundContext, BiosphereProfile, FormatProfile, InventoryContext
from brightpath.exceptions import SerializationError
from brightpath.models import BackgroundProfile, InventoryDocument, InventoryFormat, default_biosphere_profile

_ROOT_MANIFEST = "olca-schema.json"
_FORMAT_ID = InventoryFormat.OPENLCA_JSONLD.value

_PACKAGE_METADATA_KEY = "openlca package"
_PROCESS_TEMPLATE_KEY = "openlca process"
_LOCATION_TEMPLATE_KEY = "openlca location"
_EXCHANGE_TEMPLATE_KEY = "openlca exchange"
_FLOW_TEMPLATE_KEY = "openlca flow"
_FLOW_PROPERTY_TEMPLATE_KEY = "openlca flow property"
_UNIT_GROUP_TEMPLATE_KEY = "openlca unit group"
_PARAMETER_TEMPLATE_KEY = "openlca parameter"
_BRIGHTPATH_OTHER_PROPERTIES_KEY = "brightpath"
_GLOBAL_PARAMETER_TARGET_KEY = "brightpath parameter target"
_BRIGHTPATH_FLOW_EXCHANGE_PROPERTIES_KEY = "brightpathExchangeProperties"

_SUPPORTED_AUXILIARY_FOLDERS = frozenset(
    {
        "actors",
        "currencies",
        "dq_systems",
        "social_indicators",
        "sources",
    }
)
_SUPPORTED_ROOT_FOLDERS = frozenset(
    {
        "processes",
        "flows",
        "flow_properties",
        "unit_groups",
        "locations",
        "parameters",
        *_SUPPORTED_AUXILIARY_FOLDERS,
    }
)
_UNSUPPORTED_ROOT_FOLDERS = frozenset(
    {
        "epds",
        "lcia_categories",
        "lcia_methods",
        "product_systems",
        "projects",
        "results",
    }
)

_ROOT_FOLDER_LABELS = {
    "actors": "Actor",
    "currencies": "Currency",
    "dq_systems": "DQSystem",
    "epds": "EPD",
    "flow_properties": "FlowProperty",
    "flows": "Flow",
    "lcia_categories": "ImpactCategory",
    "lcia_methods": "ImpactMethod",
    "locations": "Location",
    "parameters": "Parameter",
    "processes": "Process",
    "product_systems": "ProductSystem",
    "projects": "Project",
    "results": "Result",
    "social_indicators": "SocialIndicator",
    "sources": "Source",
    "unit_groups": "UnitGroup",
}

_DATASET_TEMPLATE_KEYS = frozenset({_PROCESS_TEMPLATE_KEY, _LOCATION_TEMPLATE_KEY})
_EXCHANGE_TEMPLATE_KEYS = frozenset(
    {
        _EXCHANGE_TEMPLATE_KEY,
        _FLOW_TEMPLATE_KEY,
        _FLOW_PROPERTY_TEMPLATE_KEY,
        _UNIT_GROUP_TEMPLATE_KEY,
        _LOCATION_TEMPLATE_KEY,
    }
)
_PARAMETER_TEMPLATE_KEYS = frozenset({_PARAMETER_TEMPLATE_KEY})

_DATASET_MAPPED_KEYS = frozenset(
    {
        "name",
        "reference product",
        "product",
        "location",
        "unit",
        "code",
        "comment",
        "exchanges",
        "parameters",
    }
)
_EXCHANGE_MAPPED_KEYS = frozenset(
    {
        "name",
        "reference product",
        "product",
        "location",
        "unit",
        "code",
        "categories",
        "type",
        "amount",
        "formula",
        "comment",
        "uncertainty type",
        "loc",
        "scale",
        "shape",
        "minimum",
        "maximum",
        "min",
        "max",
    }
)
_PARAMETER_MAPPED_KEYS = frozenset(
    {
        "name",
        "amount",
        "formula",
        "group",
        "comment",
        "uncertainty type",
        "loc",
        "scale",
        "shape",
        "minimum",
        "maximum",
        "min",
        "max",
    }
)
_BRIGHTWAY_TRIANGLE_UNCERTAINTY = 5
_ALLOWED_BIOSPHERE_ROOTS = frozenset({"air", "water", "soil", "natural resource"})


@dataclass(frozen=True)
class OpenLCAJSONLDPackage:
    """Dictionary-backed components parsed from one openLCA package."""

    data: list[dict]
    database_name: str
    metadata: dict
    database_parameters: list[dict] | None = None
    project_parameters: list[dict] | None = None


@dataclass(frozen=True)
class _ProducerInfo:
    process_id: str
    process_name: str
    reference_product: str
    location: str
    unit: str
    flow_id: str


@dataclass(frozen=True)
class _PreparedDataset:
    dataset: dict[str, Any]
    process: Any
    process_ref: Any
    location_ref: Any | None
    production_exchange: dict[str, Any]
    production_flow: Any
    production_flow_ref: Any
    production_flow_property_ref: Any
    production_unit_ref: Any


@dataclass(frozen=True)
class _RenderedPackage:
    actors: dict[str, Any]
    currencies: dict[str, Any]
    dq_systems: dict[str, Any]
    social_indicators: dict[str, Any]
    sources: dict[str, Any]
    locations: dict[str, Any]
    unit_groups: dict[str, Any]
    flow_properties: dict[str, Any]
    flows: dict[str, Any]
    parameters: dict[str, Any]
    processes: dict[str, Any]


def load_openlca_jsonld_package(
    path: str | Path,
    *,
    database_name: str | None = None,
) -> OpenLCAJSONLDPackage:
    """Parse a zipped openLCA JSON-LD package into BrightPath dictionaries."""

    schema, _zipio = _olca_modules()
    source = Path(path).expanduser()
    if not source.is_file():
        raise FileNotFoundError(f"openLCA JSON-LD package not found: {source}")

    manifest, raw_entities = _read_raw_package(source)
    unsupported = [folder for folder in _UNSUPPORTED_ROOT_FOLDERS if raw_entities.get(folder)]
    if unsupported:
        labels = ", ".join(_ROOT_FOLDER_LABELS[folder] for folder in sorted(unsupported))
        raise ValueError(
            "BrightPath supports process-only openLCA JSON-LD packages; "
            f"unsupported root entities were found: {labels}."
        )
    unknown = [
        folder for folder in raw_entities if folder not in _SUPPORTED_ROOT_FOLDERS and folder not in {_ROOT_MANIFEST}
    ]
    if unknown:
        labels = ", ".join(sorted(unknown))
        raise ValueError(f"Unsupported openLCA JSON-LD package folders: {labels}.")
    if not raw_entities.get("processes"):
        raise ValueError("openLCA JSON-LD packages must contain at least one Process entity.")

    flow_lookup = _materialize_entities(schema.Flow, raw_entities.get("flows", []))
    flow_property_lookup = _materialize_entities(schema.FlowProperty, raw_entities.get("flow_properties", []))
    unit_group_lookup = _materialize_entities(schema.UnitGroup, raw_entities.get("unit_groups", []))
    location_lookup = _materialize_entities(schema.Location, raw_entities.get("locations", []))

    producers_by_process, producers_by_flow = _build_producer_indexes(
        schema,
        raw_entities.get("processes", []),
        flow_lookup,
        flow_property_lookup,
        unit_group_lookup,
        location_lookup,
    )

    inventory_data: list[dict] = []
    for raw_process in raw_entities.get("processes", []):
        process = _hydrate_entity(schema.Process, raw_process)
        info = producers_by_process[process.id or ""]
        inventory_data.append(
            _process_to_legacy_dataset(
                process=process,
                raw_process=raw_process,
                producer=info,
                producers_by_flow=producers_by_flow,
                flow_lookup=flow_lookup,
                raw_flows=_raw_entity_lookup(raw_entities.get("flows", [])),
                flow_property_lookup=flow_property_lookup,
                raw_flow_properties=_raw_entity_lookup(raw_entities.get("flow_properties", [])),
                unit_group_lookup=unit_group_lookup,
                raw_unit_groups=_raw_entity_lookup(raw_entities.get("unit_groups", [])),
                location_lookup=location_lookup,
                raw_locations=_raw_entity_lookup(raw_entities.get("locations", [])),
            )
        )

    raw_global_parameters = raw_entities.get("parameters", [])
    database_parameters: list[dict] = []
    project_parameters: list[dict] = []
    for raw_parameter in raw_global_parameters:
        parameter = _hydrate_entity(schema.Parameter, raw_parameter)
        target = _global_parameter_target(raw_parameter)
        legacy = _parameter_to_legacy(parameter, raw_parameter)
        if target == "project":
            project_parameters.append(legacy)
        else:
            database_parameters.append(legacy)

    package_metadata = {
        "manifest": manifest,
    }
    for folder in sorted(_SUPPORTED_AUXILIARY_FOLDERS):
        values = raw_entities.get(folder)
        if values:
            package_metadata[folder] = deepcopy(values)

    metadata = {_PACKAGE_METADATA_KEY: package_metadata}
    return OpenLCAJSONLDPackage(
        data=inventory_data,
        database_name=database_name or source.stem,
        metadata=metadata,
        database_parameters=(database_parameters or None),
        project_parameters=(project_parameters or None),
    )


def load_openlca_jsonld(
    path: str | Path,
    *,
    background_profile: BackgroundProfile | None = None,
    biosphere_profile: BiosphereProfile | None = None,
    context: InventoryContext | None = None,
    database_name: str | None = None,
) -> InventoryDocument:
    """Load a zipped openLCA JSON-LD package into an inventory document."""

    package = load_openlca_jsonld_package(path, database_name=database_name)
    if context is None:
        if background_profile is None:
            raise TypeError("background_profile or context must be provided.")
        profile = background_profile.normalized()
        technosphere = profile.to_technosphere_profile()
        context = InventoryContext(
            format=FormatProfile(_FORMAT_ID, encoding="utf-8"),
            background=BackgroundContext(
                technosphere=technosphere,
                biosphere=biosphere_profile or default_biosphere_profile(technosphere),
            ),
        )
    else:
        if context.format.format_id != _FORMAT_ID:
            raise ValueError(f"Explicit context format must be {_FORMAT_ID}.")
        if (
            background_profile is not None
            and background_profile.normalized()
            != BackgroundProfile.from_technosphere_profile(context.background.technosphere)
        ):
            raise ValueError("background_profile conflicts with context.technosphere.")
        if biosphere_profile is not None and biosphere_profile != context.background.biosphere:
            raise ValueError("biosphere_profile conflicts with context.biosphere.")

    return InventoryDocument(
        data=package.data,
        context=context,
        database_name=package.database_name,
        metadata=package.metadata,
        database_parameters=package.database_parameters,
        project_parameters=package.project_parameters,
    )


def write_openlca_jsonld(
    document: InventoryDocument,
    path: str | Path,
) -> Path:
    """Write an inventory document as a zipped openLCA JSON-LD package."""

    if not isinstance(document, InventoryDocument):
        raise TypeError("document must be an InventoryDocument.")

    _schema, zipio = _olca_modules()
    destination = Path(path).expanduser()
    if destination.suffix == "":
        destination = destination.with_suffix(".zip")
    if destination.suffix.lower() != ".zip":
        raise ValueError("openLCA JSON-LD exports must use a .zip filename.")
    destination = destination.resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)

    rendered = render_openlca_jsonld_package(document)
    with zipio.ZipWriter(destination) as writer:
        for store in (
            rendered.actors,
            rendered.currencies,
            rendered.dq_systems,
            rendered.social_indicators,
            rendered.sources,
            rendered.locations,
            rendered.unit_groups,
            rendered.flow_properties,
            rendered.flows,
            rendered.parameters,
            rendered.processes,
        ):
            for entity in store.values():
                writer.write(entity)

    return destination


def render_openlca_jsonld_package(document: InventoryDocument) -> _RenderedPackage:
    """Build all openLCA root entities in memory without writing a ZIP archive."""

    schema, _zipio = _olca_modules()
    builder = _OpenLCAPackageBuilder(schema, document.metadata)
    return builder.build(document)


def _olca_modules() -> tuple[Any, Any]:
    try:
        schema = importlib.import_module("olca_schema")
        zipio = importlib.import_module("olca_schema.zipio")
    except ModuleNotFoundError as error:
        raise RuntimeError(
            "openLCA JSON-LD support requires the optional 'olca-schema' dependency. "
            "Install BrightPath in a Python 3.12+ environment with its declared dependencies."
        ) from error
    return schema, zipio


def _read_raw_package(source: Path) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]:
    try:
        with zipfile.ZipFile(source) as archive:
            if _ROOT_MANIFEST not in archive.namelist():
                raise ValueError("The ZIP archive is missing the olca-schema.json manifest.")
            try:
                manifest = json.loads(archive.read(_ROOT_MANIFEST).decode("utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
                raise ValueError(f"Could not read the openLCA package manifest: {error}") from error
            if int(manifest.get("version", 0) or 0) != 2:
                raise ValueError(
                    f"Unsupported openLCA package manifest version {manifest.get('version')!r}; expected 2."
                )

            raw_entities: dict[str, list[dict[str, Any]]] = {}
            for info in archive.infolist():
                if info.is_dir() or info.filename == _ROOT_MANIFEST or not info.filename.endswith(".json"):
                    continue
                parts = [part for part in info.filename.split("/") if part]
                if len(parts) != 2:
                    raise ValueError(f"Unsupported JSON-LD package path {info.filename!r}.")
                folder, _name = parts
                try:
                    payload = json.loads(archive.read(info).decode("utf-8"))
                except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
                    raise ValueError(f"Could not parse {info.filename!r}: {error}") from error
                if not isinstance(payload, dict):
                    raise ValueError(f"Expected JSON object in {info.filename!r}.")
                raw_entities.setdefault(folder, []).append(payload)
            return manifest, raw_entities
    except zipfile.BadZipFile as error:
        raise ValueError(f"The archive is not a valid ZIP file: {error}") from error


def _materialize_entities(entity_class: Any, raw_entities: list[dict[str, Any]]) -> dict[str, Any]:
    materialized: dict[str, Any] = {}
    for raw_entity in raw_entities:
        entity = _hydrate_entity(entity_class, raw_entity)
        if not entity.id:
            raise ValueError(f"{entity_class.__name__} entities must define @id.")
        materialized[entity.id] = entity
    return materialized


def _raw_entity_lookup(raw_entities: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for raw_entity in raw_entities:
        entity_id = str(raw_entity.get("@id") or "").strip()
        if entity_id:
            lookup[entity_id] = deepcopy(raw_entity)
    return lookup


def _hydrate_entity(entity_class: Any, raw_entity: dict[str, Any] | None) -> Any:
    if raw_entity:
        entity = entity_class.from_dict(deepcopy(raw_entity))
    else:
        entity = entity_class()
    post_init = getattr(entity, "__post_init__", None)
    if callable(post_init):
        post_init()
    return entity


def _build_producer_indexes(
    schema: Any,
    raw_processes: list[dict[str, Any]],
    flow_lookup: dict[str, Any],
    flow_property_lookup: dict[str, Any],
    unit_group_lookup: dict[str, Any],
    location_lookup: dict[str, Any],
) -> tuple[dict[str, _ProducerInfo], dict[str, list[_ProducerInfo]]]:
    by_process: dict[str, _ProducerInfo] = {}
    by_flow: dict[str, list[_ProducerInfo]] = {}
    for raw_process in raw_processes:
        process = _hydrate_entity(schema.Process, raw_process)
        if not process.id:
            raise ValueError("Process entities must define @id.")
        production_exchange, production_flow = _select_quantitative_reference(process, flow_lookup)
        info = _ProducerInfo(
            process_id=process.id,
            process_name=str(process.name or ""),
            reference_product=str(production_flow.name or ""),
            location=_resolve_location_code(process.location, location_lookup),
            unit=_resolve_unit_name(production_exchange, production_flow, flow_property_lookup, unit_group_lookup),
            flow_id=str(production_flow.id or ""),
        )
        by_process[process.id] = info
        if info.flow_id:
            by_flow.setdefault(info.flow_id, []).append(info)
    return by_process, by_flow


def _select_quantitative_reference(process: Any, flow_lookup: dict[str, Any]) -> tuple[Any, Any]:
    process_label = str(process.name or process.id or "unnamed process")
    non_elementary_outputs: list[tuple[Any, Any]] = []
    for exchange in process.exchanges or []:
        flow = _require_flow(exchange, flow_lookup, process_label)
        if exchange.is_avoided_product:
            raise ValueError(
                f"Process {process_label!r} contains avoided-product exchanges, which BrightPath cannot map."
            )
        flow_type_name = str(getattr(getattr(flow, "flow_type", None), "name", "") or "")
        if not exchange.is_input and flow_type_name != "ELEMENTARY_FLOW":
            non_elementary_outputs.append((exchange, flow))
    if not non_elementary_outputs:
        raise ValueError(f"Process {process_label!r} does not define a non-elementary output exchange.")
    if len(non_elementary_outputs) > 1:
        raise ValueError(
            f"Process {process_label!r} has multiple non-elementary output exchanges; "
            "BrightPath v1 supports only single-output process packages."
        )
    return non_elementary_outputs[0]


def _require_flow(exchange: Any, flow_lookup: dict[str, Any], process_label: str) -> Any:
    flow_id = str(getattr(getattr(exchange, "flow", None), "id", "") or "")
    if not flow_id:
        raise ValueError(f"Process {process_label!r} contains an exchange without a flow reference.")
    try:
        return flow_lookup[flow_id]
    except KeyError as error:
        raise ValueError(f"Process {process_label!r} references missing flow {flow_id!r}.") from error


def _process_to_legacy_dataset(
    *,
    process: Any,
    raw_process: dict[str, Any],
    producer: _ProducerInfo,
    producers_by_flow: dict[str, list[_ProducerInfo]],
    flow_lookup: dict[str, Any],
    raw_flows: dict[str, dict[str, Any]],
    flow_property_lookup: dict[str, Any],
    raw_flow_properties: dict[str, dict[str, Any]],
    unit_group_lookup: dict[str, Any],
    raw_unit_groups: dict[str, dict[str, Any]],
    location_lookup: dict[str, Any],
    raw_locations: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    process_location = _resolve_location_code(process.location, location_lookup)
    dataset = {
        "name": producer.process_name,
        "reference product": producer.reference_product,
        "location": process_location,
        "unit": producer.unit,
        "code": producer.process_id,
        "comment": str(process.description or ""),
        "exchanges": [],
    }
    dataset.update(_brightpath_other_properties(raw_process))
    dataset[_PROCESS_TEMPLATE_KEY] = deepcopy(raw_process)
    if location_raw := _raw_ref_entity(process.location, raw_locations):
        dataset[_LOCATION_TEMPLATE_KEY] = location_raw

    raw_exchanges = raw_process.get("exchanges") or []
    for index, exchange in enumerate(process.exchanges or []):
        raw_exchange = (
            raw_exchanges[index] if index < len(raw_exchanges) and isinstance(raw_exchanges[index], dict) else {}
        )
        legacy_exchange = _exchange_to_legacy(
            exchange=exchange,
            raw_exchange=raw_exchange,
            process=process,
            producer=producer,
            producers_by_flow=producers_by_flow,
            flow_lookup=flow_lookup,
            raw_flows=raw_flows,
            flow_property_lookup=flow_property_lookup,
            raw_flow_properties=raw_flow_properties,
            unit_group_lookup=unit_group_lookup,
            raw_unit_groups=raw_unit_groups,
            location_lookup=location_lookup,
            raw_locations=raw_locations,
        )
        dataset["exchanges"].append(legacy_exchange)

    raw_parameters = raw_process.get("parameters") or []
    parameters: list[dict[str, Any]] = []
    for index, parameter in enumerate(process.parameters or []):
        raw_parameter = (
            raw_parameters[index] if index < len(raw_parameters) and isinstance(raw_parameters[index], dict) else {}
        )
        parameters.append(_parameter_to_legacy(parameter, raw_parameter))
    if parameters:
        dataset["parameters"] = parameters
    return dataset


def _exchange_to_legacy(
    *,
    exchange: Any,
    raw_exchange: dict[str, Any],
    process: Any,
    producer: _ProducerInfo,
    producers_by_flow: dict[str, list[_ProducerInfo]],
    flow_lookup: dict[str, Any],
    raw_flows: dict[str, dict[str, Any]],
    flow_property_lookup: dict[str, Any],
    raw_flow_properties: dict[str, dict[str, Any]],
    unit_group_lookup: dict[str, Any],
    raw_unit_groups: dict[str, dict[str, Any]],
    location_lookup: dict[str, Any],
    raw_locations: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    flow = _require_flow(exchange, flow_lookup, str(process.name or process.id or "unnamed process"))
    flow_property = _resolve_flow_property(exchange, flow, flow_property_lookup)
    flow_type = flow.flow_type
    legacy: dict[str, Any] = {}

    if flow_type == flow_type.ELEMENTARY_FLOW:
        legacy["type"] = "biosphere"
        legacy["name"] = str(flow.name or "")
        legacy["categories"] = _decode_biosphere_categories(flow.category)
        location_ref = exchange.location or flow.location
    else:
        is_production = str(flow.id or "") == producer.flow_id and not exchange.is_input
        legacy["type"] = "production" if is_production else "technosphere"
        legacy["reference product"] = str(flow.name or "")
        location_ref = exchange.location or flow.location or process.location
        provider_name = ""
        provider_location = _resolve_location_code(location_ref, location_lookup)

        if legacy["type"] == "production":
            provider_name = producer.process_name
            provider_location = producer.location or provider_location
        elif exchange.default_provider is not None:
            provider_name = str(exchange.default_provider.name or "")
            provider_location = (
                _resolve_provider_location(exchange.default_provider, location_lookup) or provider_location
            )
        else:
            candidates = producers_by_flow.get(str(flow.id or ""), [])
            if len(candidates) == 1:
                provider_name = candidates[0].process_name
                provider_location = candidates[0].location or provider_location

        legacy["name"] = provider_name
        legacy["location"] = provider_location

    unit_name = _resolve_unit_name(exchange, flow, flow_property_lookup, unit_group_lookup)
    if unit_name:
        legacy["unit"] = unit_name
    if exchange.amount is not None:
        legacy["amount"] = exchange.amount
    if exchange.amount_formula:
        legacy["formula"] = str(exchange.amount_formula)
    if exchange.description:
        legacy["comment"] = str(exchange.description)
    if legacy["type"] != "production":
        if flow.id:
            legacy["code"] = str(flow.id)
    else:
        legacy["code"] = producer.process_id
    legacy.update(_legacy_uncertainty(exchange.uncertainty))
    legacy.update(_brightpath_other_properties(raw_exchange))
    legacy[_EXCHANGE_TEMPLATE_KEY] = deepcopy(raw_exchange)
    if raw_flow := raw_flows.get(str(flow.id or "")):
        legacy.update(_flow_exchange_properties(raw_flow, raw_exchange))
        legacy[_FLOW_TEMPLATE_KEY] = raw_flow
    if flow_property is not None and flow_property.id and flow_property.id in raw_flow_properties:
        legacy[_FLOW_PROPERTY_TEMPLATE_KEY] = raw_flow_properties[flow_property.id]
    if flow_property is not None and getattr(flow_property, "unit_group", None) is not None:
        unit_group_id = str(flow_property.unit_group.id or "")
        if unit_group_id and unit_group_id in raw_unit_groups:
            legacy[_UNIT_GROUP_TEMPLATE_KEY] = raw_unit_groups[unit_group_id]
    if location_raw := _raw_ref_entity(location_ref, raw_locations):
        legacy[_LOCATION_TEMPLATE_KEY] = location_raw
    return legacy


def _parameter_to_legacy(parameter: Any, raw_parameter: dict[str, Any]) -> dict[str, Any]:
    legacy = {
        "name": str(parameter.name or ""),
    }
    if parameter.value is not None:
        legacy["amount"] = parameter.value
    if parameter.formula:
        legacy["formula"] = str(parameter.formula)
    if parameter.description:
        legacy["comment"] = str(parameter.description)
    legacy.update(_brightpath_other_properties(raw_parameter))
    if not legacy.get("group"):
        legacy["group"] = _parameter_group(raw_parameter, parameter)
    legacy.update(_legacy_uncertainty(parameter.uncertainty))
    legacy[_PARAMETER_TEMPLATE_KEY] = deepcopy(raw_parameter)
    return legacy


def _parameter_group(raw_parameter: dict[str, Any], parameter: Any) -> str:
    other_properties = raw_parameter.get("otherProperties")
    if isinstance(other_properties, dict):
        brightpath = other_properties.get(_BRIGHTPATH_OTHER_PROPERTIES_KEY)
        if isinstance(brightpath, dict):
            value = brightpath.get("group")
            if value is not None:
                return str(value)
        value = other_properties.get("group")
        if value is not None:
            return str(value)
    if getattr(parameter, "category", None):
        return str(parameter.category)
    return ""


def _global_parameter_target(raw_parameter: dict[str, Any]) -> str:
    other_properties = raw_parameter.get("otherProperties")
    if not isinstance(other_properties, dict):
        return "database"
    value = other_properties.get(_GLOBAL_PARAMETER_TARGET_KEY)
    if value is None:
        brightpath = other_properties.get(_BRIGHTPATH_OTHER_PROPERTIES_KEY)
        if isinstance(brightpath, dict):
            value = brightpath.get(_GLOBAL_PARAMETER_TARGET_KEY)
    return "project" if str(value or "").strip().lower() == "project" else "database"


def _brightpath_other_properties(raw_entity: dict[str, Any]) -> dict[str, Any]:
    other_properties = raw_entity.get("otherProperties")
    if not isinstance(other_properties, dict):
        return {}
    brightpath = other_properties.get(_BRIGHTPATH_OTHER_PROPERTIES_KEY)
    return deepcopy(brightpath) if isinstance(brightpath, dict) else {}


def _flow_exchange_properties(raw_flow: dict[str, Any], raw_exchange: dict[str, Any]) -> dict[str, Any]:
    other_properties = raw_flow.get("otherProperties")
    if not isinstance(other_properties, dict):
        return {}
    values = other_properties.get(_BRIGHTPATH_FLOW_EXCHANGE_PROPERTIES_KEY)
    if not isinstance(values, dict):
        return {}
    internal_id = str(raw_exchange.get("internalId") or "")
    selected = values.get(internal_id)
    return deepcopy(selected) if isinstance(selected, dict) else {}


def _legacy_uncertainty(uncertainty: Any) -> dict[str, Any]:
    if uncertainty is None:
        return {}
    distribution = getattr(uncertainty, "distribution_type", None)
    if distribution is None:
        return {}

    name = str(getattr(distribution, "name", distribution))
    if name == "LOG_NORMAL_DISTRIBUTION":
        return {
            "uncertainty type": 2,
            "loc": uncertainty.geom_mean if uncertainty.geom_mean is not None else uncertainty.mean,
            "scale": uncertainty.geom_sd if uncertainty.geom_sd is not None else uncertainty.sd,
            **({"minimum": uncertainty.minimum} if uncertainty.minimum is not None else {}),
            **({"maximum": uncertainty.maximum} if uncertainty.maximum is not None else {}),
        }
    if name == "NORMAL_DISTRIBUTION":
        return {
            "uncertainty type": 3,
            "loc": uncertainty.mean,
            "scale": uncertainty.sd,
            **({"minimum": uncertainty.minimum} if uncertainty.minimum is not None else {}),
            **({"maximum": uncertainty.maximum} if uncertainty.maximum is not None else {}),
        }
    if name == "UNIFORM_DISTRIBUTION":
        return {
            "uncertainty type": 4,
            **({"minimum": uncertainty.minimum} if uncertainty.minimum is not None else {}),
            **({"maximum": uncertainty.maximum} if uncertainty.maximum is not None else {}),
        }
    if name == "TRIANGLE_DISTRIBUTION":
        return {
            "uncertainty type": _BRIGHTWAY_TRIANGLE_UNCERTAINTY,
            **({"minimum": uncertainty.minimum} if uncertainty.minimum is not None else {}),
            **({"maximum": uncertainty.maximum} if uncertainty.maximum is not None else {}),
            **({"loc": uncertainty.mode} if uncertainty.mode is not None else {}),
        }
    return {}


def _resolve_flow_property(exchange: Any, flow: Any, flow_property_lookup: dict[str, Any]) -> Any | None:
    flow_property_id = str(getattr(getattr(exchange, "flow_property", None), "id", "") or "")
    if flow_property_id:
        return flow_property_lookup.get(flow_property_id)
    for factor in getattr(flow, "flow_properties", None) or []:
        factor_property = getattr(factor, "flow_property", None)
        factor_id = str(getattr(factor_property, "id", "") or "")
        if getattr(factor, "is_ref_flow_property", False) and factor_id:
            return flow_property_lookup.get(factor_id)
    for factor in getattr(flow, "flow_properties", None) or []:
        factor_property = getattr(factor, "flow_property", None)
        factor_id = str(getattr(factor_property, "id", "") or "")
        if factor_id:
            return flow_property_lookup.get(factor_id)
    return None


def _resolve_unit_name(
    exchange: Any, flow: Any, flow_property_lookup: dict[str, Any], unit_group_lookup: dict[str, Any]
) -> str:
    flow_property = _resolve_flow_property(exchange, flow, flow_property_lookup)
    exchange_unit = getattr(exchange, "unit", None)
    if flow_property is not None:
        unit_group_id = str(getattr(getattr(flow_property, "unit_group", None), "id", "") or "")
        unit_group = unit_group_lookup.get(unit_group_id)
        if unit_group is not None:
            if exchange_unit is not None:
                for unit in unit_group.units or []:
                    if (exchange_unit.id and exchange_unit.id == unit.id) or (
                        exchange_unit.name and exchange_unit.name == unit.name
                    ):
                        return str(unit.name or exchange_unit.name or "")
            for unit in unit_group.units or []:
                if unit.is_ref_unit:
                    return str(unit.name or "")
            if unit_group.units:
                return str(unit_group.units[0].name or "")
    return str(getattr(exchange_unit, "name", "") or "")


def _resolve_location_code(location_ref: Any, location_lookup: dict[str, Any]) -> str:
    location_id = str(getattr(location_ref, "id", "") or "")
    if location_id:
        location = location_lookup.get(location_id)
        if location is not None:
            return str(location.code or location.name or location_ref.name or location_ref.location or "")
    return str(getattr(location_ref, "name", "") or getattr(location_ref, "location", "") or "")


def _resolve_provider_location(provider_ref: Any, location_lookup: dict[str, Any]) -> str:
    if provider_ref is None:
        return ""
    value = str(getattr(provider_ref, "location", "") or "")
    if value:
        return value
    return _resolve_location_code(provider_ref, location_lookup)


def _raw_ref_entity(reference: Any, raw_lookup: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    entity_id = str(getattr(reference, "id", "") or "")
    if entity_id and entity_id in raw_lookup:
        return deepcopy(raw_lookup[entity_id])
    return None


def _decode_biosphere_categories(category: Any) -> tuple[str, ...]:
    raw = str(category or "").strip()
    if not raw:
        return ()
    parts = [part.strip() for part in raw.split("/") if part.strip()]
    if not parts:
        return ()
    first = parts[0].casefold()
    if first.startswith("emissions to "):
        first = first.replace("emissions to ", "", 1).strip()
    elif first in {"resource", "resources", "natural resources"}:
        first = "natural resource"
    parts[0] = first
    return tuple(parts)


def _merge_brightpath_other_properties(existing: Any, extras: dict[str, Any]) -> dict[str, Any] | None:
    merged = deepcopy(existing) if isinstance(existing, dict) else {}
    if extras:
        brightpath = merged.get(_BRIGHTPATH_OTHER_PROPERTIES_KEY)
        if not isinstance(brightpath, dict):
            brightpath = {}
        brightpath.update(deepcopy(extras))
        merged[_BRIGHTPATH_OTHER_PROPERTIES_KEY] = brightpath
    return merged or None


def _dataset_extras(dataset: dict[str, Any]) -> dict[str, Any]:
    return {
        key: deepcopy(value)
        for key, value in dataset.items()
        if key not in _DATASET_MAPPED_KEYS | _DATASET_TEMPLATE_KEYS
    }


def _exchange_extras(exchange: dict[str, Any]) -> dict[str, Any]:
    return {
        key: deepcopy(value)
        for key, value in exchange.items()
        if key not in _EXCHANGE_MAPPED_KEYS | _EXCHANGE_TEMPLATE_KEYS
    }


def _parameter_extras(parameter: dict[str, Any], *, target: str) -> dict[str, Any]:
    extras = {
        key: deepcopy(value)
        for key, value in parameter.items()
        if key not in _PARAMETER_MAPPED_KEYS | _PARAMETER_TEMPLATE_KEYS
    }
    if parameter.get("group"):
        extras.setdefault("group", deepcopy(parameter.get("group")))
    if target in {"database", "project"}:
        extras.setdefault(_GLOBAL_PARAMETER_TARGET_KEY, target)
    return extras


def _schema_uncertainty(schema: Any, legacy: dict[str, Any]) -> Any | None:
    uncertainty_type = legacy.get("uncertainty type")
    if uncertainty_type in (None, "", 0):
        return None
    uncertainty = schema.Uncertainty()
    if uncertainty_type == 2:
        uncertainty.distribution_type = schema.UncertaintyType.LOG_NORMAL_DISTRIBUTION
        if legacy.get("loc") is not None:
            uncertainty.geom_mean = float(legacy["loc"])
        if legacy.get("scale") is not None:
            uncertainty.geom_sd = float(legacy["scale"])
    elif uncertainty_type == 3:
        uncertainty.distribution_type = schema.UncertaintyType.NORMAL_DISTRIBUTION
        if legacy.get("loc") is not None:
            uncertainty.mean = float(legacy["loc"])
        if legacy.get("scale") is not None:
            uncertainty.sd = float(legacy["scale"])
    elif uncertainty_type == 4:
        uncertainty.distribution_type = schema.UncertaintyType.UNIFORM_DISTRIBUTION
    elif uncertainty_type == _BRIGHTWAY_TRIANGLE_UNCERTAINTY:
        uncertainty.distribution_type = schema.UncertaintyType.TRIANGLE_DISTRIBUTION
        if legacy.get("loc") is not None:
            uncertainty.mode = float(legacy["loc"])
    else:
        return None
    if legacy.get("minimum") is not None:
        uncertainty.minimum = float(legacy["minimum"])
    if legacy.get("maximum") is not None:
        uncertainty.maximum = float(legacy["maximum"])
    return uncertainty


def _require_mapping(value: Any, label: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise SerializationError(f"{label} must be a dictionary when present.")
    return deepcopy(value)


def _production_exchange(dataset: dict[str, Any]) -> dict[str, Any]:
    matches = [
        exchange
        for exchange in dataset.get("exchanges", [])
        if isinstance(exchange, dict) and exchange.get("type") == "production"
    ]
    if len(matches) != 1:
        raise SerializationError(
            f"Dataset {dataset.get('name')!r} must contain exactly one production exchange to export openLCA JSON-LD."
        )
    return deepcopy(matches[0])


def _schema_ref(entity: Any, **overrides: Any) -> Any:
    ref = entity.to_ref()
    for key, value in overrides.items():
        if value not in (None, ""):
            setattr(ref, key, value)
    return ref


class _OpenLCAPackageBuilder:
    def __init__(self, schema: Any, metadata: dict[str, Any]) -> None:
        self.schema = schema
        self.metadata = deepcopy(metadata)

        self.actors: dict[str, Any] = {}
        self.currencies: dict[str, Any] = {}
        self.dq_systems: dict[str, Any] = {}
        self.social_indicators: dict[str, Any] = {}
        self.sources: dict[str, Any] = {}
        self.locations: dict[str, Any] = {}
        self.unit_groups: dict[str, Any] = {}
        self.flow_properties: dict[str, Any] = {}
        self.flows: dict[str, Any] = {}
        self.parameters: dict[str, Any] = {}
        self.processes: dict[str, Any] = {}

        self.local_processes: dict[tuple[str, str, str, str], tuple[Any, Any, Any, Any]] = {}

    def build(self, document: InventoryDocument) -> _RenderedPackage:
        if not isinstance(document, InventoryDocument):
            raise TypeError("document must be an InventoryDocument.")

        self._register_package_metadata()
        prepared = [self._prepare_dataset(dataset) for dataset in document.data]
        for state in prepared:
            process = self._build_process(state)
            self.processes[process.id] = process

        for parameter in document.database_parameters or []:
            self._register_global_parameter(parameter, target="database")
        for parameter in document.project_parameters or []:
            self._register_global_parameter(parameter, target="project")

        return _RenderedPackage(
            actors=self.actors,
            currencies=self.currencies,
            dq_systems=self.dq_systems,
            social_indicators=self.social_indicators,
            sources=self.sources,
            locations=self.locations,
            unit_groups=self.unit_groups,
            flow_properties=self.flow_properties,
            flows=self.flows,
            parameters=self.parameters,
            processes=self.processes,
        )

    def _register_package_metadata(self) -> None:
        package = self.metadata.get(_PACKAGE_METADATA_KEY)
        if not isinstance(package, dict):
            return
        for folder, store, entity_class in (
            ("actors", self.actors, self.schema.Actor),
            ("currencies", self.currencies, self.schema.Currency),
            ("dq_systems", self.dq_systems, self.schema.DQSystem),
            ("social_indicators", self.social_indicators, self.schema.SocialIndicator),
            ("sources", self.sources, self.schema.Source),
        ):
            for raw_entity in package.get(folder, []) or []:
                entity = _hydrate_entity(entity_class, raw_entity if isinstance(raw_entity, dict) else {})
                if not entity.id:
                    raise SerializationError(f"Packaged {entity_class.__name__} metadata must define @id.")
                store[entity.id] = entity

    def _prepare_dataset(self, dataset: dict[str, Any]) -> _PreparedDataset:
        process_template = _require_mapping(dataset.get(_PROCESS_TEMPLATE_KEY), _PROCESS_TEMPLATE_KEY)
        process = _hydrate_entity(self.schema.Process, process_template)
        production_exchange = _production_exchange(dataset)
        process.id = str(
            process.id
            or dataset.get("code")
            or _stable_uuid(
                "process",
                dataset.get("name"),
                dataset.get("reference product"),
                dataset.get("location"),
                dataset.get("unit"),
            )
        )
        process.name = str(dataset.get("name") or process.name or "")
        process.description = str(dataset.get("comment") or process.description or "")
        process.process_type = process.process_type or self.schema.ProcessType.UNIT_PROCESS
        process.other_properties = _merge_brightpath_other_properties(
            process.other_properties, _dataset_extras(dataset)
        )

        location_ref = self._ensure_location(
            str(dataset.get("location") or ""),
            _require_mapping(dataset.get(_LOCATION_TEMPLATE_KEY), _LOCATION_TEMPLATE_KEY),
        )
        if location_ref is not None:
            process.location = location_ref
        process_ref = _schema_ref(
            process,
            location=str(dataset.get("location") or ""),
            process_type=self.schema.ProcessType.UNIT_PROCESS,
        )

        flow_property_ref, unit_ref = self._ensure_quantity_entities(
            unit_name=str(production_exchange.get("unit") or ""),
            flow_property_template=_require_mapping(
                production_exchange.get(_FLOW_PROPERTY_TEMPLATE_KEY), _FLOW_PROPERTY_TEMPLATE_KEY
            ),
            unit_group_template=_require_mapping(
                production_exchange.get(_UNIT_GROUP_TEMPLATE_KEY), _UNIT_GROUP_TEMPLATE_KEY
            ),
            label=f"dataset {dataset.get('name')!r} production exchange",
        )
        production_flow = self._ensure_flow(
            exchange=production_exchange,
            flow_type=self.schema.FlowType.PRODUCT_FLOW,
            flow_template=_require_mapping(production_exchange.get(_FLOW_TEMPLATE_KEY), _FLOW_TEMPLATE_KEY),
            flow_property_ref=flow_property_ref,
            location_ref=location_ref,
            default_name=str(production_exchange.get("reference product") or dataset.get("reference product") or ""),
        )
        production_flow_ref = _schema_ref(
            production_flow,
            flow_type=production_flow.flow_type,
            ref_unit=str(production_exchange.get("unit") or ""),
        )

        key = (
            str(dataset.get("name") or ""),
            str(dataset.get("reference product") or ""),
            str(dataset.get("location") or ""),
            str(dataset.get("unit") or ""),
        )
        self.local_processes[key] = (process_ref, production_flow_ref, flow_property_ref, unit_ref)
        return _PreparedDataset(
            dataset=deepcopy(dataset),
            process=process,
            process_ref=process_ref,
            location_ref=location_ref,
            production_exchange=production_exchange,
            production_flow=production_flow,
            production_flow_ref=production_flow_ref,
            production_flow_property_ref=flow_property_ref,
            production_unit_ref=unit_ref,
        )

    def _build_process(self, state: _PreparedDataset) -> Any:
        dataset = state.dataset
        process = state.process
        process.parameters = [
            self._build_process_parameter(parameter)
            for parameter in dataset.get("parameters", [])
            if isinstance(parameter, dict)
        ] or None

        next_internal_id = max(
            [
                int(template.get("internalId", 0) or 0)
                for template in (
                    _require_mapping(exchange.get(_EXCHANGE_TEMPLATE_KEY), _EXCHANGE_TEMPLATE_KEY)
                    for exchange in dataset.get("exchanges", [])
                    if isinstance(exchange, dict)
                )
            ]
            + [0]
        )
        exchanges = []
        for exchange in dataset.get("exchanges", []):
            if not isinstance(exchange, dict):
                raise SerializationError(f"Dataset {dataset.get('name')!r} contains a non-dictionary exchange.")
            raw_exchange = _require_mapping(exchange.get(_EXCHANGE_TEMPLATE_KEY), _EXCHANGE_TEMPLATE_KEY)
            entity = self._build_exchange(
                dataset=dataset,
                exchange=exchange,
                raw_exchange=raw_exchange,
                state=state,
            )
            if entity.internal_id is None:
                next_internal_id += 1
                entity.internal_id = next_internal_id
            self._store_exchange_extras(entity, exchange)
            exchanges.append(entity)
        process.exchanges = exchanges
        process.last_internal_id = max((exchange.internal_id or 0 for exchange in exchanges), default=0)
        return process

    def _build_exchange(
        self,
        *,
        dataset: dict[str, Any],
        exchange: dict[str, Any],
        raw_exchange: dict[str, Any],
        state: _PreparedDataset,
    ) -> Any:
        entity = self.schema.Exchange.from_dict(raw_exchange) if raw_exchange else self.schema.Exchange()
        exchange_type = str(exchange.get("type") or "")
        if exchange_type not in {"production", "technosphere", "biosphere"}:
            raise SerializationError(
                f"Exchange {exchange.get('name')!r} in dataset {dataset.get('name')!r} has unsupported type {exchange_type!r}."
            )
        if exchange.get("amount") is None:
            raise SerializationError(
                f"Exchange {exchange.get('name')!r} in dataset {dataset.get('name')!r} is missing a numeric amount."
            )

        if exchange_type == "production":
            flow = state.production_flow
            flow_ref = state.production_flow_ref
            flow_property_ref = state.production_flow_property_ref
            unit_ref = state.production_unit_ref
            location_ref = state.location_ref
            default_provider = None
            entity.is_input = False
            entity.is_quantitative_reference = True
            entity.is_avoided_product = False
        else:
            local = self.local_processes.get(
                (
                    str(exchange.get("name") or ""),
                    str(exchange.get("reference product") or exchange.get("product") or ""),
                    str(exchange.get("location") or ""),
                    str(exchange.get("unit") or ""),
                )
            )
            if exchange_type == "technosphere" and local is not None:
                flow_ref = local[1]
                flow = self.flows[str(flow_ref.id or "")]
                flow_property_ref = local[2]
                unit_ref = local[3]
                location_ref = self._ensure_location(
                    str(exchange.get("location") or ""),
                    _require_mapping(exchange.get(_LOCATION_TEMPLATE_KEY), _LOCATION_TEMPLATE_KEY),
                )
                default_provider = local[0]
            else:
                flow_property_ref, unit_ref = self._ensure_quantity_entities(
                    unit_name=str(exchange.get("unit") or ""),
                    flow_property_template=_require_mapping(
                        exchange.get(_FLOW_PROPERTY_TEMPLATE_KEY),
                        _FLOW_PROPERTY_TEMPLATE_KEY,
                    ),
                    unit_group_template=_require_mapping(
                        exchange.get(_UNIT_GROUP_TEMPLATE_KEY),
                        _UNIT_GROUP_TEMPLATE_KEY,
                    ),
                    label=f"exchange {exchange.get('name')!r}",
                )
                flow = self._ensure_flow(
                    exchange=exchange,
                    flow_type=(
                        self.schema.FlowType.ELEMENTARY_FLOW
                        if exchange_type == "biosphere"
                        else self.schema.FlowType.PRODUCT_FLOW
                    ),
                    flow_template=_require_mapping(exchange.get(_FLOW_TEMPLATE_KEY), _FLOW_TEMPLATE_KEY),
                    flow_property_ref=flow_property_ref,
                    location_ref=self._ensure_location(
                        str(exchange.get("location") or ""),
                        _require_mapping(exchange.get(_LOCATION_TEMPLATE_KEY), _LOCATION_TEMPLATE_KEY),
                    ),
                    default_name=(
                        str(exchange.get("name") or "")
                        if exchange_type == "biosphere"
                        else str(exchange.get("reference product") or exchange.get("product") or "")
                    ),
                    categories=tuple(exchange.get("categories") or ()),
                )
                flow_ref = _schema_ref(flow, flow_type=flow.flow_type, ref_unit=str(exchange.get("unit") or ""))
                location_ref = self._ensure_location(
                    str(exchange.get("location") or ""),
                    _require_mapping(exchange.get(_LOCATION_TEMPLATE_KEY), _LOCATION_TEMPLATE_KEY),
                )
                default_provider = self._default_provider(dataset, exchange)

            entity.is_input = exchange_type == "technosphere" or (
                exchange_type == "biosphere" and tuple(exchange.get("categories") or ())[:1] == ("natural resource",)
            )
            entity.is_quantitative_reference = False
            entity.is_avoided_product = False

        entity.flow = flow_ref
        entity.flow_property = flow_property_ref
        entity.unit = unit_ref
        entity.location = location_ref
        entity.default_provider = default_provider
        entity.amount = float(exchange["amount"])
        entity.amount_formula = str(exchange.get("formula") or "") or None
        entity.description = str(exchange.get("comment") or "") or None
        entity.uncertainty = _schema_uncertainty(self.schema, exchange)
        entity.base_uncertainty = None
        return entity

    def _build_process_parameter(self, parameter: dict[str, Any]) -> Any:
        template = _require_mapping(parameter.get(_PARAMETER_TEMPLATE_KEY), _PARAMETER_TEMPLATE_KEY)
        entity = _hydrate_entity(self.schema.Parameter, template)
        entity.name = str(parameter.get("name") or entity.name or "")
        entity.value = float(parameter["amount"]) if parameter.get("amount") is not None else None
        entity.formula = str(parameter.get("formula") or "") or None
        entity.description = str(parameter.get("comment") or "") or None
        entity.parameter_scope = self.schema.ParameterScope.PROCESS_SCOPE
        entity.is_input_parameter = True
        entity.other_properties = _merge_brightpath_other_properties(
            entity.other_properties,
            _parameter_extras(parameter, target=""),
        )
        return entity

    def _register_global_parameter(self, parameter: dict[str, Any], *, target: str) -> None:
        template = _require_mapping(parameter.get(_PARAMETER_TEMPLATE_KEY), _PARAMETER_TEMPLATE_KEY)
        entity = _hydrate_entity(self.schema.Parameter, template)
        entity.name = str(parameter.get("name") or entity.name or "")
        entity.value = float(parameter["amount"]) if parameter.get("amount") is not None else None
        entity.formula = str(parameter.get("formula") or "") or None
        entity.description = str(parameter.get("comment") or "") or None
        entity.parameter_scope = self.schema.ParameterScope.GLOBAL_SCOPE
        entity.is_input_parameter = True
        entity.other_properties = _merge_brightpath_other_properties(
            entity.other_properties,
            _parameter_extras(parameter, target=target),
        )
        if not entity.id:
            entity.id = _stable_uuid("parameter", target, entity.name)
        self.parameters[entity.id] = entity

    def _default_provider(self, dataset: dict[str, Any], exchange: dict[str, Any]) -> Any | None:
        key = (
            str(exchange.get("name") or ""),
            str(exchange.get("reference product") or exchange.get("product") or ""),
            str(exchange.get("location") or ""),
            str(exchange.get("unit") or ""),
        )
        local = self.local_processes.get(key)
        if local is not None:
            return local[0]
        if not exchange.get("name"):
            return None
        ref = self.schema.Ref(
            id=_stable_uuid("provider", key[0], key[1], key[2], key[3]),
            name=str(exchange.get("name") or ""),
            location=str(exchange.get("location") or ""),
            process_type=self.schema.ProcessType.UNIT_PROCESS,
            ref_type=self.schema.RefType.Process,
        )
        return ref

    def _store_exchange_extras(self, entity: Any, exchange: dict[str, Any]) -> None:
        extras = _exchange_extras(exchange)
        if not extras:
            return
        flow_id = str(getattr(getattr(entity, "flow", None), "id", "") or "")
        if not flow_id or flow_id not in self.flows:
            return
        flow = self.flows[flow_id]
        other_properties = deepcopy(flow.other_properties) if isinstance(flow.other_properties, dict) else {}
        values = other_properties.get(_BRIGHTPATH_FLOW_EXCHANGE_PROPERTIES_KEY)
        if not isinstance(values, dict):
            values = {}
        values[str(entity.internal_id or "")] = deepcopy(extras)
        other_properties[_BRIGHTPATH_FLOW_EXCHANGE_PROPERTIES_KEY] = values
        flow.other_properties = other_properties

    def _ensure_location(self, code: str, template: dict[str, Any]) -> Any | None:
        value = str(code or template.get("code") or template.get("name") or "").strip()
        if not value and not template:
            return None
        location = _hydrate_entity(self.schema.Location, template)
        location.id = str(location.id or _stable_uuid("location", value))
        location.code = value or str(location.code or location.name or "")
        location.name = str(location.name or location.code or value or "")
        self.locations[location.id] = location
        return location.to_ref()

    def _ensure_quantity_entities(
        self,
        *,
        unit_name: str,
        flow_property_template: dict[str, Any],
        unit_group_template: dict[str, Any],
        label: str,
    ) -> tuple[Any, Any]:
        normalized_unit = str(unit_name or unit_group_template.get("name") or "").strip()
        if not normalized_unit:
            raise SerializationError(f"{label} is missing a unit.")

        unit_group = _hydrate_entity(self.schema.UnitGroup, unit_group_template)
        unit_group.id = str(unit_group.id or _stable_uuid("unit-group", normalized_unit))
        unit_group.name = str(unit_group.name or f"BrightPath units: {normalized_unit}")
        units = list(unit_group.units or [])
        selected_unit = next((unit for unit in units if str(unit.name or "") == normalized_unit), None)
        if selected_unit is None:
            selected_unit = self.schema.Unit(name=normalized_unit, conversion_factor=1.0, is_ref_unit=True)
            units.append(selected_unit)
        if not selected_unit.id:
            selected_unit.id = _stable_uuid("unit", normalized_unit)
        if not any(unit.is_ref_unit for unit in units):
            selected_unit.is_ref_unit = True
        unit_group.units = units
        unit_ref = selected_unit.to_ref()

        flow_property = _hydrate_entity(self.schema.FlowProperty, flow_property_template)
        flow_property.id = str(flow_property.id or _stable_uuid("flow-property", normalized_unit))
        flow_property.name = str(flow_property.name or f"Quantity of {normalized_unit}")
        flow_property.flow_property_type = (
            flow_property.flow_property_type or self.schema.FlowPropertyType.PHYSICAL_QUANTITY
        )
        flow_property.unit_group = unit_group.to_ref()
        unit_group.default_flow_property = flow_property.to_ref()

        self.unit_groups[unit_group.id] = unit_group
        self.flow_properties[flow_property.id] = flow_property
        return flow_property.to_ref(), unit_ref

    def _ensure_flow(
        self,
        *,
        exchange: dict[str, Any],
        flow_type: Any,
        flow_template: dict[str, Any],
        flow_property_ref: Any,
        location_ref: Any | None,
        default_name: str,
        categories: tuple[str, ...] = (),
    ) -> Any:
        flow = _hydrate_entity(self.schema.Flow, flow_template)
        flow.id = str(flow.id or _stable_uuid("flow", flow_type.value, default_name, str(exchange.get("unit") or "")))
        flow.name = str(default_name or flow.name or "")
        flow.flow_type = flow.flow_type or flow_type
        if flow.flow_type == self.schema.FlowType.ELEMENTARY_FLOW:
            encoded = _encode_biosphere_categories(categories)
            if encoded:
                flow.category = encoded
        flow.location = location_ref
        flow.flow_properties = [
            self.schema.FlowPropertyFactor(
                flow_property=flow_property_ref,
                conversion_factor=1.0,
                is_ref_flow_property=True,
            )
        ]
        self.flows[flow.id] = flow
        return flow


def _encode_biosphere_categories(categories: tuple[str, ...]) -> str:
    if not categories:
        return ""
    first = str(categories[0] or "").strip().lower()
    rest = [str(value).strip() for value in categories[1:] if str(value).strip()]
    if first == "natural resource":
        head = "Resources"
    elif first in {"air", "water", "soil"}:
        head = f"Emissions to {first}"
    else:
        head = first
    return "/".join([head, *rest])


def _stable_uuid(*parts: Any) -> str:
    payload = "::".join(str(part or "") for part in parts)
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"brightpath:openlca:{payload}"))
