"""Secure ecoSpold2 activity-dataset reading and deterministic package writing."""

from __future__ import annotations

import math
import os
import shutil
import stat
import tempfile
import zipfile
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Mapping
from uuid import UUID, uuid5

from lxml import etree
from pyecospold import Defaults

from brightpath import __version__
from brightpath.core.context import BackgroundContext, BiosphereProfile, FormatProfile, InventoryContext
from brightpath.exceptions import EcoSpold2SerializationError
from brightpath.models import BackgroundProfile, InventoryDocument, InventoryFormat, default_biosphere_profile

from .ecospold2_metadata import (
    EcoSpold2DatasetMetadata,
    EcoSpold2ExportProfile,
    EcoSpold2ReferenceCatalog,
    coerce_ecospold2_profile,
    coerce_ecospold2_references,
    packaged_ecospold2_references,
)

ECOSPOLD2_NAMESPACE = "http://www.EcoInvent.org/EcoSpold02"
XML_NAMESPACE = "http://www.w3.org/XML/1998/namespace"
_NS = {"es": ECOSPOLD2_NAMESPACE}
_UUID_NAMESPACE = UUID("3d1e8e10-6136-5b0b-8a9c-b57fa71cd4ff")
_MAX_XML_BYTES = 64 * 1024 * 1024
_MAX_PACKAGE_BYTES = 256 * 1024 * 1024
_MAX_ARCHIVE_ENTRIES = 20_000
_MAX_COMPRESSION_RATIO = 200
_RESOURCE_CATEGORIES = frozenset({"natural resource", "resource", "resources"})
_PEDIGREE_NAMES = {
    "reliability": "reliability",
    "completeness": "completeness",
    "temporal correlation": "temporalCorrelation",
    "geographical correlation": "geographicalCorrelation",
    "further technological correlation": "furtherTechnologyCorrelation",
}


def _qname(name: str) -> str:
    return str(etree.QName(ECOSPOLD2_NAMESPACE, name))


def _child(parent: etree._Element, name: str, attributes: Mapping[str, Any] | None = None) -> etree._Element:
    element = etree.SubElement(parent, _qname(name))
    for key, value in (attributes or {}).items():
        if value is None or value == "":
            continue
        if isinstance(value, bool):
            value = "true" if value else "false"
        element.set(key, str(value))
    return element


def _localized(parent: etree._Element, name: str, value: object, *, language: str = "en") -> etree._Element:
    element = _child(parent, name)
    element.set(str(etree.QName(XML_NAMESPACE, "lang")), language)
    element.text = str(value or "")
    return element


def _text_and_image(parent: etree._Element, name: str, value: object, *, language: str = "en") -> None:
    text = str(value or "").strip()
    if not text:
        return
    container = _child(parent, name)
    item = _localized(container, "text", text, language=language)
    item.set("index", "0")


def _safe_parser() -> etree.XMLParser:
    return etree.XMLParser(
        resolve_entities=False, no_network=True, load_dtd=False, huge_tree=False, remove_comments=False
    )


@lru_cache(maxsize=1)
def _schema() -> etree.XMLSchema:
    return etree.XMLSchema(file=Defaults.SCHEMA_V2_FILE)


def _parse_xml(contents: bytes) -> etree._Element:
    if len(contents) > _MAX_XML_BYTES:
        raise ValueError(f"ecoSpold2 XML exceeds the {_MAX_XML_BYTES}-byte safety limit.")
    return etree.fromstring(contents, parser=_safe_parser())


def validate_ecospold2_xml(source: str | Path | bytes) -> tuple[str, ...]:
    """Return XSD errors for one ecoSpold2 activity file without raising."""

    try:
        if isinstance(source, bytes):
            contents = source
        else:
            path = Path(source)
            if not path.is_file():
                return (f"ecoSpold2 file not found: {path}",)
            if path.stat().st_size > _MAX_XML_BYTES:
                return (f"ecoSpold2 XML exceeds the {_MAX_XML_BYTES}-byte safety limit.",)
            contents = path.read_bytes()
        root = _parse_xml(contents)
        if etree.QName(root).namespace != ECOSPOLD2_NAMESPACE or etree.QName(root).localname != "ecoSpold":
            return ("The root element is not an ecoSpold2 ecoSpold element.",)
        if _schema().validate(root):
            return ()
        return tuple(str(error) for error in _schema().error_log)
    except (OSError, ValueError, etree.XMLSyntaxError) as error:
        return (str(error) or type(error).__name__,)


def _context(
    *,
    background_profile: BackgroundProfile | None,
    biosphere_profile: BiosphereProfile | None,
    context: InventoryContext | None,
) -> InventoryContext:
    if context is not None:
        if context.format.format_id != InventoryFormat.ECOSPOLD2.value:
            raise ValueError("The explicit context format must be 'ecospold2'.")
        return context
    if background_profile is None:
        raise ValueError("ecoSpold2 import requires an explicit background context.")
    profile = background_profile.normalized()
    technosphere = profile.to_technosphere_profile()
    return InventoryContext(
        format=FormatProfile(InventoryFormat.ECOSPOLD2.value, format_version="2"),
        background=BackgroundContext(
            technosphere=technosphere,
            biosphere=biosphere_profile or default_biosphere_profile(technosphere),
        ),
    )


def _safe_zip_members(path: Path) -> list[zipfile.ZipInfo]:
    try:
        archive = zipfile.ZipFile(path)
    except (OSError, zipfile.BadZipFile) as error:
        raise ValueError(f"Invalid ecoSpold2 ZIP package: {error}") from error
    with archive:
        infos = archive.infolist()
        if len(infos) > _MAX_ARCHIVE_ENTRIES:
            raise ValueError(f"ecoSpold2 ZIP package contains more than {_MAX_ARCHIVE_ENTRIES} entries.")
        total = 0
        seen = set()
        selected = []
        for info in infos:
            member = PurePosixPath(info.filename)
            if info.flag_bits & 0x1:
                raise ValueError(f"Encrypted ZIP member is not supported: {info.filename}")
            if member.is_absolute() or ".." in member.parts or "\\" in info.filename:
                raise ValueError(f"Unsafe ZIP member path: {info.filename}")
            if info.filename in seen:
                raise ValueError(f"Duplicate ZIP member: {info.filename}")
            seen.add(info.filename)
            mode = info.external_attr >> 16
            if stat.S_ISLNK(mode):
                raise ValueError(f"Symbolic links are not supported in ecoSpold2 ZIP packages: {info.filename}")
            total += info.file_size
            if info.file_size > _MAX_XML_BYTES or total > _MAX_PACKAGE_BYTES:
                raise ValueError("ecoSpold2 ZIP package exceeds the configured decompression limits.")
            if info.compress_size and info.file_size / info.compress_size > _MAX_COMPRESSION_RATIO:
                raise ValueError(f"Suspicious ZIP compression ratio for {info.filename}.")
            if not info.is_dir() and member.suffix.lower() == ".spold":
                selected.append(info)
        return selected


def _artifact_documents(source: Path) -> list[tuple[str, bytes]]:
    if source.is_dir():
        base = source / "datasets" if (source / "datasets").is_dir() else source
        files = []
        total = 0
        for path in sorted(base.rglob("*.spold")):
            if path.is_symlink():
                raise ValueError(f"Symbolic links are not supported in ecoSpold2 packages: {path}")
            size = path.stat().st_size
            total += size
            if size > _MAX_XML_BYTES or total > _MAX_PACKAGE_BYTES:
                raise ValueError("ecoSpold2 directory exceeds the configured size limits.")
            files.append((str(path.relative_to(source)), path.read_bytes()))
        return files
    if source.is_file() and zipfile.is_zipfile(source):
        members = _safe_zip_members(source)
        with zipfile.ZipFile(source) as archive:
            return [(info.filename, archive.read(info)) for info in members]
    if source.is_file():
        return [(source.name, source.read_bytes())]
    raise FileNotFoundError(f"ecoSpold2 artifact not found: {source}")


def validate_ecospold2_package(source: str | Path) -> tuple[str, ...]:
    """Validate each activity dataset in a file, directory, or ZIP package."""

    try:
        documents = _artifact_documents(Path(source).expanduser())
    except (OSError, ValueError) as error:
        return (str(error) or type(error).__name__,)
    if not documents:
        return ("No .spold activity datasets were found.",)
    errors = []
    for name, contents in documents:
        errors.extend(f"{name}: {error}" for error in validate_ecospold2_xml(contents))
    return tuple(errors)


def load_ecospold2(
    path: str | Path,
    *,
    background_profile: BackgroundProfile | None = None,
    biosphere_profile: BiosphereProfile | None = None,
    context: InventoryContext | None = None,
    database_name: str | None = None,
) -> InventoryDocument:
    """Load an ecoSpold2 file, directory, or ZIP package into canonical data."""

    source = Path(path).expanduser()
    selected_context = _context(
        background_profile=background_profile,
        biosphere_profile=biosphere_profile,
        context=context,
    )
    documents = _artifact_documents(source)
    if not documents:
        raise ValueError("No .spold activity datasets were found.")
    data = []
    for filename, contents in documents:
        errors = validate_ecospold2_xml(contents)
        if errors:
            raise ValueError(f"Invalid ecoSpold2 activity {filename}: {errors[0]}")
        root = _parse_xml(contents)
        if root.find("es:childActivityDataset", namespaces=_NS) is not None:
            raise ValueError(f"ecoSpold2 child activity datasets are unsupported: {filename}")
        datasets = root.findall("es:activityDataset", namespaces=_NS)
        if len(datasets) != 1:
            raise ValueError(f"Each ecoSpold2 file must contain exactly one activityDataset; found {len(datasets)}.")
        data.append(_parse_dataset(datasets[0], filename))
    return InventoryDocument(
        data=data,
        context=selected_context,
        database_name=database_name or source.stem,
        metadata={"ecospold2": {"source": str(source.resolve()), "files": [name for name, _ in documents]}},
    )


def _find(parent: etree._Element | None, path: str) -> etree._Element | None:
    return parent.find(path, namespaces=_NS) if parent is not None else None


def _findall(parent: etree._Element | None, path: str) -> list[etree._Element]:
    return parent.findall(path, namespaces=_NS) if parent is not None else []


def _element_text(element: etree._Element | None) -> str:
    return (element.text or "").strip() if element is not None else ""


def _first_text(parent: etree._Element | None, path: str) -> str:
    return _element_text(_find(parent, path))


def _text_block(parent: etree._Element | None) -> str:
    return "\n".join(text for text in (_element_text(item) for item in _findall(parent, "es:text")) if text)


def _attrs(element: etree._Element | None) -> dict[str, str]:
    return dict(element.attrib) if element is not None else {}


def _parse_dataset(dataset: etree._Element, filename: str) -> dict[str, Any]:
    description = _find(dataset, "es:activityDescription")
    activity = _find(description, "es:activity")
    geography = _find(description, "es:geography")
    technology = _find(description, "es:technology")
    time_period = _find(description, "es:timePeriod")
    scenario = _find(description, "es:macroEconomicScenario")
    flow_data = _find(dataset, "es:flowData")
    modelling = _find(dataset, "es:modellingAndValidation")
    admin = _find(dataset, "es:administrativeInformation")
    entry = _find(admin, "es:dataEntryBy")
    publication = _find(admin, "es:dataGeneratorAndPublication")
    file_attributes = _find(admin, "es:fileAttributes")
    exchanges = [
        *(_parse_exchange(item, biosphere=False) for item in _findall(flow_data, "es:intermediateExchange")),
        *(_parse_exchange(item, biosphere=True) for item in _findall(flow_data, "es:elementaryExchange")),
    ]
    productions = [exchange for exchange in exchanges if exchange["type"] == "production"]
    reference_product = productions[0]["name"] if len(productions) == 1 else ""
    unit = productions[0].get("unit", "") if len(productions) == 1 else ""
    classifications = [_parse_classification(item) for item in _findall(description, "es:classification")]
    metadata = {
        "activity_id": activity.get("id", "") if activity is not None else "",
        "activity_name_id": activity.get("activityNameId", "") if activity is not None else "",
        "geography_id": geography.get("geographyId", "") if geography is not None else "",
        "activity_type": int(activity.get("type", 1)) if activity is not None else 1,
        "special_activity_type": int(activity.get("specialActivityType", 0)) if activity is not None else 0,
        "technology_level": int(technology.get("technologyLevel", 3)) if technology is not None else 3,
        "technology_comment": _text_block(_find(technology, "es:comment")),
        "time_period": {
            "start_date": time_period.get("startDate", "") if time_period is not None else "",
            "end_date": time_period.get("endDate", "") if time_period is not None else "",
            "data_valid_for_entire_period": _bool_attr(time_period, "isDataValidForEntirePeriod", True),
            "comment": _text_block(_find(time_period, "es:comment")),
        },
        "macroeconomic_scenario": {
            "id": scenario.get("macroEconomicScenarioId", "") if scenario is not None else "",
            "name": _first_text(scenario, "es:name"),
            "comment": _first_text(scenario, "es:comment"),
        },
        "data_entry_person": _person(entry),
        "data_generator_person": _person(publication),
        "copyright": _bool_attr(publication, "isCopyrightProtected", False),
        "data_published_in": int(publication.get("dataPublishedIn", 0)) if publication is not None else 0,
        "published_source_id": publication.get("publishedSourceId", "") if publication is not None else "",
        "published_source_context_id": (
            publication.get("publishedSourceContextId", "") if publication is not None else ""
        ),
        "published_source_year": publication.get("publishedSourceYear", "") if publication is not None else "",
        "published_source_first_author": (
            publication.get("publishedSourceFirstAuthor", "") if publication is not None else ""
        ),
        "access_restricted_to": int(publication.get("accessRestrictedTo", 0)) if publication is not None else 0,
        "file_attributes": _file_attributes(file_attributes),
        "classifications": classifications,
        "raw": {
            "activity": _attrs(activity),
            "geography": _attrs(geography),
            "technology": _attrs(technology),
            "time_period": _attrs(time_period),
            "macroeconomic_scenario": _attrs(scenario),
            "data_entry_by": _attrs(entry),
            "data_generator_and_publication": _attrs(publication),
            "file_attributes": _attrs(file_attributes),
            "review_count": len(_findall(modelling, "es:review")),
            "impact_indicator_count": len(_findall(flow_data, "es:impactIndicator")),
            "allocation_count": sum(len(_findall(item, "es:allocation")) for item in _findall(flow_data, "*")),
            "transfer_coefficient_count": sum(
                len(_findall(item, "es:transferCoefficient")) for item in _findall(flow_data, "*")
            ),
            "child": False,
        },
    }
    parameters = [_parse_parameter(item) for item in _findall(flow_data, "es:parameter")]
    result = {
        "name": _first_text(activity, "es:activityName"),
        "reference product": reference_product,
        "location": _first_text(geography, "es:shortname"),
        "unit": unit,
        "code": metadata["activity_id"],
        "activity": metadata["activity_id"],
        "comment": _text_block(_find(activity, "es:generalComment")),
        "classifications": [(item["system"], item["value"]) for item in classifications],
        "activity type": _activity_type_name(metadata["special_activity_type"]),
        "type": "process",
        "filename": filename,
        "exchanges": exchanges,
        "ecospold2": metadata,
    }
    if parameters:
        result["parameters"] = parameters
    return result


def _activity_type_name(value: int) -> str:
    names = {
        0: "ordinary transforming activity",
        1: "market activity",
        2: "IO activity",
        3: "Residual activity",
        4: "production mix",
        5: "import activity",
        6: "supply mix",
        7: "export activity",
        8: "re-export activity",
        9: "correction activity",
        10: "market group",
    }
    return names.get(value, "ordinary transforming activity")


def _bool_attr(element: etree._Element | None, name: str, default: bool) -> bool:
    if element is None or element.get(name) is None:
        return default
    return element.get(name, "").strip().lower() in {"1", "true", "yes"}


def _person(element: etree._Element | None) -> dict[str, str]:
    if element is None:
        return {}
    fields = {
        "id": "personId",
        "name": "personName",
        "email": "personEmail",
        "company_id": "companyId",
        "company_code": "companyCode",
    }
    return {target: element.get(source, "") for target, source in fields.items() if element.get(source)}


def _file_attributes(element: etree._Element | None) -> dict[str, Any]:
    if element is None:
        return {}
    result: dict[str, Any] = {}
    for target, source, default in (
        ("major_release", "majorRelease", 1),
        ("minor_release", "minorRelease", 0),
        ("major_revision", "majorRevision", 1),
        ("minor_revision", "minorRevision", 0),
    ):
        result[target] = int(element.get(source, default))
    result["default_language"] = element.get("defaultLanguage", "en")
    result["context_id"] = element.get("contextId", "")
    result["context_name"] = _first_text(element, "es:contextName")
    return result


def _parse_classification(element: etree._Element) -> dict[str, str]:
    return {
        "id": element.get("classificationId", ""),
        "system": _first_text(element, "es:classificationSystem"),
        "value": _first_text(element, "es:classificationValue"),
    }


def _parse_exchange(element: etree._Element, *, biosphere: bool) -> dict[str, Any]:
    input_group = _first_text(element, "es:inputGroup")
    output_group = _first_text(element, "es:outputGroup")
    if biosphere:
        exchange_type = "biosphere"
        flow_id = element.get("elementaryExchangeId", "")
    else:
        exchange_type = "production" if output_group == "0" else "technosphere" if input_group else "substitution"
        flow_id = element.get("intermediateExchangeId", "")
    result: dict[str, Any] = {
        "name": _first_text(element, "es:name"),
        "unit": _first_text(element, "es:unitName"),
        "amount": float(element.get("amount", 0)),
        "type": exchange_type,
        "flow": flow_id,
        "code": element.get("id", ""),
        "ecospold2": {
            "id": element.get("id", ""),
            "unit_id": element.get("unitId", ""),
            "input_group": input_group,
            "output_group": output_group,
            "raw": _attrs(element),
        },
    }
    if not biosphere:
        result["activity"] = element.get("activityLinkId", "")
        result["reference product"] = result["name"]
        if element.get("productionVolumeAmount") is not None:
            result["production volume"] = float(element.get("productionVolumeAmount"))
    else:
        compartment = _find(element, "es:compartment")
        result["categories"] = tuple(
            value
            for value in (_first_text(compartment, "es:compartment"), _first_text(compartment, "es:subcompartment"))
            if value and value.casefold() != "unspecified"
        )
        result["ecospold2"]["subcompartment_id"] = (
            compartment.get("subcompartmentId", "") if compartment is not None else ""
        )
    for source_name, xml_name in (
        ("source_id", "sourceId"),
        ("source_context_id", "sourceContextId"),
        ("source_year", "sourceYear"),
        ("source_first_author", "sourceFirstAuthor"),
        ("page_numbers", "pageNumbers"),
    ):
        if element.get(xml_name):
            result["ecospold2"][source_name] = element.get(xml_name)
    comment = _first_text(element, "es:comment")
    if comment:
        result["comment"] = comment
    for xml_name, target in (
        ("variableName", "variable name"),
        ("mathematicalRelation", "formula"),
        ("formula", "chemical formula"),
        ("casNumber", "CAS number"),
    ):
        if element.get(xml_name):
            result[target] = element.get(xml_name)
    _parse_uncertainty(element, result)
    return result


def _parse_parameter(element: etree._Element) -> dict[str, Any]:
    result: dict[str, Any] = {
        "name": element.get("variableName") or _first_text(element, "es:name"),
        "description": _first_text(element, "es:name"),
        "amount": float(element.get("amount", 0)),
        "id": element.get("parameterId", ""),
    }
    if _first_text(element, "es:unitName"):
        result["unit"] = _first_text(element, "es:unitName")
    if element.get("mathematicalRelation"):
        result["formula"] = element.get("mathematicalRelation")
    _parse_uncertainty(element, result)
    return result


def _parse_uncertainty(element: etree._Element, result: dict[str, Any]) -> None:
    uncertainty = _find(element, "es:uncertainty")
    if uncertainty is None:
        result["uncertainty type"] = 0
        result["loc"] = result["amount"]
        return
    lognormal = _find(uncertainty, "es:lognormal")
    normal = _find(uncertainty, "es:normal")
    triangular = _find(uncertainty, "es:triangular")
    uniform = _find(uncertainty, "es:uniform")
    if lognormal is not None:
        result.update(
            {
                "uncertainty type": 2,
                "loc": float(lognormal.get("mu")),
                "scale": math.sqrt(float(lognormal.get("varianceWithPedigreeUncertainty"))),
            }
        )
    elif normal is not None:
        result.update(
            {
                "uncertainty type": 3,
                "loc": float(normal.get("meanValue")),
                "scale": math.sqrt(float(normal.get("varianceWithPedigreeUncertainty"))),
            }
        )
    elif triangular is not None:
        result.update(
            {
                "uncertainty type": 5,
                "minimum": float(triangular.get("minValue")),
                "loc": float(triangular.get("mostLikelyValue")),
                "maximum": float(triangular.get("maxValue")),
            }
        )
    elif uniform is not None:
        result.update(
            {
                "uncertainty type": 4,
                "loc": result["amount"],
                "minimum": float(uniform.get("minValue")),
                "maximum": float(uniform.get("maxValue")),
            }
        )
    else:
        result["uncertainty type"] = 0
        result["loc"] = result["amount"]
    pedigree = _find(uncertainty, "es:pedigreeMatrix")
    if pedigree is not None:
        result["pedigree"] = {
            canonical: int(pedigree.get(xml_name))
            for canonical, xml_name in _PEDIGREE_NAMES.items()
            if pedigree.get(xml_name)
        }


@dataclass(frozen=True)
class _DatasetRecord:
    dataset: Mapping[str, Any]
    metadata: EcoSpold2DatasetMetadata
    production: Mapping[str, Any]
    identity: tuple[str, str, str, str]
    activity_id: str
    activity_name_id: str
    product_id: str
    geography_id: str


def _uuid(value: object) -> str:
    try:
        return str(UUID(str(value))) if value else ""
    except (ValueError, AttributeError, TypeError):
        return ""


def _stable_uuid(kind: str, *values: object) -> str:
    return str(uuid5(_UUID_NAMESPACE, "|".join([kind, *(str(value or "").strip() for value in values)])))


def _activity_identity(value: Mapping[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(value.get("name") or "").strip(),
        str(value.get("reference product", value.get("product")) or "").strip(),
        str(value.get("location") or "").strip(),
        str(value.get("unit") or "").strip(),
    )


def _biosphere_identity(value: Mapping[str, Any]) -> tuple[str, tuple[str, ...], str]:
    categories = value.get("categories", ())
    if isinstance(categories, str):
        categories = tuple(part.strip() for part in categories.split("::") if part.strip())
    return (
        str(value.get("name") or "").strip(),
        tuple(str(part).strip() for part in categories or () if str(part).strip()),
        str(value.get("unit") or "").strip(),
    )


def _production(dataset: Mapping[str, Any]) -> Mapping[str, Any]:
    productions = [
        exchange
        for exchange in dataset.get("exchanges", ())
        if isinstance(exchange, Mapping) and exchange.get("type") == "production"
    ]
    if len(productions) != 1:
        raise EcoSpold2SerializationError(
            f"ecoSpold2 export requires exactly one production exchange in {dataset.get('name', '<unnamed>')!r}; found {len(productions)}."
        )
    return productions[0]


def _records(
    document: InventoryDocument,
    profile: EcoSpold2ExportProfile,
    references: EcoSpold2ReferenceCatalog,
) -> tuple[_DatasetRecord, ...]:
    records = []
    identities = Counter(_activity_identity(dataset) for dataset in document.data)
    duplicates = [identity for identity, count in identities.items() if count > 1]
    if duplicates:
        raise EcoSpold2SerializationError(
            f"Duplicate foreground identities cannot be linked unambiguously: {duplicates[0]!r}."
        )
    for dataset in document.data:
        metadata = profile.metadata_for(dataset)
        missing = metadata.missing_required()
        if missing:
            raise EcoSpold2SerializationError(
                f"Missing required ecoSpold2 metadata for {dataset.get('name', '<unnamed>')!r}: {', '.join(missing)}."
            )
        production = _production(dataset)
        identity = _activity_identity(dataset)
        embedded = dataset.get("ecospold2", {}) if isinstance(dataset.get("ecospold2"), Mapping) else {}
        activity_id = (
            _uuid(metadata.activity_id)
            or _uuid(dataset.get("activity"))
            or _uuid(dataset.get("code"))
            or _uuid(embedded.get("activity_id"))
            or _stable_uuid("activity", *identity)
        )
        activity_name_id = (
            _uuid(metadata.activity_name_id)
            or _uuid(embedded.get("activity_name_id"))
            or _stable_uuid("activity-name", identity[0])
        )
        production_es2 = production.get("ecospold2", {}) if isinstance(production.get("ecospold2"), Mapping) else {}
        product_id = (
            _uuid(production.get("flow"))
            or _uuid(production_es2.get("intermediate_exchange_id"))
            or _stable_uuid("intermediate-product", identity[1], identity[3])
        )
        geography_id = (
            _uuid(metadata.geography_id)
            or _uuid(embedded.get("geography_id"))
            or references.geographies.get(identity[2], "")
            or _stable_uuid("geography", identity[2])
        )
        records.append(
            _DatasetRecord(
                dataset, metadata, production, identity, activity_id, activity_name_id, product_id, geography_id
            )
        )
    return tuple(records)


def render_ecospold2_package(
    document: InventoryDocument,
    *,
    metadata_profile: EcoSpold2ExportProfile | Mapping[str, Any] | str | Path | None = None,
    reference_catalog: EcoSpold2ReferenceCatalog | Mapping[str, Any] | str | Path | None = None,
    generated_at: datetime | str | None = None,
) -> dict[str, bytes]:
    """Render all datasets and minimal foreground master data in memory."""

    if not isinstance(document, InventoryDocument):
        raise TypeError("document must be an InventoryDocument.")
    profile = coerce_ecospold2_profile(metadata_profile)
    references = (
        packaged_ecospold2_references(document.context)
        if reference_catalog is None
        else coerce_ecospold2_references(reference_catalog)
    )
    records = _records(document, profile, references)
    by_identity = {record.identity: record for record in records}
    timestamp = _timestamp(generated_at)
    package = {}
    for record in records:
        contents = _render_dataset(record, by_identity, references, timestamp)
        errors = validate_ecospold2_xml(contents)
        if errors:
            raise EcoSpold2SerializationError(f"Generated ecoSpold2 XML failed XSD validation: {errors[0]}")
        package[f"datasets/{record.activity_id}_{record.product_id}.spold"] = contents
    package["MasterData/IntermediateExchanges.xml"] = _render_intermediate_master_data(records)
    return dict(sorted(package.items()))


def render_ecospold2_dataset(
    document: InventoryDocument,
    *,
    metadata_profile: EcoSpold2ExportProfile | Mapping[str, Any] | str | Path | None = None,
    reference_catalog: EcoSpold2ReferenceCatalog | Mapping[str, Any] | str | Path | None = None,
    generated_at: datetime | str | None = None,
) -> bytes:
    """Render exactly one ecoSpold2 activity dataset."""

    package = render_ecospold2_package(
        document,
        metadata_profile=metadata_profile,
        reference_catalog=reference_catalog,
        generated_at=generated_at,
    )
    datasets = [contents for name, contents in package.items() if name.endswith(".spold")]
    if len(datasets) != 1:
        raise EcoSpold2SerializationError(f"A single .spold file requires exactly one dataset; found {len(datasets)}.")
    return datasets[0]


def _timestamp(value: datetime | str | None) -> str:
    if value is None:
        selected = datetime.now(timezone.utc)
    elif isinstance(value, datetime):
        selected = value
    else:
        selected = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if selected.tzinfo is None:
        selected = selected.replace(tzinfo=timezone.utc)
    return selected.replace(microsecond=0).isoformat()


def _render_dataset(
    record: _DatasetRecord,
    by_identity: Mapping[tuple[str, str, str, str], _DatasetRecord],
    references: EcoSpold2ReferenceCatalog,
    timestamp: str,
) -> bytes:
    dataset = record.dataset
    metadata = record.metadata
    language = metadata.file_attributes.default_language
    root = etree.Element(_qname("ecoSpold"), nsmap={None: ECOSPOLD2_NAMESPACE})
    activity_dataset = _child(root, "activityDataset")
    description = _child(activity_dataset, "activityDescription")
    activity = _child(
        description,
        "activity",
        {
            "id": record.activity_id,
            "activityNameId": record.activity_name_id,
            "type": metadata.activity_type,
            "specialActivityType": metadata.special_activity_type,
            "inheritanceDepth": 0,
        },
    )
    _localized(activity, "activityName", dataset.get("name", ""), language=language)
    _text_and_image(activity, "generalComment", dataset.get("comment"), language=language)
    for classification in metadata.classifications:
        system = str(classification.get("system") or classification.get("classificationSystem") or "").strip()
        value = str(classification.get("value") or classification.get("classificationValue") or "").strip()
        if not system or not value:
            continue
        item = _child(
            description,
            "classification",
            {"classificationId": _uuid(classification.get("id")) or _stable_uuid("classification", system)},
        )
        _localized(item, "classificationSystem", system, language=language)
        _localized(item, "classificationValue", value, language=language)
    geography = _child(description, "geography", {"geographyId": record.geography_id})
    _localized(geography, "shortname", dataset.get("location") or "GLO", language=language)
    technology = _child(description, "technology", {"technologyLevel": metadata.technology_level})
    _text_and_image(technology, "comment", metadata.technology_comment, language=language)
    period = _child(
        description,
        "timePeriod",
        {
            "startDate": metadata.time_period.start_date,
            "endDate": metadata.time_period.end_date,
            "isDataValidForEntirePeriod": metadata.time_period.data_valid_for_entire_period,
        },
    )
    _text_and_image(period, "comment", metadata.time_period.comment, language=language)
    scenario_id = metadata.macroeconomic_scenario.id or _stable_uuid(
        "macroeconomic-scenario", metadata.macroeconomic_scenario.name
    )
    scenario = _child(description, "macroEconomicScenario", {"macroEconomicScenarioId": scenario_id})
    _localized(scenario, "name", metadata.macroeconomic_scenario.name, language=language)
    if metadata.macroeconomic_scenario.comment:
        _localized(scenario, "comment", metadata.macroeconomic_scenario.comment, language=language)

    flow_data = _child(activity_dataset, "flowData")
    exchanges = tuple(dataset.get("exchanges", ()))
    intermediate = [exchange for exchange in exchanges if exchange.get("type") in {"production", "technosphere"}]
    biosphere = [exchange for exchange in exchanges if exchange.get("type") == "biosphere"]
    duplicate_counter: Counter[tuple[Any, ...]] = Counter()
    for exchange in intermediate:
        _render_exchange(flow_data, record, exchange, by_identity, references, duplicate_counter, language)
    for exchange in biosphere:
        _render_exchange(flow_data, record, exchange, by_identity, references, duplicate_counter, language)
    for parameter in _parameter_values(dataset.get("parameters")):
        _render_parameter(flow_data, record, parameter, references, language)

    _child(activity_dataset, "modellingAndValidation")
    admin = _child(activity_dataset, "administrativeInformation")
    entry = metadata.data_entry_person
    generator = metadata.data_generator_person
    entry_id = entry.id or _stable_uuid("person", entry.name, entry.email)
    generator_id = generator.id or _stable_uuid("person", generator.name, generator.email)
    _child(
        admin,
        "dataEntryBy",
        {"personId": entry_id, "personName": entry.name, "personEmail": entry.email, "isActiveAuthor": True},
    )
    publication_attributes: dict[str, Any] = {
        "personId": generator_id,
        "personName": generator.name,
        "personEmail": generator.email,
        "isCopyrightProtected": metadata.copyright,
        "dataPublishedIn": metadata.data_published_in,
        "publishedSourceId": metadata.published_source_id or None,
        "publishedSourceContextId": metadata.published_source_context_id or None,
        "publishedSourceYear": metadata.published_source_year or None,
        "publishedSourceFirstAuthor": metadata.published_source_first_author or None,
        "accessRestrictedTo": metadata.access_restricted_to,
        "companyId": generator.company_id or None,
        "companyCode": generator.company_code or None,
    }
    _child(admin, "dataGeneratorAndPublication", publication_attributes)
    file_attrs = metadata.file_attributes
    file_element = _child(
        admin,
        "fileAttributes",
        {
            "majorRelease": file_attrs.major_release,
            "minorRelease": file_attrs.minor_release,
            "majorRevision": file_attrs.major_revision,
            "minorRevision": file_attrs.minor_revision,
            "defaultLanguage": language,
            "creationTimestamp": timestamp,
            "lastEditTimestamp": timestamp,
            "fileGenerator": f"BrightPath {__version__}",
            "fileTimestamp": timestamp,
            "contextId": file_attrs.context_id or None,
        },
    )
    if file_attrs.context_name:
        _localized(file_element, "contextName", file_attrs.context_name, language=language)
    return etree.tostring(root, encoding="UTF-8", xml_declaration=True, pretty_print=True)


def _render_exchange(
    parent: etree._Element,
    record: _DatasetRecord,
    exchange: Mapping[str, Any],
    by_identity: Mapping[tuple[str, str, str, str], _DatasetRecord],
    references: EcoSpold2ReferenceCatalog,
    duplicate_counter: Counter[tuple[Any, ...]],
    language: str,
) -> None:
    amount = exchange.get("amount")
    if isinstance(amount, bool) or not isinstance(amount, (int, float)) or not math.isfinite(float(amount)):
        raise EcoSpold2SerializationError(f"Exchange {exchange.get('name', '<unnamed>')!r} has an invalid amount.")
    exchange_type = str(exchange.get("type") or "")
    es2 = exchange.get("ecospold2", {}) if isinstance(exchange.get("ecospold2"), Mapping) else {}
    unit = str(exchange.get("unit") or "").strip()
    unit_id = _uuid(es2.get("unit_id")) or references.units.get(unit) or _stable_uuid("unit", unit)
    signature = (
        exchange_type,
        exchange.get("name"),
        exchange.get("reference product", exchange.get("product")),
        exchange.get("location"),
        unit,
    )
    duplicate_counter[signature] += 1
    instance_id = (
        _uuid(es2.get("id"))
        or _uuid(exchange.get("code"))
        or _stable_uuid("exchange", record.activity_id, *signature, duplicate_counter[signature])
    )
    attributes: dict[str, Any] = {
        "id": instance_id,
        "unitId": unit_id,
        "amount": float(amount),
        "variableName": exchange.get("variable name") or exchange.get("variable_name") or None,
        "mathematicalRelation": exchange.get("formula") or None,
        "isCalculatedAmount": bool(exchange.get("formula")),
        "sourceId": _uuid(es2.get("source_id")) or None,
        "sourceContextId": _uuid(es2.get("source_context_id")) or None,
        "sourceYear": es2.get("source_year") or None,
        "sourceFirstAuthor": es2.get("source_first_author") or None,
        "pageNumbers": es2.get("page_numbers") or None,
    }
    if exchange_type == "biosphere":
        identity = _biosphere_identity(exchange)
        flow_id = _uuid(exchange.get("flow")) or references.biosphere.get(identity, "")
        if not flow_id:
            raise EcoSpold2SerializationError(f"No exact ecoSpold2 elementary-flow UUID for {identity!r}.")
        attributes.update(
            {
                "elementaryExchangeId": flow_id,
                "formula": exchange.get("chemical formula") or None,
                "casNumber": exchange.get("CAS number") or None,
            }
        )
        element = _child(parent, "elementaryExchange", attributes)
    else:
        if exchange_type == "production":
            flow_id, activity_id = record.product_id, record.activity_id
        else:
            identity = _activity_identity(exchange)
            linked = by_identity.get(identity)
            embedded_activity = _uuid(exchange.get("activity"))
            embedded_flow = _uuid(exchange.get("flow"))
            if linked is not None:
                activity_id = embedded_activity or linked.activity_id
                flow_id = embedded_flow or linked.product_id
            else:
                catalog = references.technosphere.get(identity)
                activity_id = embedded_activity or (catalog[0] if catalog else "")
                flow_id = embedded_flow or (catalog[1] if catalog else "")
            if not activity_id or not flow_id:
                raise EcoSpold2SerializationError(f"No exact ecoSpold2 technosphere UUID references for {identity!r}.")
        attributes.update(
            {
                "intermediateExchangeId": flow_id,
                "activityLinkId": activity_id,
                "productionVolumeAmount": exchange.get("production volume") or None,
            }
        )
        element = _child(parent, "intermediateExchange", attributes)
    _localized(element, "name", exchange.get("name", ""), language=language)
    _localized(element, "unitName", unit, language=language)
    if exchange.get("comment"):
        _localized(element, "comment", exchange.get("comment"), language=language)
    _render_uncertainty(element, exchange)
    if exchange_type == "biosphere":
        categories = _biosphere_identity(exchange)[1]
        compartment_name = categories[0] if categories else "unspecified"
        subcompartment_name = categories[1] if len(categories) > 1 else "unspecified"
        compartment = _child(
            element,
            "compartment",
            {
                "subcompartmentId": _uuid(es2.get("subcompartment_id"))
                or _stable_uuid("subcompartment", compartment_name, subcompartment_name)
            },
        )
        _localized(compartment, "compartment", compartment_name, language=language)
        _localized(compartment, "subcompartment", subcompartment_name, language=language)
        group = _child(element, "inputGroup" if compartment_name.casefold() in _RESOURCE_CATEGORIES else "outputGroup")
        group.text = "4"
    else:
        group = _child(element, "outputGroup" if exchange_type == "production" else "inputGroup")
        group.text = "0" if exchange_type == "production" else "5"


def _render_uncertainty(parent: etree._Element, value: Mapping[str, Any]) -> None:
    uncertainty_type = int(value.get("uncertainty type") or 0)
    if uncertainty_type not in {2, 3, 4, 5}:
        return
    uncertainty = _child(parent, "uncertainty")
    amount = float(value.get("amount", 0))
    if uncertainty_type == 2:
        scale = float(value.get("scale", 0))
        _child(
            uncertainty,
            "lognormal",
            {
                "meanValue": amount,
                "mu": float(value.get("loc", math.log(abs(amount)) if amount else 0)),
                "variance": scale**2,
                "varianceWithPedigreeUncertainty": scale**2,
            },
        )
    elif uncertainty_type == 3:
        scale = float(value.get("scale", 0))
        _child(
            uncertainty,
            "normal",
            {
                "meanValue": float(value.get("loc", amount)),
                "variance": scale**2,
                "varianceWithPedigreeUncertainty": scale**2,
            },
        )
    elif uncertainty_type == 4:
        _child(uncertainty, "uniform", {"minValue": value.get("minimum"), "maxValue": value.get("maximum")})
    else:
        _child(
            uncertainty,
            "triangular",
            {
                "minValue": value.get("minimum"),
                "mostLikelyValue": value.get("loc", amount),
                "maxValue": value.get("maximum"),
            },
        )
    pedigree = value.get("pedigree")
    if isinstance(pedigree, Mapping) and all(name in pedigree for name in _PEDIGREE_NAMES):
        _child(uncertainty, "pedigreeMatrix", {_PEDIGREE_NAMES[name]: pedigree[name] for name in _PEDIGREE_NAMES})


def _parameter_values(value: object) -> tuple[Mapping[str, Any], ...]:
    if not value:
        return ()
    if isinstance(value, Mapping):
        return tuple({"name": name, **dict(item)} for name, item in value.items() if isinstance(item, Mapping))
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(item for item in value if isinstance(item, Mapping))
    raise EcoSpold2SerializationError("Dataset parameters must be a mapping or sequence of mappings.")


def _render_parameter(
    parent: etree._Element,
    record: _DatasetRecord,
    parameter: Mapping[str, Any],
    references: EcoSpold2ReferenceCatalog,
    language: str,
) -> None:
    name = str(parameter.get("name") or "").strip()
    if not name:
        raise EcoSpold2SerializationError("ecoSpold2 parameters require a name.")
    amount = parameter.get("amount")
    if isinstance(amount, bool) or not isinstance(amount, (int, float)) or not math.isfinite(float(amount)):
        raise EcoSpold2SerializationError(f"Parameter {name!r} has an invalid amount.")
    unit = str(parameter.get("unit") or "").strip()
    element = _child(
        parent,
        "parameter",
        {
            "parameterId": _uuid(parameter.get("id")) or _stable_uuid("parameter", record.activity_id, name),
            "variableName": name,
            "amount": float(amount),
            "mathematicalRelation": parameter.get("formula") or None,
            "isCalculatedAmount": bool(parameter.get("formula")),
            "unitId": references.units.get(unit) or (_stable_uuid("unit", unit) if unit else None),
        },
    )
    _localized(element, "name", parameter.get("description") or name, language=language)
    if unit:
        _localized(element, "unitName", unit, language=language)
    _render_uncertainty(element, parameter)
    if parameter.get("comment"):
        _localized(element, "comment", parameter.get("comment"), language=language)


def _render_intermediate_master_data(records: Iterable[_DatasetRecord]) -> bytes:
    root = etree.Element(_qname("validIntermediateExchanges"), nsmap={None: ECOSPOLD2_NAMESPACE})
    seen = set()
    for record in sorted(records, key=lambda item: item.product_id):
        if record.product_id in seen:
            continue
        seen.add(record.product_id)
        item = _child(root, "intermediateExchange", {"id": record.product_id})
        _localized(item, "name", record.identity[1])
        _localized(item, "unitName", record.identity[3])
        info = _child(item, "productInformation")
        _localized(info, "text", f"Foreground product generated by BrightPath for {record.identity[0]}.")
    return etree.tostring(root, encoding="UTF-8", xml_declaration=True, pretty_print=True)


def write_ecospold2(
    document: InventoryDocument,
    path: str | Path,
    *,
    metadata_profile: EcoSpold2ExportProfile | Mapping[str, Any] | str | Path | None = None,
    reference_catalog: EcoSpold2ReferenceCatalog | Mapping[str, Any] | str | Path | None = None,
    generated_at: datetime | str | None = None,
    package_mode: str | None = None,
) -> Path:
    """Atomically write one `.spold`, a ZIP package, or a package directory."""

    destination = Path(path).expanduser().resolve()
    mode = (package_mode or "").strip().lower()
    if not mode:
        mode = "single" if destination.suffix.lower() in {".spold", ".xml"} else "zip"
    package = render_ecospold2_package(
        document,
        metadata_profile=metadata_profile,
        reference_catalog=reference_catalog,
        generated_at=generated_at,
    )
    if mode == "single":
        datasets = [contents for name, contents in package.items() if name.endswith(".spold")]
        if len(datasets) != 1:
            raise EcoSpold2SerializationError(
                f"A single .spold file requires exactly one dataset; found {len(datasets)}."
            )
        if destination.suffix.lower() not in {".spold", ".xml"}:
            destination = destination.with_suffix(".spold")
        _atomic_bytes(destination, datasets[0])
    elif mode == "zip":
        if destination.suffix.lower() != ".zip":
            destination = destination.with_suffix(".zip")
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(dir=destination.parent, suffix=".zip", delete=False) as handle:
                temporary = Path(handle.name)
            with zipfile.ZipFile(temporary, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
                for name, contents in package.items():
                    info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
                    info.compress_type = zipfile.ZIP_DEFLATED
                    info.external_attr = 0o100644 << 16
                    archive.writestr(info, contents)
            os.replace(temporary, destination)
        finally:
            if temporary is not None and temporary.exists():
                temporary.unlink()
    elif mode == "directory":
        if destination.exists():
            raise FileExistsError(f"ecoSpold2 destination directory already exists: {destination}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = Path(tempfile.mkdtemp(prefix=f".{destination.name}.", dir=destination.parent))
        try:
            for name, contents in package.items():
                target = temporary / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(contents)
            os.replace(temporary, destination)
        finally:
            if temporary.exists():
                shutil.rmtree(temporary)
    else:
        raise ValueError("ecoSpold2 package_mode must be 'single', 'zip', or 'directory'.")
    return destination


def _atomic_bytes(destination: Path, contents: bytes) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary_name = None
    try:
        with tempfile.NamedTemporaryFile(mode="wb", dir=destination.parent, delete=False) as handle:
            temporary_name = handle.name
            handle.write(contents)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, destination)
    finally:
        if temporary_name and os.path.exists(temporary_name):
            os.unlink(temporary_name)
