"""Safe ecoSpold1 XML codec for single-output process inventories."""

from __future__ import annotations

import math
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from lxml import etree
from pyecospold import Defaults

from brightpath import __version__
from brightpath.core.context import BackgroundContext, BiosphereProfile, FormatProfile, InventoryContext
from brightpath.exceptions import EcoSpold1SerializationError
from brightpath.models import BackgroundProfile, InventoryDocument, InventoryFormat, default_biosphere_profile

from .ecospold1_metadata import (
    EcoSpold1DatasetMetadata,
    EcoSpold1ExportProfile,
    coerce_ecospold1_profile,
)

ECOSPOLD1_NAMESPACE = "http://www.EcoInvent.org/EcoSpold01"
XSI_NAMESPACE = "http://www.w3.org/2001/XMLSchema-instance"
_NS = {"es": ECOSPOLD1_NAMESPACE}
_RESOURCE_CATEGORIES = frozenset({"natural resource", "natural resources", "resource", "resources", "raw"})
_ECOSPOLD_TO_BRIGHTWAY_UNCERTAINTY = {1: 2, 2: 3, 3: 5, 4: 4}
_BRIGHTWAY_TO_ECOSPOLD_UNCERTAINTY = {2: 1, 3: 2, 5: 3, 4: 4}
_MAX_ECOSPOLD1_XML_BYTES = 64 * 1024 * 1024


def _schema() -> etree.XMLSchema:
    return etree.XMLSchema(file=str(Defaults.SCHEMA_V1_FILE))


def _qname(name: str) -> etree.QName:
    return etree.QName(ECOSPOLD1_NAMESPACE, name)


def _child(parent: etree._Element, name: str, attributes: Mapping[str, Any] | None = None) -> etree._Element:
    values = {key: _xml_value(value) for key, value in dict(attributes or {}).items() if value is not None}
    return etree.SubElement(parent, _qname(name), attrib=values)


def _xml_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        if not math.isfinite(value):
            raise EcoSpold1SerializationError("ecoSpold1 XML cannot represent non-finite numeric values.")
        return format(value, ".15g")
    return str(value)


def _attribute(element: etree._Element | None, name: str, default: Any = "") -> Any:
    return default if element is None else element.get(name, default)


def _optional_attributes(element: etree._Element | None) -> dict[str, str]:
    return dict(element.attrib) if element is not None else {}


def _find(parent: etree._Element, path: str) -> etree._Element | None:
    return parent.find(path, namespaces=_NS)


def _findall(parent: etree._Element, path: str) -> list[etree._Element]:
    return parent.findall(path, namespaces=_NS)


def _required_context(
    *,
    background_profile: BackgroundProfile | None,
    biosphere_profile: BiosphereProfile | None,
    context: InventoryContext | None,
) -> InventoryContext:
    if context is not None:
        if context.format.format_id != InventoryFormat.ECOSPOLD1.value:
            raise ValueError("The explicit context format must be 'ecospold1'.")
        if (
            background_profile is not None
            and background_profile.to_technosphere_profile() != context.background.technosphere
        ):
            raise ValueError("background_profile conflicts with context.technosphere.")
        if biosphere_profile is not None and biosphere_profile != context.background.biosphere:
            raise ValueError("biosphere_profile conflicts with context.biosphere.")
        return context
    if background_profile is None:
        raise ValueError(
            "ecoSpold1 does not identify an exact Brightpath background context; "
            "provide background_profile or context."
        )
    technosphere = background_profile.to_technosphere_profile()
    return InventoryContext(
        format=FormatProfile(InventoryFormat.ECOSPOLD1.value, format_version="1"),
        background=BackgroundContext(
            technosphere=technosphere,
            biosphere=biosphere_profile or default_biosphere_profile(technosphere),
        ),
    )


def validate_ecospold1_xml(source: str | Path | bytes) -> tuple[str, ...]:
    """Return XSD validation messages without resolving external entities."""

    try:
        if isinstance(source, bytes):
            if len(source) > _MAX_ECOSPOLD1_XML_BYTES:
                return (f"ecoSpold1 XML exceeds the {_MAX_ECOSPOLD1_XML_BYTES}-byte safety limit.",)
            root = etree.fromstring(source, parser=_safe_parser())
            tree = root.getroottree()
        else:
            path = Path(source).expanduser()
            if path.stat().st_size > _MAX_ECOSPOLD1_XML_BYTES:
                return (f"ecoSpold1 XML exceeds the {_MAX_ECOSPOLD1_XML_BYTES}-byte safety limit.",)
            tree = etree.parse(str(path), parser=_safe_parser())
    except (OSError, etree.XMLSyntaxError) as error:
        return (str(error),)
    schema = _schema()
    if schema.validate(tree):
        return ()
    return tuple(str(entry) for entry in schema.error_log)


def _safe_parser() -> etree.XMLParser:
    return etree.XMLParser(
        resolve_entities=False,
        no_network=True,
        load_dtd=False,
        huge_tree=False,
        remove_comments=False,
    )


def load_ecospold1(
    path: str | Path,
    *,
    background_profile: BackgroundProfile | None = None,
    biosphere_profile: BiosphereProfile | None = None,
    context: InventoryContext | None = None,
    database_name: str | None = None,
) -> InventoryDocument:
    """Parse an XSD-valid ecoSpold1 process XML file.

    The reader accepts out-of-scope multi-output content so it can be inspected;
    format validation blocks its conversion under the supported subset.
    """

    source = Path(path).expanduser()
    if not source.is_file():
        raise FileNotFoundError(f"ecoSpold1 XML file not found: {source}")
    errors = validate_ecospold1_xml(source)
    if errors:
        raise ValueError("Invalid ecoSpold1 XML: " + errors[0])
    tree = etree.parse(str(source), parser=_safe_parser())
    root = tree.getroot()
    if etree.QName(root).namespace != ECOSPOLD1_NAMESPACE or etree.QName(root).localname != "ecoSpold":
        raise ValueError("The XML root is not an ecoSpold1 ecoSpold element.")

    resolved_context = _required_context(
        background_profile=background_profile,
        biosphere_profile=biosphere_profile,
        context=context,
    )
    data = [_parse_dataset(dataset) for dataset in _findall(root, "es:dataset")]
    if not data:
        raise ValueError("The ecoSpold1 file does not contain any datasets.")
    return InventoryDocument(
        data=data,
        context=resolved_context,
        database_name=database_name or source.stem,
        metadata={
            "ecospold1": {
                "validation_id": _attribute(root, "validationId"),
                "validation_status": _attribute(root, "validationStatus"),
            }
        },
    )


def _parse_dataset(dataset: etree._Element) -> dict[str, Any]:
    process = _find(dataset, "es:metaInformation/es:processInformation")
    if process is None:
        raise ValueError("ecoSpold1 dataset is missing processInformation.")
    reference = _find(process, "es:referenceFunction")
    geography = _find(process, "es:geography")
    technology = _find(process, "es:technology")
    time_period = _find(process, "es:timePeriod")
    information = _find(process, "es:dataSetInformation")
    modelling = _find(dataset, "es:metaInformation/es:modellingAndValidation")
    administration = _find(dataset, "es:metaInformation/es:administrativeInformation")
    if reference is None or geography is None or time_period is None or information is None:
        raise ValueError("ecoSpold1 dataset is missing required process metadata.")

    exchanges = []
    for flow_data in _findall(dataset, "es:flowData"):
        exchanges.extend(_parse_exchange(exchange) for exchange in _findall(flow_data, "es:exchange"))

    metadata = _parse_metadata(
        dataset,
        reference=reference,
        geography=geography,
        technology=technology,
        time_period=time_period,
        information=information,
        modelling=modelling,
        administration=administration,
    )
    metadata["raw"]["allocation_count"] = sum(
        len(_findall(flow_data, "es:allocation")) for flow_data in _findall(dataset, "es:flowData")
    )
    result: dict[str, Any] = {
        "name": _attribute(reference, "name").strip(),
        "reference product": _reference_product(reference, exchanges),
        "location": _attribute(geography, "location"),
        "unit": _attribute(reference, "unit"),
        "code": _attribute(dataset, "number"),
        "type": "process",
        "categories": (
            _attribute(reference, "category"),
            _attribute(reference, "subCategory"),
        ),
        "exchanges": exchanges,
        "ecospold1": metadata,
    }
    comment = _attribute(reference, "generalComment")
    if comment:
        result["comment"] = comment
    return result


def _reference_product(reference: etree._Element, exchanges: list[dict[str, Any]]) -> str:
    products = [exchange for exchange in exchanges if exchange.get("type") == "production"]
    if len(products) == 1 and products[0].get("name"):
        return str(products[0]["name"])
    return _attribute(reference, "name").strip()


def _parse_metadata(
    dataset: etree._Element,
    *,
    reference: etree._Element,
    geography: etree._Element,
    technology: etree._Element | None,
    time_period: etree._Element,
    information: etree._Element,
    modelling: etree._Element | None,
    administration: etree._Element | None,
) -> dict[str, Any]:
    sources = [
        _source_mapping(source) for source in (_findall(modelling, "es:source") if modelling is not None else [])
    ]
    people_elements = _findall(administration, "es:person") if administration is not None else []
    people = {_attribute(person, "number"): _person_mapping(person) for person in people_elements}
    data_entry = _find(administration, "es:dataEntryBy") if administration is not None else None
    publication = _find(administration, "es:dataGeneratorAndPublication") if administration is not None else None
    start = next((child for child in time_period if etree.QName(child).localname.startswith("start")), None)
    end = next((child for child in time_period if etree.QName(child).localname.startswith("end")), None)
    start_text = _period_date(start, is_end=False)
    end_text = _period_date(end, is_end=True)
    return {
        "classification": {
            "category": _attribute(reference, "category"),
            "subcategory": _attribute(reference, "subCategory"),
        },
        "time_period": {
            "start_date": start_text,
            "end_date": end_text,
            "data_valid_for_entire_period": _parse_bool(_attribute(time_period, "dataValidForEntirePeriod", "true")),
            "text": _attribute(time_period, "text"),
        },
        "sources": sources,
        "data_entry_person": people.get(_attribute(data_entry, "person"), {}),
        "data_generator_person": people.get(_attribute(publication, "person"), {}),
        "copyright": _parse_bool(_attribute(publication, "copyright", "false")),
        "data_published_in": int(_attribute(publication, "dataPublishedIn", 0) or 0),
        "access_restricted_to": int(_attribute(publication, "accessRestrictedTo", 0) or 0),
        "quality_network": int(_attribute(data_entry, "qualityNetwork", 1) or 1),
        "technology": _attribute(technology, "text"),
        "geography_text": _attribute(geography, "text"),
        "dataset_version": _attribute(information, "version", "1.0"),
        "internal_version": _attribute(information, "internalVersion", "1.0"),
        "raw": {
            "dataset": _optional_attributes(dataset),
            "reference_function": _optional_attributes(reference),
            "geography": _optional_attributes(geography),
            "technology": _optional_attributes(technology),
            "data_set_information": _optional_attributes(information),
            "representativeness": _optional_attributes(
                _find(modelling, "es:representativeness") if modelling is not None else None
            ),
            "validation": _optional_attributes(_find(modelling, "es:validation") if modelling is not None else None),
            "data_entry_by": _optional_attributes(data_entry),
            "data_generator_and_publication": _optional_attributes(publication),
            "persons": [_optional_attributes(person) for person in people_elements],
        },
    }


def _period_date(element: etree._Element | None, *, is_end: bool) -> str:
    if element is None or not (element.text or "").strip():
        return ""
    value = (element.text or "").strip()
    localname = etree.QName(element).localname
    if localname.endswith("Date"):
        return value
    if localname.endswith("YearMonth"):
        year, month = (int(part) for part in value.split("-"))
        if is_end:
            import calendar

            return f"{year:04d}-{month:02d}-{calendar.monthrange(year, month)[1]:02d}"
        return f"{year:04d}-{month:02d}-01"
    return f"{int(value):04d}-12-31" if is_end else f"{int(value):04d}-01-01"


def _source_mapping(source: etree._Element) -> dict[str, Any]:
    fields = {
        "number": "number",
        "source_type": "sourceType",
        "first_author": "firstAuthor",
        "additional_authors": "additionalAuthors",
        "year": "year",
        "title": "title",
        "page_numbers": "pageNumbers",
        "name_of_editors": "nameOfEditors",
        "title_of_anthology": "titleOfAnthology",
        "place_of_publication": "placeOfPublications",
        "publisher": "publisher",
        "journal": "journal",
        "volume_number": "volumeNo",
        "issue_number": "issueNo",
        "text": "text",
    }
    integers = {"number", "source_type", "year", "volume_number"}
    result = {}
    for target, source_name in fields.items():
        value = _attribute(source, source_name)
        if value != "":
            result[target] = int(value) if target in integers else value
    return result


def _person_mapping(person: etree._Element) -> dict[str, Any]:
    fields = {
        "number": "number",
        "name": "name",
        "address": "address",
        "telephone": "telephone",
        "telefax": "telefax",
        "email": "email",
        "company_code": "companyCode",
        "country_code": "countryCode",
    }
    result = {}
    for target, source_name in fields.items():
        value = _attribute(person, source_name)
        if value != "":
            result[target] = int(value) if target == "number" else value
    return result


def _parse_exchange(exchange: etree._Element) -> dict[str, Any]:
    input_group = _find(exchange, "es:inputGroup")
    output_group = _find(exchange, "es:outputGroup")
    input_value = (input_group.text or "").strip() if input_group is not None else ""
    output_value = (output_group.text or "").strip() if output_group is not None else ""
    if output_value == "0":
        exchange_type = "production"
    elif input_value == "4" or output_value == "4":
        exchange_type = "biosphere"
    elif input_value:
        exchange_type = "technosphere"
    elif output_value in {"1", "2", "3"}:
        exchange_type = "substitution" if output_value == "1" else "production"
    else:
        exchange_type = "unknown"
    categories = tuple(
        value for value in (_attribute(exchange, "category"), _attribute(exchange, "subCategory")) if value
    )
    result: dict[str, Any] = {
        "name": _attribute(exchange, "name").strip(),
        "unit": _attribute(exchange, "unit"),
        "amount": float(_attribute(exchange, "meanValue", 0)),
        "type": exchange_type,
        "code": _attribute(exchange, "number"),
        "ecospold1": {"input_group": input_value, "output_group": output_value},
    }
    if categories:
        result["categories"] = categories
    for xml_name, canonical_name in (
        ("location", "location"),
        ("generalComment", "comment"),
        ("CASNumber", "CAS number"),
        ("formula", "chemical formula"),
        ("referenceToSource", "source_reference"),
        ("pageNumbers", "pages"),
    ):
        value = _attribute(exchange, xml_name)
        if value:
            result[canonical_name] = value
    _parse_uncertainty(exchange, result)
    result["ecospold1"]["raw"] = _optional_attributes(exchange)
    return result


def _parse_uncertainty(exchange: etree._Element, result: dict[str, Any]) -> None:
    uncertainty = int(_attribute(exchange, "uncertaintyType", 0) or 0)
    if uncertainty not in _ECOSPOLD_TO_BRIGHTWAY_UNCERTAINTY:
        result["uncertainty type"] = 0
        result["loc"] = result["amount"]
        return
    result["uncertainty type"] = _ECOSPOLD_TO_BRIGHTWAY_UNCERTAINTY[uncertainty]
    mean = float(result["amount"])
    deviation = _attribute(exchange, "standardDeviation95")
    if uncertainty == 1 and deviation:
        result["loc"] = math.log(abs(mean)) if mean else mean
        result["scale"] = math.log(math.sqrt(float(deviation)))
        if mean < 0:
            result["negative"] = True
    elif uncertainty == 2 and deviation:
        result["loc"] = mean
        result["scale"] = float(deviation) / 2
    elif uncertainty in {3, 4}:
        if _attribute(exchange, "minValue"):
            result["minimum"] = float(_attribute(exchange, "minValue"))
        if _attribute(exchange, "maxValue"):
            result["maximum"] = float(_attribute(exchange, "maxValue"))
        result["loc"] = float(_attribute(exchange, "mostLikelyValue", mean))


def render_ecospold1(
    document: InventoryDocument,
    *,
    metadata_profile: EcoSpold1ExportProfile | Mapping[str, Any] | str | Path | None = None,
    generated_at: datetime | str | None = None,
) -> bytes:
    """Render and XSD-validate one ecoSpold1 XML document in memory."""

    if not isinstance(document, InventoryDocument):
        raise TypeError("document must be an InventoryDocument.")
    profile = coerce_ecospold1_profile(metadata_profile)
    timestamp = _timestamp(generated_at)
    root = etree.Element(
        _qname("ecoSpold"),
        nsmap={None: ECOSPOLD1_NAMESPACE, "xsi": XSI_NAMESPACE},
    )
    document_metadata = document.metadata.get("ecospold1", {})
    if isinstance(document_metadata, Mapping):
        if document_metadata.get("validation_id"):
            root.set("validationId", str(document_metadata["validation_id"]))
        if document_metadata.get("validation_status"):
            root.set("validationStatus", str(document_metadata["validation_status"]))
    for index, dataset in enumerate(document.data, start=1):
        metadata = profile.metadata_for(dataset)
        missing = metadata.missing_required()
        if missing:
            raise EcoSpold1SerializationError(
                "Missing required ecoSpold1 metadata for dataset {!r}: {}.".format(
                    dataset.get("name", index), ", ".join(missing)
                )
            )
        _render_dataset(root, dataset, metadata, dataset_number=index, timestamp=timestamp)
    contents = etree.tostring(root, encoding="UTF-8", xml_declaration=True, pretty_print=True)
    errors = validate_ecospold1_xml(contents)
    if errors:
        raise EcoSpold1SerializationError("Generated ecoSpold1 XML failed XSD validation: " + errors[0])
    return contents


def _timestamp(value: datetime | str | None) -> str:
    if value is None:
        selected = datetime.now(timezone.utc).replace(microsecond=0)
        return selected.isoformat()
    if isinstance(value, datetime):
        selected = value
        if selected.tzinfo is None:
            selected = selected.replace(tzinfo=timezone.utc)
        return selected.replace(microsecond=0).isoformat()
    selected = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if selected.tzinfo is None:
        selected = selected.replace(tzinfo=timezone.utc)
    return selected.replace(microsecond=0).isoformat()


def _render_dataset(
    root: etree._Element,
    dataset: Mapping[str, Any],
    metadata: EcoSpold1DatasetMetadata,
    *,
    dataset_number: int,
    timestamp: str,
) -> None:
    raw = dict(metadata.raw)
    raw_dataset = dict(raw.get("dataset", {}))
    raw_timestamp = raw_dataset.get("timestamp") or timestamp
    dataset_element = _child(
        root,
        "dataset",
        {
            "number": dataset_number,
            "internalSchemaVersion": raw_dataset.get("internalSchemaVersion", "1.0"),
            "generator": raw_dataset.get("generator", f"BrightPath {__version__}"),
            "timestamp": raw_timestamp,
        },
    )
    meta = _child(dataset_element, "metaInformation")
    process = _child(meta, "processInformation")
    production = _production_exchange(dataset)
    amount = float(production.get("amount", 1))
    name = str(dataset.get("name") or production.get("name") or "")
    category = metadata.classification.category
    subcategory = metadata.classification.subcategory
    raw_reference = dict(raw.get("reference_function", {}))
    reference_attributes = {
        **raw_reference,
        "datasetRelatesToProduct": raw_reference.get("datasetRelatesToProduct", True),
        "name": name,
        "localName": raw_reference.get("localName", name),
        "infrastructureProcess": dataset.get(
            "infrastructure process", _parse_bool(raw_reference.get("infrastructureProcess", False))
        ),
        "amount": amount,
        "unit": dataset.get("unit") or production.get("unit") or "",
        "category": category,
        "subCategory": subcategory,
        "localCategory": raw_reference.get("localCategory", category),
        "localSubCategory": raw_reference.get("localSubCategory", subcategory),
        "generalComment": dataset.get("comment", ""),
    }
    _child(process, "referenceFunction", reference_attributes)
    _child(
        process,
        "geography",
        {
            **dict(raw.get("geography", {})),
            "location": dataset.get("location") or "GLO",
            "text": metadata.geography_text,
        },
    )
    _child(process, "technology", {**dict(raw.get("technology", {})), "text": metadata.technology})
    period = _child(
        process,
        "timePeriod",
        {
            "dataValidForEntirePeriod": metadata.time_period.data_valid_for_entire_period,
            "text": metadata.time_period.text,
        },
    )
    start = _child(period, "startDate")
    start.text = metadata.time_period.start_date
    end = _child(period, "endDate")
    end.text = metadata.time_period.end_date
    _child(
        process,
        "dataSetInformation",
        {
            **dict(raw.get("data_set_information", {})),
            "type": dict(raw.get("data_set_information", {})).get("type", 1),
            "impactAssessmentResult": False,
            "timestamp": raw_timestamp,
            "version": metadata.dataset_version,
            "internalVersion": metadata.internal_version,
            "energyValues": 0,
            "languageCode": "en",
            "localLanguageCode": "en",
        },
    )

    modelling = _child(meta, "modellingAndValidation")
    representativeness = dict(raw.get("representativeness", {}))
    if representativeness:
        _child(modelling, "representativeness", representativeness)
    source_numbers = []
    for index, source in enumerate(metadata.sources, start=1):
        number = source.number or index
        source_numbers.append(number)
        attributes = {
            "number": number,
            "sourceType": source.source_type,
            "firstAuthor": source.first_author,
            "year": source.year,
            "title": source.title,
            "placeOfPublications": source.place_of_publication,
            "additionalAuthors": source.additional_authors or None,
            "pageNumbers": source.page_numbers or None,
            "nameOfEditors": source.name_of_editors or None,
            "titleOfAnthology": source.title_of_anthology or None,
            "publisher": source.publisher or None,
            "journal": source.journal or None,
            "volumeNo": source.volume_number or None,
            "issueNo": source.issue_number or None,
            "text": source.text or None,
        }
        _child(modelling, "source", attributes)
    validation = dict(raw.get("validation", {}))
    if validation:
        _child(modelling, "validation", validation)

    admin = _child(meta, "administrativeInformation")
    entry = metadata.data_entry_person
    generator = metadata.data_generator_person
    people: list[tuple[int, Any]] = [(entry.number or 1, entry)]
    if generator != entry:
        people.append((generator.number or 2, generator))
    entry_number = people[0][0]
    generator_number = next(number for number, person in people if person == generator)
    _child(
        admin,
        "dataEntryBy",
        {
            **dict(raw.get("data_entry_by", {})),
            "person": entry_number,
            "qualityNetwork": metadata.quality_network,
        },
    )
    publication_attributes: dict[str, Any] = {
        **dict(raw.get("data_generator_and_publication", {})),
        "person": generator_number,
        "dataPublishedIn": metadata.data_published_in,
        "copyright": metadata.copyright,
        "accessRestrictedTo": metadata.access_restricted_to,
    }
    if metadata.data_published_in == 2 and source_numbers:
        publication_attributes["referenceToPublishedSource"] = source_numbers[0]
    _child(admin, "dataGeneratorAndPublication", publication_attributes)
    rendered_numbers = set()
    for number, person in people:
        _child(
            admin,
            "person",
            {
                "number": number,
                "name": person.name,
                "address": person.address,
                "telephone": person.telephone or None,
                "telefax": person.telefax or None,
                "email": person.email or None,
                "companyCode": person.company_code,
                "countryCode": person.country_code,
            },
        )
        rendered_numbers.add(number)
    for raw_person in raw.get("persons", ()):  # Preserve reviewers and other referenced people.
        if not isinstance(raw_person, Mapping):
            continue
        number = int(raw_person.get("number", 0) or 0)
        if number > 0 and number not in rendered_numbers:
            _child(admin, "person", raw_person)
            rendered_numbers.add(number)

    flow_data = _child(dataset_element, "flowData")
    for index, exchange in enumerate(dataset.get("exchanges", ()), start=1):
        _render_exchange(flow_data, exchange, number=index)


def _production_exchange(dataset: Mapping[str, Any]) -> Mapping[str, Any]:
    exchanges = dataset.get("exchanges", ())
    productions = [
        exchange for exchange in exchanges if isinstance(exchange, Mapping) and exchange.get("type") == "production"
    ]
    if len(productions) != 1:
        raise EcoSpold1SerializationError(
            "ecoSpold1 export requires exactly one production exchange per dataset; "
            f"found {len(productions)} in {dataset.get('name', '<unnamed>')!r}."
        )
    return productions[0]


def _render_exchange(parent: etree._Element, exchange: Mapping[str, Any], *, number: int) -> None:
    amount = exchange.get("amount")
    if isinstance(amount, bool) or not isinstance(amount, (int, float)) or not math.isfinite(float(amount)):
        raise EcoSpold1SerializationError(f"Exchange {number} has a non-finite or non-numeric amount.")
    exchange_type = str(exchange.get("type") or "")
    ecospold = exchange.get("ecospold1", {})
    raw = ecospold.get("raw", {}) if isinstance(ecospold, Mapping) else {}
    attributes: dict[str, Any] = {
        **(dict(raw) if isinstance(raw, Mapping) else {}),
        "number": number,
        "name": exchange.get("name", ""),
        "unit": exchange.get("unit", ""),
        "meanValue": float(amount),
        "location": exchange.get("location") or None,
        "generalComment": exchange.get("comment") or None,
        "CASNumber": exchange.get("CAS number") or None,
        "formula": exchange.get("chemical formula") or None,
        "referenceToSource": exchange.get("source_reference") or None,
        "pageNumbers": exchange.get("pages") or None,
        "infrastructureProcess": exchange.get(
            "infrastructureProcess", _parse_bool(raw.get("infrastructureProcess", False))
        ),
    }
    categories = exchange.get("categories")
    if isinstance(categories, (tuple, list)):
        if categories:
            attributes["category"] = categories[0]
        if len(categories) > 1:
            attributes["subCategory"] = categories[1]
    _render_uncertainty(exchange, attributes)
    element = _child(parent, "exchange", attributes)
    if exchange_type == "production":
        group = _child(element, "outputGroup")
        group.text = "0"
    elif exchange_type == "technosphere":
        group = _child(element, "inputGroup")
        group.text = "5"
    elif exchange_type == "biosphere":
        first_category = str(categories[0]).casefold() if isinstance(categories, (tuple, list)) and categories else ""
        if first_category in _RESOURCE_CATEGORIES:
            group = _child(element, "inputGroup")
            group.text = "4"
        else:
            group = _child(element, "outputGroup")
            group.text = "4"
    else:
        raise EcoSpold1SerializationError(f"Unsupported ecoSpold1 exchange type {exchange_type!r}.")


def _render_uncertainty(exchange: Mapping[str, Any], attributes: dict[str, Any]) -> None:
    uncertainty = int(exchange.get("uncertainty type") or 0)
    mapped = _BRIGHTWAY_TO_ECOSPOLD_UNCERTAINTY.get(uncertainty)
    if mapped is None:
        return
    attributes["uncertaintyType"] = mapped
    if uncertainty == 2 and exchange.get("scale") is not None:
        attributes["standardDeviation95"] = math.exp(float(exchange["scale"])) ** 2
    elif uncertainty == 3 and exchange.get("scale") is not None:
        attributes["standardDeviation95"] = float(exchange["scale"]) * 2
    elif uncertainty in {4, 5}:
        attributes["minValue"] = exchange.get("minimum")
        attributes["maxValue"] = exchange.get("maximum")
        if uncertainty == 5:
            attributes["mostLikelyValue"] = exchange.get("loc", exchange.get("amount"))


def write_ecospold1(
    document: InventoryDocument,
    path: str | Path,
    *,
    metadata_profile: EcoSpold1ExportProfile | Mapping[str, Any] | str | Path | None = None,
    generated_at: datetime | str | None = None,
) -> Path:
    """Render, validate, and atomically write an ecoSpold1 XML file."""

    destination = Path(path).expanduser()
    if not destination.suffix:
        destination = destination.with_suffix(".xml")
    if destination.suffix.lower() not in {".xml", ".spold"}:
        raise ValueError("ecoSpold1 files must use an .xml or .spold suffix.")
    destination = destination.resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    contents = render_ecospold1(document, metadata_profile=metadata_profile, generated_at=generated_at)
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
    return destination


def _parse_bool(value: object) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes"}
