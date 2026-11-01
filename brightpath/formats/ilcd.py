"""ILCD 1.1 single-reference process exchange with offline reference resolution.

The codec preserves source XML and its local reference closure in JSON-compatible
dataset extensions. It never migrates a background or guesses external UUIDs.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import posixpath
import shutil
import tempfile
import zipfile
from collections import Counter
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping
from uuid import UUID, uuid5

from lxml import etree

from brightpath.core.context import BackgroundContext, FormatProfile, InventoryContext
from brightpath.core.reports import StageKind
from brightpath.migrations.scaling import scale_exchange
from brightpath.models import BackgroundProfile, InventoryDocument, default_biosphere_profile
from brightpath.profiles.biosphere_categories import wire_categories
from brightpath.units import normalize_unit

from .ilcd_metadata import coerce_profile
from .ilcd_package import (
    BASE,
    MAX_PACKAGE_BYTES,
    NS,
    ILCDError,
    ILCDPackage,
    entity_key,
    fail,
    load_package,
    parse_xml,
    q,
    uuid_text,
    validate_xml,
    xml_bytes,
)
from .ilcd_parameters import evaluate, parameter_values
from .ilcd_references import coerce_references, identity
from .openlca_references import load_openlca_reference_catalog

_UUID_NAMESPACE = UUID("19fc1187-3ceb-484d-88f8-8d1df71baad4")
_COMMENT_BLOCK_START = "[ILCD full exchange comments]\n"
_COMMENT_BLOCK_END = "\n[/ILCD full exchange comments]"
_ROUNDING_BLOCK_START = "[ILCD uncertainty rounding warnings]\n"
_ROUNDING_BLOCK_END = "\n[/ILCD uncertainty rounding warnings]"


def _native_technosphere_reference(exchange, background, flow, provider):
    """Check an external native direction against exact catalog IDs, never payload flags."""
    if background is None or not provider:
        return None
    catalog = load_openlca_reference_catalog(InventoryContext(FormatProfile("ilcd"), background))
    reference = catalog.technosphere.get(identity(exchange, "technosphere")) if catalog is not None else None
    if reference is None or reference.native_reference_is_input is None:
        return None
    if (
        reference.flow_id != flow["flow_id"]
        or reference.process_id != provider
        or reference.flow_property_id != flow["property_id"]
        or flow["flow_type"] != ("Waste flow" if reference.native_flow_type == "WASTE_FLOW" else "Product flow")
    ):
        fail(
            "reference_conflict", "ILCD native provider, flow, quantity or flow type conflicts with its exact catalog."
        )
    return reference


_MAPPED_EXCHANGE = {
    "name",
    "reference product",
    "product",
    "location",
    "unit",
    "code",
    "type",
    "amount",
    "formula",
    "categories",
    "comment",
    "ilcd",
    "input",
    "output",
    "database",
    "uncertainty type",
    "loc",
    "scale",
    "shape",
    "minimum",
    "maximum",
    "negative",
}
_MAPPED_DATASET = {
    "name",
    "reference product",
    "product",
    "location",
    "unit",
    "code",
    "exchanges",
    "parameters",
    "ilcd",
    "comment",
    "database",
}


def _text(parent, path: str) -> str:
    return parent.findtext(path, namespaces=NS) or ""


def _localized(parent, path: str) -> str:
    elements = parent.findall(path, NS)
    if not elements:
        return ""
    chosen = next((e for e in elements if e.get(q("xml", "lang")) == "en"), elements[0])
    return "".join(chosen.itertext()).strip()


def _comment_pointer(identifier: str, comment: str) -> str:
    digest = hashlib.sha256(comment.encode()).hexdigest()
    return (
        f"Full exchange comment ({len(comment)} characters; SHA-256 {digest}) "
        f"is preserved under entry {identifier} in the process general comment."
    )


def _process_comments(comment: str) -> tuple[str, dict[str, str]]:
    """Restore bounded exchange comments from the unlimited native process text.

    JSON framing survives native openLCA import/export without depending on a
    BrightPath-only XML extension. Exchange pointers authenticate the exact text.
    """
    original, marker, suffix = comment.rpartition(_COMMENT_BLOCK_START)
    if not marker or not suffix.endswith(_COMMENT_BLOCK_END):
        return comment, {}
    try:
        comments = json.loads(suffix[: -len(_COMMENT_BLOCK_END)])
    except ValueError:
        fail("comment_manifest_invalid", "Invalid full exchange comment block.")
    if not isinstance(comments, dict) or any(
        not isinstance(key, str) or not isinstance(value, str) or len(value) <= 500 for key, value in comments.items()
    ):
        fail("comment_manifest_invalid", "Full exchange comments must map exchange IDs to complete long text.")
    return original.removesuffix("\n\n"), comments


def _rounding_notes(comment: str) -> tuple[str, list[dict]]:
    """Read audit provenance only; native uncertainty fields remain authoritative."""
    original, marker, suffix = comment.rpartition(_ROUNDING_BLOCK_START)
    if not marker or not suffix.endswith(_ROUNDING_BLOCK_END):
        return comment, []
    try:
        notes = json.loads(suffix[: -len(_ROUNDING_BLOCK_END)])
    except ValueError:
        fail("uncertainty_audit_invalid", "Invalid uncertainty rounding audit block.")
    if not isinstance(notes, list) or any(not isinstance(note, dict) for note in notes):
        fail("uncertainty_audit_invalid", "Uncertainty rounding audit must contain structured records.")
    return original.removesuffix("\n\n"), notes


def _rounding_options(tolerance, warnings) -> float:
    if (
        isinstance(tolerance, bool)
        or not isinstance(tolerance, (int, float))
        or not math.isfinite(tolerance)
        or not 0 <= tolerance <= 0.02
    ):
        fail("uncertainty_tolerance_invalid", "Uncertainty relative tolerance must be finite and between 0 and 0.02.")
    if warnings is not None and not isinstance(warnings, list):
        fail("uncertainty_warnings_invalid", "Uncertainty rounding warnings must be a list or None.")
    return float(tolerance)


def _number(value: object, path: str = "amount") -> float:
    try:
        if isinstance(value, bool):
            raise ValueError()
        amount = float(value)
    except (ValueError, TypeError, OverflowError):
        fail("amount_invalid", f"Expected a finite numeric value, got {value!r}.", path)
    if not math.isfinite(amount):
        fail("amount_invalid", "ILCD amounts must be finite.", path)
    return amount


def _context_dict(context: BackgroundContext) -> dict:
    return {
        "technosphere": {
            "family": context.technosphere.family,
            "version": context.technosphere.version,
            "system_model": context.technosphere.system_model,
        },
        "biosphere": {"family": context.biosphere.family, "version": context.biosphere.version},
    }


def _fingerprint(dataset: Mapping) -> str:
    # Source XML is not part of the semantic fingerprint; this also works after
    # snapshots have passed through JSON storage or Brightway dictionaries.
    value = {key: deepcopy(dataset.get(key)) for key in ("name", "reference product", "location", "unit", "parameters")}
    value["exchanges"] = [
        {k: v for k, v in exchange.items() if k in _MAPPED_EXCHANGE - {"ilcd", "input", "output", "database", "code"}}
        for exchange in dataset.get("exchanges", [])
    ]
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=list, separators=(",", ":")).encode()).hexdigest()


def _flow_info(package: ILCDPackage, path: str, root) -> dict:
    """Follow a flow's reference property to its reference unit, without guessing."""
    reference = _text(root, "f:flowInformation/f:quantitativeReference/f:referenceToReferenceFlowProperty")
    properties = [
        p for p in root.findall("f:flowProperties/f:flowProperty", NS) if p.get("dataSetInternalID") == reference
    ]
    if len(properties) != 1:
        fail("flow_property_unresolved", "A flow must identify exactly one reference flow property.", path)
    prop = properties[0]
    factor = _number(_text(prop, "f:meanValue"))
    if factor != 1:
        fail("reference_factor_invalid", "The reference flow property's factor must be 1.", path)
    prop_ref = prop.find("f:referenceToFlowPropertyDataSet", NS)
    if prop_ref is None:
        fail("reference_missing", "Missing flow-property dataset reference.", path)
    prop_path, prop_root = package.resolve(prop_ref, path)
    unit_ref = prop_root.find(
        "fp:flowPropertiesInformation/fp:quantitativeReference/fp:referenceToReferenceUnitGroup", NS
    )
    if unit_ref is None:
        fail("reference_missing", "Missing reference unit group.", prop_path)
    unit_path, unit_root = package.resolve(unit_ref, prop_path)
    reference_unit = _text(unit_root, "u:unitGroupInformation/u:quantitativeReference/u:referenceToReferenceUnit")
    units = [u for u in unit_root.findall("u:units/u:unit", NS) if u.get("dataSetInternalID") == reference_unit]
    if len(units) != 1 or _number(_text(units[0], "u:meanValue")) != 1:
        fail("reference_unit_invalid", "A unit group must have exactly one reference unit with factor 1.", unit_path)
    name = _localized(root, "f:flowInformation/f:dataSetInformation/f:name/f:baseName")
    categories = [
        e.text or ""
        for e in root.findall(
            "f:flowInformation/f:dataSetInformation/f:classificationInformation/c:elementaryFlowCategorization/c:category",
            NS,
        )
    ]
    # Native person-transport spelling is normalized only at the ILCD boundary.
    native_unit = _text(units[0], "u:name")
    return {
        "name": name,
        "unit": normalize_unit("person kilometer" if native_unit == "p*km" else native_unit),
        "flow_type": _text(root, "f:modellingAndValidation/f:LCIMethod/f:typeOfDataSet"),
        "flow_id": entity_key(root)[1],
        "flow_version": entity_key(root)[2],
        "property_id": entity_key(prop_root)[1],
        "unit_id": units[0].get(q("olca", "unitId")) or _id("unit", entity_key(unit_root)[1], reference_unit),
        "unit_path": unit_path,
        "unit_internal_id": reference_unit,
        "categories": categories,
    }


def _payload(element, name: str) -> dict:
    node = element.find(f"c:other/bp:{name}", NS)
    if node is None or not node.text:
        return {}
    try:
        result = json.loads(node.text)
    except (ValueError, TypeError):
        fail("extension_invalid", f"Invalid BrightPath {name} extension.")
    if not isinstance(result, dict):
        fail("extension_invalid", f"BrightPath {name} extension must be a JSON object.")
    return result


def _reference_exchange(root):
    quantitative = root.find("p:processInformation/p:quantitativeReference", NS)
    if quantitative is None or quantitative.get("type") != "Reference flow(s)":
        fail("quantitative_reference_unsupported", "A single reference product output is required.")
    ids = quantitative.findall("p:referenceToReferenceFlow", NS)
    if len(ids) != 1:
        fail("multiple_reference_flows", "Exactly one reference flow is supported.")
    exchanges = root.findall("p:exchanges/p:exchange", NS)
    found = [e for e in exchanges if e.get("dataSetInternalID") == ids[0].text]
    if len(found) != 1 or _text(found[0], "p:exchangeDirection") not in {"Input", "Output"}:
        fail("reference_input_unsupported", "The reference exchange must have an input or output direction.")
    return found[0]


def _unsupported(root) -> None:
    if root.get("metaDataOnly") in {"true", "1"}:
        fail("metadata_only_unsupported", "Metadata-only ILCD records cannot become process inventories.")
    if root.findall(".//p:allocations", NS) or root.findall(".//p:functionType", NS):
        fail("allocation_unsupported", "Allocation and reminder exchanges are not supported.")
    if root.xpath(
        ".//*[namespace-uri() != $p and namespace-uri() != $c and contains(namespace-uri(), 'EPD')]",
        p=NS["p"],
        c=NS["c"],
    ):
        fail("feature_unsupported", "ILCD+EPD inventories are outside the process adapter scope.")


@dataclass(frozen=True)
class ILCDProcessPackage:
    """Parsed canonical records and local XML resources, independent of a background."""

    data: list[dict]
    resources: dict[str, str]


def _source_dialect(root, explicit: str | None) -> str:
    marked = root.get(q("bp", "dialect"))
    if marked and marked not in {"core", "openlca"}:
        fail("dialect_unsupported", f"Unsupported marked ILCD dialect: {marked}")
    if explicit and explicit not in {"core", "openlca"}:
        fail("dialect_unsupported", f"Unsupported ILCD dialect: {explicit}")
    if explicit and marked and explicit != marked:
        fail("dialect_conflict", "The explicit ILCD dialect conflicts with the package's declared dialect.")
    native = any(e.get(q("olca", "amount")) is not None for e in root.findall("p:exchanges/p:exchange", NS))
    return explicit or marked or ("openlca" if native else "core")


def _load_ilcd_package(
    source: str | Path,
    *,
    reference_directory: str | Path | None = None,
    reference_catalog=None,
    background_context: BackgroundContext | None = None,
    dialect: str | None = None,
) -> ILCDProcessPackage:
    """Parse ILCD processes without selecting or migrating a background database.

    Unresolved supplier identities are retained as findings on the dataset.
    Missing quantitative flow/property/unit references are hard parsing errors.
    """
    references = coerce_references(reference_catalog)
    if references.technosphere or references.biosphere:
        if background_context is None:
            fail("reference_context_missing", "An exact background context is required to apply reference mappings.")
        references.check_context(background_context)
    package = load_package(source, reference_directory=reference_directory)
    processes = [(path, root) for (kind, _, _), (path, root) in package.entities.items() if kind == "Process"]
    if not processes:
        fail("process_missing", "The ILCD package does not contain a process dataset.")
    records, production_flows, comment_blocks = {}, {}, {}
    snapshot_bytes = 0
    for path, root in processes:
        _unsupported(root)
        ref = _reference_exchange(root)
        flow_ref = ref.find("p:referenceToFlowDataSet", NS)
        if flow_ref is None:
            fail("reference_missing", "The production exchange is missing its flow reference.", path)
        flow_path, flow_root = package.resolve(flow_ref, path)
        flow = _flow_info(package, flow_path, flow_root)
        if (flow["flow_type"], _text(ref, "p:exchangeDirection")) not in {
            ("Product flow", "Output"),
            ("Waste flow", "Input"),
        }:
            fail("reference_flow_unsupported", "A product reference output or waste reference input is required.", path)
        code = entity_key(root)[1]
        original_comment, rounding_notes = _rounding_notes(
            _localized(root, "p:processInformation/p:dataSetInformation/c:generalComment")
        )
        process_comment, comment_blocks[code] = _process_comments(original_comment)
        # Native exporters may renumber exchange IDs. The authenticated pointer
        # remains attached to its exchange and identifies the stable text entry.
        complete_comments = {
            _comment_pointer(identifier, comment): comment for identifier, comment in comment_blocks[code].items()
        }
        pointers = [_localized(e, "p:generalComment") for e in root.findall("p:exchanges/p:exchange", NS)]
        if any(pointers.count(pointer) != 1 for pointer in complete_comments):
            fail(
                "comment_manifest_invalid", "Full comment block requires one matching exchange pointer per entry.", path
            )
        comment_blocks[code] = complete_comments
        if code in records:
            fail(
                "process_version_ambiguous",
                "Select one version of each process before importing into a foreground inventory.",
                path,
            )
        data = _payload(root, "dataset")
        data.update(
            {
                "name": _localized(root, "p:processInformation/p:dataSetInformation/p:name/p:baseName"),
                "reference product": flow["name"],
                "unit": flow["unit"],
                "code": code,
                "location": (
                    root.find("p:processInformation/p:geography/p:locationOfOperationSupplyOrProduction", NS).get(
                        "location", ""
                    )
                    if root.find("p:processInformation/p:geography/p:locationOfOperationSupplyOrProduction", NS)
                    is not None
                    else ""
                ),
                "comment": process_comment,
                "exchanges": [],
            }
        )
        findings = [
            {
                "code": "ilcd.uncertainty_rounded",
                "severity": "warning",
                "message": "Export uncertainty was rounded under an explicit tolerance; native dispersion is authoritative.",
                "path": path,
                "details": deepcopy(note),
            }
            for note in rounding_notes
        ]
        source_dialect = _source_dialect(root, dialect)
        if not dialect and not root.get(q("bp", "dialect")) and source_dialect == "openlca":
            findings.append(
                {
                    "code": "ilcd.dialect_inferred",
                    "severity": "info",
                    "message": "Full openLCA exchange attributes were detected; openLCA uncertainty conventions were applied. Supply an explicit source dialect to override this interpretation.",
                    "path": path,
                }
            )
        closure = package.closure(path, root, findings=findings)
        snapshot_bytes += sum(len(text.encode()) for text in closure.values())
        if snapshot_bytes > MAX_PACKAGE_BYTES:
            fail("snapshot_size_limit", "The combined per-process reference closures exceed the snapshot size limit.")
        data["ilcd"] = {
            "schema_version": 1,
            "dialect": source_dialect,
            "process_path": path,
            "process_id": code,
            "resources": closure,
            "issues": findings,
            "uncertainty_rounding_warnings": rounding_notes,
            "metadata": {
                "dataset_version": entity_key(root)[2],
                "process_type": _text(root, "p:modellingAndValidation/p:LCIMethodAndAllocation/p:typeOfDataSet"),
                "reference_year": _text(root, "p:processInformation/p:time/c:referenceYear"),
                "valid_until": _text(root, "p:processInformation/p:time/c:dataSetValidUntil"),
            },
        }
        publication = root.find("p:administrativeInformation/p:publicationAndOwnership", NS)
        if publication is not None:
            for key, tag in (
                ("license_type", "licenseType"),
                ("access_restrictions", "accessRestrictions"),
                ("permanent_uri", "permanentDataSetURI"),
            ):
                value = (
                    _localized(publication, f"c:{tag}")
                    if key == "access_restrictions"
                    else _text(publication, f"c:{tag}")
                )
                if value:
                    data["ilcd"]["metadata"][key] = value
            copyright_value = _text(publication, "c:copyright")
            if copyright_value:
                data["ilcd"]["metadata"]["copyright"] = copyright_value in {"true", "1"}
        if background_context is not None:
            data["ilcd"]["background_context"] = _context_dict(background_context)
        records[code] = data
        production_flows.setdefault((flow["flow_id"], flow["flow_version"]), []).append(data)
    for path, root in processes:
        data = records[entity_key(root)[1]]
        reference_id = _reference_exchange(root).get("dataSetInternalID")
        parameters = []
        for variable in root.findall("p:processInformation/p:mathematicalRelations/p:variableParameter", NS):
            formula = _text(variable, "p:formula")
            if _text(variable, "p:uncertaintyDistributionType") not in {"", "undefined"}:
                fail(
                    "parameter_uncertainty_unsupported",
                    "Parameterized uncertainty needs an explicit supported conversion.",
                    path,
                )
            parameter = _payload(variable, "parameter")
            parameter.update(
                {
                    "name": variable.get("name", ""),
                    "amount": _number(_text(variable, "p:meanValue")),
                    "ilcd parameter": xml_bytes(variable).decode(),
                }
            )
            if formula:
                parameter["formula"] = formula
            parameters.append(parameter)
        values = parameter_values(parameters)
        retained_parameters = [
            p
            for p, node in zip(
                parameters,
                root.findall("p:processInformation/p:mathematicalRelations/p:variableParameter", NS),
                strict=True,
            )
            if node.get(q("bp", "generated")) != "true"
        ]
        if retained_parameters:
            data["parameters"] = retained_parameters
        for index, element in enumerate(root.findall("p:exchanges/p:exchange", NS)):
            flow_path, flow_root = package.resolve(element.find("p:referenceToFlowDataSet", NS), path)
            flow = _flow_info(package, flow_path, flow_root)
            amount_text = _text(element, "p:resultingAmount")
            mean = _number(_text(element, "p:meanAmount"))
            variable = _text(element, "p:referenceToVariable")
            if variable and variable not in values:
                fail("formula_undefined", f"Undefined ILCD variable: {variable}", path)
            expected = mean * values.get(variable, 1)
            amount = _number(amount_text) if amount_text else expected
            if variable and not math.isclose(amount, expected, rel_tol=1e-9, abs_tol=1e-12):
                fail("formula_result_conflict", "ILCD resulting amount disagrees with its variable multiplier.", path)
            direction = _text(element, "p:exchangeDirection")
            if direction not in {"Input", "Output"}:
                fail("direction_missing", "An exchange must specify Input or Output.", path)
            production = element.get("dataSetInternalID") == reference_id
            kind = (
                "production"
                if production
                else ("biosphere" if flow["flow_type"] == "Elementary flow" else "technosphere")
            )
            waste = flow["flow_type"] == "Waste flow"
            if flow["flow_type"] not in {"Product flow", "Elementary flow", "Waste flow"} or (
                waste and direction != ("Input" if production else "Output")
            ):
                fail(
                    "waste_flow_unsupported",
                    "Waste flows require a reference input or a linked non-reference output.",
                    path,
                )
            if production and amount <= 0:
                fail("production_amount_invalid", "Reference production must have a positive amount.", path)
            exchange = _payload(element, "exchange")
            exchange.update({"name": flow["name"], "unit": flow["unit"], "type": kind, "amount": amount})
            if kind == "technosphere" and direction == "Output" and not waste:
                fail(
                    "coproduct_unsupported",
                    "Non-reference product outputs require an explicit substitution/allocation model.",
                    path,
                )
            if kind == "biosphere":
                categories = tuple(flow["categories"])
                if not categories or categories[0] not in {"air", "water", "soil", "natural resource"}:
                    data["ilcd"]["issues"].append(
                        {
                            "code": "ilcd.biosphere_mapping_required",
                            "message": "Elementary flow compartments require an explicit mapping.",
                            "path": f"exchanges[{index}]",
                        }
                    )
                exchange["categories"] = categories
            else:
                exchange.update({"reference product": flow["name"], "location": data["location"]})
            mapping_rows = references.biosphere if kind == "biosphere" else references.technosphere
            mapping = [
                r for r in mapping_rows if r["flow_id"] == flow["flow_id"] and r["flow_version"] == flow["flow_version"]
            ]
            unit_factor = 1.0
            provider = element.get(q("olca", "defaultProvider"), "")
            if provider:
                uuid_text(provider)
                mapping = [r for r in mapping if r.get("process_id") == provider]
            if len(mapping) > 1:
                fail("reference_ambiguous", "Several identities map to the same ILCD flow/provider.", path)
            supplier = None
            if kind == "production":
                exchange.update({key: data[key] for key in ("name", "reference product", "location", "unit")})
            elif kind == "technosphere":
                candidates = production_flows.get((flow["flow_id"], flow["flow_version"]), [])
                if provider:
                    candidates = [d for d in candidates if d["code"] == provider]
                if len(candidates) == 1:
                    supplier = candidates[0]
                    exchange.update({key: supplier[key] for key in ("name", "reference product", "location", "unit")})
                elif not mapping:
                    data["ilcd"]["issues"].append(
                        {
                            "code": "ilcd.supplier_unresolved",
                            "message": "No unique verified supplier identity is available for this product flow.",
                            "path": f"exchanges[{index}]",
                        }
                    )
            if mapping:
                unit_factor = _exchange_unit_factor(mapping[0], mapping[0]["identity"].get("unit", ""), flow["unit"])
                exchange.update(deepcopy(mapping[0]["identity"]))
                data["ilcd"]["issues"] = [i for i in data["ilcd"]["issues"] if i["path"] != f"exchanges[{index}]"]
            link = _payload(element, "link")
            if (
                not mapping
                and kind != "production"
                and background_context is not None
                and link.get("context") == _context_dict(background_context)
            ):
                if (
                    link.get("flow_id") != flow["flow_id"]
                    or link.get("flow_version") != flow["flow_version"]
                    or link.get("process_id", "") != provider
                ):
                    fail(
                        "reference_conflict", "Preserved canonical link disagrees with ILCD exchange references.", path
                    )
                exchange.update(
                    {
                        k: v
                        for k, v in link.get("identity", {}).items()
                        if k in {"name", "reference product", "location", "unit", "categories"}
                    }
                )
                unit_factor = _exchange_unit_factor(link, exchange["unit"], flow["unit"])
                data["ilcd"]["issues"] = [i for i in data["ilcd"]["issues"] if i["path"] != f"exchanges[{index}]"]
            native_reference = (
                _native_technosphere_reference(exchange, background_context, flow, provider)
                if kind == "technosphere" and supplier is None
                else None
            )
            if native_reference is not None and direction != (
                "Output" if native_reference.native_reference_is_input else "Input"
            ):
                fail("reference_conflict", "ILCD exchange direction conflicts with its native provider reference.")
            sign_flipped = kind == "biosphere" and direction != (
                "Input" if wire_categories(exchange.get("categories", ()))[:1] == ("natural resource",) else "Output"
            )
            if sign_flipped:
                exchange["amount"] = -amount
            formula = element.get(q("olca", "formula")) or element.get(q("bp", "formula"))
            if formula:
                if (
                    element.get(q("olca", "unitId"), flow["unit_id"]) != flow["unit_id"]
                    or element.get(q("olca", "propertyId"), flow["property_id"]) != flow["property_id"]
                ):
                    fail(
                        "formula_unit_conversion_unsupported",
                        "An openLCA formula expressed in a non-reference unit/property needs an explicit conversion.",
                        path,
                    )
                if not math.isclose(evaluate(formula, values), amount, rel_tol=1e-9, abs_tol=1e-12):
                    fail("formula_result_conflict", "openLCA formula disagrees with the exchange amount.", path)
                exchange["formula"] = formula
            elif variable:
                exchange["formula"] = f"({mean!r}) * {variable}"
            if sign_flipped and exchange.get("formula"):
                exchange["formula"] = f"-({exchange['formula']})"
            _read_uncertainty(element, exchange, dialect=data["ilcd"]["dialect"])
            if sign_flipped and data["ilcd"]["dialect"] == "openlca" and element.get(q("olca", "amount")) is not None:
                # Partial native amounts are in XML direction coordinates,
                # unlike exchange["amount"], already reflected above.
                if exchange.get("uncertainty type") == 3:
                    exchange["loc"] = -exchange["loc"]
                elif exchange.get("uncertainty type") == 2:
                    exchange["negative"] = not exchange.get("negative", False)
            if sign_flipped and exchange.get("uncertainty type") in {4, 5}:
                exchange["minimum"], exchange["maximum"] = -exchange["maximum"], -exchange["minimum"]
                if exchange["uncertainty type"] == 5:
                    exchange["loc"] = -exchange["loc"]
            if waste and native_reference is None:
                # Brightway's negative treatment reference and technosphere
                # coefficients are encoded as native waste inputs/outputs.
                # Reverse the coordinate change, including bounds and formulas.
                _scale_ilcd_exchange(exchange, -1)
                original_formula = element.get(q("bp", "wasteSourceFormula"))
                if original_formula:
                    if not math.isclose(
                        evaluate(original_formula, values), exchange["amount"], rel_tol=1e-9, abs_tol=1e-12
                    ):
                        fail("formula_result_conflict", "Preserved waste formula disagrees with signed amount.", path)
                    exchange["formula"] = original_formula
            if element.find("p:generalComment", NS) is not None:
                exchange["comment"] = _localized(element, "p:generalComment")
            full_comment = comment_blocks[data["code"]].get(exchange.get("comment", ""))
            if full_comment is not None:
                exchange["comment"] = full_comment
            if unit_factor != 1:
                _scale_ilcd_exchange(exchange, 1 / unit_factor)
                if link.get("source_formula") and link.get("context") == _context_dict(background_context):
                    original_formula = link["source_formula"]
                    if not math.isclose(
                        evaluate(original_formula, values), exchange["amount"], rel_tol=1e-9, abs_tol=1e-12
                    ):
                        fail(
                            "formula_result_conflict", "Preserved source formula disagrees with converted amount.", path
                        )
                    exchange["formula"] = original_formula
            exchange["ilcd"] = {
                "flow_id": flow["flow_id"],
                "flow_version": flow["flow_version"],
                "process_id": (
                    supplier["code"] if supplier else (mapping[0].get("process_id", "") if mapping else provider)
                ),
                "xml": xml_bytes(element).decode(),
                "identity": list(identity(exchange, kind)),
                "direction": direction,
                "internal_id": element.get("dataSetInternalID"),
                "amount_factor": unit_factor,
                "reference_unit": flow["unit"],
            }
            data["exchanges"].append(exchange)
        data["ilcd"]["fingerprint"] = _fingerprint(data)
    return ILCDProcessPackage(list(records.values()), dict(package.resources))


def load_ilcd_package(
    source: str | Path,
    *,
    reference_directory: str | Path | None = None,
    reference_catalog=None,
    background_context: BackgroundContext | None = None,
    dialect: str | None = None,
) -> ILCDProcessPackage:
    """Read local process XML and its reference closure without choosing a background.

    :param source: ILCD ZIP, package directory, or standalone process XML.
    :param reference_directory: Explicit local dependencies for standalone XML.
    :param reference_catalog: Verified mappings for an explicitly supplied context.
    :param background_context: Exact context required when applying mappings.
    :param dialect: Explicit ``core`` or ``openlca`` uncertainty convention.
    :return: Canonical records, including preserved metadata and unresolved-link findings.
    """
    try:
        return _load_ilcd_package(
            source,
            reference_directory=reference_directory,
            reference_catalog=reference_catalog,
            background_context=background_context,
            dialect=dialect,
        )
    except ILCDError as error:
        raise ILCDError(error.code, str(error), error.path, stage=StageKind.PARSE) from error


def load_ilcd(
    source: str | Path,
    *,
    context: InventoryContext | None = None,
    background_profile: BackgroundProfile | None = None,
    biosphere_profile=None,
    reference_directory=None,
    reference_catalog=None,
    database_name: str = "",
    dialect: str | None = None,
) -> InventoryDocument:
    """Load an ILCD inventory with an explicit exact background context."""
    if context is None:
        if background_profile is None:
            fail("context_required", "ILCD document import requires an explicit background context.")
        techno = background_profile.to_technosphere_profile()
        context = InventoryContext(
            FormatProfile("ilcd", format_version="1.1"),
            BackgroundContext(techno, biosphere_profile or default_biosphere_profile(techno)),
        )
    if context.format.format_id != "ilcd" or context.format.format_version not in {"", "1.1"}:
        fail("context_conflict", "ILCD import requires format ilcd, version 1.1.")
    if dialect and context.format.dialect and dialect != context.format.dialect:
        fail("dialect_conflict", "The source context and explicit ILCD dialect conflict.")
    package = load_ilcd_package(
        source,
        reference_directory=reference_directory,
        reference_catalog=reference_catalog,
        background_context=context.background,
        dialect=dialect or context.format.dialect or None,
    )
    if not context.format.dialect:
        dialects = {dataset["ilcd"]["dialect"] for dataset in package.data}
        dialect = next(iter(dialects)) if len(dialects) == 1 else ""
        context = InventoryContext(FormatProfile("ilcd", format_version="1.1", dialect=dialect), context.background)
    return InventoryDocument(data=package.data, context=context, database_name=database_name)


def _read_uncertainty(element, exchange: dict, *, dialect: str = "core") -> None:
    distribution = _text(element, "p:uncertaintyDistributionType")
    if distribution in {"", "undefined"}:
        return
    if distribution == "uniform":
        exchange.update(
            {
                "uncertainty type": 4,
                "minimum": _number(_text(element, "p:minimumAmount")),
                "maximum": _number(_text(element, "p:maximumAmount")),
            }
        )
        if exchange["minimum"] > exchange["maximum"]:
            fail("uncertainty_invalid", "Uniform uncertainty bounds are reversed.")
    elif distribution == "triangular" and dialect == "openlca":
        exchange.update(
            {
                "uncertainty type": 5,
                "minimum": _number(_text(element, "p:minimumAmount")),
                "loc": _number(element.get(q("olca", "mostLikelyValue"))),
                "maximum": _number(_text(element, "p:maximumAmount")),
            }
        )
        _triangular_parameters(exchange)
    elif distribution == "normal":
        scale = _number(_text(element, "p:relativeStandardDeviation95In")) / (1 if dialect == "openlca" else 2)
        if scale < 0:
            fail("uncertainty_invalid", "Normal uncertainty cannot have a negative scale.")
        centre = exchange["amount"]
        if dialect == "openlca" and element.get(q("olca", "amount")) is not None:
            centre = _number(element.get(q("olca", "amount")))
        exchange.update({"uncertainty type": 3, "loc": centre, "scale": scale})
    elif distribution == "log-normal":
        geometric_squared = _number(_text(element, "p:relativeStandardDeviation95In"))
        centre = exchange["amount"]
        if dialect == "openlca" and element.get(q("olca", "amount")) is not None:
            centre = _number(element.get(q("olca", "amount")))
        if (centre == 0 if dialect == "openlca" else centre <= 0) or geometric_squared < 1:
            fail(
                "uncertainty_invalid", "Log-normal uncertainty requires a positive central amount and SDg squared >= 1."
            )
        exchange.update(
            {
                "uncertainty type": 2,
                # openLCA 2.6.2 samples with log(parameter1) - sigma**2 / 2.
                # Its model calls parameter1 a geometric mean, but the actual
                # generator interprets it as an arithmetic mean. Read the
                # actual native distribution, never an audit-sidecar value.
                "loc": math.log(abs(centre)) - (0.5 * math.log(geometric_squared) ** 2 if dialect == "openlca" else 0),
                "scale": math.log(geometric_squared) / (1 if dialect == "openlca" else 2),
            }
        )
        if dialect == "openlca":
            # Explicit False matters for a foreign native distribution whose
            # positive mean accompanies a negative nominal amount: Brightway
            # otherwise infers a negative distribution from that amount.
            exchange["negative"] = centre < 0
    else:
        fail("uncertainty_unsupported", f"ILCD {distribution} uncertainty requires an explicit supported conversion.")


def _triangular_parameters(exchange: Mapping) -> tuple[float, float, float]:
    minimum, mode, maximum = (_number(exchange.get(key)) for key in ("minimum", "loc", "maximum"))
    if not minimum <= mode <= maximum or minimum == maximum:
        fail("uncertainty_invalid", "Triangular uncertainty requires minimum <= mode <= maximum and distinct bounds.")
    return minimum, mode, maximum


def _scale_ilcd_exchange(exchange: dict, factor: float) -> None:
    """Scale the ILCD triangular subset as well as shared supported distributions."""
    if exchange.get("uncertainty type") != 5:
        scale_exchange(exchange, factor)
        return
    minimum, mode, maximum = _triangular_parameters(exchange)
    exchange["amount"] = _number(exchange["amount"]) * factor
    exchange["loc"] = mode * factor
    exchange["minimum"], exchange["maximum"] = (
        (minimum * factor, maximum * factor) if factor > 0 else (maximum * factor, minimum * factor)
    )
    if "negative" in exchange:
        exchange["negative"] = exchange["amount"] < 0
    if exchange.get("formula"):
        exchange["formula"] = f"({exchange['formula']}) * ({factor!r})"


def _add(parent, prefix: str, local_name: str, text: object | None = None, **attributes):
    element = etree.SubElement(parent, q(prefix, local_name), {k: str(v) for k, v in attributes.items()})
    if text is not None:
        element.text = str(text)
    return element


def _local(parent, prefix: str, name: str, value: object):
    return _add(parent, prefix, name, value, **{q("xml", "lang"): "en"})


def _id(*parts) -> str:
    return str(uuid5(_UUID_NAMESPACE, json.dumps(parts, sort_keys=True)))


def _root(prefix: str, name: str):
    return etree.Element(
        q(prefix, name), nsmap={None: NS[prefix], "common": NS["c"], "olca": NS["olca"], "bp": NS["bp"]}, version="1.1"
    )


def _admin(root, prefix: str, version: str) -> None:
    admin = _add(root, prefix, "administrativeInformation")
    publication = _add(admin, prefix, "publicationAndOwnership")
    _add(publication, "c", "dataSetVersion", version)


def _reference(parent, prefix: str, name: str, kind: str, identifier: str, version: str, uri: str):
    return _add(
        parent,
        prefix,
        name,
        type=f"{kind.lower()} data set",
        refObjectId=uuid_text(identifier),
        version=version,
        uri=uri,
    )


def _exchange_unit_factor(reference: Mapping, source_unit: str, reference_unit: str) -> float:
    """Require explicit positive conversion evidence when exchange units differ."""
    factor = _number(reference.get("amount_factor", 1))
    if factor <= 0:
        fail("unit_mapping_required", "ILCD reference conversion factors must be positive.")
    if normalize_unit(source_unit) == normalize_unit(reference_unit):
        if factor != 1:
            fail("unit_mapping_required", "Equal units must have conversion factor 1.")
    elif "amount_factor" not in reference or normalize_unit(reference.get("reference_unit", "")) != normalize_unit(
        reference_unit
    ):
        fail("unit_mapping_required", "Exchange and ILCD reference units need an explicit conversion.")
    return factor


def _quantity_resources(unit: str, *, quantity=None, profile=None) -> tuple[dict[str, str], str]:
    if quantity is not None:
        return _reference_quantity_resources(quantity)
    normalized = normalize_unit(unit)
    if normalized in {"ton kilometer", "person kilometer", "standard cubic meter"}:
        # Use the same verified native quantity as background links. These
        # reference units have factor 1; no scientific coefficient is rescaled.
        from .ilcd_background import quantity_definition

        return _reference_quantity_resources(quantity_definition(normalized, profile=profile))
    known = {
        "kilogram": "Mass",
        "gram": "Mass",
        "ton": "Mass",
        "cubic meter": "Volume",
        "liter": "Volume",
        "megajoule": "Energy",
        "kilowatt hour": "Energy",
        "meter": "Length",
        "square meter": "Area",
        "unit": "Number of items",
        "hour": "Time",
        "second": "Time",
        "kilometer": "Length",
    }
    if normalized not in known:
        fail("unit_mapping_required", f"No explicit ILCD quantity definition is available for {unit!r}.")
    prop_id, unit_id = _id("quantity", normalized), _id("unit-group", normalized)
    unit_root = _root("u", "unitGroupDataSet")
    info = _add(unit_root, "u", "unitGroupInformation")
    data = _add(info, "u", "dataSetInformation")
    _add(data, "c", "UUID", unit_id)
    _local(data, "c", "name", f"Units of {known[normalized].lower()} ({normalized})")
    quantitative = _add(info, "u", "quantitativeReference")
    _add(quantitative, "u", "referenceToReferenceUnit", "0")
    _admin(unit_root, "u", "01.00.000")
    units = _add(unit_root, "u", "units")
    entry = _add(units, "u", "unit", dataSetInternalID="0")
    entry.set(q("olca", "unitId"), _id("unit", unit_id, "0"))
    _add(entry, "u", "name", normalized)
    _add(entry, "u", "meanValue", "1")
    prop_root = _root("fp", "flowPropertyDataSet")
    info = _add(prop_root, "fp", "flowPropertiesInformation")
    data = _add(info, "fp", "dataSetInformation")
    _add(data, "c", "UUID", prop_id)
    _local(data, "c", "name", known[normalized])
    quantitative = _add(info, "fp", "quantitativeReference")
    _reference(
        quantitative,
        "fp",
        "referenceToReferenceUnitGroup",
        "Unit group",
        unit_id,
        "01.00.000",
        f"../unitgroups/{unit_id}.xml",
    )
    _admin(prop_root, "fp", "01.00.000")
    return {
        f"unitgroups/{unit_id}.xml": xml_bytes(unit_root).decode(),
        f"flowproperties/{prop_id}.xml": xml_bytes(prop_root).decode(),
    }, prop_id


def _reference_quantity_resources(quantity: Mapping) -> tuple[dict[str, str], str]:
    """Render a verified native quantity with its full unit group."""
    prop_id, unit_id = quantity["property_id"], quantity["group_id"]
    unit_root = _root("u", "unitGroupDataSet")
    info = _add(unit_root, "u", "unitGroupInformation")
    data = _add(info, "u", "dataSetInformation")
    _add(data, "c", "UUID", unit_id)
    _local(data, "c", "name", quantity["group_name"])
    if quantity.get("comment"):
        _local(data, "c", "generalComment", quantity["comment"])
    units = sorted(quantity["units"], key=lambda unit: unit["id"])
    ref_index = next(index for index, unit in enumerate(units) if unit["id"] == quantity["reference_unit_id"])
    _add(_add(info, "u", "quantitativeReference"), "u", "referenceToReferenceUnit", str(ref_index))
    _admin(unit_root, "u", "01.00.000")
    contents = _add(unit_root, "u", "units")
    for index, unit in enumerate(units):
        node = _add(contents, "u", "unit", dataSetInternalID=str(index))
        node.set(q("olca", "unitId"), unit["id"])
        _add(node, "u", "name", unit["name"])
        _add(node, "u", "meanValue", repr(unit["factor"]))
    prop_root = _root("fp", "flowPropertyDataSet")
    info = _add(prop_root, "fp", "flowPropertiesInformation")
    data = _add(info, "fp", "dataSetInformation")
    _add(data, "c", "UUID", prop_id)
    _local(data, "c", "name", quantity["property_name"])
    if quantity.get("comment"):
        _local(data, "c", "generalComment", quantity["comment"])
    _reference(
        _add(info, "fp", "quantitativeReference"),
        "fp",
        "referenceToReferenceUnitGroup",
        "Unit group",
        unit_id,
        "01.00.000",
        f"../unitgroups/{unit_id}.xml",
    )
    _admin(prop_root, "fp", "01.00.000")
    return {
        f"unitgroups/{unit_id}.xml": xml_bytes(unit_root).decode(),
        f"flowproperties/{prop_id}.xml": xml_bytes(prop_root).decode(),
    }, prop_id


def _new_flow(
    identifier: str, name: str, unit: str, version: str, *, categories=(), quantity=None, flow_type=None, profile=None
) -> dict[str, str]:
    resources, prop_id = _quantity_resources(unit, quantity=quantity, profile=profile)
    root = _root("f", "flowDataSet")
    info = _add(root, "f", "flowInformation")
    data = _add(info, "f", "dataSetInformation")
    _add(data, "c", "UUID", identifier)
    _local(_add(data, "f", "name"), "f", "baseName", name)
    if categories:
        classification = _add(data, "f", "classificationInformation")
        category = _add(classification, "c", "elementaryFlowCategorization")
        for level, value in enumerate(categories):
            _add(category, "c", "category", value, level=level)
    quantitative = _add(info, "f", "quantitativeReference")
    _add(quantitative, "f", "referenceToReferenceFlowProperty", "0")
    method = _add(_add(root, "f", "modellingAndValidation"), "f", "LCIMethod")
    _add(method, "f", "typeOfDataSet", flow_type or ("Elementary flow" if categories else "Product flow"))
    _admin(root, "f", version)
    prop = _add(_add(root, "f", "flowProperties"), "f", "flowProperty", dataSetInternalID="0")
    _reference(
        prop,
        "f",
        "referenceToFlowPropertyDataSet",
        "Flow property",
        prop_id,
        "01.00.000",
        f"../flowproperties/{prop_id}.xml",
    )
    _add(prop, "f", "meanValue", "1")
    resources[f"flows/{identifier}.xml"] = xml_bytes(root).decode()
    return resources


def _merge(resources: dict, incoming: Mapping) -> None:
    for path, content in incoming.items():
        if path in resources and resources[path] != content:
            if etree.tostring(parse_xml(resources[path].encode()), method="c14n") != etree.tostring(
                parse_xml(content.encode()), method="c14n"
            ):
                fail("reference_conflict", f"Conflicting ILCD resource content at {path}.")
        resources[path] = content


def _set_local(parent, prefix: str, name: str, value: object):
    entries = parent.findall(q(prefix, name))
    node = next((e for e in entries if e.get(q("xml", "lang")) == "en"), None)
    if node is None and entries:
        node = entries[0]
    if node is None:
        node = _local(parent, prefix, name, value)
    else:
        node.text = str(value)
    return node


def _set_payload(element, name: str, payload: dict) -> None:
    other = element.find(q("c", "other"))
    if other is None:
        other = _add(element, "c", "other")
    for node in other.findall(q("bp", name)):
        other.remove(node)
    if payload:
        _add(other, "bp", name, json.dumps(payload, sort_keys=True, allow_nan=False))
    if not len(other):
        element.remove(other)


def render_ilcd_package(
    document: InventoryDocument,
    *,
    metadata_profile=None,
    reference_catalog=None,
    dialect: str | None = None,
    package_mode: str | None = None,
    catalog_provider=None,
    uncertainty_relative_tolerance: float = 0.0,
    uncertainty_rounding_warnings: list | None = None,
) -> dict[str, bytes]:
    """Render and validate a deterministic ILCD process package without writing.

    :param dialect: ``core`` or ``openlca``; defaults to the document dialect
        or ``core``. The latter reports supplier-link loss rather than silently
        implying that a flow reference identifies its producer.
    :param catalog_provider: Exact background catalogs for automatic openLCA
        references; defaults to the configured packaged catalog provider.
    :param uncertainty_relative_tolerance: Explicit openLCA export-only spread
        tolerance in [0, 0.02]; zero retains strict precision checks.
    :param uncertainty_rounding_warnings: Optional list extended with structured
        records only after successful rendering. Native fields remain authoritative.
    """
    if not isinstance(document, InventoryDocument):
        raise TypeError("document must be an InventoryDocument.")
    tolerance = _rounding_options(uncertainty_relative_tolerance, uncertainty_rounding_warnings)
    rounding_warnings = []
    dialect = (
        dialect or (document.context.format.dialect if document.context.format.format_id == "ilcd" else "") or "core"
    )
    if dialect not in {"core", "openlca"}:
        fail("dialect_unsupported", "ILCD dialect must be core or openlca.")
    if tolerance and dialect != "openlca":
        fail("uncertainty_tolerance_invalid", "Uncertainty rounding tolerance is supported only for dialect='openlca'.")
    if package_mode not in {None, "zip", "directory"}:
        fail("package_mode_invalid", "ILCD output must be a ZIP or directory package.")
    profile, references = coerce_profile(metadata_profile), coerce_references(reference_catalog)
    references.check_context(document.context.background)
    references.package()
    if document.database_parameters or document.project_parameters:
        fail(
            "parameter_scope_unsupported",
            "Materialize database/project parameters into process scope before ILCD export.",
        )
    datasets = document.data
    if not datasets:
        fail("process_missing", "Cannot export an empty ILCD inventory.")
    counts = Counter(d.get("code", "") for d in datasets)
    if "" in counts or any(n > 1 for n in counts.values()):
        fail("process_identity_invalid", "ILCD export requires unique persistent dataset codes.")
    from .ilcd_background import automatic_references

    references = automatic_references(document, references, dialect=dialect, catalog_provider=catalog_provider)
    resources = dict(references.resources)
    records = []
    foreground = {}
    for dataset in datasets:
        imported = dataset.get("ilcd", {})
        metadata = profile.for_dataset(dataset)
        same_context = imported.get("background_context") == _context_dict(document.context.background)
        original = imported.get("process_path")
        preserve = original and original in imported.get("resources", {})
        if preserve:
            root = parse_xml(imported["resources"][original].encode())
            _unsupported(root)
            _reference_exchange(root)
            if imported.get("fingerprint") != _fingerprint(dataset) or not same_context:
                if (
                    root.find("p:LCIAResults", NS) is not None
                    or root.find("p:modellingAndValidation/p:validation", NS) is not None
                    or root.find("p:modellingAndValidation/p:complianceDeclarations", NS) is not None
                ):
                    fail(
                        "stale_claims",
                        "Edited inventories must explicitly remove or replace preserved LCIA, review, and compliance claims.",
                    )
            # Process XML is rebuilt below; imported resources contain the original
            # process to preserve its metadata, not an additional exported process.
            auxiliary = {
                p: v
                for p, v in imported["resources"].items()
                if etree.QName(parse_xml(v.encode())).localname != "processDataSet"
            }
            _merge(resources, auxiliary)
        else:
            root = None
        process_id = (
            uuid_text(metadata["process_id"])
            if metadata.get("process_id")
            else (
                imported.get("process_id")
                if preserve and same_context
                else _id("process", dataset["code"], _context_dict(document.context.background))
            )
        )
        process_id = uuid_text(process_id)
        production = [e for e in dataset.get("exchanges", []) if e.get("type") == "production"]
        if len(production) != 1 or _number(production[0].get("amount")) == 0:
            fail("multiple_reference_flows", "ILCD export requires one nonzero reference production exchange.")
        prod = production[0]
        waste_reference = _number(prod["amount"]) < 0
        flow_type = "Waste flow" if waste_reference else "Product flow"
        flow_identity = "waste-flow" if waste_reference else "product-flow"
        if normalize_unit(prod.get("unit", "")) != normalize_unit(dataset.get("unit", "")):
            fail("production_identity_invalid", "Production unit differs from its process unit.")
        preserved_prod = prod.get("ilcd", {})
        flow_id = preserved_prod.get("flow_id") if preserve else None
        flow_version = preserved_prod.get("flow_version") if flow_id else metadata["dataset_version"]
        flow_id = (
            uuid_text(flow_id)
            if flow_id
            else _id(flow_identity, dataset["code"], _context_dict(document.context.background))
        )
        if not preserve or not preserved_prod.get("flow_id"):
            _merge(
                resources,
                _new_flow(
                    flow_id,
                    dataset["reference product"],
                    dataset["unit"],
                    flow_version,
                    flow_type=flow_type,
                    profile=document.context.background.technosphere,
                ),
            )
        else:
            existing = ILCDPackage.from_resources(resources)
            matching = [
                (path, flow)
                for (kind, uid, ver), (path, flow) in existing.entities.items()
                if kind == "Flow" and uid == flow_id and ver == flow_version
            ]
            if len(matching) != 1:
                fail("reference_missing", "The preserved reference product flow is missing.")
            old_path, old_flow = matching[0]
            old_info = _flow_info(existing, old_path, old_flow)
            if (
                old_info["name"] != dataset["reference product"]
                or old_info["unit"] != normalize_unit(dataset["unit"])
                or old_info["flow_type"] != flow_type
            ):
                flow_id = _id(
                    flow_identity,
                    dataset["code"],
                    dataset["reference product"],
                    dataset["unit"],
                    _context_dict(document.context.background),
                )
                flow_version = metadata["dataset_version"]
                _merge(
                    resources,
                    _new_flow(
                        flow_id,
                        dataset["reference product"],
                        dataset["unit"],
                        flow_version,
                        flow_type=flow_type,
                        profile=document.context.background.technosphere,
                    ),
                )
        key = identity(dataset, "technosphere")
        if key in foreground:
            fail("supplier_ambiguous", f"Several foreground processes have identity {key}.")
        record = {
            "dataset": dataset,
            "metadata": metadata,
            "root": root,
            "process_id": process_id,
            "flow_id": flow_id,
            "flow_version": flow_version,
            "same_context": same_context,
        }
        foreground[key] = record
        records.append(record)
    reference_package = ILCDPackage.from_resources(resources)
    for record in records:
        dataset, metadata = record["dataset"], record["metadata"]
        root = record["root"]
        if root is None:
            root = _root("p", "processDataSet")
            root.set("locations", "../ILCDLocations.xml")
            info = _add(root, "p", "processInformation")
            data = _add(info, "p", "dataSetInformation")
            _add(data, "c", "UUID", record["process_id"])
            _local(_add(data, "p", "name"), "p", "baseName", dataset["name"])
            if dataset.get("comment"):
                _local(data, "c", "generalComment", dataset["comment"])
            quantitative = _add(info, "p", "quantitativeReference", type="Reference flow(s)")
            _add(quantitative, "p", "referenceToReferenceFlow", "0")
            time = _add(info, "p", "time")
            _add(time, "c", "referenceYear", metadata["reference_year"])
            _add(time, "c", "dataSetValidUntil", metadata["valid_until"])
            geography = _add(info, "p", "geography")
            _add(geography, "p", "locationOfOperationSupplyOrProduction", location=dataset["location"])
            method = _add(_add(root, "p", "modellingAndValidation"), "p", "LCIMethodAndAllocation")
            _add(method, "p", "typeOfDataSet", metadata["process_type"])
            _admin(root, "p", metadata["dataset_version"])
        else:
            root.find("p:processInformation/p:dataSetInformation/c:UUID", NS).text = record["process_id"]
            _set_local(
                root.find("p:processInformation/p:dataSetInformation/p:name", NS), "p", "baseName", dataset["name"]
            )
            root.find("p:processInformation/p:quantitativeReference/p:referenceToReferenceFlow", NS).text = "0"
            for path, value in (
                ("p:processInformation/p:time/c:referenceYear", metadata["reference_year"]),
                ("p:processInformation/p:time/c:dataSetValidUntil", metadata["valid_until"]),
                ("p:modellingAndValidation/p:LCIMethodAndAllocation/p:typeOfDataSet", metadata["process_type"]),
                ("p:administrativeInformation/p:publicationAndOwnership/c:dataSetVersion", metadata["dataset_version"]),
            ):
                node = root.find(path, NS)
                if node is None:
                    fail("required_metadata_missing", f"Imported XML lacks the structured field {path}.")
                node.text = str(value)
            location = root.find("p:processInformation/p:geography/p:locationOfOperationSupplyOrProduction", NS)
            if location is not None:
                location.set("location", dataset["location"])
            comment = root.find("p:processInformation/p:dataSetInformation/c:generalComment", NS)
            if comment is not None or dataset.get("comment"):
                _set_local(
                    root.find("p:processInformation/p:dataSetInformation", NS),
                    "c",
                    "generalComment",
                    dataset.get("comment", ""),
                )
                info_data = root.find("p:processInformation/p:dataSetInformation", NS)
                data_order = [
                    "UUID",
                    "name",
                    "identifierOfSubDataSet",
                    "synonyms",
                    "complementingProcesses",
                    "classificationInformation",
                    "generalComment",
                    "referenceToExternalDocumentation",
                    "other",
                ]
                info_data[:] = sorted(
                    info_data,
                    key=lambda child: (
                        data_order.index(etree.QName(child).localname)
                        if etree.QName(child).localname in data_order
                        else len(data_order)
                    ),
                )
            for old in root.findall("p:exchanges", NS):
                root.remove(old)
        publication = root.find("p:administrativeInformation/p:publicationAndOwnership", NS)
        for key, tag in (
            ("license_type", "licenseType"),
            ("access_restrictions", "accessRestrictions"),
            ("copyright", "copyright"),
            ("permanent_uri", "permanentDataSetURI"),
        ):
            if key in metadata:
                if key == "access_restrictions":
                    _set_local(publication, "c", tag, metadata[key])
                else:
                    node = publication.find(q("c", tag))
                    if node is None:
                        node = _add(publication, "c", tag)
                    node.text = str(metadata[key]).lower() if isinstance(metadata[key], bool) else str(metadata[key])
        publication_order = [
            "dateOfLastRevision",
            "dataSetVersion",
            "referenceToPrecedingDataSetVersion",
            "permanentDataSetURI",
            "workflowAndPublicationStatus",
            "referenceToUnchangedRepublication",
            "referenceToRegistrationAuthority",
            "registrationNumber",
            "referenceToOwnershipOfDataSet",
            "copyright",
            "referenceToEntitiesWithExclusiveAccess",
            "licenseType",
            "accessRestrictions",
            "other",
        ]
        publication[:] = sorted(
            publication,
            key=lambda child: (
                publication_order.index(etree.QName(child).localname)
                if etree.QName(child).localname in publication_order
                else len(publication_order)
            ),
        )
        info = root.find("p:processInformation", NS)
        relations = info.find("p:mathematicalRelations", NS)
        if relations is not None:
            for old in relations.findall("p:variableParameter", NS):
                relations.remove(old)
        values = parameter_values(dataset.get("parameters", []))
        if dataset.get("parameters"):
            if relations is None:
                relations = _add(info, "p", "mathematicalRelations")
            for parameter in dataset["parameters"]:
                if parameter.get("uncertainty type") not in {None, 0, 1}:
                    fail(
                        "parameter_uncertainty_unsupported",
                        "ILCD process parameter uncertainty is outside the supported conversion subset.",
                    )
                variable = (
                    parse_xml(parameter["ilcd parameter"].encode())
                    if parameter.get("ilcd parameter")
                    else etree.Element(q("p", "variableParameter"))
                )
                variable.set("name", parameter["name"])
                for old in list(variable):
                    if old.tag in {q("p", "formula"), q("p", "meanValue")}:
                        variable.remove(old)
                relations.append(variable)
                if parameter.get("formula"):
                    _add(variable, "p", "formula", parameter["formula"])
                _add(variable, "p", "meanValue", repr(values[parameter["name"]]))
                _set_payload(
                    variable,
                    "parameter",
                    {k: v for k, v in parameter.items() if k not in {"name", "amount", "formula", "ilcd parameter"}},
                )
                parameter_order = [
                    "formula",
                    "meanValue",
                    "minimumValue",
                    "maximumValue",
                    "uncertaintyDistributionType",
                    "relativeStandardDeviation95In",
                    "comment",
                    "other",
                ]
                variable[:] = sorted(
                    variable,
                    key=lambda child: (
                        parameter_order.index(etree.QName(child).localname)
                        if etree.QName(child).localname in parameter_order
                        else len(parameter_order)
                    ),
                )
        exchanges = _add(root, "p", "exchanges")
        long_comments = {}
        dataset_rounding = []
        # Reference exchange gets ID 0 irrespective of source ordering.
        ordered = sorted(enumerate(dataset["exchanges"]), key=lambda item: item[1].get("type") != "production")
        for number, (exchange_index, exchange) in enumerate(ordered):
            kind = exchange.get("type")
            if kind not in {"production", "technosphere", "biosphere"}:
                fail("exchange_type_unsupported", f"Unsupported exchange type: {kind}")
            amount = _number(exchange.get("amount"))
            supplier = foreground.get(identity(exchange, "technosphere")) if kind == "technosphere" else None
            preserved = exchange.get("ilcd", {})
            if kind == "production":
                reference = record
            elif supplier is not None:
                reference = supplier
            else:
                reference = references.lookup(exchange, kind)
                old_identity = json.loads(json.dumps(preserved.get("identity", [])))
                current_identity = json.loads(json.dumps(identity(exchange, kind)))
                if (
                    reference is None
                    and record["same_context"]
                    and old_identity == current_identity
                    and preserved.get("flow_id")
                ):
                    reference = preserved
                if reference is None:
                    fail("reference_mapping_required", f"No exact ILCD {kind} mapping for {identity(exchange, kind)}.")
            provider = reference.get("process_id", "") if kind == "technosphere" else ""
            if kind == "technosphere" and not provider:
                fail("supplier_unresolved", "An ILCD technosphere export requires a verified provider.")
            if provider and dialect == "core":
                fail("supplier_link_loss", "Core ILCD cannot preserve the selected supplier; use dialect='openlca'.")
            element = (
                parse_xml(preserved["xml"].encode()) if preserved.get("xml") else etree.Element(q("p", "exchange"))
            )
            # Keep extension attributes and opaque children; regenerate all mapped
            # fields in the order prescribed by the ILCD schema.
            mapped_children = {
                "referenceToFlowDataSet",
                "exchangeDirection",
                "referenceToVariable",
                "meanAmount",
                "resultingAmount",
                "minimumAmount",
                "maximumAmount",
                "uncertaintyDistributionType",
                "relativeStandardDeviation95In",
                "generalComment",
                "other",
            }
            opaque = [deepcopy(child) for child in element if etree.QName(child).localname not in mapped_children]
            other = deepcopy(element.find(q("c", "other")))
            attributes = {
                k: v
                for k, v in element.attrib.items()
                if etree.QName(k).namespace not in {None, NS["olca"]}
                and k not in {q("bp", "formula"), q("bp", "wasteSourceFormula")}
            }
            element = etree.SubElement(exchanges, q("p", "exchange"), attributes, dataSetInternalID=str(number))
            # Resolve the actual flow path; exact versions may use versioned filenames.
            matching = [
                (path, flow)
                for (k, uid, ver), (path, flow) in reference_package.entities.items()
                if k == "Flow" and uid == reference["flow_id"] and ver == reference["flow_version"]
            ]
            if len(matching) != 1:
                fail("reference_missing", "Export requires exactly one supporting flow dataset per exchange.")
            flow_path, flow_root = matching[0]
            flow_info = _flow_info(reference_package, flow_path, flow_root)
            source_exchange = exchange
            factor = _exchange_unit_factor(reference, exchange["unit"], flow_info["unit"])
            if factor != 1:
                exchange = deepcopy(exchange)
                _scale_ilcd_exchange(exchange, factor)
                amount = _number(exchange["amount"])
            waste = flow_info["flow_type"] == "Waste flow"
            native_reference = (
                _native_technosphere_reference(
                    source_exchange, document.context.background, flow_info, reference.get("process_id", "")
                )
                if kind == "technosphere" and supplier is None
                else None
            )
            if waste and native_reference is None:
                exchange = deepcopy(exchange)
                _scale_ilcd_exchange(exchange, -1)
                amount = _number(exchange["amount"])
                if source_exchange.get("formula"):
                    # In flow-reference units, before the reversible sign change.
                    element.set(
                        q("bp", "wasteSourceFormula"),
                        source_exchange["formula"] if factor == 1 else f"({source_exchange['formula']}) * ({factor!r})",
                    )
            uri = posixpath.relpath(flow_path, "processes")
            _reference(
                element, "p", "referenceToFlowDataSet", "Flow", reference["flow_id"], reference["flow_version"], uri
            )
            natural = kind == "biosphere" and wire_categories(exchange.get("categories", ()))[:1] == (
                "natural resource",
            )
            direction = (
                "Input"
                if (kind == "technosphere" and not waste) or (kind == "production" and waste) or natural
                else "Output"
            )
            if native_reference is not None:
                direction = "Output" if native_reference.native_reference_is_input else "Input"
            _add(element, "p", "exchangeDirection", direction)
            formula = exchange.get("formula")
            if formula:
                if not math.isclose(evaluate(formula, values), amount, rel_tol=1e-9, abs_tol=1e-12):
                    fail("formula_result_conflict", "Exchange formula disagrees with its cached amount.")
                variable_name = f"_bp_exchange_{number}"
                while variable_name in values:
                    variable_name += "_"
                if relations is None:
                    relations = _add(info, "p", "mathematicalRelations")
                variable = _add(relations, "p", "variableParameter", name=variable_name)
                variable.set(q("bp", "generated"), "true")
                _add(variable, "p", "formula", formula)
                _add(variable, "p", "meanValue", repr(amount))
                _add(element, "p", "referenceToVariable", variable_name)
                element.set(q("bp", "formula"), str(formula))
            _add(element, "p", "meanAmount", "1" if formula else repr(amount))
            _add(element, "p", "resultingAmount", repr(amount))
            if (
                source_exchange.get("uncertainty type") in {2, 3}
                and _number(source_exchange.get("scale")) > 0
                and _number(exchange.get("scale")) <= 0
            ):
                fail(
                    "uncertainty_precision_unsupported",
                    "Native quantity conversion cannot replace positive spread with zero.",
                )
            rounded = _write_uncertainty(element, exchange, dialect=dialect, uncertainty_relative_tolerance=tolerance)
            if rounded is not None:
                original_scale = _number(source_exchange["scale"])
                exported_scale = rounded["native_exported_scale"]
                if exchange["uncertainty type"] == 3 and factor != 1:
                    exported_scale *= abs(1 / factor)
                relative_error = abs(exported_scale - original_scale) / original_scale
                if exported_scale <= 0 or relative_error > tolerance:
                    fail(
                        "uncertainty_precision_unsupported",
                        "ILCD readback spread exceeds the canonical-unit tolerance.",
                    )
                row = {
                    "code": "ilcd.uncertainty_rounded",
                    "dataset_code": dataset["code"],
                    "exchange_index": exchange_index,
                    "original_uncertainty": {
                        key: deepcopy(source_exchange[key])
                        for key in ("uncertainty type", "loc", "scale", "shape", "minimum", "maximum", "negative")
                        if key in source_exchange
                    },
                    "original_amount": source_exchange["amount"],
                    "original_unit": source_exchange["unit"],
                    "original_scale": original_scale,
                    "exported_scale": exported_scale,
                    **rounded,
                    "native_amount": amount,
                    "native_unit": flow_info["unit"],
                    "relative_error": relative_error,
                    "tolerance": tolerance,
                    "dialect": dialect,
                }
                dataset_rounding.append(row)
                rounding_warnings.append(row)
            if exchange.get("comment"):
                comment = str(exchange["comment"])
                if len(comment) > 500:
                    long_comments[str(number)] = comment
                    comment = _comment_pointer(str(number), comment)
                _local(element, "p", "generalComment", comment)
            if other is not None:
                element.append(other)
            element.extend(opaque)
            if dialect == "openlca":
                # A complete openLCA ExchangeExtension overrides the standard
                # ILCD uncertainty conversion in openLCA 2.6. The partial amount
                # written for an independent centre deliberately has no unitId
                # or propertyId: the nominal amount and expression stay in core
                # elements, and UncertaintyConverter reads the separate centre.
                if provider:
                    element.set(q("olca", "defaultProvider"), uuid_text(provider))
            _set_payload(
                element, "exchange", {k: v for k, v in exchange.items() if k not in _MAPPED_EXCHANGE or k == "code"}
            )
            if kind != "production":
                _set_payload(
                    element,
                    "link",
                    {
                        "identity": {
                            k: source_exchange[k]
                            for k in ("name", "reference product", "location", "unit", "categories")
                            if k in source_exchange
                        },
                        "context": _context_dict(document.context.background),
                        "flow_id": reference["flow_id"],
                        "flow_version": reference["flow_version"],
                        "process_id": provider,
                        "amount_factor": factor,
                        "reference_unit": flow_info["unit"],
                        "source_formula": source_exchange.get("formula", ""),
                    },
                )
            order = [
                "referenceToFlowDataSet",
                "location",
                "functionType",
                "exchangeDirection",
                "referenceToVariable",
                "meanAmount",
                "resultingAmount",
                "minimumAmount",
                "maximumAmount",
                "uncertaintyDistributionType",
                "relativeStandardDeviation95In",
                "allocations",
                "dataSourceType",
                "referencesToDataSource",
                "generalComment",
                "other",
            ]
            element[:] = sorted(
                element,
                key=lambda child: (
                    order.index(etree.QName(child).localname) if etree.QName(child).localname in order else len(order)
                ),
            )
        imported = dataset.get("ilcd", {})
        retained_rounding = (
            imported.get("uncertainty_rounding_warnings", [])
            if record["same_context"] and imported.get("fingerprint") == _fingerprint(dataset)
            else []
        )
        audit_rounding = [*retained_rounding, *dataset_rounding]
        if long_comments or audit_rounding:
            # The process-level FTMultiLang field is unlimited and becomes the
            # native openLCA process description. Keep every qualification there,
            # with authenticated exchange pointers, rather than truncate prose.
            complete = dataset.get("comment", "")
            if long_comments:
                complete += (
                    ("\n\n" if complete else "")
                    + _COMMENT_BLOCK_START
                    + json.dumps(long_comments, ensure_ascii=False, indent=2)
                    + _COMMENT_BLOCK_END
                )
            if audit_rounding:
                complete += (
                    ("\n\n" if complete else "")
                    + _ROUNDING_BLOCK_START
                    + json.dumps(audit_rounding, ensure_ascii=False, indent=2, allow_nan=False)
                    + _ROUNDING_BLOCK_END
                )
            _set_local(info.find("p:dataSetInformation", NS), "c", "generalComment", complete)
        root.set(q("bp", "dialect"), dialect)
        _set_payload(root, "dataset", {k: v for k, v in dataset.items() if k not in _MAPPED_DATASET})
        if relations is not None:
            model_order = ["modelDescription", "variableParameter", "other"]
            relations[:] = sorted(
                relations,
                key=lambda child: (
                    model_order.index(etree.QName(child).localname)
                    if etree.QName(child).localname in model_order
                    else len(model_order)
                ),
            )
        info_order = [
            "dataSetInformation",
            "quantitativeReference",
            "time",
            "geography",
            "technology",
            "mathematicalRelations",
            "other",
        ]
        info[:] = sorted(
            info,
            key=lambda child: (
                info_order.index(etree.QName(child).localname)
                if etree.QName(child).localname in info_order
                else len(info_order)
            ),
        )
        root_order = [
            "processInformation",
            "modellingAndValidation",
            "administrativeInformation",
            "exchanges",
            "LCIAResults",
            "other",
        ]
        root[:] = sorted(
            root,
            key=lambda child: (
                root_order.index(etree.QName(child).localname)
                if etree.QName(child).localname in root_order
                else len(root_order)
            ),
        )
        # This conventional resource is generated only for newly created packages.
        if not root.get("locations"):
            root.set("locations", "../ILCDLocations.xml")
        location_path = posixpath.normpath(posixpath.join("processes", root.get("locations", "")))
        if location_path.startswith("../") or location_path.startswith("/") or ":" in location_path:
            fail("reference_uri_unsafe", "The process locations table must be local to the package.")
        locations_namespace = BASE + "Locations"
        locations = (
            parse_xml(resources[location_path].encode())
            if location_path in resources
            else etree.Element(f"{{{locations_namespace}}}ILCDLocations", nsmap={None: locations_namespace})
        )
        existing_codes = {e.get("value") for e in locations}
        if dataset["location"] not in existing_codes:
            etree.SubElement(locations, f"{{{locations_namespace}}}location", value=dataset["location"]).text = dataset[
                "location"
            ]
        resources[location_path] = xml_bytes(locations).decode()
        _merge(resources, {f"processes/{record['process_id']}.xml": xml_bytes(root).decode()})
    package = ILCDPackage.from_resources(resources)
    # Check every exported process reference without requiring external provider
    # inventories; suppliers outside the foreground are explicit references.
    for (kind, _, _), (path, root) in package.entities.items():
        if kind == "Process":
            package.closure(path, root)
    rendered = {path: content.encode() for path, content in sorted(resources.items())}
    if uncertainty_rounding_warnings is not None:
        uncertainty_rounding_warnings.extend(deepcopy(rounding_warnings))
    return rendered


def _dispersion_field(scale: float, dispersion: float, *, logarithmic: bool, dialect: str, tolerance: float):
    rounded = round(dispersion, 3)
    divisor = 1 if dialect == "openlca" else 2
    readback = (math.log(rounded) if logarithmic else rounded) / divisor
    # Check this before isclose: exp(tiny positive sigma) can equal 1.0, and
    # rounding a positive normal SD can produce zero. Neither is acceptable.
    if scale > 0 and readback <= 0:
        fail(
            "uncertainty_precision_unsupported", "ILCD rounding cannot replace a positive uncertainty spread with zero."
        )
    exact = math.isclose(dispersion, rounded, rel_tol=1e-12, abs_tol=1e-12 if logarithmic else 0)
    if exact:
        return rounded, None
    if tolerance <= 0:
        fail(
            "uncertainty_precision_unsupported",
            (
                "ILCD's three-decimal uncertainty field cannot represent this log-normal dispersion exactly."
                if logarithmic
                else "ILCD cannot represent this normal dispersion exactly."
            ),
        )
    # The renderer checks the actual readback against the original canonical
    # spread after undoing quantity conversion. Checking a second ratio here
    # can incorrectly reject its exact boundary through floating-point drift.
    return rounded, {
        "native_original_scale": scale,
        "native_exported_scale": readback,
        "native_original_dispersion": dispersion,
        "native_rounded_dispersion": rounded,
    }


def _write_uncertainty(
    element, exchange: Mapping, *, dialect: str = "core", uncertainty_relative_tolerance: float = 0.0
) -> dict | None:
    uncertainty = exchange.get("uncertainty type", 0)
    if uncertainty in {None, 0, 1}:
        return
    if uncertainty == 4:
        lower, upper = _number(exchange.get("minimum")), _number(exchange.get("maximum"))
        if lower > upper:
            fail("uncertainty_invalid", "Uniform uncertainty bounds are reversed.")
        _add(element, "p", "minimumAmount", repr(lower))
        _add(element, "p", "maximumAmount", repr(upper))
        _add(element, "p", "uncertaintyDistributionType", "uniform")
    elif uncertainty == 5 and dialect == "openlca":
        minimum, mode, maximum = _triangular_parameters(exchange)
        _add(element, "p", "minimumAmount", repr(minimum))
        _add(element, "p", "maximumAmount", repr(maximum))
        _add(element, "p", "uncertaintyDistributionType", "triangular")
        # Native openLCA's uncertainty converter reads this independently of
        # amount/unitId/propertyId. A full extension bypasses that converter.
        element.set(q("olca", "mostLikelyValue"), repr(mode))
    elif uncertainty == 3:
        amount, scale = _number(exchange["amount"]), _number(exchange.get("scale"))
        centre = _number(exchange.get("loc", amount))
        if scale < 0 or (dialect != "openlca" and centre != amount):
            fail("uncertainty_unsupported", "Normal uncertainty requires a nonnegative scale and a matching loc.")
        if dialect == "openlca" and centre != amount:
            element.set(q("olca", "amount"), repr(centre))
        _add(element, "p", "uncertaintyDistributionType", "normal")
        dispersion = scale if dialect == "openlca" else 2 * scale
        rounded, warning = _dispersion_field(
            scale, dispersion, logarithmic=False, dialect=dialect, tolerance=uncertainty_relative_tolerance
        )
        _add(element, "p", "relativeStandardDeviation95In", str(rounded))
        return warning
    elif uncertainty == 2:
        amount, scale = _number(exchange["amount"]), _number(exchange.get("scale"))
        loc = _number(exchange.get("loc"))
        negative = bool(exchange.get("negative", amount < 0))
        if dialect == "openlca":
            if amount == 0 or scale < 0 or negative != (amount < 0):
                fail(
                    "uncertainty_unsupported",
                    "Native log-normal uncertainty requires a nonzero nominal amount, a matching negative flag, "
                    "and nonnegative scale.",
                )
        elif (
            amount <= 0 or scale < 0 or negative or not math.isclose(loc, math.log(amount), rel_tol=1e-9, abs_tol=1e-12)
        ):
            fail(
                "uncertainty_unsupported",
                "Log-normal uncertainty requires positive amount, log(amount) loc, and nonnegative scale.",
            )
        _add(element, "p", "uncertaintyDistributionType", "log-normal")
        try:
            squared = math.exp(scale if dialect == "openlca" else 2 * scale)
            rounded, warning = _dispersion_field(
                scale, squared, logarithmic=True, dialect=dialect, tolerance=uncertainty_relative_tolerance
            )
            if dialect == "openlca":
                # Match the installed native sampler, whose parameter1 is an
                # arithmetic mean, using the ACTUAL exported sigma. Otherwise
                # either its mean correction or permitted SD rounding changes
                # canonical loc. This partial extension leaves nominal amount
                # and its formula in core ILCD and keeps uncertainty conversion
                # active; adding unitId/propertyId would bypass that converter.
                exported_sigma = math.log(rounded)
                centre = math.exp(loc + 0.5 * exported_sigma**2)
                if not math.isfinite(centre) or centre <= 0:
                    fail("uncertainty_invalid", "Log-normal centre must be finite and strictly positive.")
                # Native sampling applies the sign after sampling log(abs(mean)).
                element.set(q("olca", "amount"), repr(-centre if negative else centre))
            _add(element, "p", "relativeStandardDeviation95In", str(rounded))
            return warning
        except OverflowError:
            fail("uncertainty_invalid", "Log-normal dispersion or native centre is too large.")
    else:
        fail("uncertainty_unsupported", f"Unsupported uncertainty type: {uncertainty}")


def write_ilcd(
    document: InventoryDocument,
    destination: str | Path,
    *,
    package_mode: str | None = None,
    uncertainty_relative_tolerance: float = 0.0,
    uncertainty_rounding_warnings: list | None = None,
    **kwargs,
) -> Path:
    """Validate in memory, then atomically publish a ZIP or new package directory."""
    destination = Path(destination)
    _rounding_options(uncertainty_relative_tolerance, uncertainty_rounding_warnings)
    pending_warnings = []
    mode = package_mode or ("zip" if destination.suffix.lower() == ".zip" else "directory")
    rendered = render_ilcd_package(
        document,
        package_mode=mode,
        uncertainty_relative_tolerance=uncertainty_relative_tolerance,
        uncertainty_rounding_warnings=pending_warnings,
        **kwargs,
    )
    if not destination.parent.is_dir():
        fail("destination_invalid", "The destination parent directory must exist.")
    with tempfile.TemporaryDirectory(prefix="brightpath-ilcd-", dir=destination.parent) as temporary:
        temp = Path(temporary)
        if mode == "zip":
            staged = temp / "inventory.zip"
            with zipfile.ZipFile(staged, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                for name, content in rendered.items():
                    info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
                    info.compress_type = zipfile.ZIP_DEFLATED
                    info.external_attr = 0o100644 << 16
                    archive.writestr(info, content)
            errors = validate_ilcd_package(staged)
            if errors:
                fail("package_invalid", "; ".join(errors))
            os.replace(staged, destination)
        else:
            if destination.exists():
                fail("destination_exists", "ILCD directory export requires a new destination directory.")
            staged = temp / "package"
            staged.mkdir()
            for name, content in rendered.items():
                path = staged / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(content)
            shutil.move(str(staged), str(destination))
    if uncertainty_rounding_warnings is not None:
        uncertainty_rounding_warnings.extend(deepcopy(pending_warnings))
    return destination


def validate_ilcd_xml(source: str | Path | bytes) -> tuple[str, ...]:
    """Return XSD findings for one supported XML entity without mutating it."""
    try:
        contents = source if isinstance(source, bytes) else Path(source).read_bytes()
        validate_xml(parse_xml(contents))
        return ()
    except (OSError, ValueError, etree.Error) as error:
        return (str(error),)


def validate_ilcd_package(source: str | Path) -> tuple[str, ...]:
    """Return schema and local-reference findings for an ILCD package."""
    try:
        package = load_package(source)
        processes = [(path, root) for (kind, _, _), (path, root) in package.entities.items() if kind == "Process"]
        if not processes:
            fail("process_missing", "No process dataset was found.")
        for path, root in processes:
            package.closure(path, root)
        return ()
    except (OSError, ValueError, etree.Error) as error:
        return (str(error),)
