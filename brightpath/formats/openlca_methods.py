"""Local version-specific ecoinvent method-package references; no licensed data are bundled."""

from __future__ import annotations

import csv
import json
import re
import uuid
import zipfile
from copy import deepcopy
from pathlib import Path

from brightpath.exceptions import SerializationError
from brightpath.formats.openlca_references import OpenLCABiosphereReference

_UNITS = {
    "kilogram": "kg",
    "cubic meter": "m3",
    "square meter": "m2",
    "square meter-year": "m2*a",
    "megajoule": "MJ",
    "kilo Becquerel": "kBq",
    "mole": "mol",
}
# These two source UUIDs express gas volumes under standard conditions. This is
# an ecoinvent label correspondence, not a general physical m3 conversion.
_STANDARD_GAS_IDS = {
    "7c337428-fb1b-45c7-bbb2-2ee4d29e17ba",
    "3ed5f377-344f-423a-b5ec-9a9a1162b944",
}
_COMPARTMENTS = {
    "Emission to air": "air",
    "Emission to water": "water",
    "Emission to soil": "soil",
    "Resource": "natural resource",
    "Inventory indicator": "inventory indicator",
    "non-urban air or from high stacks": "low population density",
    "urban air close to ground": "high population density",
    "ground-": "ground water",
    "ground-, long-term": "ground water, long-term",
}


def _categories(values):
    return tuple(_COMPARTMENTS.get(value, value) for value in values)


def _identity(exchange):
    return (exchange.get("name"), tuple(exchange.get("categories") or ()), exchange.get("unit"))


def _source_identity(identity, code):
    name, categories, unit = identity
    if code in _STANDARD_GAS_IDS and unit == "Sm3":
        unit = "standard cubic meter"
    return name, categories, unit


def _uuid(value):
    try:
        return str(uuid.UUID(str(value)))
    except (ValueError, TypeError, AttributeError) as error:
        raise ValueError(f"Invalid elementary-flow UUID: {value!r}.") from error


def _reference_flag(record, current, legacy):
    if current in record and legacy in record and record[current] != record[legacy]:
        raise ValueError(f"Conflicting {current} and {legacy} flags.")
    return record.get(current, record.get(legacy, False))


class OpenLCAMethodMapping:
    """Match a local method package to an explicit versioned ecoinvent biosphere CSV.

    The CSV has no header and contains name, compartment, subcompartment, unit,
    and UUID (the Premise biosphere CSV layout), separated by commas or semicolons.
    The caller declares the exact version of the CSV and method package via
    ``biosphere_version``; versions are not inferred from filenames or flow metadata.
    The default remains 3.12 for compatibility. Directory and ZIP
    packages are supported. Import the original method package into openLCA
    before the inventory: characterized flows are exported as external references.

    ``conflict_policy="error"`` rejects incompatible package metadata. Explicit
    ``"preserve"`` keeps conflicting source flows under deterministic separate IDs
    without characterization and records both identities in the coverage report.
    It does not relax contradictory source UUID or malformed quantity checks.
    """

    def __init__(
        self,
        package: str | Path,
        biosphere_csv: str | Path,
        *,
        biosphere_version: str = "3.12",
        conflict_policy: str = "error",
    ):
        if not isinstance(biosphere_version, str) or not re.fullmatch(
            r"3\.(?:[5-9]|1[0-2])(?:\.[0-9]+)?", biosphere_version
        ):
            raise ValueError("Specify an exact ecoinvent biosphere version from 3.5 through 3.12.")
        if conflict_policy not in {"error", "preserve"}:
            raise ValueError("conflict_policy must be error or preserve.")
        self.conflict_policy = conflict_policy
        self._conflicts = {}
        self._export_source_codes = {}
        self.biosphere_version = biosphere_version
        self.source = str(Path(package).expanduser().resolve())
        self._source = {}
        self._identities = {}
        with Path(biosphere_csv).open(encoding="utf-8-sig", newline="") as stream:
            delimiters = []
            for delimiter in (",", ";"):
                stream.seek(0)
                if len(next(csv.reader(stream, delimiter=delimiter), [])) == 5:
                    delimiters.append(delimiter)
            if len(delimiters) != 1:
                raise ValueError("Biosphere CSV must have five comma- or semicolon-separated columns.")
            stream.seek(0)
            for row in csv.reader(stream, delimiter=delimiters[0]):
                if len(row) != 5:
                    raise ValueError("Biosphere CSV rows must contain exactly five columns.")
                name, compartment, subcompartment, unit, code = row
                code = _uuid(code)
                identity = (name, (compartment, subcompartment), unit)
                normalized = _source_identity(identity, code)
                if (code in self._source and self._source[code] != normalized) or (
                    identity in self._identities and self._identities[identity] != code
                ):
                    raise ValueError(f"Conflicting biosphere UUID or identity: {row!r}.")
                self._source[code] = normalized
                self._identities[identity] = code
        if not self._source:
            raise ValueError("The biosphere CSV is empty.")
        entities = self._read_entities(Path(self.source))
        self._references = {}
        for code, flow in entities["flows"].items():
            if code not in self._source:
                continue
            name, categories, unit = self._source[code]
            if flow.get("flowType") != "ELEMENTARY_FLOW":
                raise ValueError(f"Flow {code} is not an elementary flow.")
            # The first category segment in ecoinvent's method package is
            # "Elementary flows"; compare normalized compartment labels below it.
            category = flow.get("category", "")
            if isinstance(category, dict):
                parts = [*category.get("categoryPath", []), category.get("name", "")]
            elif isinstance(category, str):
                parts = category.split("/")
            else:
                raise ValueError(f"Flow {code} has an invalid category.")
            if parts and parts[0] == "Elementary flows":
                parts = parts[1:]
            if name != flow.get("name") or _categories(categories) != _categories(parts):
                if self.conflict_policy == "error":
                    raise ValueError(f"Flow {code} has an incompatible name or compartment.")
                self._preserve_conflict(code, flow, parts, "name_or_compartment_mismatch")
                continue
            factors = [
                f
                for f in flow.get("flowProperties", [])
                if _reference_flag(f, "isRefFlowProperty", "referenceFlowProperty")
            ]
            if len(factors) != 1 or factors[0].get("conversionFactor") != 1:
                raise ValueError(f"Flow {code} must have exactly one reference quantity with factor 1.")
            try:
                prop = entities["flow_properties"][factors[0]["flowProperty"]["@id"]]
                group = entities["unit_groups"][prop["unitGroup"]["@id"]]
            except KeyError as error:
                raise ValueError(f"Flow {code} has a missing quantity or unit group.") from error
            units = [u for u in group.get("units", []) if _reference_flag(u, "isRefUnit", "referenceUnit")]
            if len(units) != 1 or units[0].get("conversionFactor") != 1:
                raise ValueError(f"Flow {code} must have exactly one reference unit with factor 1.")
            target_unit = units[0]
            expected = _UNITS.get(unit, unit)
            if code in _STANDARD_GAS_IDS and unit == "standard cubic meter":
                expected = "m3"
            if expected != target_unit.get("name"):
                if self.conflict_policy == "error":
                    raise ValueError(f"Flow {code} has incompatible units: {unit!r} / {target_unit.get('name')!r}.")
                self._preserve_conflict(code, flow, parts, "unit_mismatch", target_unit.get("name"))
                continue
            self._references[code] = OpenLCABiosphereReference(
                flow_id=code,
                flow_name=name,
                flow_property_id=_uuid(prop["@id"]),
                flow_property_name=prop["name"],
                unit_id=_uuid(target_unit["@id"]),
                unit_name=target_unit["name"],
            )
        if not self._references and not self._conflicts:
            raise ValueError("The method package has no UUID matches with the biosphere CSV.")

    def _preserve_conflict(self, code, flow, categories, reason, unit=None):
        identity = self._source[code]
        export_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                json.dumps(
                    ["brightpath-openlca-conflicting-biosphere-v1", self.biosphere_version, code, identity],
                    ensure_ascii=False,
                ),
            )
        )
        self._export_source_codes[export_id] = code
        self._conflicts[code] = {
            "source_uuid": code,
            "export_uuid": export_id,
            "source_name": identity[0],
            "source_categories": list(identity[1]),
            "source_unit": identity[2],
            "package_name": flow.get("name"),
            "package_categories": categories,
            "package_unit": unit,
            "reason": reason,
        }

    @staticmethod
    def _read_entities(path):
        if path.suffix.lower() == ".zolca":
            raise ValueError(
                "A .zolca database backup is not a JSON-LD package. Export it as JSON-LD from openLCA first."
            )
        result = {folder: {} for folder in ("flows", "flow_properties", "unit_groups")}

        def add(folder, content):
            value = json.loads(content)
            code = _uuid(value["@id"])
            if code in result[folder]:
                raise ValueError(f"Duplicate {folder} UUID: {code}.")
            result[folder][code] = value

        if path.is_dir():
            for folder in result:
                for item in sorted((path / folder).glob("*.json")):
                    add(folder, item.read_text(encoding="utf-8"))
        else:
            with zipfile.ZipFile(path) as archive:
                for item in archive.infolist():
                    parts = item.filename.split("/")
                    if len(parts) == 2 and parts[0] in result and parts[1].endswith(".json"):
                        add(parts[0], archive.read(item).decode("utf-8"))
        return result

    def check_context(self, context):
        """Require the exact biosphere version, independently of the technosphere."""
        profile = context.background.biosphere
        if (profile.family, profile.version) != ("ecoinvent", self.biosphere_version):
            raise SerializationError(
                f"This method mapping requires an explicit ecoinvent {self.biosphere_version} biosphere context."
            )

    def resolve(self, exchange):
        """Return export UUID and reference, rejecting contradictory source identifiers."""
        identity = _identity(exchange)
        code = self._identities.get(identity)
        supplied = (exchange.get("openlca flow") or {}).get("@id")
        if supplied:
            supplied = _uuid(supplied)
            supplied = self._export_source_codes.get(supplied, supplied)
        link = exchange.get("input")
        if isinstance(link, (tuple, list)) and len(link) == 2:
            try:
                link_code = str(uuid.UUID(str(link[1])))
                link_code = self._export_source_codes.get(link_code, link_code)
            except ValueError:
                link_code = None
            if link_code is not None:
                if supplied and supplied != link_code:
                    raise SerializationError("Conflicting elementary-flow input and openLCA UUIDs.")
                supplied = link_code
        if supplied:
            supplied = _uuid(supplied)
            if (code and code != supplied) or (
                supplied in self._source and self._source[supplied] != _source_identity(identity, supplied)
            ):
                raise SerializationError(f"Elementary flow {identity!r} conflicts with UUID {supplied}.")
            code = supplied
        if code in self._conflicts:
            return self._conflicts[code]["export_uuid"], None
        return code, self._references.get(code)

    def audit(self, document):
        """Return JSON-serializable coverage for this inventory, without mutation."""
        self.check_context(document.context)
        missing = {}
        matched = set()
        count = 0
        for dataset in document.data:
            for exchange in dataset.get("exchanges", []):
                if exchange.get("type") != "biosphere":
                    continue
                count += 1
                code, reference = self.resolve(exchange)
                if reference:
                    matched.add(code)
                else:
                    identity = _identity(exchange)
                    source_code = self._export_source_codes.get(code, code)
                    conflict = self._conflicts.get(source_code)
                    missing[(code, identity)] = {
                        "uuid": code,
                        "name": identity[0],
                        "categories": list(identity[1]),
                        "unit": identity[2],
                        "reason": (
                            "conflicting_method_package_flow"
                            if conflict
                            else "absent_from_method_package" if code in self._source else "unknown_source_flow"
                        ),
                        **({"conflict": deepcopy(conflict)} if conflict else {}),
                    }
        return deepcopy(
            {
                "source": self.source,
                "biosphere": {"family": "ecoinvent", "version": self.biosphere_version},
                "source_flows": len(self._source),
                "mapped_source_flows": len(self._references),
                "source_flows_absent_from_package": len(self._source) - len(self._references) - len(self._conflicts),
                "conflict_policy": self.conflict_policy,
                "source_flows_conflicting_with_package": len(self._conflicts),
                "package_conflicts": list(self._conflicts.values()),
                "inventory_biosphere_exchanges": count,
                "inventory_mapped_flows": len(matched),
                "inventory_unmapped_flows": list(missing.values()),
                "requires_method_package_import": True,
            }
        )
