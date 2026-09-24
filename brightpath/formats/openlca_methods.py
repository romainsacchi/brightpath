"""Local ecoinvent 3.12 method-package references; no licensed data are bundled."""

from __future__ import annotations

import csv
import json
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


def _uuid(value):
    try:
        return str(uuid.UUID(str(value)))
    except (ValueError, TypeError, AttributeError) as error:
        raise ValueError(f"Invalid elementary-flow UUID: {value!r}.") from error


class OpenLCAMethodMapping:
    """Match a local method package to an explicit ecoinvent 3.12 biosphere CSV.

    The CSV has no header and contains name, compartment, subcompartment, unit,
    and UUID (the Premise ``flows_biosphere_312.csv`` layout). Directory and ZIP
    packages are supported. Import the original method package into openLCA
    before the inventory: characterized flows are exported as external references.
    """

    def __init__(self, package: str | Path, biosphere_csv: str | Path):
        self.source = str(Path(package).expanduser().resolve())
        self._source = {}
        self._identities = {}
        with Path(biosphere_csv).open(encoding="utf-8-sig", newline="") as stream:
            for row in csv.reader(stream):
                if len(row) != 5:
                    raise ValueError("Biosphere CSV rows must contain exactly five columns.")
                name, compartment, subcompartment, unit, code = row
                code = _uuid(code)
                identity = (name, (compartment, subcompartment), unit)
                if code in self._source or identity in self._identities:
                    raise ValueError(f"Duplicate biosphere UUID or identity: {row!r}.")
                self._source[code] = identity
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
            parts = str(flow.get("category", "")).split("/")
            if parts and parts[0] == "Elementary flows":
                parts = parts[1:]
            if name != flow.get("name") or _categories(categories) != _categories(parts):
                raise ValueError(f"Flow {code} has an incompatible name or compartment.")
            factors = [f for f in flow.get("flowProperties", []) if f.get("isRefFlowProperty")]
            if len(factors) != 1 or factors[0].get("conversionFactor") != 1:
                raise ValueError(f"Flow {code} must have exactly one reference quantity with factor 1.")
            try:
                prop = entities["flow_properties"][factors[0]["flowProperty"]["@id"]]
                group = entities["unit_groups"][prop["unitGroup"]["@id"]]
            except KeyError as error:
                raise ValueError(f"Flow {code} has a missing quantity or unit group.") from error
            units = [u for u in group.get("units", []) if u.get("isRefUnit")]
            if len(units) != 1 or units[0].get("conversionFactor") != 1:
                raise ValueError(f"Flow {code} must have exactly one reference unit with factor 1.")
            target_unit = units[0]
            expected = _UNITS.get(unit, unit)
            if code in _STANDARD_GAS_IDS and unit == "standard cubic meter":
                expected = "m3"
            if expected != target_unit.get("name"):
                raise ValueError(f"Flow {code} has incompatible units: {unit!r} / {target_unit.get('name')!r}.")
            self._references[code] = OpenLCABiosphereReference(
                flow_id=code,
                flow_name=name,
                flow_property_id=_uuid(prop["@id"]),
                flow_property_name=prop["name"],
                unit_id=_uuid(target_unit["@id"]),
                unit_name=target_unit["name"],
            )
        if not self._references:
            raise ValueError("The method package has no UUID matches with the biosphere CSV.")

    @staticmethod
    def _read_entities(path):
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
        if (profile.family, profile.version) != ("ecoinvent", "3.12"):
            raise SerializationError("This method mapping requires an explicit ecoinvent 3.12 biosphere context.")

    def resolve(self, exchange):
        """Return source UUID and reference, rejecting contradictory identifiers."""
        identity = _identity(exchange)
        code = self._identities.get(identity)
        supplied = (exchange.get("openlca flow") or {}).get("@id")
        link = exchange.get("input")
        if isinstance(link, (tuple, list)) and len(link) == 2:
            try:
                link_code = str(uuid.UUID(str(link[1])))
            except ValueError:
                link_code = None
            if link_code is not None:
                if supplied and supplied != link_code:
                    raise SerializationError("Conflicting elementary-flow input and openLCA UUIDs.")
                supplied = link_code
        if supplied:
            supplied = _uuid(supplied)
            if (code and code != supplied) or (supplied in self._source and self._source[supplied] != identity):
                raise SerializationError(f"Elementary flow {identity!r} conflicts with UUID {supplied}.")
            code = supplied
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
                    missing[(code, identity)] = {
                        "uuid": code,
                        "name": identity[0],
                        "categories": list(identity[1]),
                        "unit": identity[2],
                        "reason": "absent_from_method_package" if code in self._source else "unknown_source_flow",
                    }
        return deepcopy(
            {
                "source": self.source,
                "biosphere": {"family": "ecoinvent", "version": "3.12"},
                "source_flows": len(self._source),
                "mapped_source_flows": len(self._references),
                "source_flows_absent_from_package": len(self._source) - len(self._references),
                "inventory_biosphere_exchanges": count,
                "inventory_mapped_flows": len(matched),
                "inventory_unmapped_flows": list(missing.values()),
                "requires_method_package_import": True,
            }
        )
