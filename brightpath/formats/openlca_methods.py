"""Link ecoinvent elementary flows to version-matched local openLCA methods."""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
import math
import re
import uuid
import zipfile
from collections.abc import Mapping
from copy import deepcopy
from functools import lru_cache
from pathlib import Path

from brightpath import DATA_DIR
from brightpath.exceptions import SerializationError
from brightpath.formats.openlca_references import OpenLCABiosphereReference

_REFERENCE_DIRECTORY = DATA_DIR / "export" / "openlca_references"
_LEGACY_NEXUS_VERSIONS = frozenset({"3.8", "3.9", "3.9.1", "3.10", "3.11"})
_LEGACY_NEXUS_UUID_MAP = _REFERENCE_DIRECTORY / "ecoinvent_legacy_nexus_flow_uuids.json"

# Nexus 3.13 groups these primary production factors under Economic/unspecified.
_ECONOMIC_313_IDS = frozenset(
    {
        "21dcb31a-fddb-464d-8d0d-4ce2738b2cb7",  # Rent
        "9ca5a400-6313-491f-a72a-8f36d8230944",  # Net tax
        "a6e4fb65-8dcf-4126-9561-0b51a16fae77",  # Expenditures on primary production factors
        "acb9b748-3464-4c0d-9055-639c420e4dc3",  # Net operating surplus
        "df1b2a34-1788-4555-b991-f8ff67c66998",  # Labour cost
    }
)

_UNITS = {
    "kilogram": "kg",
    "cubic meter": "m3",
    "cubic meter-year": "m3*a",
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


@lru_cache(maxsize=1)
def _legacy_nexus_uuid_map() -> Mapping[str, str]:
    return json.loads(_LEGACY_NEXUS_UUID_MAP.read_text(encoding="utf-8"))


@lru_cache(maxsize=6)
def _bundled_method_references(version: str) -> dict:
    manifest = json.loads((_REFERENCE_DIRECTORY / "ecoinvent_methods_manifest.json").read_text(encoding="utf-8"))
    resource = manifest["versions"].get(version)
    if resource is None:
        supported = ", ".join(manifest["versions"])
        raise ValueError(
            f"Brightpath has no bundled openLCA method references for ecoinvent {version}. "
            f"Bundled versions are {supported}."
        )
    raw = (_REFERENCE_DIRECTORY / resource["file"]).read_bytes()
    if hashlib.sha256(raw).hexdigest() != resource["sha256"]:
        raise ValueError(f"Bundled openLCA method references for ecoinvent {version} do not match their manifest.")
    data = json.loads(gzip.decompress(raw))
    if data.get("schema_version") != 1 or data.get("biosphere_version") != version:
        raise ValueError(f"Invalid bundled openLCA method references for ecoinvent {version}.")
    return data


def _read_source_rows(path: str | Path) -> list[list[str]]:
    with Path(path).open(encoding="utf-8-sig", newline="") as stream:
        delimiters = []
        for delimiter in (",", ";"):
            stream.seek(0)
            if len(next(csv.reader(stream, delimiter=delimiter), [])) == 5:
                delimiters.append(delimiter)
        if len(delimiters) != 1:
            raise ValueError("Biosphere CSV must have five comma- or semicolon-separated columns.")
        stream.seek(0)
        return list(csv.reader(stream, delimiter=delimiters[0]))


class OpenLCAMethodMapping:
    """Link an exact ecoinvent biosphere to an openLCA method package.

    Supply only ``biosphere_version`` to use Brightpath's bundled references.
    An optional local ``package`` overrides the target definitions. An optional
    ``biosphere_csv`` overrides the source identities. Local files can also be
    paired for versions outside the bundled reference set.

    The CSV has no header and contains name, compartment, subcompartment, unit,
    and UUID (the Premise biosphere CSV layout), separated by commas or semicolons.
    The caller declares the exact version of the CSV and method package via
    ``biosphere_version``; versions are not inferred from filenames or flow metadata.
    The default remains 3.12 for compatibility. Directory and ZIP
    packages are supported. Import the original method package into openLCA
    before the inventory: characterized flows are exported as external references.

    The bundled legacy Nexus UUID map is applied for supported releases when
    both endpoints exist and no direct match takes precedence. Pass an explicit
    ``flow_uuid_map`` of ecoinvent to openLCA UUIDs to replace the bundled map.
    Use an empty mapping for direct matches only. Remapped flows undergo the
    same validation as direct matches.

    ``conflict_policy="error"`` rejects incompatible package metadata. Explicit
    ``"preserve"`` keeps conflicting source flows under deterministic separate IDs
    without characterization and records both identities in the coverage report.
    It does not relax contradictory source UUID or malformed quantity checks.
    """

    def __init__(
        self,
        package: str | Path | None = None,
        biosphere_csv: str | Path | None = None,
        *,
        biosphere_version: str = "3.12",
        conflict_policy: str | None = None,
        flow_uuid_map: Mapping[str, str] | None = None,
    ):
        if not isinstance(biosphere_version, str) or not re.fullmatch(
            r"3\.(?:[5-9]|1[0-3])(?:\.[0-9]+)?", biosphere_version
        ):
            raise ValueError("Specify an exact ecoinvent biosphere version from 3.5 through 3.13.")
        if conflict_policy is None:
            conflict_policy = "preserve" if biosphere_version == "3.8" and biosphere_csv is None else "error"
        if conflict_policy not in {"error", "preserve"}:
            raise ValueError("conflict_policy must be error or preserve.")
        self.conflict_policy = conflict_policy
        self._conflicts = {}
        self._export_source_codes = {}
        self.biosphere_version = biosphere_version
        self._source = {}
        self._identities = {}
        bundled = None
        if package is None or biosphere_csv is None:
            bundled_version = "3.9.1" if biosphere_version == "3.9" else biosphere_version
            bundled = _bundled_method_references(bundled_version)
        source_rows = bundled["source_rows"] if biosphere_csv is None else _read_source_rows(biosphere_csv)
        for row in source_rows:
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
        if package is None:
            self.source = bundled["method_package"]["filename"]
            self.method_package_sha256 = bundled["method_package"]["sha256"]
            entities = bundled["entities"]
        else:
            self.source = str(Path(package).expanduser().resolve())
            self.method_package_sha256 = None
            entities = self._read_entities(Path(self.source))
        package_flows = entities["flows"]
        if flow_uuid_map is None and biosphere_version in _LEGACY_NEXUS_VERSIONS:
            flow_uuid_map = {
                source_id: package_id
                for source_id, package_id in _legacy_nexus_uuid_map().items()
                if source_id not in package_flows and package_id not in self._source
            }
        package_sources = self._remap_sources(flow_uuid_map, package_flows)
        self._references = {}
        self._remapped_source_flows = 0
        for package_id, flow in package_flows.items():
            code = package_sources.get(package_id, package_id)
            if code not in self._source:
                continue
            name, categories, unit = self._source[code]
            if flow.get("flowType") != "ELEMENTARY_FLOW":
                raise ValueError(f"Flow {package_id} is not an elementary flow.")
            # The first category segment in ecoinvent's method package is
            # "Elementary flows"; compare normalized compartment labels below it.
            category = flow.get("category", "")
            if isinstance(category, dict):
                parts = [*category.get("categoryPath", []), category.get("name", "")]
            elif isinstance(category, str):
                parts = category.split("/")
            else:
                raise ValueError(f"Flow {package_id} has an invalid category.")
            if parts and parts[0] == "Elementary flows":
                parts = parts[1:]
            economic_313 = (
                biosphere_version == "3.13"
                and code == package_id
                and code in _ECONOMIC_313_IDS
                and categories == ("economic", "primary production factor")
                and parts == ["Economic", "unspecified"]
                and unit == "EUR2005"
            )
            category_matches = economic_313 or _categories(categories) == _categories(parts)
            if name != flow.get("name") or not category_matches:
                if self.conflict_policy == "error":
                    raise ValueError(f"Flow {package_id} has an incompatible name or compartment.")
                self._preserve_conflict(code, flow, parts, "name_or_compartment_mismatch")
                continue
            factors = [
                f
                for f in flow.get("flowProperties", [])
                if _reference_flag(f, "isRefFlowProperty", "referenceFlowProperty")
            ]
            if len(factors) != 1 or factors[0].get("conversionFactor") != 1:
                raise ValueError(f"Flow {package_id} must have exactly one reference quantity with factor 1.")
            try:
                prop = entities["flow_properties"][factors[0]["flowProperty"]["@id"]]
                group = entities["unit_groups"][prop["unitGroup"]["@id"]]
            except KeyError as error:
                raise ValueError(f"Flow {package_id} has a missing quantity or unit group.") from error
            units = [u for u in group.get("units", []) if _reference_flag(u, "isRefUnit", "referenceUnit")]
            if len(units) != 1 or units[0].get("conversionFactor") != 1:
                raise ValueError(f"Flow {package_id} must have exactly one reference unit with factor 1.")
            reference_unit = units[0]
            target_unit = reference_unit
            expected = _UNITS.get(unit, unit)
            if code in _STANDARD_GAS_IDS and unit == "standard cubic meter":
                expected = "m3"
            if economic_313 and prop.get("name") == "Market value" and reference_unit.get("name") == "EUR 2024":
                expected = "EUR 2005"
                matching_units = [candidate for candidate in group["units"] if candidate.get("name") == expected]
                if len(matching_units) == 1:
                    factor = matching_units[0].get("conversionFactor")
                    if type(factor) in (int, float) and math.isfinite(factor) and factor > 0:
                        target_unit = matching_units[0]
            if expected != target_unit.get("name"):
                if self.conflict_policy == "error":
                    raise ValueError(
                        f"Flow {package_id} has incompatible units: {unit!r} / {target_unit.get('name')!r}."
                    )
                self._preserve_conflict(code, flow, parts, "unit_mismatch", target_unit.get("name"))
                continue
            self._references[code] = OpenLCABiosphereReference(
                flow_id=package_id,
                flow_name=name,
                flow_property_id=_uuid(prop["@id"]),
                flow_property_name=prop["name"],
                unit_id=_uuid(target_unit["@id"]),
                unit_name=target_unit["name"],
                reference_unit_name=reference_unit["name"] if target_unit is not reference_unit else None,
            )
            if code != package_id:
                self._remapped_source_flows += 1
                self._export_source_codes[package_id] = code
        if not self._references and not self._conflicts:
            raise ValueError("The method package has no UUID matches with the biosphere CSV.")

    def _remap_sources(
        self, flow_uuid_map: Mapping[str, str] | None, package_flows: Mapping[str, object]
    ) -> dict[str, str]:
        """Index applicable pairs by package UUID without overriding direct matches."""
        if flow_uuid_map is None:
            return {}
        if not isinstance(flow_uuid_map, Mapping):
            raise TypeError("flow_uuid_map must map ecoinvent source UUIDs to openLCA package UUIDs.")
        seen_sources, seen_targets = set(), set()
        package_sources = {}
        for source, target in flow_uuid_map.items():
            source_id, package_id = _uuid(source), _uuid(target)
            if source_id in seen_sources or package_id in seen_targets:
                raise ValueError("flow_uuid_map must be one-to-one.")
            seen_sources.add(source_id)
            seen_targets.add(package_id)
            if source_id == package_id or source_id not in self._source or package_id not in package_flows:
                continue
            if source_id in package_flows or package_id in self._source:
                raise ValueError(f"UUID remap {source_id} to {package_id} conflicts with a direct UUID match.")
            package_sources[package_id] = source_id
        return package_sources

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
            "package_uuid": _uuid(flow["@id"]),
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
        reference = self._references.get(code)
        return (reference.flow_id, reference) if reference else (code, None)

    def audit(self, document):
        """Return JSON-serializable coverage for this inventory, without mutation."""
        self.check_context(document.context)
        missing = {}
        matched = set()
        remapped = {}
        count = 0
        for dataset in document.data:
            for exchange in dataset.get("exchanges", []):
                if exchange.get("type") != "biosphere":
                    continue
                count += 1
                code, reference = self.resolve(exchange)
                if reference:
                    matched.add(code)
                    source_code = self._export_source_codes.get(code)
                    if source_code is not None:
                        remapped[source_code] = code
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
                **({"method_package_sha256": self.method_package_sha256} if self.method_package_sha256 else {}),
                "biosphere": {"family": "ecoinvent", "version": self.biosphere_version},
                "source_flows": len(self._source),
                "mapped_source_flows": len(self._references),
                "remapped_source_flows": self._remapped_source_flows,
                "source_flows_absent_from_package": len(self._source) - len(self._references) - len(self._conflicts),
                "conflict_policy": self.conflict_policy,
                "source_flows_conflicting_with_package": len(self._conflicts),
                "package_conflicts": list(self._conflicts.values()),
                "inventory_biosphere_exchanges": count,
                "inventory_mapped_flows": len(matched),
                "inventory_uuid_remaps": remapped,
                "inventory_unmapped_flows": list(missing.values()),
                "requires_method_package_import": True,
            }
        )
