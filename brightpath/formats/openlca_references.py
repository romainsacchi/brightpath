"""Exact target-database references for linked openLCA JSON-LD exports."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from brightpath import DATA_DIR
from brightpath.core.context import InventoryContext

TechnosphereIdentity = tuple[str, str, str, str]
BiosphereIdentity = tuple[str, tuple[str, ...], str]

_REFERENCE_DIRECTORY = DATA_DIR / "export" / "openlca_references"
_PACKAGED_CATALOGS = {
    ("uvek", "2025", "cutoff", "ecoinvent", "3.10"): "uvek__2025__cutoff__ecoinvent__3.10.json",
}


@dataclass(frozen=True)
class OpenLCATechnosphereReference:
    """Existing openLCA process, product-flow, quantity, unit, and location references."""

    process_id: str
    process_name: str
    flow_id: str
    flow_name: str
    flow_property_id: str
    flow_property_name: str
    unit_id: str
    unit_name: str
    location_id: str
    location: str


@dataclass(frozen=True)
class OpenLCABiosphereReference:
    """Existing characterized openLCA elementary-flow, quantity, and unit references."""

    flow_id: str
    flow_name: str
    flow_property_id: str
    flow_property_name: str
    unit_id: str
    unit_name: str


@dataclass(frozen=True)
class OpenLCAReferenceCatalog:
    """Exact reference lookup for one openLCA background database profile."""

    technosphere: dict[TechnosphereIdentity, OpenLCATechnosphereReference]
    biosphere: dict[BiosphereIdentity, OpenLCABiosphereReference]
    source: str


def load_openlca_reference_catalog(context: InventoryContext) -> OpenLCAReferenceCatalog | None:
    """Load packaged exact openLCA references for *context*, when available."""

    technosphere = context.background.technosphere
    biosphere = context.background.biosphere
    filename = _PACKAGED_CATALOGS.get(
        (
            technosphere.family,
            technosphere.version,
            technosphere.system_model,
            biosphere.family,
            biosphere.version,
        )
    )
    return _load_catalog(_REFERENCE_DIRECTORY / filename) if filename else None


@lru_cache(maxsize=None)
def _load_catalog(path: Path) -> OpenLCAReferenceCatalog:
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"Could not load openLCA reference catalog {path}: {error}") from error
    if payload.get("schema_version") != 1 or payload.get("format") != "openlca_jsonld":
        raise ValueError(f"Unsupported openLCA reference catalog schema in {path}.")
    manifest_resource = _manifest_resource(path)
    if manifest_resource.get("size") != len(raw) or manifest_resource.get("sha256") != hashlib.sha256(raw).hexdigest():
        raise ValueError(f"OpenLCA reference catalog {path} does not match its integrity manifest.")
    if payload.get("profile") != manifest_resource.get("profile"):
        raise ValueError(f"OpenLCA reference catalog {path} has a profile that conflicts with its manifest.")
    coverage = payload.get("coverage")
    if not isinstance(coverage, dict):
        raise ValueError(f"OpenLCA reference catalog {path} has invalid coverage metadata.")
    for field in ("technosphere_references", "biosphere_references", "missing_biosphere_references"):
        if coverage.get(field) != manifest_resource.get(field):
            raise ValueError(
                f"OpenLCA reference catalog {path} has {field!r} metadata that conflicts with its manifest."
            )

    technosphere: dict[TechnosphereIdentity, OpenLCATechnosphereReference] = {}
    for index, row in enumerate(payload.get("technosphere", [])):
        label = f"{path} technosphere row {index}"
        identity = (
            _text(row, "name", label),
            _text(row, "reference_product", label),
            _text(row, "location", label),
            _text(row, "unit", label),
        )
        if identity in technosphere:
            raise ValueError(f"Duplicate technosphere identity in {label}: {identity!r}.")
        technosphere[identity] = OpenLCATechnosphereReference(
            process_id=_uuid(row, "process_id", label),
            process_name=_text(row, "process_name", label),
            flow_id=_uuid(row, "flow_id", label),
            flow_name=_text(row, "flow_name", label),
            flow_property_id=_uuid(row, "flow_property_id", label),
            flow_property_name=_text(row, "flow_property_name", label),
            unit_id=_uuid(row, "unit_id", label),
            unit_name=_text(row, "unit_name", label),
            location_id=_uuid(row, "location_id", label),
            location=identity[2],
        )

    biosphere: dict[BiosphereIdentity, OpenLCABiosphereReference] = {}
    for index, row in enumerate(payload.get("biosphere", [])):
        label = f"{path} biosphere row {index}"
        categories = row.get("categories")
        if (
            not isinstance(categories, list)
            or not categories
            or not all(isinstance(value, str) and value for value in categories)
        ):
            raise ValueError(f"{label} has invalid categories.")
        identity = (
            _text(row, "name", label),
            tuple(categories),
            _text(row, "unit", label),
        )
        if identity in biosphere:
            raise ValueError(f"Duplicate biosphere identity in {label}: {identity!r}.")
        biosphere[identity] = OpenLCABiosphereReference(
            flow_id=_uuid(row, "flow_id", label),
            flow_name=_text(row, "flow_name", label),
            flow_property_id=_uuid(row, "flow_property_id", label),
            flow_property_name=_text(row, "flow_property_name", label),
            unit_id=_uuid(row, "unit_id", label),
            unit_name=_text(row, "unit_name", label),
        )

    if not technosphere or not biosphere:
        raise ValueError(f"OpenLCA reference catalog {path} must contain both reference axes.")
    if manifest_resource.get("technosphere_references") != len(technosphere):
        raise ValueError(f"OpenLCA reference catalog {path} has an invalid technosphere count.")
    if manifest_resource.get("biosphere_references") != len(biosphere):
        raise ValueError(f"OpenLCA reference catalog {path} has an invalid biosphere count.")
    return OpenLCAReferenceCatalog(
        technosphere=technosphere,
        biosphere=biosphere,
        source=str(payload.get("source") or ""),
    )


def _manifest_resource(path: Path) -> dict:
    manifest_path = path.parent / "RESOURCE_MANIFEST.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"Could not load openLCA reference manifest {manifest_path}: {error}") from error
    if manifest.get("schema_version") != 1 or not isinstance(manifest.get("resources"), list):
        raise ValueError(f"Unsupported openLCA reference manifest schema in {manifest_path}.")
    matches = [resource for resource in manifest["resources"] if resource.get("file") == path.name]
    if len(matches) != 1:
        raise ValueError(f"OpenLCA reference catalog {path} must have exactly one manifest entry.")
    return matches[0]


def _text(row: object, field: str, label: str) -> str:
    if not isinstance(row, dict):
        raise ValueError(f"{label} must be an object.")
    value = row.get(field)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{label} has invalid {field!r}.")
    return value


def _uuid(row: object, field: str, label: str) -> str:
    value = _text(row, field, label)
    try:
        return str(uuid.UUID(value))
    except ValueError as error:
        raise ValueError(f"{label} has invalid UUID field {field!r}: {value!r}.") from error
