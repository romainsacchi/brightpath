"""Exact ILCD reference mappings from explicit or verified catalog evidence."""

from __future__ import annotations

import math
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping

from brightpath.models import BackgroundProfile

from .ilcd_metadata import read_mapping
from .ilcd_package import ILCDPackage, fail, uuid_text


def identity(data: Mapping, kind: str) -> tuple:
    if kind == "biosphere":
        return str(data.get("name", "")), tuple(data.get("categories", ())), str(data.get("unit", ""))
    return tuple(str(data.get(key, "")) for key in ("name", "reference product", "location", "unit"))


@dataclass(frozen=True)
class ILCDReferenceCatalog:
    """Mappings and supporting XML resources for one exact background context.

    Each row contains ``identity``, ``flow_id`` and ``flow_version``; a
    technosphere row additionally supplies ``process_id``. Supporting flow,
    property and unit-group XML is stored in ``resources`` keyed by package path.
    """

    context: Mapping = field(default_factory=dict)
    technosphere: tuple[dict, ...] = ()
    biosphere: tuple[dict, ...] = ()
    resources: Mapping[str, str] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, value: Mapping | None) -> "ILCDReferenceCatalog":
        data = deepcopy(dict(value or {}))
        if data.get("schema_version", 1) != 1 or set(data) - {
            "schema_version",
            "context",
            "technosphere",
            "biosphere",
            "resources",
            "source",
        }:
            fail("references_invalid", "Unsupported ILCD reference catalog schema or keys.")
        for key in ("context", "resources"):
            if not isinstance(data.get(key, {}), dict):
                fail("references_invalid", f"ILCD {key} must be a mapping.")
        for axis in ("technosphere", "biosphere"):
            configured = data.get("context", {}).get(axis)
            if configured is not None:
                if not isinstance(configured, dict):
                    fail("references_invalid", f"ILCD context {axis} must be a mapping.")
                normalized = BackgroundProfile(
                    configured.get("family", ""), configured.get("version", ""), configured.get("system_model", "")
                ).normalized()
                configured.update({"family": normalized.family, "version": normalized.version})
                if axis == "technosphere":
                    configured["system_model"] = normalized.system_model
        for kind in ("technosphere", "biosphere"):
            rows = data.get(kind, [])
            if not isinstance(rows, (list, tuple)) or any(not isinstance(row, dict) for row in rows):
                fail("references_invalid", f"ILCD {kind} references must be records.")
            seen = set()
            for row in rows:
                if not isinstance(row.get("identity"), dict) or not row.get("flow_version"):
                    fail("references_invalid", "ILCD mappings require identity and exact flow_version.")
                row["flow_id"] = uuid_text(row.get("flow_id"))
                if "amount_factor" in row:
                    factor = row["amount_factor"]
                    if (
                        isinstance(factor, bool)
                        or not isinstance(factor, (float, int))
                        or not math.isfinite(factor)
                        or factor <= 0
                    ):
                        fail("references_invalid", "ILCD amount_factor must be finite and positive.")
                    if not isinstance(row.get("reference_unit"), str) or not row["reference_unit"]:
                        fail("references_invalid", "An ILCD conversion factor requires its reference_unit.")
                if kind == "technosphere":
                    row["process_id"] = uuid_text(row.get("process_id"))
                key = identity(row["identity"], kind)
                if key in seen:
                    fail("reference_ambiguous", f"Duplicate {kind} reference identity: {key}")
                seen.add(key)
        return cls(
            data.get("context", {}),
            tuple(data.get("technosphere", [])),
            tuple(data.get("biosphere", [])),
            data.get("resources", {}),
        )

    @classmethod
    def from_file(cls, path: str | Path) -> "ILCDReferenceCatalog":
        return cls.from_mapping(read_mapping(path))

    def check_context(self, context) -> None:
        if not self.technosphere and not self.biosphere:
            return
        expected = {
            "technosphere": {
                "family": context.technosphere.family,
                "version": context.technosphere.version,
                "system_model": context.technosphere.system_model,
            },
            "biosphere": {"family": context.biosphere.family, "version": context.biosphere.version},
        }
        if dict(self.context) != expected:
            fail("reference_context_conflict", "ILCD reference catalog does not match the exact background context.")

    def lookup(self, data: Mapping, kind: str) -> dict | None:
        key = identity(data, kind)
        matches = [row for row in getattr(self, kind) if identity(row["identity"], kind) == key]
        if len(matches) > 1:
            fail("reference_ambiguous", f"Ambiguous ILCD {kind} mapping: {key}")
        return deepcopy(matches[0]) if matches else None

    def package(self) -> ILCDPackage:
        package = ILCDPackage.from_resources(dict(self.resources))
        if any(key[0] == "Process" for key in package.entities):
            fail(
                "reference_catalog_process_forbidden",
                "An ILCD reference catalog must not embed background process inventories.",
            )
        return package


def coerce_references(value: ILCDReferenceCatalog | Mapping | None) -> ILCDReferenceCatalog:
    return value if isinstance(value, ILCDReferenceCatalog) else ILCDReferenceCatalog.from_mapping(value)
