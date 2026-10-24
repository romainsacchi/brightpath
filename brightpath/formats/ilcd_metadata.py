"""Explicit, JSON-compatible metadata profiles for ILCD process exports."""

from __future__ import annotations

import json
import re
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

import yaml

from .ilcd_package import fail

PROCESS_TYPES = frozenset(
    {"Unit process, single operation", "Unit process, black box", "LCI result", "Partly terminated system"}
)
_FIELDS = {
    "dataset_version",
    "process_type",
    "reference_year",
    "valid_until",
    "process_id",
    "license_type",
    "access_restrictions",
    "copyright",
    "permanent_uri",
}


def read_mapping(path: str | Path) -> dict:
    """Read a JSON or safe YAML configuration file."""
    with Path(path).open(encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        fail("configuration_invalid", "An ILCD configuration must be a mapping.")
    return data


@dataclass(frozen=True)
class ILCDExportProfile:
    """Required process metadata defaults and exact dataset-code overrides.

    :param defaults: Metadata used when a dataset has no preserved value.
    :param datasets: Overrides keyed by the dataset's persistent canonical code.
    """

    defaults: Mapping[str, Any] = field(default_factory=dict)
    datasets: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, value: Mapping | None) -> "ILCDExportProfile":
        data = deepcopy(dict(value or {}))
        if data.get("schema_version", 1) != 1 or set(data) - {"schema_version", "defaults", "datasets"}:
            fail("metadata_invalid", "Unsupported ILCD metadata profile schema or keys.")
        if not isinstance(data.get("defaults", {}), dict) or not isinstance(data.get("datasets", {}), dict):
            fail("metadata_invalid", "ILCD defaults and dataset overrides must be mappings.")
        if any(not isinstance(v, dict) for v in data.get("datasets", {}).values()):
            fail("metadata_invalid", "Each ILCD dataset override must be a mapping.")
        return cls(data.get("defaults", {}), data.get("datasets", {}))

    @classmethod
    def from_file(cls, path: str | Path) -> "ILCDExportProfile":
        return cls.from_mapping(read_mapping(path))

    def for_dataset(self, dataset: Mapping) -> dict:
        imported = dataset.get("ilcd", {}).get("metadata", {})
        result = {
            **deepcopy(dict(self.defaults)),
            **deepcopy(imported),
            **deepcopy(dict(self.datasets.get(str(dataset.get("code", "")), {}))),
        }
        if set(result) - _FIELDS:
            fail(
                "metadata_field_unsupported",
                f"Unsupported ILCD metadata fields: {', '.join(sorted(set(result) - _FIELDS))}",
            )
        if "copyright" in result and not isinstance(result["copyright"], bool):
            fail("metadata_invalid", "ILCD copyright metadata must be a boolean.")
        for key in ("dataset_version", "process_type", "reference_year", "valid_until"):
            if result.get(key) in (None, ""):
                fail("required_metadata_missing", f"ILCD export requires {key}.", f"ilcd.metadata.{key}")
        if not re.fullmatch(r"\d{2}\.\d{2}(?:\.\d{3})?", str(result["dataset_version"])):
            fail("metadata_invalid", "ILCD dataset_version must have the form 00.01 or 00.01.000.")
        if result["process_type"] not in PROCESS_TYPES:
            fail("metadata_invalid", "Unsupported ILCD process_type.")
        try:
            if any(
                isinstance(result[key], float) and not result[key].is_integer()
                for key in ("reference_year", "valid_until")
            ):
                raise ValueError()
            start, end = int(result["reference_year"]), int(result["valid_until"])
        except (TypeError, ValueError):
            fail("metadata_invalid", "ILCD reference years must be integers.")
        if not 1000 <= start <= end <= 9999:
            fail("metadata_invalid", "ILCD validity years must be ordered four-digit years.")
        return result

    def to_dict(self) -> dict:
        return json.loads(
            json.dumps({"schema_version": 1, "defaults": dict(self.defaults), "datasets": dict(self.datasets)})
        )


def coerce_profile(value: ILCDExportProfile | Mapping | None) -> ILCDExportProfile:
    return value if isinstance(value, ILCDExportProfile) else ILCDExportProfile.from_mapping(value)
