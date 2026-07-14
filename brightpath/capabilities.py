"""Serializable capability discovery from registered code and resources."""

from __future__ import annotations

from dataclasses import dataclass

from .adapters import AdapterRegistry, default_adapter_registry
from .background import PackageCatalogProvider
from .migrations.resources import (
    load_biosphere_resources,
    load_technosphere_resources,
    load_uvek_technosphere_resource,
)


@dataclass(frozen=True)
class FormatCapability:
    """Operations advertised by one registered format adapter."""

    format_id: str
    version: str
    dialect: str
    read: tuple[str, ...]
    write: tuple[str, ...]
    detect: tuple[str, ...]

    def to_dict(self) -> dict:
        """Return a JSON-compatible snapshot."""

        return {
            "format_id": self.format_id,
            "version": self.version,
            "dialect": self.dialect,
            "read": list(self.read),
            "write": list(self.write),
            "detect": list(self.detect),
        }


@dataclass(frozen=True)
class MigrationCapability:
    """One packaged forward migration edge and its reverse status."""

    axis: str
    family: str
    system_model: str
    source_series: str
    target_series: str
    target_family: str = ""
    forward: str = "available"
    reverse: str = "inferred_policy_controlled"

    def to_dict(self) -> dict:
        """Return a JSON-compatible snapshot."""

        return {
            "axis": self.axis,
            "family": self.family,
            "target_family": self.target_family or self.family,
            "system_model": self.system_model,
            "source_series": self.source_series,
            "target_series": self.target_series,
            "forward": self.forward,
            "reverse": self.reverse,
        }


def format_capabilities(registry: AdapterRegistry | None = None) -> tuple[FormatCapability, ...]:
    """Discover formats from an injected registry or the built-in registry."""

    selected = registry or default_adapter_registry()
    if not isinstance(selected, AdapterRegistry):
        raise TypeError("registry must be an AdapterRegistry.")
    values = []
    for descriptor, adapter in zip(selected.descriptors, selected.adapters, strict=True):
        capabilities = adapter.capabilities
        values.append(
            FormatCapability(
                format_id=descriptor.format_id,
                version=descriptor.version,
                dialect=descriptor.dialect,
                read=tuple(sorted(kind.value for kind in capabilities.read_artifact_kinds)),
                write=tuple(sorted(kind.value for kind in capabilities.write_artifact_kinds)),
                detect=tuple(sorted(kind.value for kind in capabilities.detection_artifact_kinds)),
            )
        )
    return tuple(values)


def migration_capabilities() -> tuple[MigrationCapability, ...]:
    """Discover only migration edges backed by packaged non-placeholder data."""

    values = [
        MigrationCapability(
            axis="technosphere",
            family="ecoinvent",
            system_model="cutoff",
            source_series=source,
            target_series=target,
        )
        for source, target in sorted(load_technosphere_resources("cutoff"), key=_edge_key)
    ]
    values.extend(
        MigrationCapability(
            axis="biosphere",
            family="ecoinvent",
            system_model="",
            source_series=source,
            target_series=target,
        )
        for source, target in sorted(load_biosphere_resources(), key=_edge_key)
    )
    uvek = load_uvek_technosphere_resource()
    values.extend(
        MigrationCapability(
            axis="technosphere",
            family=uvek["source_profile"]["family"],
            target_family=uvek["target_profile"]["family"],
            system_model=system_model,
            source_series=source_version,
            target_series=uvek["target_profile"]["version"],
            reverse="unavailable",
        )
        for source_version in uvek["source_profile"]["versions"]
        for system_model in uvek["source_profile"]["system_models"]
    )
    return tuple(values)


def capability_snapshot(registry: AdapterRegistry | None = None) -> dict:
    """Return the complete capability snapshot used by CLI and documentation."""

    catalogs = PackageCatalogProvider()
    return {
        "formats": [capability.to_dict() for capability in format_capabilities(registry)],
        "migrations": [capability.to_dict() for capability in migration_capabilities()],
        "catalogs": {
            "technosphere": [
                {
                    "family": profile.family,
                    "version": profile.version,
                    "system_model": profile.system_model,
                }
                for profile in catalogs.technosphere_profiles()
            ],
            "biosphere": [
                {"family": profile.family, "version": profile.version} for profile in catalogs.biosphere_profiles()
            ],
        },
    }


def _edge_key(edge: tuple[str, str]) -> tuple[tuple[int, ...], tuple[int, ...]]:
    return _version_key(edge[0]), _version_key(edge[1])


def _version_key(value: str) -> tuple[int, ...]:
    return tuple(int(part) for part in value.split("."))
