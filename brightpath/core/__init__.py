"""Core context and canonical-schema contracts."""

from .context import (
    BackgroundContext,
    BiosphereProfile,
    ContextHint,
    FormatProfile,
    InventoryContext,
    TechnosphereProfile,
    VersionResolution,
    resolve_migration_series,
    resolve_profile_migration_series,
)
from .schema import (
    CANONICAL_SCHEMA_VERSION,
    CanonicalDataset,
    CanonicalExchange,
    CanonicalInventory,
    CanonicalParameter,
    DatasetIdentity,
    ExchangeIdentity,
    ExtensionMap,
    Uncertainty,
)

__all__ = (
    "BackgroundContext",
    "BiosphereProfile",
    "CANONICAL_SCHEMA_VERSION",
    "CanonicalDataset",
    "CanonicalExchange",
    "CanonicalInventory",
    "CanonicalParameter",
    "ContextHint",
    "DatasetIdentity",
    "ExchangeIdentity",
    "ExtensionMap",
    "FormatProfile",
    "InventoryContext",
    "TechnosphereProfile",
    "Uncertainty",
    "VersionResolution",
    "resolve_migration_series",
    "resolve_profile_migration_series",
)
