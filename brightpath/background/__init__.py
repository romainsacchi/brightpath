"""Background catalog contracts and provider implementations."""

from .catalogs import (
    BiosphereCatalog,
    CatalogIntegrityError,
    CatalogNotFoundError,
    CatalogProvider,
    CompositeCatalogProvider,
    DirectoryCatalogProvider,
    InMemoryCatalogProvider,
    PackageCatalogProvider,
    TechnosphereCatalog,
    catalog_provider_from_environment,
)
from .execution import execute_background_migration
from .migration import (
    MigrationAxis,
    MigrationPlan,
    MigrationRouteStep,
    plan_background_migration,
)
from .validation import validate_background_links

__all__ = (
    "BiosphereCatalog",
    "CatalogIntegrityError",
    "CatalogNotFoundError",
    "CatalogProvider",
    "CompositeCatalogProvider",
    "DirectoryCatalogProvider",
    "InMemoryCatalogProvider",
    "MigrationAxis",
    "MigrationPlan",
    "MigrationRouteStep",
    "PackageCatalogProvider",
    "TechnosphereCatalog",
    "catalog_provider_from_environment",
    "execute_background_migration",
    "plan_background_migration",
    "validate_background_links",
)
