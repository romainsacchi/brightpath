ADR 0003: Catalog and resource governance
=========================================

Status
------

Accepted, with redistribution review required before the stable 1.0 release.

Decision
--------

Technosphere and biosphere profiles and catalogs are independent. UVEK 2025,
for example, must explicitly declare the biosphere release it uses instead of
embedding that choice in a generator script.

Catalogs are supplied through explicit providers. Packaged, directory,
in-memory, and composite providers are supported; environment variables are
only an application-boundary convenience. Conflicting catalogs for the same
profile and different digests are errors.

Every released catalog and migration resource declares a schema version,
status, exact source and target contexts, counts, generator and version,
provenance, SPDX license, input digests, and payload checksum. Draft and
placeholder resources are excluded from capability discovery.

The existing ecoinvent identity catalogs and UVEK mapping files require an
explicit licensing and provenance review. Provider-based local generation or a
separately licensed data package will be used if redistribution is not
permitted. No proprietary inventories or credentials are packaged.
