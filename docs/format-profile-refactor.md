# Format, Context, and Background Architecture

## Status

Implemented in the BrightPath 1.0 pre-release. This document records the
refactor from the deleted 0.x source-specific converters to the current exact
context and injected pipeline.

## Problem

BrightPath 0.x treated conversion as two coupled workflows:

```text
Brightway -> SimaPro
SimaPro -> Brightway
```

That mixed file parsing, structural validation, background identity, migration,
and target serialization. It also made UVEK look like a SimaPro-only option.
A same-format ecoinvent migration consequently implied an unrelated software
conversion.

## Current Model

The exact context is:

```text
InventoryContext
+-- FormatProfile(format_id, format_version, dialect, encoding)
+-- BackgroundContext
    +-- TechnosphereProfile(family, exact version, system model)
    +-- BiosphereProfile(family, exact version)
```

The axes are independent. `BAFU` is accepted only as a boundary alias and is
normalized to `uvek`. Exact patch versions are preserved. Migration resource
lookup returns a separate `VersionResolution`, so ecoinvent 3.10.1 may use the
3.10 resource series without becoming 3.10 in context or output.

## Pipeline

`InventoryPipeline` receives an immutable `AdapterRegistry` and an explicit
`CatalogProvider`:

```text
detect -> read -> normalize -> structural validation
                    |              |
                    |              +-> exact catalog validation
                    +-> optional background planning/execution
                    +-> optional format conversion/preflight
                                   |
                                   v
                              explicit write
```

Generic validation orders canonical structure, optional adapter-owned format
validation, then optional exact background-link validation. Format validation
owns intrinsic grammar only; conversion preflight exclusively owns target
representability, loss, and mapping ambiguity. Read/write capabilities declare
`can_validate_format` and writers additionally declare
`can_preflight_conversion`. Registry construction rejects missing flags and
non-callable hooks before capability discovery; failing or malformed report
contracts fail safely at execution. Adapters can declare
`requires_catalog_provider` so the pipeline injects its provider during reads.
SimaPro uses this to normalize flow names against the exact biosphere profile.

Format conversion preserves `BackgroundContext`. Background migration
preserves `FormatProfile`. Neither operation infers the other. Validation is
read-only, normalization returns a copy, and migration is transactional.

## Canonical Inventory

`CanonicalInventory` is a versioned software-neutral representation with typed
datasets, exchanges, parameters, identities, uncertainty, metadata, and exact
context. Unknown values are preserved in source namespaces. `InventoryDocument`
provides the transitional Brightway-style dictionary bridge used by existing
facades and codecs, with copy-on-read semantics.

Writers must preflight representability and report unsupported features or
information loss. They must not discard unknown metadata silently.
BW2IO `input` and `output` keys are reconstructible graph metadata and do not
count as losses or block strict round trips.

The compatibility `inventory_format` property returns an `InventoryFormat`
member for known IDs and the string identifier for custom formats. The enum is
therefore not a capability registry.

## Format Adapters

Executable support is defined by registered adapters, not enum membership.
The built-in registry currently contains:

- `brightway_excel`
- `brightway_csv`
- `brightway_tsv`
- `simapro_csv`

All four detect, read, and write files. Brightway and SimaPro CSV are
distinguished by bounded content evidence; the `.csv` suffix is never a silent
default. OpenLCA Excel and ecoSpold2 remain reserved identifiers and canonical
extension namespaces, but no adapter is registered or advertised.

Qualified descriptor lookup selects an exact version/dialect first. A generic
adapter is eligible only when `compatible_format_versions` and
`compatible_dialects` explicitly admit every requested qualifier. The built-in
Brightway Excel generic adapter admits the `bw2io` dialect only. Without a
generic adapter, an unqualified request can be ambiguous across multiple
qualified adapters.

## Background Catalogs

Technosphere and biosphere catalogs are independent typed resources loaded by
an injected provider. Package, directory, in-memory, and composite providers
are available. Directory resources are checked against exact embedded profiles
and, when present, manifest hashes, sizes, schema versions, and identity counts.

Biosphere identities for one family/version can be split across system-model
catalog files. The provider validates each shard and a common schema version,
then unions the identities and computes a deterministic composite digest.
Legacy `brightpath.catalogs` functions bridge through these independent
providers using the documented default biosphere.

`BRIGHTPATH_REFERENCE_DIR` is interpreted only by the application helper. It
places a custom directory ahead of packaged fallback rather than creating
hidden core state.

The packaged catalog manifest is intentionally marked `legal_review_required`.
It is an integrity and provenance record, not a redistribution license. Legal
approval or separately licensed/local catalogs are a gate for a stable public
release.

## Background Migration

Planning and execution are separate. The planner resolves independent
technosphere and biosphere routes and applies policy without reading catalogs
or changing data. The executor validates the exact source, applies rules to a
copy, validates exact target links and coverage, then commits or rolls back.

Strict policy rejects inferred reverse routes, ambiguity, deletions,
information loss, unresolved links, incomplete coverage, and unit-changing
rules without numeric factors. Permissive policy reports these as warnings and
losses; it does not certify output.

The legacy engine function `brightpath.migrations.migrate_inventory` is not a
public export. Public calls use transactional execution with endpoint
validation and rollback.

Packaged ecoinvent cut-off technosphere edges cover 3.5 to 3.12. Biosphere
edges stop at 3.10 to 3.11. The missing 3.11 to 3.12 biosphere edge prevents a
complete 3.12 migration. Consequential, cross-model, ecoinvent-to-UVEK, and
UVEK-version routes are unavailable. The UVEK mapping placeholder is excluded
from capability discovery.

## Reports and Exceptions

Pipeline operations return an immutable `OperationResult` and
`OperationReport`. Ordered stage reports contain issues, non-lossy changes,
explicit losses, metrics, and policy metadata, with deterministic JSON round
trips. Atomic sidecars can include SHA-256 artifact digests.

One exception hierarchy carries the same report through `.report`. Upload and
facade compatibility attributes do not create separate validation exception
types.

## Compatibility

`BrightwayInventory` and `SimaProInventory` remain v1 convenience facades and
accept an exact `context=`. `BackgroundProfile` remains a technosphere-only
compatibility projection. The 0.x `BrightwayConverter` and `SimaproConverter`
classes will not return.

## Acceptance Rules

- Same-format writing never implies conversion.
- Format conversion preserves exact background context.
- Migration preserves software format and requires explicit component targets.
- Source data is never mutated.
- UVEK is independent of software format.
- CSV ambiguity is reported rather than guessed.
- Placeholder data cannot advertise or execute a route.
- OpenLCA Excel and ecoSpold2 remain undiscoverable until complete adapters and
  independent fixtures pass their contracts.
- No proprietary background inventory is packaged.
