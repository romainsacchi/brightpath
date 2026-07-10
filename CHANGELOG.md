# Changelog

All notable changes to this project will be documented in this file.

## 1.0.0a1 - Unreleased

### Breaking

- Deleted `BrightwayConverter` and `SimaproConverter` and replaced them with the independent
  `BrightwayInventory` and `SimaProInventory` APIs.
- Separated file-format operations from background family, version, and system-model migration.
- Removed implicit UVEK amount conversion and distribution-transport injection from SimaPro
  serialization; background transformations now require an explicit migration route.
- Replaced the combined background assumption with exact, independent technosphere and biosphere
  profiles. Ecoinvent patch versions are preserved instead of truncated to major/minor.
- Made migration and conversion loss handling strict by default. Inferred reverse routes require an
  explicit permissive policy, and unsafe unit-changing rules are never applied without factors.
- Unified operation failures around immutable reports; upload validation and writer validation now
  use the same exception hierarchy.
- Removed the legacy public `brightpath.migrations.migrate_inventory` entry point, which could not
  represent independent biosphere contexts or transactional validation.
- Enforced software-format identity at facade and reader boundaries; contradictory exact and legacy
  context arguments now fail instead of being ignored.
- Required exact `InventoryContext` and exact catalog access before SimaPro upload analysis parses a
  file; legacy partial `source_profile` inference remains Brightway-only.

### Added

- Added copy-on-write `InventoryDocument`, `BrightwayInventory`, and `SimaProInventory` models.
- Added a versioned canonical schema, exact `InventoryContext`, namespaced extensions, immutable
  operation reports, and explicit conversion/migration policies.
- Added an injected format adapter registry with bounded content probes. Brightway Excel, Brightway
  CSV/TSV, and SimaPro CSV are first-class read/write adapters; ambiguous CSV is never guessed.
- Added adapter-owned format validation and conversion-preflight contracts so new formats can
  extend the pipeline without central format switches.
- Added explicit adapter contract flags and version/dialect compatibility allowlists; incomplete
  adapters are rejected before registry capability discovery.
- Added the dependency-injected `InventoryPipeline` for detection, parsing, normalization,
  independent validation, migration, format conversion, writing, and audit sidecars.
- Added independent technosphere and biosphere catalog providers, validation coverage metrics,
  migration planning, transactional execution, and exact source/target validation.
- Added the `brightpath` CLI with `formats`, `inspect`, `validate`, `convert-format`, and
  `migrate-background` commands, dry runs, JSON reports, and stable exit codes.
- Added independent Brightway Excel loading, normalization, structured validation, and writing.
- Added independent SimaPro CSV loading, rendering, structured validation, parameter preservation,
  ecoinvent cut-off/consequential marker checks, and ecoinvent/UVEK naming profiles.
- Added exact catalog injection for SimaPro biosphere normalization instead of applying one fixed
  ecoinvent flow list to every declared biosphere release.
- Added bidirectional ecoinvent cut-off migration routes from 3.5 through 3.12 with structured step
  reports for replacements, disaggregation, aggregation, irreversible deletion, ambiguity, and unit
  changes.
- Packaged and attributed Premise's CC-BY-4.0 ecoinvent technosphere and biosphere migration
  resources.
- Added an explicit, non-routable placeholder for future ecoinvent-to-UVEK 2025 mappings.
- Normalized `BAFU` to the canonical `UVEK` background family name.
- Preserved nested unknown Brightway workbook fields through tagged JSON values during round trips.
- Reserved `openlca_excel` and `ecospold2` format identifiers for future adapters.
- Added task-oriented Sphinx documentation, generated API reference, strict CI documentation builds,
  and a modern Read the Docs configuration.
- Added SHA-256 integrity manifests for catalog and migration resources, with explicit provenance
  and a stable-release legal-review gate for generated reference catalogs.

### Fixed

- Accepted `hectare` and `ha` as valid Brightway technosphere units during inventory validation and catalog matching.
- Accepted `person kilometer` plus legacy `product` technosphere fields during Brightway inventory normalization.
- Replaced repeated full-catalog canonical technosphere scans with indexed matching to keep large workbook analysis responsive.
- Fixed SimaPro uncertainty labels to use values accepted by `bw2io` and kept duplicate parsed
  identities available for validation instead of aborting file loading.
- Normalized SimaPro parameter identifiers across supported `bw2io` releases while preserving
  database, project, and process parameter scopes.
- Initialized Brightway core migrations inside SimaPro loading so clean environments do not depend
  on an earlier Brightway import.
- Routed compatibility catalog access through exact independent providers and combined verified
  model-specific biosphere shards into one version-specific biosphere catalog.
- Separated intrinsic format validation from conversion representability/loss policy and treated
  reconstructible BW2IO `input`/`output` graph keys as non-lossy metadata.
- Declared setuptools 77 or newer and Twine 6.1 or newer for reproducible SPDX license metadata
  builds and Metadata 2.4 release validation.

## 0.0.4 - 2026-05-14

### Added

- Added direct Brightway Excel export from `SimaproConverter.convert_to_brightway(format="excel", filename=...)`.
- Added `--excel-output` to the ecoinvent 3.10 Si wafer import diagnostic script.
- Documented direct SimaPro CSV to Brightway Excel conversion in the README.

## 0.0.3 - 2026-05-14

### Added

- Added `AGENTS.md` with repository-specific contributor guidance.
- Added a comprehensive pytest suite covering converter defaults, validation, SimaPro parsing, CSV output safety, pure helper behavior, and packaging expectations.
- Added development extras for tests, formatting, linting, security checks, and builds.
- Added `BrightwayConverter.unused_exchanges` to expose unused exchange reports programmatically.
- Added an optional `filename` argument to `BrightwayConverter.convert_to_simapro()` for explicit CSV export names.
- Added ecoinvent 3.10-specific SimaPro-to-Brightway biosphere mappings and import diagnostics.
- Added a release workflow for PyPI and Anaconda publishing on tags or GitHub releases.

### Changed

- Refactored Brightway-to-SimaPro row generation into smaller internal helpers while preserving the existing converter entrypoint.
- Replaced CI auto-format commits with check-only quality, test, security, and build jobs.
- Expanded CI tests across Linux, macOS, and Windows.
- Tightened package manifests to exclude cache files and operating-system metadata from built distributions.
- Made UVEK conversion and distribution transport helpers return transformed copies instead of mutating caller-owned inputs.
- Made SimaPro biosphere formatting return a copy by default.

### Fixed

- Fixed `BrightwayConverter` initialization when `export_dir` or metadata is omitted.
- Prevented repeated conversions from mutating source inventory data.
- Prevented SimaPro inventory cleanup from writing surprising sibling `_edited.csv` files by default.
- Replaced silent duplicate dataset dropping with explicit duplicate validation.
- Hardened SimaPro technosphere and biosphere parsing for malformed names, locations, and categories.
- Normalized common ecoinvent 3.10 SimaPro biosphere names and final waste indicators to reduce false unlinked exchanges.
- Added CSV formula escaping for text values that spreadsheet software could interpret as formulas.
- Added collision-safe default CSV export filenames.
