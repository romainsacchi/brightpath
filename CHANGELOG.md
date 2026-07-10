# Changelog

All notable changes to this project will be documented in this file.

## Unreleased

## 1.0.0 - 2026-07-10

### Breaking

- Deleted `BrightwayConverter` and `SimaproConverter` and replaced them with the independent
  `BrightwayInventory` and `SimaProInventory` APIs.
- Separated file-format operations from background family, version, and system-model migration.
- Removed implicit UVEK amount conversion and distribution-transport injection from SimaPro
  serialization; background transformations now require an explicit migration route.

### Added

- Added copy-on-write `InventoryDocument`, `BrightwayInventory`, and `SimaProInventory` models.
- Added independent Brightway Excel loading, normalization, structured validation, and writing.
- Added independent SimaPro CSV loading, rendering, structured validation, parameter preservation,
  ecoinvent cut-off/consequential marker checks, and ecoinvent/UVEK naming profiles.
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

### Fixed

- Accepted `hectare` and `ha` as valid Brightway technosphere units during inventory validation and catalog matching.
- Accepted `person kilometer` plus legacy `product` technosphere fields during Brightway inventory normalization.
- Replaced repeated full-catalog canonical technosphere scans with indexed matching to keep large workbook analysis responsive.
- Fixed SimaPro uncertainty labels to use values accepted by `bw2io` and kept duplicate parsed
  identities available for validation instead of aborting file loading.
- Normalized SimaPro parameter identifiers across supported `bw2io` releases while preserving
  database, project, and process parameter scopes.

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
