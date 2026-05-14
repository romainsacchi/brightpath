# Changelog

All notable changes to this project will be documented in this file.

## Unreleased

### Added

- Added `AGENTS.md` with repository-specific contributor guidance.
- Added a comprehensive pytest suite covering converter defaults, validation, SimaPro parsing, CSV output safety, pure helper behavior, and packaging expectations.
- Added development extras for tests, formatting, linting, security checks, and builds.
- Added `BrightwayConverter.unused_exchanges` to expose unused exchange reports programmatically.
- Added an optional `filename` argument to `BrightwayConverter.convert_to_simapro()` for explicit CSV export names.

### Changed

- Refactored Brightway-to-SimaPro row generation into smaller internal helpers while preserving the existing converter entrypoint.
- Replaced CI auto-format commits with check-only quality, test, security, and build jobs.
- Tightened package manifests to exclude cache files and operating-system metadata from built distributions.
- Made UVEK conversion and distribution transport helpers return transformed copies instead of mutating caller-owned inputs.
- Made SimaPro biosphere formatting return a copy by default.

### Fixed

- Fixed `BrightwayConverter` initialization when `export_dir` or metadata is omitted.
- Prevented repeated conversions from mutating source inventory data.
- Prevented SimaPro inventory cleanup from writing surprising sibling `_edited.csv` files by default.
- Replaced silent duplicate dataset dropping with explicit duplicate validation.
- Hardened SimaPro technosphere and biosphere parsing for malformed names, locations, and categories.
- Added CSV formula escaping for text values that spreadsheet software could interpret as formulas.
- Added collision-safe default CSV export filenames.
