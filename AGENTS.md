# Repository Guidelines

## Project Purpose and Domain Model

BrightPath reads foreground life-cycle inventory files, validates and normalizes their datasets,
optionally migrates their background links, and writes them in an LCA software exchange format.
Treat the following concerns as independent axes:

- **File format:** Brightway Excel and SimaPro CSV have v1 facades; upload analysis also accepts
  Brightway CSV/TSV. OpenLCA Excel and ecospold2 may be added later.
- **Background family:** `ecoinvent` or `uvek`.
- **Background version:** for example ecoinvent `3.6` through `3.12`, or UVEK `2025`.
- **System model:** for example `cutoff` or `consequential` where the background family supports it.

Use `UVEK` in new code, documentation, data names, and public output. Accept `BAFU` only as a
legacy input alias and normalize it at the boundary. Spell the software name `SimaPro` in new prose
and identifiers. The 0.x converter classes and modules were deleted in version 1.0; do not restore
compatibility wrappers that couple a source class to an assumed target format.

Conversion between file formats and migration between background profiles are separate operations.
Loading a Brightway Excel workbook must not imply exporting SimaPro CSV, and writing the same
format after an ecoinvent migration must be supported.

## Current Structure

`brightpath/` contains the Python package:

- `brightway.py` and `simapro.py` define the public `BrightwayInventory` and `SimaProInventory`
  facades.
- `formats/` contains syntax-only Brightway Excel and SimaPro CSV readers and writers.
- `profiles/` contains background-family naming behavior used by format adapters.
- `migrations/` resolves and applies explicit background-profile routes, while
  `data/migrations/` contains the attributed Premise migration resources.
- `validation/` performs read-only structural and background-link validation.
- `analysis/analyzer.py` parses uploads, normalizes Brightway-style dictionaries, infers background
  profiles, checks links against catalogs, and returns structured issues.
- `models.py` defines `InventoryDocument`, `InventoryFormat`, `BackgroundProfile`, issue, candidate,
  validation, and analysis result models.
- `catalogs.py` loads packaged reference catalogs used for profile inference and link validation.
- `utils.py` retains shared analysis heuristics and SimaPro mapping loaders. New format, profile,
  validation, or migration behavior belongs in its focused module rather than this file.
- `data/export/` contains packaged mappings and generated reference catalogs; keep package-data
  declarations in `MANIFEST.in` synchronized with additions.
- `tests/` is the pytest target. `scripts/` contains maintainable utilities, while `dev/` contains
  diagnostics, notebooks, and local inventory files that are not normal package inputs.
- `docs/` contains the Sphinx user and API documentation. Keep task-oriented guides synchronized
  with the README and public facade behavior.

The Sphinx site starts at `docs/index.rst`, and its maintainer architecture is in
`docs/architecture.rst`. `docs/format-profile-refactor.md` retains the original v1 design rationale.
Version 1.0 intentionally removes the 0.x converter classes instead of maintaining compatibility
wrappers.

## Architecture Rules

- Format readers parse into one software-neutral inventory representation; format writers only
  render that representation.
- Background migration receives an explicit source and target `BackgroundProfile`. It must be a
  no-op when the normalized profiles are equal and must not be triggered by a format writer.
- Structural validation, format-specific validation, and background-link validation are distinct
  stages. Return structured `Issue` objects instead of relying on logger output or printed tables.
- Keep parsing, validation, normalization, and migration callable independently. A caller must be
  able to inspect issues without exporting or mutating its source data.
- Preserve caller-owned input. Copy before normalization or transformation, and add regression tests
  for idempotence and non-mutation.
- Preserve unknown dataset and exchange fields needed for round trips. Do not silently discard
  metadata merely because the canonical model does not interpret it.
- Do not infer a target format or target background profile from the source. Defaults may preserve
  the source, but cross-format or cross-profile operations must be explicit.
- CSV extensions are ambiguous between Brightway and SimaPro. Prefer an explicit format identifier;
  format inference must inspect content and report ambiguity rather than guessing silently.
- Keep ecoinvent system-model support explicit. Do not apply cut-off migration rules to
  consequential inventories.

## Background Migrations

Premise's `inventory_imports.py` and its JSON migration resources are the reference implementation
for ecoinvent inventory migration. Migration rules include replacements and one-to-many
disaggregation; reverse routes can require aggregation and can be lossy. Deletions are not generally
reversible.

If migration resources are copied into BrightPath:

- record their source, generator, version, and license;
- package them under a dedicated migration-data directory rather than `data/export/`;
- validate their schema when loading;
- resolve multi-step routes explicitly and return a report for every applied step;
- test forward and reverse behavior independently, including ambiguous aggregation and deleted
  biosphere flows;
- verify the transformed links against the exact target reference catalog;
- never import proprietary ecoinvent inventory contents into the repository.

Do not add a runtime dependency on a local Premise checkout or read files by absolute path. Either
use a declared library dependency or maintain attributed package resources inside BrightPath.

## Build, Test, and Development Commands

- `python -m pip install -r requirements.txt` installs runtime dependencies.
- `python -m pip install -e .` installs BrightPath in editable mode.
- `python -m pytest` runs the configured test suite from `tests/`.
- `python -m sphinx -W --keep-going -b html docs docs/_build/html` builds documentation and treats
  warnings as failures after installing the `docs` extra.
- `python -m build` builds source and wheel distributions when `build` is installed.

Use Python 3.9 to 3.11, matching `pyproject.toml`. Run the focused tests while iterating and the full
suite before handing off a structural change.

## Coding and Testing Conventions

Format Python with Black using the repository's 120-character line length. Use 4-space indentation,
descriptive snake_case functions and variables, PascalCase classes, lowercase module filenames, and
Sphinx-style docstrings for public APIs. Prefer enums or validated value objects over repeated raw
format and profile strings.

Add tests directly under `tests/` with names such as `test_simapro_inventory.py`. For architecture
work, cover:

- parsing and writing separately for each format;
- validation without conversion;
- same-format, same-profile round trips;
- same-format ecoinvent version migration;
- format conversion with an unchanged background profile;
- combined format and profile conversion as an explicit pipeline;
- UVEK/BAFU alias normalization;
- unsupported family, version, and system-model combinations;
- source-data non-mutation and repeated-call idempotence;
- loading every new packaged mapping or migration resource from an editable install and a built
  wheel.

## Commit and Data Hygiene

Use short imperative commit subjects such as `Add UVEK profile alias` or `Separate format parsing`.
Keep each commit focused. Pull requests should describe the source and target formats/profiles
affected, data resources changed, compatibility impact, and tests run.

The worktree can contain large untracked files under `dev/`. Do not delete, rewrite, stage, or commit
them unless the task explicitly calls for those exact files. Keep generated notebook checkpoints,
ad hoc exports, credentials, and proprietary database data out of normal commits.
