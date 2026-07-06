# Format/Profile Refactor Plan

## Purpose

BrightPath currently succeeds as a converter library, but its internals still mix two different
concerns:

- software format parsing and rendering,
- background database profile compatibility and transformation.

This document defines a refactor target that separates those concerns while keeping the current
public workflow stable.

## Current coupling

The present codebase combines format and profile behavior in ways that make new workflows harder
to add:

- `BrightwayConverter` converts toward SimaPro rows while selecting behavior by `database`,
- `format_exchange_name(...)` formats technosphere targets differently for `ecoinvent` and
  `uvek`,
- `check_exchanges_for_conversion(...)` applies profile-specific exchange modifications,
- `add_distri_transport(...)` injects UVEK-specific transport enrichment during one export path,
- `SimaproConverter` assumes an ecoinvent-version-driven target interpretation.

This means the implementation is closer to:

`source format -> hardcoded target workflow`

than to:

`source format + source profile -> canonical inventory -> optional target profile + target format`

## Target model

BrightPath should treat these as independent axes:

- source format
- source background profile
- target background profile
- target format

### Examples the new model should support

- Brightway inventory linked to UVEK -> render to SimaPro with ecoinvent-style targets
- SimaPro CSV linked to UVEK -> normalize to canonical inventory -> export toward Brightway
  conventions
- SimaPro CSV linked to ecoinvent cut-off -> analyze only, with no export
- Brightway Excel linked to ecoinvent consequential -> validate and report compatibility issues

## Canonical inventory layer

Introduce a canonical, software-neutral inventory representation.

Suggested models:

- `BackgroundProfile`
- `InventoryBundle`
- `DatasetRecord`
- `ExchangeRecord`
- `Issue`
- `AnalysisResult`
- `CompatibilityReport`

The canonical inventory should preserve enough information to:

- validate uploaded inventories,
- emit candidate summaries,
- transform profiles,
- render to one or more output formats,
- surface warnings without relying on log scraping.

## Proposed module layout

```text
brightpath/
  __init__.py
  models.py
  analysis/
    __init__.py
    analyzer.py
    compatibility.py
  formats/
    __init__.py
    brightway_excel.py
    brightway_table.py
    simapro_csv.py
  pipeline/
    __init__.py
    parse.py
    convert.py
  profiles/
    __init__.py
    base.py
    ecoinvent.py
    uvek.py
    registry.py
  transforms/
    __init__.py
    biosphere.py
    technosphere.py
    transport.py
    relinking.py
  validation/
    __init__.py
    inventory.py
    brightway.py
    simapro.py
```

The existing `bwconverter.py`, `simaproconverter.py`, and `utils.py` should remain during the
transition, but increasingly delegate to the new modules.

## Responsibility split

### `formats/*`

Responsible for:

- reading a file into canonical inventory data,
- writing canonical inventory data to a concrete software format,
- handling software-specific syntax and workbook/CSV layout details.

Not responsible for:

- background profile logic,
- contributor workflow validation,
- review workflow behavior.

### `profiles/*`

Responsible for:

- supported background families and system models,
- profile-specific naming rules,
- blacklist rules,
- exchange conversion factors,
- profile-specific transport enrichment,
- reference mapping tables.

### `validation/*`

Responsible for:

- structural validation of canonical inventories,
- software-specific input requirements,
- profile compatibility checks,
- converting hard failures and soft warnings into structured issues.

### `analysis/*`

Responsible for returning a structured report for upload-like workflows:

- detected format,
- inferred or normalized profile,
- file-level issues,
- candidate summaries,
- candidate-level issues,
- normalized inventory bundle.

### `pipeline/*`

Responsible for orchestration:

- parse,
- validate,
- optionally transform profiles,
- optionally render.

## Additive API target

BrightPath should add a new API without breaking existing classes.

Suggested entrypoints:

```python
from brightpath.analysis import analyze_inventory
from brightpath.models import BackgroundProfile

result = analyze_inventory(
    path="inventory.xlsx",
    source_format="brightway_excel",
    source_profile=BackgroundProfile(
        family="ecoinvent",
        version="3.10",
        system_model="cutoff",
    ),
)
```

```python
from brightpath.pipeline import convert_inventory

conversion = convert_inventory(
    path="inventory.csv",
    source_format="simapro_csv",
    source_profile=BackgroundProfile("uvek", "2025", "cutoff"),
    target_format="brightway_excel",
    target_profile=BackgroundProfile("ecoinvent", "3.10", "cutoff"),
)
```

## Backward compatibility

The public imports in `brightpath.__init__` must remain stable:

- `BrightwayConverter`
- `SimaproConverter`
- `DATA_DIR`

### Compatibility wrapper strategy

`BrightwayConverter`

- keep accepting `filepath` or `data`,
- keep defaulting to current behavior,
- internally call the new pipeline with:
  - `source_format="brightway_excel"` when `filepath` is used,
  - canonical inventory input when `data` is used,
  - `target_format="simapro_csv"` for current export paths.

`SimaproConverter`

- keep accepting `filepath`, `ecoinvent_version`, and `db_name`,
- internally call the new parser and conversion pipeline,
- keep returning Brightway-style data or Brightway Excel exports exactly as today.

## Old-to-new mapping

The following functions should migrate into the new structure:

- `import_bw_inventories(...)` -> `formats.brightway_excel.parse_brightway_excel(...)`
- `validate_brightway_inventory(...)` -> `validation.inventory.validate_canonical_inventory(...)`
- `check_simapro_inventory(...)` -> `formats.simapro_csv.preclean_simapro_csv(...)`
- `format_technosphere_exchange(...)` -> `formats.simapro_csv.parse_simapro_exchange_name(...)`
- `format_biosphere_exchange(...)` -> `transforms.biosphere.normalize_biosphere_exchange(...)`
- `format_exchange_name(...)` -> `profiles.*.format_technosphere_target_name(...)`
- `check_exchanges_for_conversion(...)` -> `profiles.*.transform_exchange_for_profile(...)`
- `add_distri_transport(...)` -> `profiles.uvek.apply_distribution_transport(...)`
- `ensure_unique_datasets(...)` -> `validation.inventory.ensure_unique_dataset_identity(...)`
- `collect_unused_exchanges(...)` -> `analysis.compatibility.collect_unmapped_exchanges(...)`

## Warning model

Warnings should no longer be available only through logger output or secondary attributes.

BrightPath should return structured `Issue` instances with:

- `severity`
- `code`
- `message`
- `path`
- `suggested_fix`

Examples:

- missing biosphere mapping -> warning
- duplicate dataset identity after normalization -> error
- unsupported profile combination -> error
- exchange left unmapped but ignored by target format -> warning

## Recommended implementation phases

### Phase 1: additive analysis layer

Add:

- `models.py`
- `analysis/analyzer.py`
- structured `Issue` and `AnalysisResult`
- Brightway Excel analysis
- SimaPro CSV analysis

No public converter behavior changes.

### Phase 2: profile objects and registry

Add:

- `BackgroundProfile`
- profile registry
- explicit ecoinvent and UVEK profile modules

Refactor database-specific utilities to use profile modules internally.

### Phase 3: conversion pipeline

Add:

- explicit `parse_inventory(...)`
- explicit `convert_inventory(...)`

Refit `BrightwayConverter` and `SimaproConverter` as wrappers.

### Phase 4: broaden format support

Add native Brightway CSV and TSV parsing as first-class format modules.

### Phase 5: remove internal coupling

Deprecate direct callers of utility helpers that still encode profile behavior in generic names.

## Minimum first PR

The smallest useful first PR should:

1. add the new `models.py` dataclasses,
2. add `analysis/analyzer.py`,
3. expose `analyze_inventory(...)`,
4. support:
   - Brightway Excel analysis,
   - SimaPro CSV analysis,
5. return structured issues and candidate summaries,
6. preserve all existing converter behavior and tests.

That first PR is enough for CLIC to replace its mocked upload validator for the two most
important input paths.

## Non-goals for the first PR

- rewriting both converters entirely,
- removing `utils.py`,
- changing current export defaults,
- changing current public constructor signatures,
- solving every target-profile transformation case immediately.

## Acceptance criteria

The refactor target is successful when:

- existing `BrightwayConverter` and `SimaproConverter` tests still pass,
- a caller can analyze an upload without performing a conversion,
- warnings are available programmatically,
- source format handling is independent from source and target background profiles,
- new profile combinations can be added without editing generic format parsers.
