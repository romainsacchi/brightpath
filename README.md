# BrightPath

[![License: BSD 3-Clause](https://img.shields.io/badge/license-BSD--3--Clause-blue?style=flat-square)](https://opensource.org/license/bsd-3-clause)

BrightPath validates foreground life-cycle inventories, migrates their
background database links, and writes them in LCA software exchange formats.

Version 1 separates two operations that were coupled in the 0.x converter
classes:

- **format conversion** changes the software representation;
- **background migration** changes linked database identities.

These operations are always explicit and independent. A Brightway Excel file
can be validated, migrated from ecoinvent 3.6 to 3.8, and written back to
Brightway Excel without producing SimaPro output. Likewise, converting to
SimaPro does not silently change the ecoinvent version.

The complete Sphinx user and API documentation starts at
[`docs/index.rst`](docs/index.rst).

## Current Support

| Capability | Support |
|---|---|
| Brightway Excel | Load, normalize, validate, migrate, convert, write |
| SimaPro CSV | Load, normalize, validate, render, migrate, convert, write |
| Brightway CSV/TSV | Upload analysis only |
| ecoinvent migration | Cut-off 3.5 through 3.12, forward and reverse |
| Reference catalogs | ecoinvent 3.6-3.12 cut-off/consequential; UVEK 2025 cut-off |
| UVEK | Brightway Excel and SimaPro CSV; `BAFU` accepted as a legacy alias |
| OpenLCA Excel / ecospold2 | Format identifiers reserved; adapters not implemented |

The version 0.x `BrightwayConverter` and `SimaproConverter` classes and modules
were deleted. Version 1 uses `BrightwayInventory` and `SimaProInventory`.

## Installation

BrightPath supports Python 3.9 through 3.11.

```bash
python -m pip install brightpath
```

## Background Profiles

A background profile is independent of the file format and contains a database
family, version, and system model:

```python
from brightpath import BackgroundProfile

ei310_cutoff = BackgroundProfile("ecoinvent", "3.10", "cutoff")
uvek_2025 = BackgroundProfile("uvek", "2025", "cutoff")
```

Profiles normalize common input variants: `BAFU` becomes `uvek`, `cut-off`
becomes `cutoff`, and ecoinvent `3.10.1` becomes `3.10`.

## Brightway Excel

Load a workbook with its current background profile:

```python
from brightpath import BackgroundProfile, BrightwayInventory

inventory = BrightwayInventory.from_excel(
    "lci-ecoinvent-3.6.xlsx",
    background_profile=BackgroundProfile("ecoinvent", "3.6", "cutoff"),
)

normalized = inventory.normalize()
report = normalized.validate()

for issue in report.issues:
    print(issue.severity, issue.code, issue.path, issue.message)

if report.is_valid:
    normalized.write_excel("lci-ecoinvent-3.6-checked.xlsx")
```

BrightPath applies the standard `bw2io.ExcelImporter` strategies while loading.
`normalize()` returns a copy that repairs legacy product fields, category
sequences, and production identities. Loading, normalization, validation, and
migration do not mutate caller-owned data.

Workbooks written by BrightPath embed their background profile, so reopening
those files does not require passing the profile again.

## SimaPro CSV

The profile is required because it determines how SimaPro names are parsed:

```python
from brightpath import BackgroundProfile, SimaProInventory

inventory = SimaProInventory.from_csv(
    "inventory.csv",
    background_profile=BackgroundProfile("ecoinvent", "3.10", "cutoff"),
    database_name="foreground-inventory",
)

report = inventory.validate(
    check_background_links=True,
    check_simapro_rendering=True,
)

if report.is_valid:
    inventory.write_csv("inventory-checked.csv")
```

SimaPro validation reports duplicate parsed identities, unsupported rendering
requirements, and contradictory or mismatched cut-off/consequential name
markers. Database, project, and process parameters are preserved where the
format represents them. CSV output is semicolon-delimited and Latin-1 encoded.

Use `inventory.render()` to inspect rows and format-specific issues without
writing a file.

## In-Memory Inventories

Both facades accept canonical Brightway-style dictionaries:

```python
from brightpath import BackgroundProfile, BrightwayInventory

data = [
    {
        "name": "foreground service",
        "reference product": "service",
        "location": "GLO",
        "unit": "unit",
        "exchanges": [
            {
                "type": "production",
                "name": "foreground service",
                "reference product": "service",
                "product": "service",
                "location": "GLO",
                "unit": "unit",
                "amount": 1.0,
            }
        ],
    }
]

inventory = BrightwayInventory.from_data(
    data,
    background_profile=BackgroundProfile("ecoinvent", "3.10", "cutoff"),
    database_name="foreground-model",
)
```

`SimaProInventory.from_data()` accepts the same canonical structure. SimaPro
output additionally requires a `simapro category` on production exchanges and
supported SimaPro units.

## Format Conversion

Conversion is explicit and preserves the background profile:

```python
simapro = normalized.to_simapro()
assert simapro.background_profile == normalized.background_profile
simapro.write_csv("lci-ecoinvent-3.6.csv")

brightway = inventory.to_brightway()
assert brightway.background_profile == inventory.background_profile
brightway.write_excel("lci-ecoinvent-3.10.xlsx")
```

Calling `to_simapro()` or `to_brightway()` does not migrate ecoinvent versions
and does not convert ecoinvent links to UVEK. SimaPro's categories, units,
sections, encoding, and field set still determine what survives a written CSV
round trip; `render()` checks known output requirements.

## Background Migration

Migration preserves the current software format:

```python
target = BackgroundProfile("ecoinvent", "3.8", "cutoff")
migrated = normalized.migrate_background(target)

assert isinstance(migrated, BrightwayInventory)
migrated.write_excel("lci-ecoinvent-3.8.xlsx")
```

The engine resolves multi-step routes and supports reverse routes:

```python
older = migrated.migrate_background(
    BackgroundProfile("ecoinvent", "3.6", "cutoff")
)

report = older.last_migration_report
for step in report.steps:
    print(step.source_version, step.target_version, step.direction)

for issue in report.all_issues:
    print(issue.severity, issue.code, issue.path, issue.message)
```

Reverse routes can be lossy because forward rules can disaggregate exchanges,
merge identities, or delete biosphere flows. Unit-changing rules are reported
because the imported resources do not provide amount-conversion factors.
Target-catalog validation is appended to the report by default.

The same `migrate_background()` method works on `SimaProInventory` and returns
SimaPro again.

## Migrate and Convert

Compose the operations when both axes must change:

```python
target = BackgroundProfile("ecoinvent", "3.12", "cutoff")

migrated_brightway = normalized.migrate_background(target)
if migrated_brightway.last_migration_report.has_errors:
    raise RuntimeError("Review target-link errors before export")

migrated_simapro = migrated_brightway.to_simapro()
migrated_simapro.write_csv("lci-ecoinvent-3.12.csv")
```

No conversion step is assumed by migration, and no migration step is assumed
by conversion.

## Validation and Reference Catalogs

Validation returns structured `Issue` objects and is read-only. Disable only
the catalog stage when validating drafts or profiles without a local catalog:

```python
structural_report = inventory.validate(check_background_links=False)
```

Declare valid foreground datasets stored outside the current file:

```python
report = inventory.validate(
    additional_foreground_targets=[
        (
            "shared transport service",
            "transport service",
            "CH",
            "ton kilometer",
        )
    ]
)
```

Set `BRIGHTPATH_REFERENCE_DIR` to use custom catalogs. The directory must
contain exact-profile JSON files such as `ecoinvent__3.5__cutoff.json` and
replaces the packaged catalog directory. The
[validation guide](docs/workflows/validation.rst)
documents catalog-generation helpers.

## Upload Analysis

Use the separate analysis API for intake workflows that should not export or
migrate files:

```python
from brightpath.analysis import analyze_inventory

result = analyze_inventory(
    path="foreground.xlsx",
    source_profile=BackgroundProfile("ecoinvent", "3.10", "cutoff"),
)

for issue in result.file_issues:
    print(issue.severity, issue.code, issue.message)

for candidate in result.candidates:
    print(candidate.name, candidate.reference_product, candidate.location)
    for issue in candidate.issues:
        print(issue.severity, issue.code, issue.message)
```

Analysis supports Brightway Excel, Brightway block-format CSV/TSV, and SimaPro
CSV. `.csv` is inferred as SimaPro; pass `SOURCE_FORMAT_BRIGHTWAY_CSV`
explicitly for Brightway CSV. A missing or partial profile can be inferred from
installed catalogs, with an issue recording the decision.

`validate_inventory()` raises `brightpath.analysis.InventoryValidationError`
and exposes the full analysis result through `.result`. Writer validation uses
`brightpath.InventoryValidationError` and exposes `.report`.

## UVEK

UVEK is a background database family, not a SimaPro export mode:

```python
uvek_inventory = BrightwayInventory.from_excel(
    "lci-uvek-2025.xlsx",
    background_profile=BackgroundProfile("uvek", "2025", "cutoff"),
)

uvek_inventory.validate()
uvek_inventory.to_simapro().write_csv("lci-uvek-2025.csv")
```

The package contains a clearly marked placeholder for future
ecoinvent-to-UVEK mappings. Cross-family migration raises
`MigrationUnavailableError`; an empty placeholder is never treated as a
successful migration.

## Current Limits

- Background migration is implemented only for ecoinvent cut-off 3.5-3.12.
- Consequential inventories can be formatted and catalog-validated but not
  migrated between versions.
- ecoinvent/UVEK and cross-system-model migrations are unavailable.
- ecoinvent 3.5 migration rules are packaged, but its validation catalog must
  be supplied separately.
- Brightway CSV/TSV are analysis inputs, not full read/write facades.
- OpenLCA Excel and ecospold2 adapters are planned but not implemented.

## Migration Data

The packaged ecoinvent migration resources were imported from Premise and
retain their source, generator, contributor, and CC-BY-4.0 metadata. See
[`brightpath/data/migrations/ATTRIBUTION.md`](brightpath/data/migrations/ATTRIBUTION.md)
for provenance. Proprietary ecoinvent inventories are not included.

## Development

```bash
python -m pip install -e ".[dev,docs]"
python -m pytest
python -m sphinx -W --keep-going -b html docs docs/_build/html
python -m build
```

See [CONTRIBUTING.md](CONTRIBUTING.md), [AGENTS.md](AGENTS.md), and the
[architecture guide](docs/architecture.rst).

## License

BrightPath source code is distributed under the [BSD-3-Clause license](LICENSE).
Packaged migration resources retain the licenses declared in their individual
files.
