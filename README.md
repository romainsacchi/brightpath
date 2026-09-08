# BrightPath

[![License: BSD 3-Clause](https://img.shields.io/badge/license-BSD--3--Clause-blue?style=flat-square)](https://opensource.org/license/bsd-3-clause)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue?style=flat-square)](https://www.python.org/)

**Convert and update foreground life-cycle inventories without losing track of what changed.**

BrightPath helps LCA practitioners move foreground inventories between Brightway,
SimaPro, and openLCA exchange formats. It can also update links from one ecoinvent
release to another. Every operation can be checked before the result is used.

> **Project status — work in progress**
>
> BrightPath 1.0 is currently an alpha release. Interfaces, supported routes, and
> conversion results may change between releases. The heuristic ecoinvent-to-UVEK
> mapping is under active development and is especially subject to change. Keep your
> source files, record the BrightPath version used, and review every output.

```text
your foreground inventory
          |
          v
  inspect and validate
          |
          v
convert format and/or update background links
          |
          v
    new file + report
```

## What can I do with it?

| I want to… | BrightPath can… |
|---|---|
| Move a Brightway inventory to SimaPro | Read Brightway Excel and write SimaPro CSV |
| Move a SimaPro inventory to Brightway | Read SimaPro CSV and write Brightway Excel |
| Exchange foreground processes with openLCA | Read and write process-only JSON-LD ZIP packages |
| Check a file before importing it | Report structural, format, and background-link problems |
| Update an inventory to a newer ecoinvent release | Migrate technosphere and biosphere links while keeping the file format |

BrightPath works with **foreground inventories**: the processes and exchanges you
model or want to share. It does not include proprietary background databases or
import the result into a Brightway project, SimaPro database, or openLCA database
for you.

## One important idea

A foreground inventory has two independent parts:

- its **file format**: Brightway Excel, SimaPro CSV, and so on;
- its **background links**: for example, ecoinvent 3.10 cut-off.

Converting a format does not update background links. Updating background links does
not change the format. BrightPath keeps these actions separate so that a conversion
cannot silently change the database behind your model.

## Installation

BrightPath requires Python 3.12.

```bash
python -m pip install brightpath
```

Check what your installed version supports:

```bash
brightpath formats
```

New to Python? The [getting-started guide](docs/getting-started.rst) explains the
setup and the main concepts in more detail.

## Your first conversion

### Brightway Excel to SimaPro CSV

The example below reads a foreground workbook linked to ecoinvent 3.10 cut-off,
normalizes it, and writes a SimaPro CSV file:

```python
from brightpath import BackgroundProfile, BrightwayInventory

source_background = BackgroundProfile("ecoinvent", "3.10", "cutoff")

inventory = BrightwayInventory.from_excel(
    "foreground.xlsx",
    background_profile=source_background,
)

output = inventory.normalize().to_simapro().write_csv("foreground-simapro.csv")
print(f"Created {output}")
```

Use the background family, version, and system model that the source file actually
uses. BrightPath needs this information to interpret and validate its links.

### SimaPro CSV to Brightway Excel

The reverse workflow is just as short:

```python
from brightpath import BackgroundProfile, SimaProInventory

source_background = BackgroundProfile("ecoinvent", "3.10", "cutoff")

inventory = SimaProInventory.from_csv(
    "foreground-simapro.csv",
    background_profile=source_background,
)

output = inventory.to_brightway().write_excel("foreground-brightway.xlsx")
print(f"Created {output}")
```

Writing validates the inventory by default. If BrightPath finds a blocking problem,
it stops instead of producing a file that looks successful.

For production workflows that need dry runs, audit reports, or detailed control over
information loss, use the [command-line workflow](docs/workflows/cli.rst) or the
[conversion pipeline](docs/workflows/conversion.rst).

## Inspect, validate, and convert from the command line

Brightway Excel files previously written by BrightPath contain their format and
background context. For those files, the basic commands are compact:

```bash
# See what is in the file
brightpath inspect foreground.xlsx

# Check the inventory without changing it
brightpath validate foreground.xlsx

# Preview a conversion without writing the output
brightpath convert-format foreground.xlsx foreground.csv \
  --target-format simapro_csv \
  --dry-run

# Perform the conversion and save an audit report
brightpath convert-format foreground.xlsx foreground.csv \
  --target-format simapro_csv \
  --report conversion-report.json
```

For a file created elsewhere, state its context explicitly. This example converts a
Brightway Excel inventory linked to ecoinvent 3.10 cut-off:

```bash
brightpath convert-format foreground.xlsx foreground.csv \
  --source-format brightway_excel \
  --source-technosphere-family ecoinvent \
  --source-technosphere-version 3.10 \
  --source-technosphere-system-model cutoff \
  --source-biosphere-family ecoinvent \
  --source-biosphere-version 3.10 \
  --target-format simapro_csv \
  --report conversion-report.json
```

Why so explicit? A `.csv` file can be either Brightway or SimaPro, and similar names
can refer to different background releases. Explicit inputs make mistakes visible.

### More conversion recipes

Convert a Brightway block CSV file to the equivalent tab-separated format:

```bash
brightpath convert-format foreground.csv foreground.tsv \
  --source-format brightway_csv \
  --source-technosphere-family ecoinvent \
  --source-technosphere-version 3.10 \
  --source-technosphere-system-model cutoff \
  --source-biosphere-family ecoinvent \
  --source-biosphere-version 3.10 \
  --target-format brightway_tsv
```

Convert a UVEK 2025 Brightway workbook to an openLCA JSON-LD package:

```bash
brightpath convert-format foreground-uvek.xlsx foreground-uvek.zip \
  --source-format brightway_excel \
  --source-technosphere-family uvek \
  --source-technosphere-version 2025 \
  --source-technosphere-system-model cutoff \
  --source-biosphere-family ecoinvent \
  --source-biosphere-version 3.10 \
  --target-format openlca_jsonld \
  --report openlca-conversion-report.json
```

The openLCA output is a process-only exchange package. It references compatible
background entities but does not copy the UVEK background database into the ZIP.

## Update ecoinvent links without changing format

This example updates both technosphere and biosphere links from ecoinvent 3.10 to
3.11 and writes another Brightway workbook:

```python
from brightpath import (
    BackgroundContext,
    BackgroundProfile,
    BiosphereProfile,
    BrightwayInventory,
    TechnosphereProfile,
)

inventory = BrightwayInventory.from_excel(
    "foreground-ei310.xlsx",
    background_profile=BackgroundProfile("ecoinvent", "3.10", "cutoff"),
)

target_background = BackgroundContext(
    technosphere=TechnosphereProfile("ecoinvent", "3.11", "cutoff"),
    biosphere=BiosphereProfile("ecoinvent", "3.11"),
)

migrated = inventory.migrate_background(target_background)
migrated.write_excel("foreground-ei311.xlsx")
```

Migration uses a strict policy by default: incomplete, ambiguous, lossy, or unsafe
changes stop the operation. A permissive review mode is available, but its output is
not automatically scientifically valid. See the [migration guide](docs/workflows/migration.rst)
before using a migrated inventory in an assessment.

### Relink an ecoinvent foreground to UVEK 2025

The same migration workflow can relink a foreground inventory from ecoinvent to
UVEK. This example starts with ecoinvent 3.11 cut-off and keeps the Brightway Excel
format:

```python
from brightpath import (
    BackgroundContext,
    BackgroundProfile,
    BiosphereProfile,
    BrightwayInventory,
    TechnosphereProfile,
)

inventory = BrightwayInventory.from_excel(
    "foreground-ei311.xlsx",
    background_profile=BackgroundProfile("ecoinvent", "3.11", "cutoff"),
)

uvek_background = BackgroundContext(
    technosphere=TechnosphereProfile("uvek", "2025", "cutoff"),
    biosphere=BiosphereProfile("ecoinvent", "3.10"),
)

uvek_inventory = inventory.migrate_background(uvek_background)
output = uvek_inventory.write_excel("foreground-uvek-2025.xlsx")
print(f"Created {output}")
```

UVEK 2025 uses ecoinvent 3.10 biosphere identities, which is why the target combines
a UVEK technosphere with an ecoinvent biosphere. The ecoinvent-to-UVEK activity
mapping is heuristic: it finds compatible UVEK activities but does not claim that
they are scientifically equivalent. The rules and selected matches are still being
refined and may change between BrightPath releases. Record the package version and
review the migration report before using the result:

```python
for issue in uvek_inventory.last_migration_report.issues:
    print(issue.severity.value, issue.message)
```

## Supported formats and backgrounds

### File formats

| Format | Read | Write | Notes |
|---|:---:|:---:|---|
| Brightway Excel | ✓ | ✓ | `bw2io`-compatible workbooks |
| Brightway block CSV/TSV | ✓ | ✓ | Content is inspected because CSV extensions are ambiguous |
| SimaPro CSV | ✓ | ✓ | Semicolon-delimited, Latin-1 exchange files |
| openLCA JSON-LD ZIP | ✓ | ✓ | Process-only exchange packages |

OpenLCA Excel and ecoSpold2 are not currently supported.

### Background data

- Exact reference catalogs are available for ecoinvent 3.6–3.12 and UVEK 2025.
- Packaged ecoinvent cut-off migrations connect releases from 3.5 through 3.12.
- Reverse migrations are inferred and require permissive review.
- A heuristic ecoinvent-to-UVEK 2025 route is available and must be reviewed.
- Consequential version-to-version and cross-system-model migrations are not available.

Support is discovered from the installed package, so `brightpath formats` is the
authoritative list for your installation.

## Where to go next

- [Getting started](docs/getting-started.rst) — installation and a complete first workflow
- [Command line](docs/workflows/cli.rst) — inspect, validate, convert, migrate, and save reports
- [Brightway workflows](docs/workflows/brightway.rst) — Excel, CSV, TSV, and Python usage
- [SimaPro workflows](docs/workflows/simapro.rst) — reading, validation, categories, and writing
- [Conversion guide](docs/workflows/conversion.rst) — format changes and information-loss policies
- [Migration guide](docs/workflows/migration.rst) — background updates and review policies
- [Validation guide](docs/workflows/validation.rst) — understand and resolve reported issues
- [API reference](docs/api.rst) — complete Python interface

## Good to know

- BrightPath is a work in progress; pin the version used for reproducible workflows.
- Strict mode is the default for conversions and migrations.
- `BAFU` is accepted as an old input name; new output uses `UVEK`.
- Packaged migration resources include source and license metadata, but no proprietary
  ecoinvent inventories. See the [data attribution](brightpath/data/migrations/ATTRIBUTION.md).
- The packaged reference-catalog manifest still carries a `legal_review_required`
  release-governance marker.

## Development

```bash
python -m pip install -e ".[dev,docs]"
python -m pytest
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidelines and
[CHANGELOG.md](CHANGELOG.md) for release notes.

## License

BrightPath source code is distributed under the [BSD-3-Clause license](LICENSE).
Packaged migration resources retain the licenses declared in their files.
