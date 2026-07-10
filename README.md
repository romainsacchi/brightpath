# BrightPath

[![License: BSD 3-Clause](https://img.shields.io/badge/license-BSD--3--Clause-blue?style=flat-square)](https://opensource.org/license/bsd-3-clause)

BrightPath reads foreground life-cycle inventories, validates and normalizes
their datasets, optionally migrates their background links, and writes an
explicit LCA software exchange format.

The three concerns below are independent:

- **software format**, such as Brightway Excel or SimaPro CSV;
- **technosphere background**, including family, exact version, and system
  model;
- **biosphere background**, including family and exact version.

Changing a file format never changes background links. Migrating background
links never chooses a new file format. This allows a Brightway Excel workbook
linked to ecoinvent 3.6 to be migrated to ecoinvent 3.8 and written back as
Brightway Excel without producing SimaPro output.

The full Sphinx guide starts at [`docs/index.rst`](docs/index.rst).

## Current Support

| Capability | Status |
|---|---|
| Brightway Excel | Detect, read, normalize, validate, convert, write |
| Brightway block CSV/TSV | Detect, read, normalize, validate, convert, write |
| SimaPro CSV | Detect, read, normalize, validate, convert, write |
| ecoinvent technosphere migration | Cut-off edges 3.5→3.12; reverse is inferred and policy-controlled |
| ecoinvent biosphere migration | Edges 3.5→3.11; **3.11→3.12 is unavailable** |
| Reference catalogs | ecoinvent 3.6–3.12 cut-off/consequential and UVEK 2025 cut-off |
| UVEK | Valid in Brightway and SimaPro; `BAFU` accepted only as an input alias |
| OpenLCA Excel / ecoSpold2 | Structurally reserved, but no adapter is registered or advertised |

Run `brightpath formats` to discover capabilities from the installed adapters
and migration resources instead of relying on this static table.

## Installation

BrightPath supports Python 3.10 and 3.11.

```bash
python -m pip install brightpath
```

## Exact Inventory Context

New code should describe all axes with `InventoryContext`:

```python
from brightpath import (
    BackgroundContext,
    BiosphereProfile,
    FormatProfile,
    InventoryContext,
    TechnosphereProfile,
)

source_context = InventoryContext(
    format=FormatProfile("brightway_excel", dialect="bw2io"),
    background=BackgroundContext(
        technosphere=TechnosphereProfile("ecoinvent", "3.10", "cutoff"),
        biosphere=BiosphereProfile("ecoinvent", "3.10"),
    ),
)
```

Exact versions are preserved. Resolving migration resources is a separate,
auditable operation:

```python
patch = TechnosphereProfile("ecoinvent", "3.10.1", "cutoff")
resolution = patch.resolve_migration_series()
assert resolution.exact_version == "3.10.1"
assert resolution.migration_series == "3.10"
```

The target still needs an exact matching catalog. Mapping a patch release to a
migration series does not relabel the inventory or invent a validation
catalog. `BAFU` is normalized to `uvek` at the profile boundary, and `cut-off`
is normalized to `cutoff`.

`BackgroundProfile` remains a technosphere-only facade argument. Prefer the
exact context types above for pipeline and new application code.

## Build an Injected Pipeline

`InventoryPipeline` owns no process-global adapter registry or catalog
provider. Construct its dependencies at the application boundary:

```python
from brightpath import InventoryPipeline
from brightpath.adapters import default_adapter_registry
from brightpath.background import catalog_provider_from_environment

pipeline = InventoryPipeline(
    registry=default_adapter_registry(),
    catalog_provider=catalog_provider_from_environment(),
)
```

`catalog_provider_from_environment()` uses packaged catalogs and, when
`BRIGHTPATH_REFERENCE_DIR` is set, places that directory first with packaged
catalogs as fallback.

Each registered adapter owns two independently callable safety hooks:
`validate_format(document)` checks intrinsic grammar of a document already
declaring that format, while `preflight_conversion(document, policy=...)`
exclusively checks target representability, loss, and mapping ambiguity.
Readable/writable adapters must declare `can_validate_format` and writers must
also declare `can_preflight_conversion`; registry construction rejects missing
flags or non-callable hooks before capability discovery. Hook failures and
malformed reports remain explicit operation errors. Adapters can also declare
`requires_catalog_provider`; `InventoryPipeline.read()` injects its provider
for those readers. SimaPro uses this to normalize biosphere names against the
exact declared biosphere catalog.

Qualified format descriptors are resolved conservatively. An exact
`(format_id, version, dialect)` adapter wins. A generic adapter handles a
qualified request only when `compatible_format_versions` and
`compatible_dialects` explicitly allow every requested qualifier. The built-in
Brightway Excel adapter accepts the `bw2io` dialect and no other dialect. An
unqualified request is ambiguous when only multiple qualified adapters exist.

## Inspect and Validate

Content detection examines the artifact. A `.csv` suffix is never silently
treated as SimaPro: Brightway CSV and SimaPro CSV are distinguished by their
content, and absent or tied evidence is reported as an error. Supply an
explicit format when an intake boundary already knows it.

```python
from brightpath.core import ContextHint, FormatProfile

hint = ContextHint(
    format=FormatProfile("brightway_csv"),
    background=source_context.background,
)
read = pipeline.read("foreground.csv", hint=hint)
if not read.succeeded or read.value is None:
    raise RuntimeError(read.report.to_json(indent=2))

normalized = pipeline.normalize(read.value)
if normalized.value is None:
    raise RuntimeError(normalized.report.to_json(indent=2))

validation = pipeline.validate(
    normalized.value,
    check_format=True,
    check_background_links=True,
)
for issue in validation.report.issues:
    print(issue.severity.value, issue.stage.value, issue.code, issue.path)

if validation.error:
    raise RuntimeError("Inventory validation failed")
```

Validation is read-only and orders its stages as canonical structure, optional
source-format validation, then optional exact background-link validation.
`check_format=False` skips only the adapter hook;
`check_background_links=False` skips only catalog checks. Normalization returns
a copy. Caller-owned data and the source document are not mutated.

## Upload Analysis

Brightway upload analysis can still use a complete or partial legacy
`source_profile` and infer missing background fields from catalogs. SimaPro
analysis cannot: it requires an exact `InventoryContext` before parsing so the
reader never guesses technosphere, biosphere, or system model.

```python
from brightpath import (
    BackgroundContext,
    BiosphereProfile,
    FormatProfile,
    InventoryContext,
    TechnosphereProfile,
)
from brightpath.analysis import SOURCE_FORMAT_SIMAPRO_CSV, analyze_inventory
from brightpath.background import catalog_provider_from_environment

simapro_context = InventoryContext(
    format=FormatProfile("simapro_csv", encoding="latin-1"),
    background=BackgroundContext(
        technosphere=TechnosphereProfile("ecoinvent", "3.11", "cutoff"),
        biosphere=BiosphereProfile("ecoinvent", "3.11"),
    ),
)
analysis = analyze_inventory(
    path="foreground.csv",
    source_format=SOURCE_FORMAT_SIMAPRO_CSV,
    source_context=simapro_context,
    catalog_provider=catalog_provider_from_environment(),
)
```

Missing context is an inspectable result, not an attempted parse:

```python
missing = analyze_inventory(
    path="foreground.csv",
    source_format=SOURCE_FORMAT_SIMAPRO_CSV,
)
assert missing.inventory_data == []
assert missing.candidates == []
assert missing.file_issues[0].code == "simapro_source_context_required"
```

`validate_inventory()` accepts the same `source_context` and
`catalog_provider` arguments and raises the shared `InventoryValidationError`
when this structured error is present. Contradictory legacy
`source_profile` values return `simapro_source_profile_conflict`; catalog
construction or loading returns the structured
`simapro_biosphere_catalog_missing`, `simapro_biosphere_catalog_invalid`, or
`simapro_biosphere_catalog_failed` issue instead of attempting a parse.

## Migrate and Keep the Same Format

Migration changes both background components only when requested and leaves
the format context unchanged:

```python
from brightpath import BackgroundContext, BiosphereProfile, TechnosphereProfile
from brightpath.core import MigrationPolicy

target_background = BackgroundContext(
    technosphere=TechnosphereProfile("ecoinvent", "3.11", "cutoff"),
    biosphere=BiosphereProfile("ecoinvent", "3.11"),
)

migration = pipeline.migrate(
    normalized.value,
    target_background,
    policy=MigrationPolicy.strict(),
)
if not migration.succeeded:
    raise RuntimeError(migration.report.to_json(indent=2))

assert migration.value.context.format == normalized.value.context.format
written = pipeline.write(
    migration.value,
    "foreground-ei311.xlsx",
    sidecar=True,
)
if not written.succeeded:
    raise RuntimeError(written.report.to_json(indent=2))
```

Migration is transactional: an error-policy condition returns the unchanged
source document and records why the candidate was rolled back. Strict policy
requires valid source and target links, 100% coverage, and no inferred reverse,
ambiguous, deleted, lossy, or unsafe unit-change behavior. Permissive policy
turns these conditions into warnings and sets minimum coverage to zero:

```python
review_migration = pipeline.migrate(
    normalized.value,
    target_background,
    policy=MigrationPolicy.permissive(),
)
for loss in review_migration.report.losses:
    print(loss.code, loss.path, loss.message)
```

Permissive means “continue and report”; it does not establish scientific
validity.

## Convert Format Only

Format conversion preserves the complete background context:

```python
from brightpath.core import ConversionPolicy

conversion_policy = ConversionPolicy.strict()
converted = pipeline.convert(
    normalized.value,
    "simapro_csv",
    policy=conversion_policy,
)
if converted.value is None or not converted.succeeded:
    raise RuntimeError(converted.report.to_json(indent=2))

assert converted.value.context.background == normalized.value.context.background
output = pipeline.write(
    converted.value,
    "foreground.csv",
    target_format="simapro_csv",
    policy=conversion_policy,
    sidecar=True,
)
```

SimaPro preflight reports unsupported or unused exchanges, conflicting
`product`/`reference product` aliases, numeric or uncertainty transformations,
and other representability problems. `on_ambiguous_mapping` controls ambiguous
target mappings independently of other information loss. After changing the
format context, `validate_target=True` runs the target adapter's intrinsic
`validate_format` hook; `on_invalid_target` controls only those grammar
findings. Target validation cannot override a loss, representability, or
ambiguity decision already made by preflight.
`ConversionPolicy.permissive()` downgrades unsafe conditions to warnings but
never hides them. Set `validate_target=False` only when the caller will run
target-format validation separately.

Brightway/BW2IO `input` and `output` keys are reconstructible graph-link
metadata. Writers may regenerate them during import, so their presence does
not count as information loss and does not block strict Brightway or SimaPro
round trips.

## Migrate and Convert Explicitly

Compose the two operations when both axes change:

```python
migrated = pipeline.migrate(
    normalized.value,
    target_background,
    policy=MigrationPolicy.strict(),
)
if not migrated.succeeded:
    raise RuntimeError(migrated.report.to_json(indent=2))

simapro = pipeline.convert(
    migrated.value,
    "simapro_csv",
    policy=ConversionPolicy.strict(),
)
if simapro.value is None or not simapro.succeeded:
    raise RuntimeError(simapro.report.to_json(indent=2))

result = pipeline.write(
    simapro.value,
    "foreground-ei311.csv",
    target_format="simapro_csv",
    policy=ConversionPolicy.strict(),
    sidecar="foreground-ei311.audit.json",
)
```

No intermediate file is required, and neither operation infers the other.

## UVEK in Brightway and SimaPro

UVEK is a background family, not a SimaPro mode. The currently packaged UVEK
2025 catalog uses the ecoinvent 3.10 biosphere identities, which can be stated
directly:

```python
uvek_background = BackgroundContext(
    technosphere=TechnosphereProfile("uvek", "2025", "cutoff"),
    biosphere=BiosphereProfile("ecoinvent", "3.10"),
)

brightway_context = InventoryContext(
    format=FormatProfile("brightway_excel"),
    background=uvek_background,
)
uvek_read = pipeline.read(
    "foreground-uvek.xlsx",
    hint=brightway_context.as_hint(),
)

uvek_simapro = pipeline.convert(uvek_read.value, "simapro_csv")
assert uvek_simapro.value.context.background == uvek_background
pipeline.write(uvek_simapro.value, "foreground-uvek.csv")
```

The same background context can originate in SimaPro and be written as
Brightway. No ecoinvent↔UVEK migration rules exist; the packaged placeholder is
excluded from capability discovery and cannot produce a successful route.

## Custom Catalog Provider

Applications can inject exact catalogs without environment variables:

```python
from brightpath import InventoryPipeline
from brightpath.adapters import default_adapter_registry
from brightpath.background import (
    BiosphereCatalog,
    InMemoryCatalogProvider,
    TechnosphereCatalog,
)

provider = InMemoryCatalogProvider(
    technosphere=(
        TechnosphereCatalog(
            profile=source_context.background.technosphere,
            identities=frozenset(
                {
                    (
                        "market for electricity, low voltage",
                        "electricity, low voltage",
                        "CH",
                        "kilowatt hour",
                    )
                }
            ),
            source="application catalog",
        ),
    ),
    biosphere=(
        BiosphereCatalog(
            profile=source_context.background.biosphere,
            identities=frozenset(),
            source="application catalog",
        ),
    ),
)
custom_pipeline = InventoryPipeline(default_adapter_registry(), provider)
```

`DirectoryCatalogProvider`, `PackageCatalogProvider`, and
`CompositeCatalogProvider` cover filesystem, packaged, and fallback use cases.
Providers verify exact embedded profiles; directory providers also verify
manifest digests, sizes, schema versions, and identity counts. Biosphere
identities can be repeated or partitioned across system-model files for one
family/version; the directory provider validates compatible schema versions,
unions all shards, and records a deterministic composite digest.

The legacy `brightpath.catalogs` functions remain compatibility bridges. They
load through the independent provider stack and project a technosphere plus
the documented default biosphere into one `BackgroundCatalog`; new code should
use `CatalogProvider` directly when the axes differ.

## Command Line

```bash
brightpath formats

brightpath inspect foreground.csv \
  --source-format brightway_csv \
  --source-technosphere-family ecoinvent \
  --source-technosphere-version 3.8 \
  --source-technosphere-system-model cutoff \
  --source-biosphere-family ecoinvent \
  --source-biosphere-version 3.8

brightpath validate foreground.csv \
  --source-format brightway_csv \
  --source-technosphere-family ecoinvent \
  --source-technosphere-version 3.8 \
  --source-technosphere-system-model cutoff \
  --source-biosphere-family ecoinvent \
  --source-biosphere-version 3.8 \
  --json

brightpath convert-format foreground.xlsx foreground.csv \
  --source-format brightway_excel \
  --source-technosphere-family ecoinvent \
  --source-technosphere-version 3.8 \
  --source-technosphere-system-model cutoff \
  --source-biosphere-family ecoinvent \
  --source-biosphere-version 3.8 \
  --target-format simapro_csv \
  --dry-run \
  --report conversion.audit.json
```

`migrate-background` takes the five target background axes and preserves the
source format. All commands default to `--policy strict`; `--policy
permissive` is an explicit review mode. `--json` produces machine-readable
output, while `--report` atomically writes an immutable report sidecar with
SHA-256 digests for existing source/output artifacts. See
[`docs/workflows/cli.rst`](docs/workflows/cli.rst) for complete commands and
stable exit codes.

## Facade API

`BrightwayInventory` and `SimaProInventory` remain convenient v1 facades. Both
accept an exact `context=` and expose `.context`; their format conversion
methods preserve the background:

```python
from brightpath import BrightwayInventory

inventory = BrightwayInventory.from_excel(
    "foreground.xlsx",
    context=source_context,
)
simapro = inventory.to_simapro()
assert simapro.context.background == inventory.context.background
```

Direct SimaPro reads accept the same exact provider used by the pipeline:

```python
from brightpath import SimaProInventory
from brightpath.background import catalog_provider_from_environment

simapro_source = SimaProInventory.from_csv(
    "foreground.csv",
    context=InventoryContext(
        format=FormatProfile("simapro_csv", encoding="latin-1"),
        background=source_context.background,
    ),
    catalog_provider=catalog_provider_from_environment(),
)
```

The reader loads `context.background.biosphere` from that provider before
normalizing SimaPro flow names. If `catalog_provider` is omitted, the facade
uses the environment/package provider stack. The generic pipeline injects its
own provider because the SimaPro adapter declares that dependency.

The deleted 0.x `BrightwayConverter` and `SimaproConverter` classes are not
compatibility APIs. They coupled source, target format, and background
behavior. The old `brightpath.migrations.migrate_inventory` function is also
not a public export; use `InventoryPipeline.migrate()` or facade
`migrate_background()` so source/target validation and transactional rollback
cannot be bypassed.

`InventoryDocument.inventory_format` retains `InventoryFormat` enum values for
known identifiers and returns a custom adapter's identifier as a string. Enum
membership therefore remains a compatibility projection, not a support gate.

## Reports and Audit

Pipeline operations return `OperationResult[value]` with an immutable
`OperationReport`. Reports contain ordered immutable stage reports, issues,
non-lossy changes, explicit losses, metrics, and policy metadata. They support
deterministic JSON round trips:

```python
from brightpath.core import OperationReport

payload = validation.report.to_json(indent=2)
same_report = OperationReport.from_json(payload)
assert same_report == validation.report
```

`pipeline.write(..., sidecar=True)` writes `<output>.brightpath.json`.
`write_report_sidecar()` can record multiple precomputed artifact digests and
uses an atomic replacement; its parent directory must already exist.

## Important Limits

- The ecoinvent technosphere has a 3.11→3.12 rule, but no corresponding
  biosphere resource is packaged. A complete migration that changes the
  biosphere from 3.11 to 3.12 is unavailable.
- Reverse migration is inferred from forward resources and is blocked by
  strict policy. Permissive execution records aggregation, deletion,
  ambiguity, and information loss.
- Rules that change units without an explicit numeric factor are not applied;
  the skipped rule and loss are reported.
- Consequential, cross-system-model, ecoinvent↔UVEK, and UVEK-version
  migrations are not available.
- OpenLCA Excel and ecoSpold2 identifiers reserve future schema namespaces,
  but they are not registered adapters and do not appear in capability output.
- The packaged reference catalog manifest is marked
  `legal_review_required`. This is a release-governance gate: integrity and
  provenance are recorded, but the manifest is not a redistribution license.
  Legal approval or separately licensed/local catalogs are required before a
  stable public release.

## Migration Data

The packaged ecoinvent migration resources were imported from Premise and
retain source, generator, contributor, and CC-BY-4.0 metadata. See
[`brightpath/data/migrations/ATTRIBUTION.md`](brightpath/data/migrations/ATTRIBUTION.md).
Proprietary ecoinvent inventories are not included.

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
