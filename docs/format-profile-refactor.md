# Format and Background Profile Architecture

## Status

Implemented for BrightPath 1.0. The API is intentionally incompatible with the deleted 0.x
converter classes. Brightway Excel and SimaPro CSV are adapters over the same inventory document
and background migration pipeline. OpenLCA Excel and ecospold2 remain future adapters.

## Problem

BrightPath 0.x modeled conversion as two source-specific workflows:

```text
Brightway -> SimaPro
SimaPro -> Brightway
```

Those workflows mixed four independent concerns:

- parsing or writing a software exchange format;
- validating inventory structure;
- identifying the linked background profile;
- transforming background links.

As a result, migrating a Brightway Excel inventory from ecoinvent 3.6 to 3.8 unnecessarily implied
SimaPro output, while UVEK behavior was exposed as a SimaPro-only database option.

## Decision

BrightPath uses this pipeline:

```text
source format + source background profile
    -> InventoryDocument
    -> optional normalization
    -> validation
    -> optional background migration
    -> target format
```

Format conversion and background migration are never implicit consequences of one another.

## Independent Axes

### File format

Current and planned format identifiers include:

- `brightway_excel`
- `brightway_csv`
- `brightway_tsv`
- `simapro_csv`
- future `openlca_excel`
- future `ecospold2`

An extension alone is not always sufficient for detection. CSV adapters must inspect content or
require an explicit format identifier when Brightway and SimaPro interpretations are both possible.

### Background profile

`BackgroundProfile` contains:

- `family`: `ecoinvent` or `uvek`;
- `version`: e.g. `3.10` or `2025`;
- `system_model`: e.g. `cutoff` or `consequential`.

`BAFU` is accepted only as a legacy alias for the canonical family name `UVEK`.

UVEK can be used with Brightway, SimaPro, or any future format. It is not a format-specific export
option.

## Core Inventory Model

`InventoryDocument` is the format-neutral boundary. It owns:

- Brightway-style dataset and exchange dictionaries;
- the source or current `BackgroundProfile`;
- the current `InventoryFormat` view;
- database metadata and parameters;
- migration report history.

The initial canonical representation remains dictionary-backed because `bw2io`, the Premise
migration resources, and existing inventories already share that representation. The document uses
copy-on-read and copy-on-write semantics so validation and transformation cannot mutate caller-owned
data.

Unknown fields remain in the dictionaries. Brightway Excel uses tagged JSON strings for nested
values that the generic workbook layout cannot otherwise represent, allowing BrightPath round trips
without silently dropping those fields.

## Format Adapters

Format adapters are responsible only for syntax and serialization:

```text
formats/brightway_excel.py
formats/simapro_csv.py
formats/openlca_excel.py     # future
formats/ecospold2.py         # future
```

A reader returns `InventoryDocument`. A writer receives `InventoryDocument`. Neither chooses or
changes the background profile.

User-facing `BrightwayInventory` and `SimaProInventory` classes are thin facades. They delegate
normalization, validation, migration, and writing to independent services. Calling `to_simapro()`
or `to_brightway()` changes the format view without changing the background profile.

## Validation

Validation is read-only and returns `ValidationReport` with structured `Issue` objects. The stages
are independently callable:

- canonical inventory structure;
- format-specific requirements;
- plausibility warnings;
- background catalog link compatibility.

Writers may validate before export, but validation does not export or normalize data. Callers can
disable background-link validation when only structural checks are needed.

## Ecoinvent Migration

BrightPath packages the Premise migration JSON resources rather than importing Premise's internal
`inventory_imports.py` module. This avoids a runtime dependency on Premise's unrelated database,
scenario, geography, and data-processing dependencies.

The migration engine:

1. normalizes patch versions to the migration graph's major/minor versions;
2. resolves the shortest deterministic route;
3. applies each edge in order;
4. reverses operation order on backward edges;
5. returns a structured report;
6. validates resulting links against the exact target catalog when requested.

Forward rules can replace identities and disaggregate one exchange into several allocated targets.
Backward routes reverse replacements and aggregate disaggregated targets.

### Loss policy

Bidirectional routing does not imply perfect reversibility:

- several source identities can map to the same target identity;
- aggregation cannot preserve all metadata from every split exchange;
- deleted biosphere exchanges cannot be reconstructed;
- some rules change units without providing amount conversion factors;
- no 3.11-to-3.12 biosphere migration resource is currently included.

These cases produce structured issues. They are never hidden in logger output. Target catalog
validation remains the final compatibility check.

Only cut-off technosphere migration resources are currently packaged. Consequential inventories can
be validated against their catalogs, but migration is rejected until consequential rules exist.

## UVEK Migration

UVEK 2025 is already a valid background profile for loading, validation, and same-profile writing.

No real ecoinvent-to-UVEK mapping is available yet. The package contains a resource marked
`status: placeholder` with empty rules to reserve the schema and location. It is not registered as a
route, and cross-family migration raises `MigrationUnavailableError`.

Future UVEK mappings belong under `data/migrations/uvek/` and must define direction, unit and amount
transformations, mapping provenance, and target-catalog validation behavior.

## Public API

The v1 APIs compose explicitly:

```python
from brightpath import BackgroundProfile, BrightwayInventory, SimaProInventory

inventory = BrightwayInventory.from_excel(
    "inventory.xlsx",
    background_profile=BackgroundProfile("ecoinvent", "3.6", "cutoff"),
)

normalized = inventory.normalize()
validation = normalized.validate()
migrated = normalized.migrate_background(
    BackgroundProfile("ecoinvent", "3.8", "cutoff")
)
migrated.write_excel("inventory-ei38.xlsx")

simapro = migrated.to_simapro()
simapro.write_csv("inventory-ei38.csv")

loaded_simapro = SimaProInventory.from_csv(
    "inventory-ei38.csv",
    background_profile=BackgroundProfile("ecoinvent", "3.8", "cutoff"),
)
loaded_simapro.to_brightway().write_excel("inventory-ei38-roundtrip.xlsx")
```

The 0.x `BrightwayConverter` and `SimaproConverter` modules are deleted. SimaPro parsing records
duplicate identities and system-model mismatches as structured validation issues. It does not reject
an otherwise parseable file before validation.

## Implementation Sequence

Completed:

1. Brightway Excel read, validate, normalize, migrate, and write.
2. SimaPro CSV read, validate, migrate, and write over `InventoryDocument`.
3. Analysis composition through the v1 SimaPro reader and validator.
4. Deletion of converter modules and implicit UVEK transforms.

Next adapters:

1. Brightway CSV/TSV as first-class facade inputs.
2. OpenLCA Excel.
3. ecospold2.

## Acceptance Criteria

- same-format, same-profile writing does not imply conversion;
- ecoinvent background migration works forward and backward with explicit reports;
- validation is read-only;
- source data is not mutated;
- UVEK is independent of file format;
- placeholder mappings cannot masquerade as successful migrations;
- new package data is present in both editable installs and built distributions;
- no proprietary ecoinvent inventory data is packaged.
