Limits and compatibility
========================

Version 1 compatibility
-----------------------

Version 1 intentionally removed the 0.x ``BrightwayConverter`` and
``SimaproConverter`` classes. They coupled source format, target format, and
background behavior. No compatibility wrapper restores that assumption.

The v1 ``BrightwayInventory`` and ``SimaProInventory`` facades remain, including
the technosphere-only ``BackgroundProfile`` argument. New application code
should use ``InventoryContext`` and ``InventoryPipeline`` to express exact
format, technosphere, biosphere, policies, and injected catalogs.

``brightpath.migrations.migrate_inventory`` is not a public v1 export. Its
legacy engine implementation remains internal for focused compatibility tests;
public migration uses the transactional executor or facade methods so source
and target validation cannot be bypassed.

Format boundaries
-----------------

* The built-in registry supports Brightway Excel, Brightway block CSV,
  Brightway block TSV, and SimaPro CSV as file detection/read/write adapters.
* Only Brightway Excel and SimaPro CSV have dedicated v1 facades. Brightway
  CSV/TSV use the generic pipeline.
* CSV suffixes are ambiguous. Detection inspects content and reports absent or
  tied evidence; callers should pass an explicit format when known.
* SimaPro CSV is semicolon-delimited and Latin-1 encoded. It has stricter unit,
  category, section, and field representation than the canonical inventory.
* OpenLCA Excel and ecoSpold2 are reserved identifiers/namespaces only. They
  have no registered adapter and do not appear in ``brightpath formats``.
* Custom format identifiers are valid canonical context values.
  ``InventoryDocument.inventory_format`` returns their string ID while
  retaining enum members for known legacy IDs. A custom format is executable
  only when its adapter is registered.
* Every registered writer must implement adapter-owned format validation and
  conversion preflight. A missing or invalid hook fails safely.
* BrightPath writes exchange artifacts. It does not install databases into
  Brightway, SimaPro, or another LCA application.

Migration boundaries
--------------------

* Packaged ecoinvent cut-off technosphere edges cover 3.5→3.12.
* Packaged ecoinvent biosphere edges cover 3.5→3.11. The missing 3.11→3.12
  biosphere resource prevents a complete migration that changes both
  components to 3.12.
* Reverse routes are inferred from forward data and fail under strict policy.
  Permissive policy records the inference and all known losses.
* Forward resources can contain deletion rules. Strict policy rejects a route
  with policy-controlled deletion risk; permissive mode is an explicit review
  choice.
* Rules that change units without numeric conversion factors are skipped and
  reported. BrightPath never changes a unit while retaining an unconverted
  amount.
* Consequential, cross-system-model, ecoinvent↔UVEK, and UVEK-version
  migrations are unavailable.
* The ecoinvent-to-UVEK resource is a placeholder excluded from discovery. It
  is never interpreted as an empty successful mapping.

Exact version boundaries
------------------------

Exact patch versions are preserved. ecoinvent 3.10.1 can resolve to migration
series 3.10 for route planning, but it remains 3.10.1 in context and output.
Strict endpoint validation requires exact 3.10.1 catalogs; packaged 3.10
catalogs are not substituted.

Two distinct versions within the same migration series cannot be migrated
because no resource establishes their equivalence. Series resolution is a
resource lookup rule, not semantic version compatibility.

Validation boundaries
---------------------

Background validation performs exact identity matching, not fuzzy semantic
linking. Technosphere identity is name, reference product, location, and unit;
biosphere identity is name, categories, and unit. External foreground targets
must be declared explicitly.

Catalog absence and catalog integrity failures are reported separately from
unresolved links. A catalog is loaded only when links on its axis need it; an
inventory with no such links can report ``not_required``.

SimaPro parsing is an exception to lazy link-validation loading: its adapter
requires the exact biosphere catalog while reading so it can normalize flow
names against the declared profile. Direct facade reads use an explicit
provider or the application default; pipeline reads use the injected provider.

System-model catalog files may contain complementary biosphere shards. The
directory provider unions them only after validating every resource and a
common schema version; a corrupt shard invalidates the combined catalog.

The packaged catalogs cover ecoinvent 3.6–3.12 and UVEK 2025, while migration
resources begin at ecoinvent 3.5. Strict 3.5 execution therefore needs a custom
exact catalog provider.

Canonical and round-trip boundaries
-----------------------------------

Unknown fields are retained in canonical source namespaces, but a target file
can preserve only what its grammar supports. SimaPro cannot represent arbitrary
canonical metadata. Conversion and write reports identify known unsupported or
unused features, but no generic checker can prove semantic equality for an
uninterpreted vendor extension.

Normalization and migration copy source data. Reports are immutable and JSON
serializable, but v1 facade ``ValidationReport`` and upload ``AnalysisResult``
remain mutable compatibility projections.

Data and licensing
------------------

Premise migration resources retain their declared attribution and CC-BY-4.0
metadata. BrightPath does not package complete proprietary ecoinvent
inventories.

The packaged identity-catalog manifest has status ``legal_review_required``.
It records integrity and provenance but is not a license grant. Redistribution
approval or separately licensed/local provider data is a mandatory gate before
a stable public release.
