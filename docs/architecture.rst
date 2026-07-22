Architecture
============

System boundary
---------------

BrightPath separates syntax, canonical data, background identity, policy, and
orchestration:

.. code-block:: text

   artifact
      |
      v
   injected AdapterRegistry -- content detection / explicit format
      |
      v
   syntax-only format adapter
      |
      v
   CanonicalInventory + exact InventoryContext
      |
      +--> safe normalization
      +--> structural validation
      +--> adapter-owned intrinsic source-format validation
      +--> exact catalog validation (injected CatalogProvider)
      +--> background planner + transactional executor
      +--> adapter-owned target representability preflight
      |
      v
   explicit target adapter --> artifact + immutable audit report

No stage infers a target on another axis. A format adapter cannot migrate a
background, and a migration service cannot select an output format.

Context model
-------------

``InventoryContext`` contains:

* ``FormatProfile(format_id, format_version, dialect, encoding)``;
* ``BackgroundContext`` containing an exact ``TechnosphereProfile`` and exact
  ``BiosphereProfile``.

Technosphere includes family, exact version, and system model. Biosphere has
its own family and exact version. ``BAFU`` is accepted only at boundaries and
becomes ``uvek``. Exact patch versions are preserved; migration-series
resolution returns a separate ``VersionResolution`` audit value.

``ContextHint`` represents incomplete parse/detection input without inventing
missing axes. Operations that require a complete context call
``require_complete()`` or obtain embedded context from a reader.

Canonical schema
----------------

``CanonicalInventory`` is a versioned immutable owner of canonical datasets,
exchanges, parameters, uncertainty, metadata, and context. Unknown fields are
kept in immutable source-namespaced extension maps. Legacy Brightway-style
dictionary bridges remain because existing codecs and facades use that shape.

``InventoryDocument`` wraps this canonical model and provides copy-on-read
dictionary properties. Normalization and migration always work on copies.
Writers report unsupported target representation instead of silently dropping
canonical or extension data.

BW2IO ``input`` and ``output`` keys are reconstructible graph metadata. They
do not count as information loss or block strict round trips; other unknown
fields remain subject to target representability preflight.

The legacy ``inventory_format`` property returns ``InventoryFormat`` for known
IDs and a string for custom IDs. This keeps old comparisons stable without
making enum membership a prerequisite for an adapter.

Dependency injection
--------------------

``InventoryPipeline`` is an immutable orchestrator with two required
dependencies:

* ``AdapterRegistry`` defines executable format support;
* ``CatalogProvider`` supplies exact technosphere and biosphere catalogs.

There is no core global registry or provider. ``default_adapter_registry()``
and ``catalog_provider_from_environment()`` are application-boundary
constructors used by the CLI and examples. Tests and services can inject
independent registries and in-memory providers.

Adapter registry
----------------

Each adapter owns a ``FormatDescriptor``, ``AdapterCapabilities``, and
``detect/read/write/validate_format/preflight_conversion`` methods. Artifact
kinds are explicit and currently use files. Registry construction is immutable
and rejects conflicting descriptors. Read capability requires
``can_validate_format``; write capability requires both
``can_validate_format`` and ``can_preflight_conversion``. Declared contracts
must have callable hooks. These checks run during registry construction, so an
incomplete adapter cannot appear in capability discovery. Dispatchers still
reject failing hooks or malformed ``StageReport`` values at execution time.

``validate_format`` owns only the intrinsic grammar of a document already in
that format. ``preflight_conversion`` exclusively owns target
representability, information loss, and mapping ambiguity. Post-conversion
target validation cannot revise a preflight policy decision.

``requires_catalog_provider`` is an adapter read capability. The pipeline
injects its provider with ``setdefault``, allowing an explicit application
override in ``adapter_kwargs``. SimaPro declares this capability because its
reader resolves flow names against the exact biosphere profile.

Detection is content-based and produces candidates with confidence and
evidence. Brightway CSV and SimaPro CSV share a suffix, so the registry reports
no match or ambiguity rather than guessing. Passing an explicit format still
checks that the registered adapter supports the requested artifact kind.

Descriptor lookup is exact, then compatible generic. A registered qualified
version/dialect wins. If no exact descriptor exists, the registry considers a
generic descriptor only when its ``compatible_format_versions`` and
``compatible_dialects`` explicitly admit every requested qualifier. The
built-in Brightway Excel generic adapter admits only the ``bw2io`` dialect. An
unqualified request without a generic adapter may return multiple qualified
adapters and require disambiguation.

The built-in registry contains Brightway Excel, Brightway CSV, Brightway TSV,
and SimaPro CSV. ``InventoryFormat`` also reserves OpenLCA Excel and ecoSpold2,
but enum membership does not confer capability; no adapter means those formats
are undiscoverable and unsupported.

Background catalogs
-------------------

Technosphere and biosphere catalogs are independent typed values. Providers
must return the exact requested profile or raise an integrity error.

``DirectoryCatalogProvider`` validates the combined JSON payload and, when
present, its resource manifest. ``PackageCatalogProvider`` uses installed
package data. ``InMemoryCatalogProvider`` supports application stores and
tests. ``CompositeCatalogProvider`` implements ordered fallback.

Several system-model files can contain biosphere shards for the same exact
family/version. The directory provider validates every shard and a common
schema version, unions identities, and derives a deterministic composite
digest and source list. It does not require every system model to carry an
identical biosphere subset.

``brightpath.catalogs`` remains a compatibility bridge. Its combined-profile
APIs resolve through the independent provider stack and use the documented
legacy biosphere default. New code addresses each axis through
``CatalogProvider`` directly.

The ``BRIGHTPATH_REFERENCE_DIR`` environment variable is interpreted only by
``catalog_provider_from_environment()``. It composes the custom directory ahead
of packaged fallback; background validation itself never reads environment
state.

Background migration
--------------------

Planning and execution are independent:

``plan_background_migration()``
   Purely resolves technosphere and biosphere resource routes, records exact
   and series versions, applies reverse/loss policy, and returns an immutable
   ``MigrationPlan`` with a planning report.

``execute_background_migration()``
   Validates exact source links, plans, applies rules to copied dictionaries,
   matches biosphere sources by their complete ``(name, categories, unit)``
   identities, consults each step-target biosphere catalog to disambiguate
   partial targets and reverse rules, validates exact final-target links and
   coverage, and commits only when the selected policy allows it.

The executor never applies a unit-changing rule without an explicit numeric
factor. Reverse routes are inferred and policy-controlled. Deletion-rule
presence is planning metadata; deletion policy is applied only when execution
matches and removes an exchange. Rollback returns the source document and
removes uncommitted changes from the report.

Technosphere and biosphere resources connect adjacent ecoinvent release series
from 3.5→3.6 through 3.11→3.12. Consequential version-to-version, cross-model,
UVEK-to-ecoinvent, and UVEK-version migration remain unavailable. A
separate ecoinvent 3.6–3.12 to UVEK 2025 compatibility resource is explicitly
heuristic, records a warning and loss, and is verified against the exact UVEK
target catalog before commit.

Policies and reports
--------------------

``ConversionPolicy`` and ``MigrationPolicy`` are immutable and JSON
serializable. Strict is the default; permissive downgrades policy-controlled
conditions to warnings but preserves issues and losses.

Conversion preflight applies ``on_ambiguous_mapping`` separately from ordinary
information loss. After the format context changes, ``validate_target=True``
runs the selected adapter's intrinsic format-validation hook;
``on_invalid_target`` controls whether grammar errors abort, warn, or are
allowed. It cannot override representability, loss, or ambiguity decisions
made by preflight.

All pipeline methods return ``OperationResult`` with an immutable
``OperationReport``. A report contains deterministic ordered ``StageReport``
objects with ``Issue``, ``Change``, ``Loss``, metrics, and metadata. Report
JSON rejects unsupported values and non-finite floats. Atomic sidecars can
record artifact paths, sizes, and SHA-256 digests.

One exception hierarchy in ``brightpath.exceptions`` carries these shared
reports. Compatibility attributes such as an upload ``result`` or facade
``legacy_report`` do not define separate exception classes.

Module ownership
----------------

.. list-table::
   :header-rows: 1
   :widths: 37 63

   * - Module
     - Responsibility
   * - ``brightpath.core.context``
     - Exact format, technosphere, biosphere, and hint values
   * - ``brightpath.core.schema``
     - Canonical schema and legacy dictionary bridges
   * - ``brightpath.core.pipeline``
     - Dependency-injected orchestration
   * - ``brightpath.core.policies``
     - Explicit conversion and migration decisions
   * - ``brightpath.core.reports`` / ``audit``
     - Immutable findings, JSON, hashes, and atomic sidecars
   * - ``brightpath.adapters``
     - Adapter protocol, capabilities, registry, and built-ins
   * - ``brightpath.adapters.preflight``
     - Adapter-contract dispatch and built-in format/preflight rules
   * - ``brightpath.formats``
     - Syntax-only codecs
   * - ``brightpath.background.catalogs``
     - Exact typed catalog providers
   * - ``brightpath.background.validation``
     - Read-only exact background-link validation
   * - ``brightpath.background.migration``
     - Pure route and policy planning
   * - ``brightpath.background.execution``
     - Transactional rule execution and endpoint validation
   * - ``brightpath.brightway`` / ``simapro``
     - v1 convenience facades
   * - ``brightpath.analysis``
     - Fault-tolerant upload compatibility workflow
   * - ``brightpath.capabilities`` / ``cli``
     - Installed capability discovery and application entry point

Architecture invariants
-----------------------

Maintainers must preserve these contracts:

* format conversion preserves ``BackgroundContext``;
* background migration preserves ``FormatProfile``;
* targets are explicit and are not inferred from sources;
* validation is read-only and independently callable;
* caller-owned input and source documents are not mutated;
* adapters report unsupported representation and information loss;
* format validation owns intrinsic grammar while conversion preflight owns
  representability, loss, and ambiguity policy;
* validation stages remain ordered as structure, optional format, optional
  background links;
* exact versions are retained and series resolution is separately reported;
* capability discovery includes only registered code and non-placeholder data;
* unsupported routes produce structured errors rather than identity mappings;
* unknown canonical fields survive whenever the target format can represent
  them.

Adding a format adapter
-----------------------

1. Study and name the exact target dialect; do not use a generic software name
   for incompatible variants.
2. Add syntax-only parsing/rendering under ``brightpath.formats`` using a
   maintained domain parser where one exists.
3. Implement the adapter protocol, both report hooks, and precise artifact
   capabilities. Declare ``can_validate_format``, ``can_preflight_conversion``,
   any compatible qualifier allowlists, and any catalog-provider read
   dependency.
4. Add bounded content probing with evidence and ambiguity tests.
5. Add representability preflight and explicit ``Loss`` findings.
6. Register only after read, write, same-format round-trip, conversion,
   non-mutation, and independent fixture tests pass.
7. Update capability-driven docs and the CLI tests.

OpenLCA Excel and ecoSpold2 should remain unregistered until these contracts
are met; a reserved identifier or empty adapter is not partial support.

Adding migration data
---------------------

New resources belong under ``brightpath/data/migrations`` and need exact source
and target context, schema status, generator/version, provenance, SPDX license,
input digests, payload checksum, and tests in both directions. Placeholder and
draft data must remain excluded from capability discovery.

Every route needs independent technosphere/biosphere coverage and exact target
catalog validation. Never copy proprietary background inventories into the
repository.

The low-level legacy ``migrate_inventory`` function remains internal to the
migration engine and is not exported from ``brightpath.migrations``. Public
workflows use the transactional executor or facades so endpoint validation and
rollback cannot be skipped.

Resource governance
-------------------

The packaged reference-catalog manifest is deliberately marked
``legal_review_required``. Its hashes and provenance establish integrity, not
redistribution permission. This is a stable-release gate documented in
:doc:`adr/0003-catalog-and-resource-governance`; maintainers must obtain legal
approval or move to separately licensed/local provider data before release.
