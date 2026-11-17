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
SimaPro CSV, openLCA JSON-LD, single-output ecoSpold1 XML, and single-output
ecoSpold2 packages, and ILCD 1.1 single-reference process packages.
ILCD retains per-process XML/reference closures for independently stored CLIC
snapshots and separates core from openLCA uncertainty conventions explicitly.
Its openLCA export derives used references from exact background UUID catalogs;
the pipeline injects its catalog provider for both preflight and writing.
``InventoryFormat`` also reserves OpenLCA Excel, but enum
membership does not confer capability; no adapter means a format is
undiscoverable and unsupported.

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
and UVEK-version migration remain unavailable. A
separate ecoinvent 3.6–3.12 to UVEK 2025 compatibility resource is explicitly
heuristic, records a warning and loss, and is verified against the exact UVEK
target catalog before commit.

Reviewed UVEK-to-ecoinvent cut-off exports use an independent directional
resource and foreground recipes. Both UVEK directions share the named v3
migration schema; standard ecoinvent files normalize into that representation
without changing their on-disk attribution or rule bodies. See
:doc:`migration-resource-schema`. Amount-factor execution shares uncertainty
and formula scaling across the two directions and rolls back unsupported cases.

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
   * - ``brightpath.migrations.schema`` / ``resources``
     - Common migration representation, validation, serialization and loading
   * - ``brightpath.migrations.scaling``
     - Shared quantity, uncertainty and formula scaling
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

OpenLCA Excel should remain unregistered until these contracts are met; a
reserved identifier or empty adapter is not partial support.

Adding migration data
---------------------

New resources belong under ``brightpath/data/migrations`` and need exact source
and target context, schema status, generator/version, provenance, SPDX license,
input digests, payload checksum, and tests in both directions. Placeholder and
draft data must remain excluded from capability discovery.

Every route needs independent technosphere/biosphere coverage and exact target
catalog validation. Never copy proprietary background inventories into the
repository.

Required UVEK inventory reviews can depend on component mappings created late
in the maintainer generator. Their final authoring stage is data driven:
``data/export/uvek_required_inventory_reviews.json`` lists the packaged review
files and their checksums. ``scripts/review_uvek_required_inventories.py`` checks
parent rules, exact source profiles, component-rule fingerprints and borrowed
recipe checksums before applying the reviews atomically. It validates the final
migration schema and supports identical replay. This stage changes generated
mapping data; it does not add a runtime conversion path or permit conversions
between system models.

Some required-inventory evidence also contains ``additional_approvals`` for
unchanged correspondences. These are caller-owned scientific decisions, not
instructions to compile another rule or infer permissions for other releases.
Premise binds each decision to its exact source/target context and fingerprint,
and carries its limitations into fresh and cached conversion provenance.

Names and comments alone do not establish a quantity or composition basis.
The source-3.8 truck evidence in
``data/export/uvek_trucks_native_40_review.json`` records four examples:
nitric acid labelled as a 50% solution still references one kg of pure acid;
the inspected glass-filled-polyamide inventories already use Nylon 66 despite
a stale melamine comment; butyl acetate has an inconsistent CAS field, while
its formula, route and actual reactants identify the named compound; and
ordinary regional diesel contains biodiesel whereas the separately named
fossil supply does not. Preserve these discrepancies and functional screening
limitations in review evidence. Do not invent concentration factors, change a
correct inventory to match stale prose, identify a compound by an inconsistent
CAS, or silently replace fossil supply with a blend. None of these checks
establishes source/native LCIA or functional-performance equivalence.

Market recipes can also vary between source releases even when names and units
match. ``data/export/uvek_trucks_five_markets_38_review.json`` keeps the inspected
Swiss gas reference at 39 MJ per source cubic metre in ecoinvent 3.8, while the
existing source-3.9 recipe remains at 36 MJ. Neither number establishes an equal
standard-volume or carbon-content basis. The complete native delivered-fuel
inventory supplies network losses; do not add source loss or combustion burdens
again. The same review keeps pure-kg KOH shares without a second concentration
correction, retains source-specific delivery uncertainty, and requires nonempty
WEEE shredding rather than an empty recycling handover. It reuses an existing
wafer recipe only after checking every coefficient and metadata field; its two
adjacent-float uncertainty-scale differences are explicitly recorded.

The source-3.8 recycling transport review selects complete native diesel
services with the source EURO standard and gross-weight band. Its new mixtures
condition and normalize only positive native size/duty weights, never caller
tonne-kilometres. The native Swiss-2020 fleet evidence, including under RER
labels, remains a historical screening proxy. The 7.5t model represents the
3.5--7.5t band, 18/26/32t models represent 16--32t, and a 40t model approximates
the open-ended >32t class. Review evidence records those limits, matching nominal
2025/2026 subsets, excluded propulsion/standards and preserved older physical
mappings. Repeated source countries share the same reviewed mixture. The
Chinese rail component is explicitly reserved for the required parent recipes;
it does not grant a global source-3.8 rail correspondence or change the existing
source-3.9 dolomite dependency. See
``data/export/uvek_recycling_transport_38_review.json``.

The recycling material review keeps source-market delivery and producer entries
separate while using complete native production models. Soap retains its common
13% moisture basis; injection moulding and film extrusion retain their common
processing bases of 0.994 and 0.976 kg output per kg service. None needs a second
yield or moisture multiplier. The plated chromium-steel sheet remains per square
metre, with its source mass of 15.6365 kg/m2; do not turn that into an additional
area/mass correction. Corrugated boxes use complete packaging rather than base
paper. The reserved acid-market recipe has no global source-3.8 permission and
leaves the existing manganese dependency unchanged. Full original market vectors
remain local; checksums support licensed reproduction of the derived native
recipes. See ``data/export/uvek_recycling_materials_38_review.json``.

The required source-3.8 truck recycling parents use complete Swiss mechanical
recycling of the same recycled HDPE and amorphous-PET products. Original market
shares and final delivery remain separate; Swiss operation, yield, utilities,
residues and emissions replace the other regions' production as explicit
geographic and operating-data proxies. Collection, sorting, washing, containers,
infrastructure and paid processing remain nonempty. Thirty closed helper recipes
retain heat portfolios, uncontrolled waste-treatment tails and independent
biosphere flows. Negative treatment references change sign once. Five verified
empty cut-off stocks and two zero-allocated waste-wood heat outputs retain their
explicit zero authority, without zeroing nonempty treatment. Existing stock
bindings and historical-model audit contexts are reused; four inherited tiny
HDPE-supply omissions retain their prior review. The original source component
vectors remain local; public data contain derived native-facing recipes and
identity/reference/hash witnesses. See
``data/export/uvek_recycling_ch_38_review.json``. These mappings do not qualify
the complete truck sector or establish source/native impact equivalence.

Nine ecoinvent-3.12 truck interfaces reuse existing complete native maintenance,
disposal and wear-emission services. Their independently inspected unit
references retain positive maintenance quantities and the negative-source to
positive-native treatment convention. Only exact approval evidence is added;
transformation rules, helpers and runtime behavior remain unchanged. Vehicle
design/lifetime, recovery, wear composition, release compartments and vintage
differences remain explicit screening limitations. See
``data/export/uvek_trucks_native_services_312_review.json``.

Thirteen source-3.12 truck material markets retain original producer entries
and delivery with complete native suppliers. Salt, fluorspar and pig-iron
coproduct quantities remain separate, while native production replaces their
allocated inventories. Flexible-foam MDI/TDI/density differences remain explicit
whole-product proxies. Ferrochromium and fluorspar retain their stated alloy or
purity mass basis; the 6.34g transistor remains referenced per kg. The nominal-kg
native stibnite proxy does not establish the source concentrate assay or equate
Antimony and Stibnite resource definitions. Existing approved physical targets,
factors and recipes remain unchanged; the unavailable licensed source-3.6 salt
record is carried forward without claiming fresh extraction. See
``data/export/uvek_trucks_materials_312_review.json``.

Three source-3.12 waste markets retain all twelve original treatment shares
and their separate road delivery. Thirty-three uncontrolled-treatment models
retain the inspected signed emissions through the existing ecoinvent-3.10
biosphere route. The fifteen unsanitary-landfill models also retain diesel
machinery and skid-steer excavation through complete native operations. Only
the municipal-incineration branch uses the corresponding complete native
incinerator. The polyurethane building-waste aggregate is excluded because its
deconstruction, construction sorting and disposal shares do not match these
markets. No new recovery credit, omission or zero-burden treatment is added.
The native glass incinerator's zero-water label conflicts with its documented
two-percent water composition; this remains an explicit screening limitation,
without an invented moisture correction. See
``data/export/uvek_trucks_waste_312_models_review.json`` and
``data/export/uvek_trucks_waste_312_review.json``.

The thirty-six closed recipes pass seventy-two signed LCI solves against
thirty-six independently reconstructed source/native demand graphs, without
writing a Brightway database. Earlier source-3.8 and source-3.9 physical waste
routes remain unchanged. These mapping checks do not by themselves qualify
Premise's complete truck transformation, regional consumer links or export.

The low-level legacy ``migrate_inventory`` function remains internal to the
migration engine and is not exported from ``brightpath.migrations``. Public
workflows use the transactional executor or facades so endpoint validation and
rollback cannot be skipped.

Two inspected source-3.9 markets shared by battery and final-energy preparation
retain their original producer entries and freight. The petroleum-refinery
pitch route uses complete native European refinery bitumen as a nominal-kg
production proxy, with explicit grade, allocation and refinery-vintage limits;
paper pitch dispersants and coal-tar routes are excluded. Nickel sulfate keeps
its two source supply shares. All 290 exchanges in its cobalt-production
coproduct inventory equal 0.11337491313401228 times the corresponding source
cobalt-metal inventory. That ratio scales the complete native cobalt burden,
without asserting physical cobalt consumption to synthesize nickel sulfate.
The native cobalt model's documented tailings and further-treatment-emission
exclusions remain explicit limitations. The separate chemical-production branch
retains its fifteen operations and eight signed elementary flows through
existing reviewed suppliers and canonical biosphere links. No hydration,
stoichiometric or assay correction is inferred, and cobalt sulfate is excluded
as a different chemical. See
``data/export/uvek_shared_pitch_nickel_39_review.json``. Four closed helpers,+eight signed LCIs and four independent native-demand comparisons establish
native-model consistency; they do not qualify the complete battery sector or
establish source/native impact equivalence.

Resource governance
-------------------

The packaged reference-catalog manifest is deliberately marked
``legal_review_required``. Its hashes and provenance establish integrity, not
redistribution permission. This is a stable-release gate documented in
:doc:`adr/0003-catalog-and-resource-governance`; maintainers must obtain legal
approval or move to separately licensed/local provider data before release.
