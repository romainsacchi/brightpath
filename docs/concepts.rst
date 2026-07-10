Core concepts
=============

Exact inventory context
-----------------------

BrightPath makes software and background identity explicit:

.. list-table::
   :header-rows: 1
   :widths: 28 34 38

   * - Value object
     - Example
     - Responsibility
   * - ``FormatProfile``
     - ``brightway_excel`` with the ``bw2io`` dialect
     - File grammar, dialect, version, and encoding
   * - ``TechnosphereProfile``
     - ecoinvent 3.10.1 cut-off
     - Exact activity database and system model
   * - ``BiosphereProfile``
     - ecoinvent 3.10.1
     - Exact elementary-flow database
   * - ``BackgroundContext``
     - One technosphere and one biosphere profile
     - Background identity selected for an operation
   * - ``InventoryContext``
     - Format plus background
     - Complete source or target context
   * - ``ContextHint``
     - Format only, background only, or complete
     - Partial information at detection and parse boundaries

``BAFU`` is normalized to ``uvek`` only at an input boundary. UVEK is not tied
to SimaPro: it can be the technosphere context of any registered software
format. The biosphere remains explicit; for example, the current UVEK 2025
resources use ecoinvent 3.10 biosphere identities.

Exact versions and migration series
-----------------------------------

Profiles retain exact version strings. A resolver can map an ecoinvent patch
release to the major/minor resource series without changing the profile:

.. code-block:: python

   from brightpath import TechnosphereProfile

   profile = TechnosphereProfile("ecoinvent", "3.10.1", "cutoff")
   resolution = profile.resolve_migration_series()

   assert profile.version == "3.10.1"
   assert resolution.exact_version == "3.10.1"
   assert resolution.migration_series == "3.10"
   assert resolution.strategy == "ecoinvent-major-minor"

This distinction matters because migration resources may exist for a series
while an exact validation catalog does not. A series match never fabricates an
exact catalog or silently relabels output.

Independent operations
----------------------

.. list-table::
   :header-rows: 1
   :widths: 28 24 24 24

   * - Operation
     - Format
     - Background
     - Source
   * - ``pipeline.detect()``
     - Observed only
     - Unchanged
     - Unchanged
   * - ``pipeline.read()``
     - Selected explicitly or by content
     - Supplied or embedded
     - Unchanged
   * - ``pipeline.normalize()``
     - Preserved
     - Preserved
     - Unchanged
   * - ``pipeline.validate()``
     - Preserved
     - Preserved
     - Unchanged
   * - ``pipeline.convert()``
     - Changed explicitly
     - Preserved
     - Unchanged
   * - ``pipeline.migrate()``
     - Preserved
     - Changed explicitly
     - Unchanged
   * - ``pipeline.write()``
     - Explicit target or document format
     - Preserved
     - Unchanged

Canonical inventory
-------------------

Readers produce a versioned software-neutral canonical inventory. Datasets,
exchanges, parameters, identities, and uncertainty have typed canonical
models. The v1 facades still expose Brightway-style dictionaries through
``InventoryDocument.data`` for compatibility.

Unknown fields are retained under source namespaces during canonicalization,
then merged back through the legacy dictionary bridge. Writers must report
features that their target grammar cannot represent instead of silently
discarding them.

``InventoryDocument`` deep-copies inputs and returns copies from its public
dictionary properties. Normalization and migration construct new documents.
Migration execution is transactional and returns the source document when an
error-policy condition prevents commit.

``InventoryDocument.inventory_format`` is a compatibility projection. Known
identifiers return their historical ``InventoryFormat`` enum member; a custom
adapter identifier returns as a string. Custom formats therefore do not need
to modify the enum before participating in the canonical model.

Adapter capabilities and detection
----------------------------------

The adapter registry, not the ``InventoryFormat`` enum, defines executable
support. The built-in registry contains four file adapters:

.. list-table::
   :header-rows: 1
   :widths: 32 17 17 17 17

   * - Format identifier
     - Detect
     - Read
     - Write
     - Facade
   * - ``brightway_excel``
     - Yes
     - Yes
     - Yes
     - Brightway
   * - ``brightway_csv``
     - Yes
     - Yes
     - Yes
     - Pipeline
   * - ``brightway_tsv``
     - Yes
     - Yes
     - Yes
     - Pipeline
   * - ``simapro_csv``
     - Yes
     - Yes
     - Yes
     - SimaPro

Adapters probe bounded content: OOXML structure and workbook labels for
Brightway Excel, block grammar and delimiters for Brightway CSV/TSV, and
SimaPro signatures, headers, process labels, and sections for SimaPro CSV.
Detection returns evidence and confidence. It reports absent, weak, or tied
evidence instead of choosing a CSV interpretation by suffix.

Qualified descriptors use exact-then-generic lookup. An exact version/dialect
adapter wins. If it is absent, one registered unqualified adapter for the same
format family is the fallback. An unqualified request with no generic adapter
can match multiple qualified adapters and must then be disambiguated.

Every production adapter owns ``validate_format`` and
``preflight_conversion`` hooks. Source-format validation is independently
callable and conversion preflight reports target representability before the
format context changes. Missing, failing, or malformed hooks are explicit
contract errors. ``AdapterCapabilities.requires_catalog_provider`` lets a
reader request the pipeline's exact catalog provider; the SimaPro adapter uses
this dependency for biosphere-name normalization.

OpenLCA Excel and ecoSpold2 are reserved identifiers and extension namespaces,
but no built-in adapter advertises them. ``brightpath formats`` is the
authoritative installed-capability view.

Policies
--------

Conversion and migration use immutable explicit policies. Strict policies are
the defaults.

``ConversionPolicy`` controls unsupported features, information loss,
ambiguous mappings, invalid target representation, and target validation.
``on_ambiguous_mapping`` is applied to findings such as conflicting SimaPro
``product`` and ``reference product`` aliases. When ``validate_target`` is
true, conversion changes the format context and then invokes the target
adapter's ``validate_format`` hook; ``on_invalid_target`` controls any errors
from that stage.
``MigrationPolicy`` additionally controls invalid source links, unresolved
links, ambiguous rules, deletions, inferred reverse routes, unit changes
without numeric factors, target validity, and minimum coverage.

Permissive policies downgrade unsafe conditions to warnings and continue when
possible. They do not hide issues or certify the output.

Reports and audit
-----------------

Every pipeline operation returns ``OperationResult`` with an immutable
``OperationReport``. Reports are grouped into ordered stages and expose:

* ``Issue`` for validation or operation conditions;
* ``Change`` for non-lossy transformations;
* ``Loss`` for explicit information loss;
* immutable JSON-compatible metrics and metadata.

Reports have deterministic ``to_json()`` and ``from_json()`` round trips.
Audit sidecars are written atomically and can include SHA-256 digests and sizes
for input and output artifacts.
