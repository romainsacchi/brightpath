Architecture
============

Pipeline
--------

BrightPath uses a canonical, dictionary-backed ``InventoryDocument`` between
format adapters and background services:

.. code-block:: text

   format reader
       -> InventoryDocument
       -> optional normalization
       -> read-only validation
       -> optional background migration
       -> explicit format writer

The canonical representation aligns with ``bw2io`` and the imported Premise
migration resources. ``InventoryDocument`` deep-copies input and returns copies
from its public data properties.

Module ownership
----------------

.. list-table::
   :header-rows: 1
   :widths: 34 66

   * - Module
     - Responsibility
   * - ``brightpath.brightway``
     - Public Brightway facade and workflow composition
   * - ``brightpath.simapro``
     - Public SimaPro facade and workflow composition
   * - ``brightpath.formats``
     - Syntax-only readers, renderers, and writers
   * - ``brightpath.profiles``
     - Background-family naming rules used by adapters
   * - ``brightpath.validation``
     - Read-only structural and reference-catalog checks
   * - ``brightpath.migrations``
     - Route resolution, rule application, and audit reporting
   * - ``brightpath.catalogs``
     - Exact background reference catalogs
   * - ``brightpath.analysis``
     - Fault-tolerant upload parsing, inference, and candidate summaries
   * - ``brightpath.models``
     - Neutral document, profile, format, issue, and result models

Invariants
----------

Maintainers should preserve these contracts:

* readers and writers never select a different background profile;
* migration never selects a different software format;
* validation never mutates, normalizes, migrates, or exports;
* transforming methods return new documents and facades;
* unknown canonical fields remain in the in-memory document, while each writer
  documents and tests the subset its target format can represent;
* unsupported routes fail explicitly rather than using empty or approximate
  mapping resources;
* format-specific rules remain in adapters or profile naming modules, not in
  migration code.

Adding a format adapter
-----------------------

OpenLCA Excel or ecospold2 support should follow the existing boundary:

1. Add syntax-only load and write functions under ``brightpath.formats``.
2. Parse into or render from ``InventoryDocument`` without changing its
   background profile.
3. Add a facade, or extend an existing facade only when the user-facing object
   genuinely represents that software format.
4. Keep format validation independently callable.
5. Test parsing, rendering, same-format round trips, conversion with unchanged
   profiles, and explicit combined migration/conversion.

Adding migration data
---------------------

New rules belong under ``brightpath/data/migrations`` with source, generator,
contributor, and license metadata. Resource loaders validate their shape.
Tests must cover each direction, disaggregation/aggregation behavior,
irreversible deletions, package installation, and exact target-catalog
validation. Proprietary source inventories must not be committed.
