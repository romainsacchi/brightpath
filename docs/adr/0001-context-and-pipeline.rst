ADR 0001: Context and pipeline
==============================

Status
------

Accepted for the BrightPath 1.0 pre-release series.

Decision
--------

BrightPath will preserve the exact declared software and background context.
An inventory context consists of a format profile, a technosphere profile, and
a biosphere profile. Exact database releases such as ecoinvent ``3.10.1`` are
not rewritten to ``3.10``. Catalog and migration-series resolution is a
separate, reported operation.

The canonical schema has its own version and stores software-specific values
in extension namespaces. Brightway-style dictionaries remain available
through transitional lossless bridges while existing facades are migrated.

All public workflows compose the same stages:

.. code-block:: text

   detect -> parse -> canonicalize -> normalize -> resolve context
          -> validate structure -> validate source format -> validate source links
          -> optional background migration -> validate target links
          -> optional target preflight -> format conversion
          -> validate target format -> render -> write

Format conversion and background migration remain separate calls. Validation
is read-only. Normalization performs only deterministic repairs; catalog-aware
reconciliation is explicit and reports every change.

Compatibility
-------------

``BrightwayInventory`` and ``SimaProInventory`` remain convenience facades over
the same document and focused services. New orchestration uses the generic,
dependency-injected pipeline. The deleted 0.x converter classes will not return.
Ambiguous files require an explicit format instead of a guessed default.
Adapter-owned format-validation and representability hooks are mandatory
contracts. The legacy non-transactional ``migrate_inventory`` function is not
exported from ``brightpath.migrations``.

BrightPath 1.0 supports Python 3.10 and 3.11. The codebase already uses Python
3.10 syntax, so claiming Python 3.9 support would be inaccurate.
