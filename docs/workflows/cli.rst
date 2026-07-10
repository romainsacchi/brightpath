Command-line workflows
======================

The ``brightpath`` command exposes capability discovery, inspection,
validation, format conversion, and background migration. Commands default to
strict policy and never infer a target format or target background.

Discover installed capabilities
-------------------------------

.. code-block:: console

   brightpath formats
   brightpath formats --json

The result is generated from the installed adapter registry, non-placeholder
migration resources, and available catalog profiles. Reserved but
unimplemented formats such as OpenLCA Excel and ecoSpold2 do not appear.

Supply a source context
-----------------------

Every inventory operation accepts these source options:

.. code-block:: text

   --source-format FORMAT_ID
   --source-technosphere-family FAMILY
   --source-technosphere-version VERSION
   --source-technosphere-system-model MODEL
   --source-biosphere-family FAMILY
   --source-biosphere-version VERSION

The five background options are all-or-none. They may be omitted when the
artifact embeds a complete background context. An explicit ``--source-format``
is recommended at CSV intake boundaries; otherwise BrightPath probes content
and fails on absent or ambiguous evidence.

Inspect
-------

.. code-block:: console

   brightpath inspect foreground.csv \
     --source-format brightway_csv \
     --source-technosphere-family ecoinvent \
     --source-technosphere-version 3.8 \
     --source-technosphere-system-model cutoff \
     --source-biosphere-family ecoinvent \
     --source-biosphere-version 3.8

Inspection reports the resolved format, exact technosphere, exact biosphere,
dataset count, and exchange count without writing or migrating.

Validate
--------

.. code-block:: console

   brightpath validate foreground.tsv \
     --source-format brightway_tsv \
     --source-technosphere-family ecoinvent \
     --source-technosphere-version 3.8 \
     --source-technosphere-system-model cutoff \
     --source-biosphere-family ecoinvent \
     --source-biosphere-version 3.8 \
     --json \
     --report validation.audit.json

Validation checks canonical structure, the source adapter's format invariants,
and exact technosphere/biosphere links in that order. The JSON payload contains
separate read and validation reports. The audit file contains the combined
immutable operation report and SHA-256 digest of the source.

Convert format
--------------

Conversion requires an explicit registered target format and preserves the
complete background context:

.. code-block:: console

   brightpath convert-format foreground.xlsx foreground.csv \
     --source-format brightway_excel \
     --source-technosphere-family ecoinvent \
     --source-technosphere-version 3.8 \
     --source-technosphere-system-model cutoff \
     --source-biosphere-family ecoinvent \
     --source-biosphere-version 3.8 \
     --target-format simapro_csv \
     --policy strict \
     --report conversion.audit.json

Use ``--dry-run`` to run detection, parsing, adapter-owned conversion
preflight, and target-format validation without creating the destination. The
positional destination remains required so the same command can be rerun
without changing its shape:

.. code-block:: console

   brightpath convert-format foreground.xlsx foreground.csv \
     --source-format brightway_excel \
     --source-technosphere-family ecoinvent \
     --source-technosphere-version 3.8 \
     --source-technosphere-system-model cutoff \
     --source-biosphere-family ecoinvent \
     --source-biosphere-version 3.8 \
     --target-format simapro_csv \
     --dry-run --json

Migrate and preserve format
---------------------------

The target technosphere and biosphere are both explicit. This example reads
Brightway Excel linked to ecoinvent 3.10, migrates both background components
to 3.11, and writes Brightway Excel:

.. code-block:: console

   brightpath migrate-background foreground-ei310.xlsx foreground-ei311.xlsx \
     --source-format brightway_excel \
     --source-technosphere-family ecoinvent \
     --source-technosphere-version 3.10 \
     --source-technosphere-system-model cutoff \
     --source-biosphere-family ecoinvent \
     --source-biosphere-version 3.10 \
     --target-technosphere-family ecoinvent \
     --target-technosphere-version 3.11 \
     --target-technosphere-system-model cutoff \
     --target-biosphere-family ecoinvent \
     --target-biosphere-version 3.11 \
     --policy strict \
     --report migration.audit.json

``--dry-run`` executes the migration in memory, including source and target
validation, but does not write the destination. Migration is transactional:
when strict policy rejects a condition, the report records the rollback and no
changed inventory is committed.

Reverse routes are inferred from forward resources. Strict policy rejects
them. ``--policy permissive`` permits an inspectable review run and records
warnings/losses for reverse aggregation, ambiguity, deletions, and incomplete
coverage. It does not certify the output.

Migrate and then convert
------------------------

The CLI keeps these operations intentionally separate. First migrate to the
same software format, then convert that artifact:

.. code-block:: console

   brightpath migrate-background foreground-ei310.xlsx foreground-ei311.xlsx \
     --source-format brightway_excel \
     --source-technosphere-family ecoinvent \
     --source-technosphere-version 3.10 \
     --source-technosphere-system-model cutoff \
     --source-biosphere-family ecoinvent \
     --source-biosphere-version 3.10 \
     --target-technosphere-family ecoinvent \
     --target-technosphere-version 3.11 \
     --target-technosphere-system-model cutoff \
     --target-biosphere-family ecoinvent \
     --target-biosphere-version 3.11

   brightpath convert-format foreground-ei311.xlsx foreground-ei311.csv \
     --source-format brightway_excel \
     --source-technosphere-family ecoinvent \
     --source-technosphere-version 3.11 \
     --source-technosphere-system-model cutoff \
     --source-biosphere-family ecoinvent \
     --source-biosphere-version 3.11 \
     --target-format simapro_csv

Use the Python pipeline when an in-memory composition without an intermediate
artifact is required.

Machine output and audit reports
--------------------------------

All subcommands accept:

``--json``
   Print stable machine-readable output instead of the human summary.

``--report PATH``
   Atomically write the combined operation report. Existing source and output
   artifacts are recorded with absolute paths, byte sizes, and SHA-256 hashes.
   The destination parent directory must already exist.

``--policy strict|permissive``
   Select handling for unsafe conversion or migration conditions. Strict is
   the default. The option is accepted by inspection and validation for a
   uniform command surface, although those operations do not transform data.

Exit codes
----------

.. list-table::
   :header-rows: 1
   :widths: 15 85

   * - Code
     - Meaning
   * - ``0``
     - Operation succeeded under the selected policy
   * - ``2``
     - Command usage or incomplete context options
   * - ``3``
     - Source detection or read failure
   * - ``4``
     - Validation failure
   * - ``5``
     - Background planning, migration, or target-validation failure
   * - ``6``
     - Conversion, serialization, sidecar, or unexpected application failure
