Upload analysis
===============

The analysis API is for intake services, web uploads, batch screening, and
other workflows that need inspectable results without choosing an output
format. It parses, normalizes for validation, checks links, and returns dataset
summaries. It does not write or migrate the inventory.

This API retains the v1 ``BackgroundProfile`` result projection for upload
compatibility. Brightway analysis may infer a missing or partial legacy source
profile. SimaPro requires an exact ``InventoryContext`` before parsing. Use
:class:`~brightpath.InventoryPipeline` when an application also needs immutable
stage reports, format conversion, or migration.

Analyze a file
--------------

.. code-block:: python

   from brightpath import BackgroundProfile
   from brightpath.analysis import analyze_inventory

   result = analyze_inventory(
       path="foreground.xlsx",
       source_profile=BackgroundProfile("ecoinvent", "3.10", "cutoff"),
   )

   print(result.detected_software, result.detected_format)
   print(result.source_profile.label())

   for issue in result.file_issues:
       print("file", issue.severity, issue.code, issue.message)

   for candidate in result.candidates:
       print(
           candidate.index,
           candidate.name,
           candidate.reference_product,
           candidate.location,
           candidate.unit,
       )
       for issue in candidate.issues:
           print("dataset", issue.severity, issue.code, issue.message)

Candidate summaries also expose ``description_hint`` and ``source_hint``.
BrightPath reads dedicated dataset metadata when present and can split a
trailing ``Source:`` section from a comment.

Analyze SimaPro with exact context
----------------------------------

SimaPro names and biosphere flow normalization depend on the exact database
releases. Supply format, technosphere, biosphere, and an exact catalog provider
before parsing:

.. code-block:: python

   from brightpath import (
       BackgroundContext,
       BiosphereProfile,
       FormatProfile,
       InventoryContext,
       TechnosphereProfile,
   )
   from brightpath.analysis import (
       SOURCE_FORMAT_SIMAPRO_CSV,
       analyze_inventory,
   )
   from brightpath.background import catalog_provider_from_environment

   source_context = InventoryContext(
       format=FormatProfile("simapro_csv", encoding="latin-1"),
       background=BackgroundContext(
           technosphere=TechnosphereProfile("ecoinvent", "3.11", "cutoff"),
           biosphere=BiosphereProfile("ecoinvent", "3.11"),
       ),
   )
   result = analyze_inventory(
       path="foreground.csv",
       source_format=SOURCE_FORMAT_SIMAPRO_CSV,
       source_context=source_context,
       catalog_provider=catalog_provider_from_environment(),
   )

``result.source_profile`` remains the legacy technosphere projection, while
parsing and link checks use both exact axes from ``source_context``. The
application provider is used when ``catalog_provider`` is omitted, but it must
contain the exact declared biosphere catalog.

Missing SimaPro context is structured
-------------------------------------

Passing a partial ``source_profile`` is not a substitute for SimaPro context.
Analysis returns before parsing with a stable file-level issue:

.. code-block:: python

   missing = analyze_inventory(
       path="foreground.csv",
       source_format=SOURCE_FORMAT_SIMAPRO_CSV,
       source_profile=BackgroundProfile("ecoinvent", "3.11", "cutoff"),
   )

   assert missing.inventory_data == []
   assert missing.candidates == []
   assert missing.file_issues[0].code == "simapro_source_context_required"

A non-SimaPro ``source_context.format`` similarly returns
``simapro_source_context_format_mismatch``. When both arguments are present, a
legacy ``source_profile`` that contradicts the exact technosphere returns
``simapro_source_profile_conflict``. Catalog access reports
``simapro_biosphere_catalog_missing`` when the exact resource is absent,
``simapro_biosphere_catalog_invalid`` for integrity/profile failures, and
``simapro_biosphere_catalog_failed`` for other provider failures. These are
inspectable ``AnalysisResult`` errors; no best-effort SimaPro parse occurs.

Supported inputs and inference
------------------------------

.. list-table::
   :header-rows: 1
   :widths: 24 28 48

   * - Suffix
     - Automatic interpretation
     - Explicit option
   * - ``.xlsx``
     - Brightway Excel
     - ``SOURCE_FORMAT_BRIGHTWAY_EXCEL``
   * - ``.tsv``
     - Brightway TSV
     - ``SOURCE_FORMAT_BRIGHTWAY_TSV``
   * - ``.csv``
     - Content-probed; ambiguity is an error
     - SimaPro or Brightway CSV constant
   * - ``.xls``
     - Rejected
     - Convert to ``.xlsx`` first

CSV is ambiguous by suffix. For an existing artifact, the same built-in
adapter registry used by the pipeline inspects content for Brightway block
grammar or SimaPro signatures. It reports absent or tied evidence instead of
guessing. Declare the format explicitly whenever the intake boundary already
knows it:

.. code-block:: python

   from brightpath.analysis import (
       SOURCE_FORMAT_BRIGHTWAY_CSV,
       analyze_inventory,
   )

   result = analyze_inventory(
       path="foreground.csv",
       source_format=SOURCE_FORMAT_BRIGHTWAY_CSV,
       source_profile=BackgroundProfile("ecoinvent", "3.10", "cutoff"),
   )

Brightway profile inference
---------------------------

For Brightway inputs, ``source_profile`` can be omitted or partial. BrightPath
scores technosphere targets against available catalogs and fills missing
profile fields when it finds matches. This behavior does not apply to SimaPro.

.. code-block:: python

   result = analyze_inventory(
       path="foreground.xlsx",
       source_profile=BackgroundProfile(family="ecoinvent"),
   )

Inference is evidence, not certainty. A unique catalog match produces the
informational issue ``background_profile_inferred``. Equal best matches use the
newest version and prefer cut-off, while returning
``background_profile_assumed`` so the caller can request confirmation.

Strict upload validation
------------------------

Use ``validate_inventory`` when an invalid upload should raise:

.. code-block:: python

   from brightpath import InventoryValidationError
   from brightpath.analysis import validate_inventory

   try:
       result = validate_inventory(
           path="foreground.xlsx",
           source_profile=BackgroundProfile("ecoinvent", "3.10", "cutoff"),
       )
   except InventoryValidationError as exc:
       result = exc.result
       print("Upload contains errors")

SimaPro validation accepts the same exact arguments as analysis:

.. code-block:: python

   result = validate_inventory(
       path="foreground.csv",
       source_format=SOURCE_FORMAT_SIMAPRO_CSV,
       source_context=source_context,
       catalog_provider=catalog_provider_from_environment(),
   )

If ``source_context`` is absent, ``validate_inventory`` raises the shared
``InventoryValidationError`` containing the structured
``simapro_source_context_required`` result.

``brightpath.analysis.InventoryValidationError`` is the same class re-exported
from the package root. It always exposes the shared immutable ``.report`` and
retains the upload ``.result`` compatibility attribute. Facade writer
preflight uses the same class and may additionally expose ``.legacy_report``.

External foreground targets
---------------------------

The analysis functions accept the same
``additional_foreground_targets=[(name, reference_product, location, unit)]``
argument as facade validation. Use it when one uploaded file legitimately
links to a foreground dataset supplied elsewhere.

Canonicalized data
------------------

``result.inventory_data`` contains a normalized copy used during validation.
It can support previews or downstream selection, but analysis does not return a
format facade. Construct ``BrightwayInventory.from_data`` or
``SimaProInventory.from_data`` explicitly when a later workflow needs to write,
convert, or migrate the selected data.
