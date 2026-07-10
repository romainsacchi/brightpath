Upload analysis
===============

The analysis API is for intake services, web uploads, batch screening, and
other workflows that need inspectable results without choosing an output
format. It parses, normalizes for validation, checks links, and returns dataset
summaries. It does not write or migrate the inventory.

This API retains the v1 ``BackgroundProfile`` result for upload compatibility.
Use :class:`~brightpath.InventoryPipeline` when an application needs exact
independent technosphere/biosphere contexts, immutable stage reports, format
conversion, or migration.

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

Profile inference
-----------------

``source_profile`` can be omitted or partial. BrightPath scores technosphere
targets against available catalogs and fills missing profile fields when it
finds matches.

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
