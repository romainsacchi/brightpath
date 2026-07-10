Upload analysis
===============

The analysis API is for intake services, web uploads, batch screening, and
other workflows that need inspectable results without choosing an output
format. It parses, normalizes for validation, checks links, and returns dataset
summaries. It does not write or migrate the inventory.

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
     - SimaPro CSV
     - SimaPro or Brightway CSV constant
   * - ``.xls``
     - Rejected
     - Convert to ``.xlsx`` first

CSV is ambiguous. A Brightway block-format CSV must be declared explicitly:

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

   from brightpath.analysis import InventoryValidationError, validate_inventory

   try:
       result = validate_inventory(
           path="foreground.xlsx",
           source_profile=BackgroundProfile("ecoinvent", "3.10", "cutoff"),
       )
   except InventoryValidationError as exc:
       result = exc.result
       print("Upload contains errors")

This exception belongs to ``brightpath.analysis`` and exposes ``.result``.
``brightpath.InventoryValidationError`` is the writer preflight exception and
exposes ``.report``.

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
