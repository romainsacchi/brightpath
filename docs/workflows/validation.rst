Validation and catalogs
=======================

Validation is read-only and returns ``ValidationReport``. It never normalizes,
migrates, writes, or mutates inventory data.

Validation stages
-----------------

Facade validation checks:

* canonical dataset and exchange structure;
* inventory plausibility warnings;
* duplicate dataset identities;
* production exchange identity consistency;
* optional technosphere and biosphere links against an exact catalog.

SimaPro validation can additionally render rows and check ecoinvent system
model markers.

Inspect structured issues
-------------------------

.. code-block:: python

   report = inventory.validate()

   for issue in report.issues:
       print(
           issue.severity,
           issue.code,
           issue.path,
           issue.message,
           issue.suggested_fix,
       )

   if report.is_valid:
       print("No validation errors")

Warnings do not make ``report.has_errors`` true. Applications should still
present them for review.

Structural validation only
--------------------------

Disable background links when validating a draft or an inventory whose exact
catalog is not installed:

.. code-block:: python

   report = inventory.validate(check_background_links=False)

An incomplete profile is accepted in this mode. Link validation requires a
complete family, version, and system model.

SimaPro rendering validation
----------------------------

.. code-block:: python

   report = simapro_inventory.validate(
       check_background_links=False,
       check_simapro_rendering=True,
   )

This checks target-format requirements without writing a file.

External foreground targets
---------------------------

Technosphere exchanges may point to foreground datasets managed outside the
current file. Declare those identities explicitly:

.. code-block:: python

   external_targets = [
       (
           "shared transport service",
           "transport service",
           "CH",
           "ton kilometer",
       )
   ]

   report = inventory.validate(
       additional_foreground_targets=external_targets,
   )

Each tuple is ``(name, reference product, location, unit)``. These targets are
accepted as foreground links; they are not added to the inventory or catalog.

Packaged catalogs
-----------------

BrightPath packages exact identity catalogs for:

* ecoinvent 3.6 through 3.12, cut-off;
* ecoinvent 3.6 through 3.12, consequential;
* UVEK 2025, cut-off.

A profile must match a catalog exactly. ecoinvent 3.5 migration rules are
packaged, but a 3.5 catalog is not; supply a custom catalog to validate that
source profile with background checks enabled.

Use a custom catalog directory
------------------------------

Set ``BRIGHTPATH_REFERENCE_DIR`` to a directory containing catalog JSON files:

.. code-block:: console

   export BRIGHTPATH_REFERENCE_DIR=/path/to/reference_catalogs

This directory replaces the packaged catalog directory for the process. Files
must use the canonical name
``family__version__system_model.json``, for example
``ecoinvent__3.5__cutoff.json``.

Generate a catalog from database data
-------------------------------------

Given canonical dictionaries for the complete background database:

.. code-block:: python

   from pathlib import Path

   from brightpath import BackgroundProfile
   from brightpath.catalogs import (
       collect_biosphere_catalog_entries,
       collect_technosphere_catalog_entries,
       write_background_catalog,
   )

   profile = BackgroundProfile("ecoinvent", "3.5", "cutoff")
   catalog_path = write_background_catalog(
       profile,
       technosphere=collect_technosphere_catalog_entries(background_database_data),
       biosphere=collect_biosphere_catalog_entries(background_database_data),
       output_dir=Path("reference_catalogs"),
   )
   print(catalog_path)

Do not generate a background catalog from only a foreground inventory: it
would omit the external identities that link validation is intended to check.
Do not distribute proprietary database contents.

Writer failures
---------------

Writers validate by default and raise the package-root exception:

.. code-block:: python

   from brightpath import InventoryValidationError

   try:
       inventory.write_excel("inventory.xlsx")
   except InventoryValidationError as exc:
       for issue in exc.report.issues:
           print(issue.code, issue.message)

The upload-analysis API has a different exception with an ``.result``
attribute; see :doc:`analysis`.
