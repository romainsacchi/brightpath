Getting started
===============

Installation
------------

BrightPath supports Python 3.10 and 3.11.

.. code-block:: console

   python -m pip install brightpath

For an editable development installation:

.. code-block:: console

   git clone https://github.com/romainsacchi/brightpath.git
   cd brightpath
   python -m pip install -e ".[dev,docs]"

Describe the background explicitly
----------------------------------

Every inventory has a background profile with three fields: database family,
version, and system model.

.. code-block:: python

   from brightpath import BackgroundProfile

   ei36_cutoff = BackgroundProfile(
       family="ecoinvent",
       version="3.6",
       system_model="cutoff",
   )

   uvek_2025 = BackgroundProfile("uvek", "2025", "cutoff")

``BackgroundProfile`` normalizes common boundary values. ``BAFU`` becomes
``uvek``, ``cut-off`` becomes ``cutoff``, and an ecoinvent patch version such
as ``3.10.1`` becomes ``3.10``.

First Brightway workflow
------------------------

Load, normalize, validate, migrate, and write Brightway Excel without changing
software format:

.. code-block:: python

   from pathlib import Path

   from brightpath import BackgroundProfile, BrightwayInventory

   source = BrightwayInventory.from_excel(
       Path("inventory-ei36.xlsx"),
       background_profile=BackgroundProfile("ecoinvent", "3.6", "cutoff"),
   )
   normalized = source.normalize()

   source_report = normalized.validate()
   if source_report.has_errors:
       for issue in source_report.issues:
           if issue.severity == "error":
               print(issue.code, issue.path, issue.message)
       raise RuntimeError("Source inventory is invalid")

   migrated = normalized.migrate_background(
       BackgroundProfile("ecoinvent", "3.8", "cutoff")
   )
   migration_report = migrated.last_migration_report
   if migration_report.has_errors:
       for issue in migration_report.all_issues:
           print(issue.severity, issue.code, issue.message)
       raise RuntimeError("Target links require review")

   output = migrated.write_excel("inventory-ei38.xlsx")
   print(output)

``source``, ``normalized``, and ``migrated`` are independent objects. Reading
``.data`` also returns a deep copy, so modifying it cannot mutate the facade.

First SimaPro workflow
----------------------

Load, inspect, and write SimaPro CSV without changing the background profile:

.. code-block:: python

   from brightpath import BackgroundProfile, SimaProInventory

   inventory = SimaProInventory.from_csv(
       "inventory.csv",
       background_profile=BackgroundProfile("ecoinvent", "3.10", "cutoff"),
       database_name="foreground-inventory",
   )

   report = inventory.validate(
       check_background_links=True,
       check_simapro_rendering=True,
   )
   if report.is_valid:
       inventory.write_csv("inventory-checked.csv")

Next steps
----------

* Keep the format and migrate links: :doc:`workflows/migration`.
* Keep the background and change format: :doc:`workflows/conversion`.
* Configure link validation: :doc:`workflows/validation`.
* Analyze uploads without exporting: :doc:`workflows/analysis`.
