Brightway Excel workflows
=========================

Load a workbook
---------------

Pass the profile that the workbook currently links to:

.. code-block:: python

   from brightpath import BackgroundProfile, BrightwayInventory

   inventory = BrightwayInventory.from_excel(
       "foreground-ei310.xlsx",
       background_profile=BackgroundProfile("ecoinvent", "3.10", "cutoff"),
   )

Workbooks written by BrightPath embed the normalized background profile. The
argument can therefore be omitted when reopening one of those workbooks:

.. code-block:: python

   reopened = BrightwayInventory.from_excel("foreground-ei310-roundtrip.xlsx")
   print(reopened.background_profile.label())

For third-party workbooks, pass the profile explicitly. An incomplete profile
prevents background-link validation and migration.

Normalize legacy fields
-----------------------

Normalization is explicit and copy-on-write:

.. code-block:: python

   normalized = inventory.normalize()

It promotes legacy ``product`` keys to ``reference product``, normalizes
biosphere category sequences, and makes the production exchange identity match
its dataset. It does not export, validate, or migrate links.

Create an inventory in memory
-----------------------------

.. code-block:: python

   from brightpath import BackgroundProfile, BrightwayInventory

   inventory = BrightwayInventory.from_data(
       data,
       background_profile=BackgroundProfile("ecoinvent", "3.10", "cutoff"),
       database_name="foreground-model",
       metadata={"owner": "LCA team"},
       database_parameters=[{"name": "loss", "amount": 0.03}],
       project_parameters=[{"name": "year", "amount": 2030}],
   )

BrightPath deep-copies these values. The ``data`` variable can be safely reused
or modified after construction.

Validate without exporting
---------------------------

.. code-block:: python

   report = inventory.validate()
   for issue in report.issues:
       print(issue.severity, issue.code, issue.path, issue.message)

   if report.has_errors:
       raise RuntimeError("Correct the inventory before writing")

Use ``check_background_links=False`` when only structure, plausibility,
dataset uniqueness, and production identities should be checked. See
:doc:`validation` for catalogs and external foreground targets.

Write the same format
---------------------

.. code-block:: python

   output = inventory.write_excel("foreground-ei310-checked")
   # output ends with .xlsx and is an absolute pathlib.Path

Writing validates by default. ``validate=False`` bypasses that preflight but
does not make otherwise unserializable values valid. A destination with a
non-``.xlsx`` suffix is rejected.

Migrate and stay in Brightway
-----------------------------

.. code-block:: python

   migrated = inventory.migrate_background(
       BackgroundProfile("ecoinvent", "3.12", "cutoff")
   )
   migrated.write_excel("foreground-ei312.xlsx")

The facade remains ``BrightwayInventory`` throughout. Inspect
``migrated.last_migration_report`` before writing, especially for reverse or
multi-step routes.

Convert only when requested
---------------------------

.. code-block:: python

   simapro = inventory.to_simapro()
   assert simapro.background_profile == inventory.background_profile

SimaPro export imposes additional requirements, including a ``simapro
category`` on production exchanges and SimaPro-supported units. Call
``simapro.validate(check_simapro_rendering=True)`` before writing.

Scope of the facade
-------------------

``BrightwayInventory`` reads and writes exchange workbooks. It does not select
a Brightway project or register a database in a local Brightway installation.
Use normal Brightway/``bw2io`` APIs to import the resulting workbook into a
project.
