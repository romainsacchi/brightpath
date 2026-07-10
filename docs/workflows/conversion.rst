Format conversion
=================

Format conversion changes the software representation and preserves the
background profile. Background migration is a separate call.

Brightway to SimaPro
--------------------

.. code-block:: python

   from brightpath import BackgroundProfile, BrightwayInventory

   brightway = BrightwayInventory.from_excel(
       "foreground-ei310.xlsx",
       background_profile=BackgroundProfile("ecoinvent", "3.10", "cutoff"),
   ).normalize()

   simapro = brightway.to_simapro()
   assert simapro.background_profile == brightway.background_profile

   report = simapro.validate(check_simapro_rendering=True)
   if report.is_valid:
       simapro.write_csv("foreground-ei310.csv")

``to_simapro()`` changes the format view only. It does not rename links for a
different ecoinvent version and does not convert ecoinvent links to UVEK.

SimaPro to Brightway
--------------------

.. code-block:: python

   from brightpath import BackgroundProfile, SimaProInventory

   simapro = SimaProInventory.from_csv(
       "foreground-ei310.csv",
       background_profile=BackgroundProfile("ecoinvent", "3.10", "cutoff"),
   )
   brightway = simapro.to_brightway()
   brightway.write_excel("foreground-ei310.xlsx")

Migrate, then change format
---------------------------

Compose the two explicit operations when both axes must change:

.. code-block:: python

   target = BackgroundProfile("ecoinvent", "3.12", "cutoff")

   migrated_brightway = brightway.migrate_background(target)
   migration_report = migrated_brightway.last_migration_report
   if migration_report.has_errors:
       raise RuntimeError("Review target-link errors before export")

   migrated_simapro = migrated_brightway.to_simapro()
   migrated_simapro.write_csv("foreground-ei312.csv")

The same composition works in the other direction:

.. code-block:: python

   migrated_simapro = simapro.migrate_background(target)
   migrated_brightway = migrated_simapro.to_brightway()
   migrated_brightway.write_excel("foreground-ei312.xlsx")

Round trips
-----------

Facades can be converted in memory without writing intermediate files:

.. code-block:: python

   same_data = brightway.to_simapro().to_brightway()
   assert same_data.data == brightway.data
   assert same_data.background_profile == brightway.background_profile

Actual file round trips are constrained by the target format. Brightway Excel
can preserve nested unknown fields through tagged JSON values. SimaPro CSV has
stricter units, categories, sections, and Latin-1 encoding and does not encode
arbitrary unknown fields. ``render()`` checks known format requirements, but it
is not proof that every uninterpreted field will survive a file round trip.

No implicit database installation
---------------------------------

Conversion writes exchange files. It does not install a Brightway database,
open SimaPro, or alter a proprietary background database. Those application
steps remain under the caller's control.
