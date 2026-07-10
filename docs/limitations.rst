Limits and compatibility
========================

Version 1 compatibility
-----------------------

Version 1 intentionally deletes the 0.x ``BrightwayConverter`` and
``SimaproConverter`` modules and classes. There are no compatibility wrappers
because those APIs coupled source format, target format, and background
behavior.

Replace a one-step converter call with an explicit pipeline:

.. code-block:: python

   inventory = BrightwayInventory.from_excel(
       "source.xlsx",
       background_profile=source_profile,
   )
   migrated = inventory.migrate_background(target_profile)
   simapro = migrated.to_simapro()
   simapro.write_csv("target.csv")

Omit ``migrate_background`` when the background profile should remain
unchanged, and omit ``to_simapro`` when Brightway output is required.

Current format boundaries
-------------------------

* Brightway Excel and SimaPro CSV are the only full read/write facades.
* Brightway CSV and TSV are analysis inputs only.
* OpenLCA Excel and ecospold2 identifiers reserve future adapter names but have
  no implementation.
* SimaPro CSV output is Latin-1 and cannot encode arbitrary Unicode.
* SimaPro rendering requires supported units and production categories.
* BrightPath writes exchange files; it does not install databases into LCA
  software.

Current migration boundaries
----------------------------

* Only ecoinvent cut-off versions 3.5 through 3.12 have migration routes.
* Consequential inventories can be formatted and validated, but not migrated.
* Cross-system-model and cross-family migrations are unavailable.
* UVEK 2025 can be used in either software format, but there is no
  ecoinvent/UVEK mapping yet.
* Reverse migrations can aggregate data or encounter irreversible deletions.
* Unit-changing rules are reported without changing exchange amounts.

Validation boundaries
---------------------

Background-link validation checks exact identity tuples against a catalog; it
does not perform fuzzy semantic linking. Use the exact source/target profile
and review every error after migration. A custom reference directory replaces,
rather than extends, the packaged catalog directory.

Data and licensing
------------------

Migration rule files retain their declared CC-BY-4.0 metadata and attribution
to Premise contributors. BrightPath does not package proprietary ecoinvent
inventory contents. Custom catalogs should likewise be handled according to
the source database license.
