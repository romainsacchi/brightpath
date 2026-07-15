Brightway workflows
===================

BrightPath has a convenience facade for Brightway Excel and pipeline adapters
for Brightway Excel, block CSV, and block TSV.

Load Brightway Excel with exact context
---------------------------------------

.. code-block:: python

   from brightpath import (
       BackgroundContext,
       BiosphereProfile,
       BrightwayInventory,
       FormatProfile,
       InventoryContext,
       TechnosphereProfile,
   )

   context = InventoryContext(
       format=FormatProfile("brightway_excel", dialect="bw2io"),
       background=BackgroundContext(
           technosphere=TechnosphereProfile("ecoinvent", "3.10", "cutoff"),
           biosphere=BiosphereProfile("ecoinvent", "3.10"),
       ),
   )
   inventory = BrightwayInventory.from_excel(
       "foreground-ei310.xlsx",
       context=context,
   )

Workbooks written by BrightPath embed their exact context. It can be omitted
when reopening such a workbook:

.. code-block:: python

   reopened = BrightwayInventory.from_excel("foreground-roundtrip.xlsx")
   print(reopened.context.background.technosphere.label())
   print(reopened.context.background.biosphere.label())

Third-party workbooks should receive an explicit context. The legacy
``background_profile=BackgroundProfile(...)`` argument still creates a
technosphere profile and documented default biosphere, but cannot express all
background combinations.

Normalize and validate
----------------------

.. code-block:: python

   normalized = inventory.normalize()
   report = normalized.validate()

   for issue in report.issues:
       print(issue.severity, issue.code, issue.path, issue.message)

   if report.has_errors:
       raise RuntimeError("Correct the inventory before writing")

Normalization promotes legacy ``product`` fields, normalizes biosphere
category sequences, and synchronizes production identities. It returns a new
facade. Validation is read-only and can receive an explicit
``catalog_provider=``.

Create a facade from dictionaries
---------------------------------

.. code-block:: python

   inventory = BrightwayInventory.from_data(
       data,
       context=context,
       database_name="foreground-model",
       metadata={"owner": "LCA team"},
       database_parameters=[{"name": "loss", "amount": 0.03}],
       project_parameters=[{"name": "year", "amount": 2030}],
   )

Inputs are copied. ``.data``, ``.metadata``, and parameter properties also
return copies.

Write Brightway Excel
---------------------

.. code-block:: python

   output = normalized.write_excel("foreground-ei310-checked")

The result is an absolute :class:`pathlib.Path` ending in ``.xlsx``. Writing
validates by default. ``validate=False`` skips that facade preflight but cannot
make an unserializable value valid.

Brightway CSV and TSV
---------------------

Delimited Brightway files are full read/write pipeline formats, not analysis-
only inputs:

.. code-block:: python

   from brightpath import InventoryPipeline
   from brightpath.adapters import default_adapter_registry
   from brightpath.background import catalog_provider_from_environment
   from brightpath.core import ContextHint, FormatProfile

   pipeline = InventoryPipeline(
       default_adapter_registry(),
       catalog_provider_from_environment(),
   )
   read = pipeline.read(
       "foreground.csv",
       hint=ContextHint(
           format=FormatProfile("brightway_csv"),
           background=context.background,
       ),
   )
   pipeline.write(read.value, "foreground.tsv", target_format="brightway_tsv")

The block grammar is the same canonical Brightway layout with a comma or tab
delimiter. Detection inspects the delimiter and block markers. It does not
assume that every ``.csv`` file is SimaPro.

Migrate while staying in Brightway
----------------------------------

Use the injected pipeline when technosphere and biosphere targets, strict
validation, coverage, reverse handling, and loss policy must be explicit:

.. code-block:: python

   from brightpath.core import MigrationPolicy

   target = BackgroundContext(
       technosphere=TechnosphereProfile("ecoinvent", "3.11", "cutoff"),
       biosphere=BiosphereProfile("ecoinvent", "3.11"),
   )
   migrated = pipeline.migrate(
       read.value,
       target,
       policy=MigrationPolicy.strict(),
   )
   if migrated.succeeded:
       pipeline.write(migrated.value, "foreground-ei311.csv")

The document remains in its Brightway format. The facade's
``migrate_background()`` method also accepts a complete ``BackgroundContext``,
a ``MigrationPolicy``, a catalog provider, and external foreground
targets. It raises ``MigrationError`` with the immutable operation report when
the selected policy prevents commit. Legacy ``BackgroundProfile`` and
``TechnosphereProfile`` targets remain available; callers can pair them with
an explicit ``biosphere_profile`` or preserve the existing biosphere. Use the
pipeline when the migration result should be returned as a value instead of
raised as an exception.

Convert only when requested
---------------------------

.. code-block:: python

   simapro = normalized.to_simapro()
   assert simapro.context.background == normalized.context.background

``to_simapro()`` changes only the format view. SimaPro writing has additional
unit, category, section, encoding, and representability constraints. Use
``pipeline.convert(..., policy=ConversionPolicy.strict())`` when those
conditions must be represented in an immutable operation report.

Scope
-----

BrightPath writes exchange artifacts. It does not select a Brightway project,
register a database in a local installation, or distribute proprietary
background inventory contents.
