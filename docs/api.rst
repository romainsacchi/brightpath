API reference
=============

Format facades
--------------

.. autoclass:: brightpath.BrightwayInventory
   :members:

.. autoclass:: brightpath.SimaProInventory
   :members:

Profiles and validation models
------------------------------

.. autoclass:: brightpath.BackgroundProfile
   :members:

.. autoclass:: brightpath.InventoryFormat
   :members:
   :undoc-members:

.. autoclass:: brightpath.Issue

.. autoclass:: brightpath.ValidationReport
   :members:

Analysis API
------------

.. autofunction:: brightpath.analysis.infer_source_format

.. autofunction:: brightpath.analysis.analyze_inventory

.. autofunction:: brightpath.analysis.validate_inventory

.. autoclass:: brightpath.analysis.InventoryValidationError
   :members:

.. autoclass:: brightpath.models.AnalysisResult
   :members:

.. autoclass:: brightpath.models.CandidateSummary

.. autodata:: brightpath.analysis.SOURCE_FORMAT_BRIGHTWAY_EXCEL
   :annotation: = "brightway_excel"

.. autodata:: brightpath.analysis.SOURCE_FORMAT_BRIGHTWAY_CSV
   :annotation: = "brightway_csv"

.. autodata:: brightpath.analysis.SOURCE_FORMAT_BRIGHTWAY_TSV
   :annotation: = "brightway_tsv"

.. autodata:: brightpath.analysis.SOURCE_FORMAT_SIMAPRO_CSV
   :annotation: = "simapro_csv"

Migration API
-------------

.. autofunction:: brightpath.migrations.available_ecoinvent_versions

.. autoclass:: brightpath.migrations.MigrationReport
   :members:

.. autoclass:: brightpath.migrations.MigrationStepReport
   :members:

Catalog API
-----------

.. autoclass:: brightpath.catalogs.BackgroundCatalog

.. autofunction:: brightpath.catalogs.available_catalog_profiles

.. autofunction:: brightpath.catalogs.catalog_directory

.. autofunction:: brightpath.catalogs.catalog_filename

.. autofunction:: brightpath.catalogs.catalog_path

.. autofunction:: brightpath.catalogs.collect_technosphere_catalog_entries

.. autofunction:: brightpath.catalogs.collect_biosphere_catalog_entries

.. autofunction:: brightpath.catalogs.load_background_catalog

.. autofunction:: brightpath.catalogs.write_background_catalog

Rendering and internal document model
-------------------------------------

.. autoclass:: brightpath.formats.simapro_csv.SimaProRenderResult
   :members:

.. autoclass:: brightpath.models.InventoryDocument
   :members:

Exceptions
----------

.. autoexception:: brightpath.exceptions.BrightPathError

.. autoexception:: brightpath.InventoryValidationError
   :members:

.. autoexception:: brightpath.MigrationError

.. autoexception:: brightpath.MigrationUnavailableError

.. autoexception:: brightpath.ExcelSerializationError

.. autoexception:: brightpath.SimaProSerializationError
