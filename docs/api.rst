API reference
=============

Facade API
----------

.. autoclass:: brightpath.BrightwayInventory
   :members:
   :exclude-members: migrate_background

.. autoclass:: brightpath.SimaProInventory
   :members:
   :exclude-members: migrate_background

Exact context
-------------

.. autoclass:: brightpath.core.context.FormatProfile
   :members:

.. autoclass:: brightpath.core.context.TechnosphereProfile
   :members:

.. autoclass:: brightpath.core.context.BiosphereProfile
   :members:

.. autoclass:: brightpath.core.context.BackgroundContext
   :members:

.. autoclass:: brightpath.core.context.InventoryContext
   :members:

.. autoclass:: brightpath.core.context.ContextHint
   :members:

.. autoclass:: brightpath.core.context.VersionResolution
   :members:

.. autofunction:: brightpath.core.context.resolve_migration_series

.. autofunction:: brightpath.core.context.resolve_profile_migration_series

Pipeline
--------

.. autoclass:: brightpath.InventoryPipeline
   :members:
   :exclude-members: read

.. py:method:: InventoryPipeline.read(artifact, *, hint=ContextHint(), explicit_format=None, artifact_kind="file", adapter_kwargs=None)

   Detect and parse an artifact into an ``InventoryDocument`` operation
   result. A complete background hint is forwarded as an exact context;
   detection, format conflicts, and parser failures are represented in the
   immutable report.

Policies
--------

.. autoclass:: brightpath.core.policies.PolicyAction
   :members:
   :undoc-members:

.. autoclass:: brightpath.core.policies.ConversionPolicy
   :members:

.. autoclass:: brightpath.core.policies.MigrationPolicy
   :members:

Immutable reports and audit
---------------------------

.. autoclass:: brightpath.core.reports.Severity
   :members:
   :undoc-members:

.. autoclass:: brightpath.core.reports.OperationKind
   :members:
   :undoc-members:

.. autoclass:: brightpath.core.reports.StageKind
   :members:
   :undoc-members:

.. autoclass:: brightpath.core.reports.Issue
   :members:

.. autoclass:: brightpath.core.reports.Change
   :members:

.. autoclass:: brightpath.core.reports.Loss
   :members:

.. autoclass:: brightpath.core.reports.StageReport
   :members:

.. autoclass:: brightpath.core.reports.OperationReport
   :members:

.. py:class:: brightpath.core.reports.OperationResult(value, report)

   Immutable generic pairing of an operation value and
   :class:`~brightpath.core.reports.OperationReport`. ``changed``, ``lossy``,
   ``error``, and ``succeeded`` project report state. ``to_dict()`` and
   ``to_json()`` accept an optional value encoder; the corresponding class
   methods accept a value decoder.

.. autoclass:: brightpath.core.audit.ArtifactDigest
   :members:

.. autofunction:: brightpath.core.audit.digest_artifact

.. autofunction:: brightpath.core.audit.write_report_sidecar

Canonical schema
----------------

.. autodata:: brightpath.core.schema.CANONICAL_SCHEMA_VERSION

.. autoclass:: brightpath.core.schema.ExtensionMap
   :members:

.. autoclass:: brightpath.core.schema.DatasetIdentity
   :members:

.. autoclass:: brightpath.core.schema.ExchangeIdentity
   :members:

.. autoclass:: brightpath.core.schema.Uncertainty
   :members:

.. autoclass:: brightpath.core.schema.CanonicalParameter
   :members:

.. autoclass:: brightpath.core.schema.CanonicalExchange
   :members:

.. autoclass:: brightpath.core.schema.CanonicalDataset
   :members:

.. autoclass:: brightpath.core.schema.CanonicalInventory
   :members:

Adapters and registry
---------------------

.. autoclass:: brightpath.adapters.ArtifactKind
   :members:
   :undoc-members:

.. autoclass:: brightpath.adapters.FormatDescriptor
   :members:
   :exclude-members: id, identifier

.. autoclass:: brightpath.adapters.AdapterCapabilities
   :members:

``can_validate_format`` declares the intrinsic grammar hook and
``can_preflight_conversion`` declares the target representability/loss hook.
Readable adapters require the former; writable adapters require both. Registry
construction enforces the flags and callable hooks before capability discovery.
``compatible_format_versions`` and ``compatible_dialects`` are explicit
allowlists for resolving a qualified descriptor through a generic adapter. The
built-in Brightway Excel adapter allows only the ``bw2io`` dialect.
``requires_catalog_provider`` declares that a reader needs the pipeline's exact
catalog provider; it is ``True`` for the built-in SimaPro adapter.

.. autoclass:: brightpath.adapters.DetectionCandidate
   :members:

.. autoclass:: brightpath.adapters.DetectionIssue
   :members:

.. autoclass:: brightpath.adapters.DetectionReport
   :members:

.. autoclass:: brightpath.adapters.AdapterRegistry
   :members:

.. autoclass:: brightpath.adapters.FormatAdapter
   :members:

.. autoclass:: brightpath.adapters.BrightwayExcelAdapter
   :members:

.. autoclass:: brightpath.adapters.BrightwayDelimitedAdapter
   :members:

.. autoclass:: brightpath.adapters.SimaProCSVAdapter
   :members:

.. autofunction:: brightpath.adapters.coerce_format_descriptor

.. autofunction:: brightpath.adapters.default_adapter_registry

.. autofunction:: brightpath.adapters.validate_adapter_format

.. autofunction:: brightpath.adapters.preflight_conversion

.. autofunction:: brightpath.adapters.preflight.validate_brightway_format

.. autofunction:: brightpath.adapters.preflight.validate_simapro_format

.. autofunction:: brightpath.adapters.preflight.preflight_brightway_conversion

.. autofunction:: brightpath.adapters.preflight.preflight_simapro_conversion

Catalog providers
-----------------

.. autoclass:: brightpath.background.TechnosphereCatalog
   :members:

.. autoclass:: brightpath.background.BiosphereCatalog
   :members:

.. autoclass:: brightpath.background.CatalogProvider
   :members:

.. autoclass:: brightpath.background.InMemoryCatalogProvider
   :members:

.. autoclass:: brightpath.background.DirectoryCatalogProvider
   :members:

.. autoclass:: brightpath.background.PackageCatalogProvider
   :members:

.. autoclass:: brightpath.background.CompositeCatalogProvider
   :members:

.. autofunction:: brightpath.background.catalog_provider_from_environment

.. autoexception:: brightpath.background.CatalogNotFoundError

.. autoexception:: brightpath.background.CatalogIntegrityError

Legacy combined catalog bridge
------------------------------

.. autoclass:: brightpath.catalogs.BackgroundCatalog

.. autofunction:: brightpath.catalogs.available_catalog_profiles

.. py:function:: brightpath.catalogs.load_background_catalog(profile)

   Load exact technosphere and documented-default biosphere axes through the
   provider stack and return the legacy combined ``BackgroundCatalog``.

.. autofunction:: brightpath.catalogs.catalog_directory

.. autofunction:: brightpath.catalogs.catalog_filename

.. autofunction:: brightpath.catalogs.catalog_path

.. autofunction:: brightpath.catalogs.collect_technosphere_catalog_entries

.. autofunction:: brightpath.catalogs.collect_biosphere_catalog_entries

.. autofunction:: brightpath.catalogs.write_background_catalog

Background validation and migration
-----------------------------------

.. py:function:: brightpath.background.validate_background_links(inventory, context, provider, *, foreground_technosphere_targets=())

   Validate technosphere and biosphere links against exact catalogs and return
   an immutable background-validation ``StageReport``. The function does not
   mutate ``inventory``.

.. autoclass:: brightpath.background.MigrationAxis
   :members:
   :undoc-members:

.. autoclass:: brightpath.background.MigrationRouteStep
   :members:

.. py:class:: brightpath.background.migration.MigrationPlan

   Immutable independent technosphere/biosphere route plan. ``steps`` combines
   both axes; ``requires_migration``, ``changed``, ``executable``, and
   ``succeeded`` summarize its planning report.

.. autofunction:: brightpath.background.plan_background_migration

.. py:function:: brightpath.background.execute_background_migration(document, target, provider, policy=MigrationPolicy.strict(), *, foreground_technosphere_targets=())

   Validate, plan, transactionally execute, and verify one background
   migration. Returns an ``OperationResult`` containing the committed document
   or the unchanged source on policy failure.

Capability discovery
--------------------

.. autoclass:: brightpath.capabilities.FormatCapability
   :members:

.. autoclass:: brightpath.capabilities.MigrationCapability
   :members:

.. autofunction:: brightpath.capabilities.format_capabilities

.. autofunction:: brightpath.capabilities.migration_capabilities

.. autofunction:: brightpath.capabilities.capability_snapshot

Upload analysis
---------------

.. autofunction:: brightpath.analysis.infer_source_format

.. autofunction:: brightpath.analysis.analyze_inventory

.. autofunction:: brightpath.analysis.validate_inventory

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

Facade compatibility models
---------------------------

.. autoclass:: brightpath.BackgroundProfile
   :members:

.. autoclass:: brightpath.InventoryFormat
   :members:
   :undoc-members:

.. autoclass:: brightpath.Issue

.. autoclass:: brightpath.ValidationReport
   :members:

.. autoclass:: brightpath.models.InventoryDocument
   :members:

.. autoclass:: brightpath.formats.simapro_csv.SimaProRenderResult
   :members:

.. autofunction:: brightpath.formats.simapro_csv.load_simapro_csv

.. autofunction:: brightpath.formats.simapro_csv.normalize_simapro_import_data

.. autoclass:: brightpath.migrations.MigrationReport
   :members:

.. autoclass:: brightpath.migrations.MigrationStepReport
   :members:

Exceptions
----------

.. autoexception:: brightpath.exceptions.BrightPathError
   :members:

.. autoexception:: brightpath.OperationError

.. autoexception:: brightpath.FormatDetectionError

.. py:exception:: brightpath.InventoryValidationError

   Shared validation exception. ``report`` is always the immutable operation
   report; compatibility paths may additionally expose ``legacy_report`` or
   ``result``.

.. autoexception:: brightpath.MigrationError

.. autoexception:: brightpath.MigrationUnavailableError

.. autoexception:: brightpath.ConversionError

.. autoexception:: brightpath.SerializationError

.. autoexception:: brightpath.ExcelSerializationError

.. autoexception:: brightpath.SimaProSerializationError
