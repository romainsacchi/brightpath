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

Describe every context axis
---------------------------

New code should use a complete :class:`~brightpath.core.context.InventoryContext`.
The technosphere and biosphere releases are independent:

.. code-block:: python

   from brightpath import (
       BackgroundContext,
       BiosphereProfile,
       FormatProfile,
       InventoryContext,
       TechnosphereProfile,
   )

   source_context = InventoryContext(
       format=FormatProfile("brightway_excel", dialect="bw2io"),
       background=BackgroundContext(
           technosphere=TechnosphereProfile("ecoinvent", "3.10", "cutoff"),
           biosphere=BiosphereProfile("ecoinvent", "3.10"),
       ),
   )

``BAFU`` is accepted as a legacy input alias and immediately becomes ``uvek``.
``cut-off`` becomes ``cutoff``. Exact versions are not truncated:

.. code-block:: python

   patch = TechnosphereProfile("ecoinvent", "3.10.1", "cutoff")
   resolution = patch.resolve_migration_series()

   assert patch.version == "3.10.1"
   assert resolution.migration_series == "3.10"

Resource-series resolution is recorded separately so that validation and
output keep the exact declared version.

Construct application services
------------------------------

The pipeline receives its adapter registry and catalog provider explicitly:

.. code-block:: python

   from brightpath import InventoryPipeline
   from brightpath.adapters import default_adapter_registry
   from brightpath.background import catalog_provider_from_environment

   pipeline = InventoryPipeline(
       default_adapter_registry(),
       catalog_provider_from_environment(),
   )

The environment helper is application-boundary convenience. Core validation
and migration services do not read process-global configuration. The pipeline
also passes this provider to readers whose adapter declares it as a dependency;
SimaPro uses the exact biosphere catalog during name normalization.

Inspect and validate
--------------------

Pass the exact source context as a parser hint. This also makes an ambiguous
CSV boundary explicit:

.. code-block:: python

   read = pipeline.read(
       "foreground.xlsx",
       hint=source_context.as_hint(),
   )
   if read.value is None or not read.succeeded:
       raise RuntimeError(read.report.to_json(indent=2))

   normalized = pipeline.normalize(read.value)
   if normalized.value is None:
       raise RuntimeError(normalized.report.to_json(indent=2))

   validation = pipeline.validate(
       normalized.value,
       check_format=True,
       check_background_links=True,
   )
   for issue in validation.report.issues:
       print(issue.severity.value, issue.stage.value, issue.code, issue.path)

   if validation.error:
       raise RuntimeError("Inventory is not valid")

Every operation returns an immutable report. Reading, normalization,
validation, and transformation preserve caller-owned input.

Validation reports stages in the order structure, source format, then
background links. The last two stages can be disabled independently. Format
rules and conversion preflight belong to the selected adapter, so custom
adapters must implement both report hooks before registration.

Migrate without changing format
-------------------------------

.. code-block:: python

   from brightpath.core import MigrationPolicy

   target_background = BackgroundContext(
       technosphere=TechnosphereProfile("ecoinvent", "3.11", "cutoff"),
       biosphere=BiosphereProfile("ecoinvent", "3.11"),
   )
   migrated = pipeline.migrate(
       normalized.value,
       target_background,
       policy=MigrationPolicy.strict(),
   )
   if not migrated.succeeded:
       raise RuntimeError(migrated.report.to_json(indent=2))

   assert migrated.value.context.format == normalized.value.context.format
   written = pipeline.write(
       migrated.value,
       "foreground-ei311.xlsx",
       sidecar=True,
   )

Strict migration validates both endpoints and requires complete, lossless,
unambiguous coverage. Use :class:`~brightpath.core.policies.MigrationPolicy`
``.permissive()`` only when the application will review all warnings and
losses.

Convert without changing background
-----------------------------------

.. code-block:: python

   from brightpath.core import ConversionPolicy

   converted = pipeline.convert(
       normalized.value,
       "simapro_csv",
       policy=ConversionPolicy.strict(),
   )
   if converted.value is None or not converted.succeeded:
       raise RuntimeError(converted.report.to_json(indent=2))

   assert converted.value.context.background == normalized.value.context.background
   pipeline.write(converted.value, "foreground.csv", sidecar=True)

Next steps
----------

* Use the command line: :doc:`workflows/cli`.
* Keep the format and migrate links: :doc:`workflows/migration`.
* Keep the background and change format: :doc:`workflows/conversion`.
* Configure exact catalog validation: :doc:`workflows/validation`.
* Use the v1 facades: :doc:`workflows/brightway` and
  :doc:`workflows/simapro`.
