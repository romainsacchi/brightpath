Validation and catalogs
=======================

Validation is read-only. Structural, adapter-owned format, and exact
background-link validation are independent stages and never normalize,
migrate, write, or mutate the source document.

Pipeline validation
-------------------

.. code-block:: python

   result = pipeline.validate(
       document,
       check_format=True,
       check_background_links=True,
   )

   for stage in result.report.stages:
       print(stage.stage.value, stage.has_errors, dict(stage.metrics))
   for issue in result.report.issues:
       print(
           issue.severity.value,
           issue.stage.value,
           issue.code,
           issue.path,
           issue.message,
           issue.suggested_fix,
       )

   if result.error:
       raise RuntimeError("Validation failed")

Warnings do not set ``result.error``. Applications should still present them.
The immutable operation report can be serialized with ``to_json()``.

Validation stages
-----------------

Stages are returned in stable order:

1. canonical structural validation;
2. source-format validation when ``check_format=True``;
3. exact background validation when ``check_background_links=True``.

The structural stage checks canonical dataset and exchange shape, requires a
non-empty ``comment`` on every dataset, checks plausibility, duplicate
identities, and production identity consistency. A missing or whitespace-only
dataset comment is a blocking error. The selected source adapter owns
``validate_format(document)`` and checks intrinsic grammar without converting,
writing, or applying loss policy. Missing, failing, or malformed adapter
contracts are format-validation errors. The background stage independently
checks:

* technosphere exchange identities against the exact technosphere catalog;
* biosphere exchange identities against the exact biosphere catalog;
* local datasets and explicitly supplied external foreground identities before
  treating a technosphere link as background.

Each axis reports total, resolved, unresolved, and unchecked links, resolution
coverage, catalog status, identity count, resource digest, schema version, and
source.

Structural validation only
--------------------------

.. code-block:: python

   structural = pipeline.validate(
       document,
       check_format=False,
       check_background_links=False,
   )

This is useful for drafts, but it establishes neither source-format
representability nor that external links match the declared background.

Format validation only
----------------------

Run structure plus the adapter-owned format hook without loading catalogs:

.. code-block:: python

   format_result = pipeline.validate(
       document,
       check_format=True,
       check_background_links=False,
   )

Brightway adapters validate their block-layout grammar. The SimaPro adapter
checks intrinsic renderer grammar such as required categories, units, and
amounts. Unsupported metadata, precision changes, omitted exchanges,
information loss, and ambiguous mappings belong exclusively to the separate
``preflight_conversion`` hook. Format validation cannot override those policy
decisions.

External foreground targets
---------------------------

Technosphere exchanges can point to foreground datasets managed outside the
current artifact. Declare their exact four-field identities:

.. code-block:: python

   external_targets = [
       (
           "shared transport service",
           "transport service",
           "CH",
           "ton kilometer",
       )
   ]
   result = pipeline.validate(
       document,
       additional_foreground_targets=external_targets,
   )

These identities are accepted as foreground links. They are not added to the
inventory or to a background catalog.

Typed catalog providers
-----------------------

Catalogs are independent typed resources:

* ``TechnosphereCatalog`` belongs to an exact
  ``TechnosphereProfile(family, version, system_model)``;
* ``BiosphereCatalog`` belongs to an exact
  ``BiosphereProfile(family, version)``.

All validation and migration services receive a ``CatalogProvider``. Built-in
implementations are:

``PackageCatalogProvider``
   Loads the resources installed with BrightPath.

``DirectoryCatalogProvider``
   Loads combined JSON catalogs from an explicit directory and verifies the
   embedded profile. When a resource manifest is present it also verifies
   SHA-256, size, schema version, profile, and identity counts. For one exact
   biosphere family/version it validates all matching system-model files,
   requires a common schema version, unions their identity shards, and records
   a composite digest and source list.

``InMemoryCatalogProvider``
   Accepts typed catalog objects for tests, services, databases, or other
   application-owned stores.

``CompositeCatalogProvider``
   Tries providers in order and rejects duplicate profiles within a provider.

Environment application boundary
--------------------------------

.. code-block:: console

   export BRIGHTPATH_REFERENCE_DIR=/path/to/reference_catalogs

``catalog_provider_from_environment()`` then returns a composite provider with
the configured directory first and packaged catalogs as fallback. The custom
directory extends and can override application lookup; it does not disable
packaged fallback. Core provider classes do not read this environment variable.

Combined catalog files use names such as
``ecoinvent__3.10__cutoff.json``. Requested profiles must match embedded
metadata exactly. A 3.10 catalog is not accepted for 3.10.1.

Inject an in-memory provider
----------------------------

.. code-block:: python

   from brightpath import InventoryPipeline
   from brightpath.adapters import default_adapter_registry
   from brightpath.background import (
       BiosphereCatalog,
       InMemoryCatalogProvider,
       TechnosphereCatalog,
   )

   provider = InMemoryCatalogProvider(
       technosphere=(
           TechnosphereCatalog(
               profile=context.background.technosphere,
               identities=frozenset(
                   {
                       (
                           "market for electricity, low voltage",
                           "electricity, low voltage",
                           "CH",
                           "kilowatt hour",
                       )
                   }
               ),
               source="application catalog",
           ),
       ),
       biosphere=(
           BiosphereCatalog(
               profile=context.background.biosphere,
               identities=frozenset(
                   {
                       (
                           "Carbon dioxide, fossil",
                           ("air", "urban air close to ground"),
                           "kilogram",
                       )
                   }
               ),
               source="application catalog",
           ),
       ),
   )
   pipeline = InventoryPipeline(default_adapter_registry(), provider)

The provider is injected once and is used by both validation and migration
endpoint checks. It is also injected into readers whose adapter declares
``requires_catalog_provider``, currently SimaPro CSV.

Legacy combined catalog API
---------------------------

The functions under ``brightpath.catalogs`` remain compatibility bridges. For
example, ``load_background_catalog(BackgroundProfile(...))`` loads exact axes
through the provider stack and combines the technosphere with the documented
legacy biosphere default. ``available_catalog_profiles()`` projects available
technosphere profiles back to ``BackgroundProfile``.

These functions cannot express independently selected background axes. New
code should call ``provider.load_technosphere()`` and
``provider.load_biosphere()`` directly.

Packaged catalog coverage
-------------------------

The installed identity catalogs currently expose:

* ecoinvent 3.6 through 3.12 cut-off technosphere;
* ecoinvent 3.6 through 3.12 consequential technosphere;
* ecoinvent 3.6 through 3.12 biosphere;
* UVEK 2025 cut-off technosphere, with identities generated from the current
  UVEK source workbook;
* UVEK 2025 biosphere identities from that legacy combined catalog resource.

Provider discovery consequently lists both ``BiosphereProfile("uvek",
"2025")`` and the ecoinvent profiles. The documented compatibility context for
UVEK 2025 nevertheless uses ecoinvent 3.10 biosphere identities; state this
explicitly as ``BiosphereProfile("ecoinvent", "3.10")`` in new contexts.

Migration resources exist for ecoinvent 3.5, but no packaged 3.5 validation
catalog exists. Supply an exact application catalog before strict execution.

Catalog release gate
--------------------

``brightpath/data/export/reference_catalogs/RESOURCE_MANIFEST.json`` is marked
``legal_review_required``. The manifest records integrity and provenance; it is
not a redistribution license. This status is an explicit governance gate for a
stable public release. Maintainers must obtain the applicable legal approval
or use separately licensed/local catalogs before release.

Do not generate or distribute catalogs in violation of source database terms,
and never commit complete proprietary background inventories.

Facade validation and exceptions
--------------------------------

``BrightwayInventory.validate()`` and ``SimaProInventory.validate()`` return
the v1 ``ValidationReport`` projection and accept an explicit
``catalog_provider=``. SimaPro can also run render preflight:

.. code-block:: python

   report = simapro.validate(
       check_background_links=True,
       check_simapro_rendering=True,
       catalog_provider=provider,
   )

The package has one ``InventoryValidationError`` class. It always exposes the
shared immutable ``.report`` and retains ``.legacy_report`` or ``.result`` only
when raised from a facade or upload-analysis compatibility path:

.. code-block:: python

   from brightpath import InventoryValidationError

   try:
       inventory.write_excel("inventory.xlsx")
   except InventoryValidationError as error:
       for issue in error.report.issues:
           print(issue.code, issue.message)
