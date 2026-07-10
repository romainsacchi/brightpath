SimaPro CSV workflows
=====================

Load with exact context
-----------------------

SimaPro names encode database-specific conventions, so pass the exact
technosphere and biosphere context when it is not embedded elsewhere:

.. code-block:: python

   from brightpath import (
       BackgroundContext,
       BiosphereProfile,
       FormatProfile,
       InventoryContext,
       SimaProInventory,
       TechnosphereProfile,
   )
   from brightpath.background import catalog_provider_from_environment

   context = InventoryContext(
       format=FormatProfile("simapro_csv", encoding="latin-1"),
       background=BackgroundContext(
           technosphere=TechnosphereProfile("ecoinvent", "3.10", "cutoff"),
           biosphere=BiosphereProfile("ecoinvent", "3.10"),
       ),
   )
   inventory = SimaProInventory.from_csv(
       "foreground.csv",
       context=context,
       database_name="foreground-model",
       catalog_provider=catalog_provider_from_environment(),
   )

The reader parses SimaPro names into canonical ``name``, ``reference product``,
and ``location`` fields. Database, project, and process parameters are
preserved where the format represents them.

Exact biosphere normalization
-----------------------------

SimaPro flow names are normalized against the exact
``context.background.biosphere`` catalog. ``catalog_provider`` must supply that
profile; a different ecoinvent release is not substituted. Missing catalog or
integrity failures therefore fail the read instead of silently applying fixed
3.10 naming data.

When ``catalog_provider`` is omitted, ``SimaProInventory.from_csv()`` uses the
environment/package provider stack. ``InventoryPipeline.read()`` injects its
own provider automatically because ``SimaProCSVAdapter`` declares
``requires_catalog_provider=True``. Applications can still override it through
``adapter_kwargs``:

.. code-block:: python

   application_provider = catalog_provider_from_environment()
   read = pipeline.read(
       "foreground.csv",
       hint=context.as_hint(),
       adapter_kwargs={"catalog_provider": application_provider},
   )

Inspect and validate
--------------------

.. code-block:: python

   for dataset in inventory.data:
       print(
           dataset["name"],
           dataset["reference product"],
           dataset["location"],
       )

   report = inventory.validate(
       check_background_links=True,
       check_simapro_rendering=True,
   )
   for issue in report.issues:
       print(issue.severity, issue.code, issue.path, issue.message)

Rendering validation checks production categories, supported units,
unrepresentable exchanges, and other output conditions. Validation also
reports mixed cut-off/consequential markers or a mismatch between SimaPro name
markers and the selected ecoinvent technosphere.

The generic pipeline runs adapter-owned SimaPro format validation by default:

.. code-block:: python

   result = pipeline.validate(
       read.value,
       check_format=True,
       check_background_links=True,
   )

Its stable stage order is structure, SimaPro format, then background links.

Render without writing
----------------------

.. code-block:: python

   rendered = inventory.render()
   if rendered.has_errors:
       for issue in rendered.issues:
           print(issue.code, issue.message)
   else:
       print(f"Prepared {len(rendered.rows)} rows")

Rendering is read-only and is useful for previews. The generic pipeline wraps
known representability findings in conversion-preflight issues and explicit
losses governed by ``ConversionPolicy``.

Create from canonical data
--------------------------

.. code-block:: python

   simapro = SimaProInventory.from_data(
       data,
       context=context,
       database_name="foreground-model",
   )

Production exchanges need a supported ``simapro category`` for output:

.. code-block:: python

   production_exchange = {
       "type": "production",
       "name": "foreground material production",
       "reference product": "foreground material",
       "product": "foreground material",
       "location": "CH",
       "unit": "kilogram",
       "amount": 1.0,
       "simapro category": "Materials/Other",
   }

Write SimaPro CSV
-----------------

.. code-block:: python

   output = inventory.write_csv("foreground-checked")

Output is semicolon-delimited and Latin-1 encoded. Characters outside Latin-1
raise ``SimaProSerializationError``. ``validate=False`` bypasses facade
structural/catalog preflight, but rendering and encoding still apply.

UVEK in SimaPro
---------------

UVEK is an independent background family, not an export option. The current
UVEK 2025 technosphere uses ecoinvent 3.10 biosphere identities:

.. code-block:: python

   uvek_context = InventoryContext(
       format=FormatProfile("simapro_csv", encoding="latin-1"),
       background=BackgroundContext(
           technosphere=TechnosphereProfile("uvek", "2025", "cutoff"),
           biosphere=BiosphereProfile("ecoinvent", "3.10"),
       ),
   )
   uvek = SimaProInventory.from_csv(
       "foreground-uvek.csv",
       context=uvek_context,
       catalog_provider=catalog_provider_from_environment(),
   )
   brightway = uvek.to_brightway()

   assert brightway.context.background == uvek_context.background

The same context is valid for Brightway output. Conversion does not trigger an
ecoinvent↔UVEK migration; no such mapping rules are available.

CSV detection
-------------

The SimaPro adapter looks for bounded content evidence: export signatures,
format/separator declarations, process fields, and exchange sections. A
``.csv`` suffix alone is not evidence. When the application already knows the
source, pass ``FormatProfile("simapro_csv")`` or CLI
``--source-format simapro_csv``.
