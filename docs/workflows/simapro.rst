SimaPro CSV workflows
=====================

Load with exact context
-----------------------

SimaPro names encode database-specific conventions, and a SimaPro CSV is not
treated as an exact background-context source. New code should therefore pass
the exact technosphere and biosphere context explicitly:

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

Facade rendering validation checks production categories, supported units,
unrepresentable exchanges, and other output conditions. Facade validation also
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
The adapter-owned format stage checks intrinsic SimaPro grammar only.
Representability, information loss, and mapping ambiguity are exclusively
conversion-preflight concerns and cannot be overridden by target validation.

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

Production exchanges need a supported ``simapro category`` for output. Its
first component is the SimaPro category type (``material``, ``energy``,
``transport``, ``processing``, ``use``, ``waste treatment``, or
``waste scenario``); remaining components form the process subcategory:

.. code-block:: python

   production_exchange = {
       "type": "production",
       "name": "foreground material production",
       "reference product": "foreground material",
       "product": "foreground material",
       "location": "CH",
       "unit": "kilogram",
       "amount": 1.0,
       "simapro category": "material/Other",
   }

BrightPath preserves supplied categories by default, including intentional
custom foreground categories. For ecoinvent 3.9 or 3.9.1 cut-off inventories,
the writer can instead look for a path observed in the ecoinvent 3.9.1 SimaPro
9.5 reference export:

.. code-block:: python

   from brightpath import SimaProCategoryMode

   preview = inventory.render(
       category_mode=SimaProCategoryMode.INFER_EXISTING,
   )
   output = inventory.write_csv(
       "foreground-existing-categories",
       category_mode=SimaProCategoryMode.INFER_EXISTING,
   )

The generic pipeline forwards the same explicit option to the SimaPro
adapter:

.. code-block:: python

   result = pipeline.write(
       document,
       "foreground-existing-categories.csv",
       target_format="simapro_csv",
       adapter_kwargs={"category_mode": "infer_existing"},
   )

Resolution first preserves an already observed path, then looks for an exact
reference-product, unit, and process-role match. A near product name is used
only when multiple reference products agree on a sufficiently specific
category hierarchy. Market activities are assigned to an observed ``Market``
path; other production activities use ``Transformation``. Each replacement is
included in the render result as a ``simapro_category_inferred`` warning with
its method, confidence, and candidates. The generic pipeline also retains this
warning in its operation report. Ambiguous or low-confidence results are
reported and the supplied custom category is preserved. Rendering and writing
operate on copies and never change the inventory's canonical data.

For example, ``carbon dioxide, captured`` is not an ecoinvent 3.9.1 reference
product. The related gas products agree on ``Chemicals/Gases``, so a production
activity resolves to ``material/Chemicals/Gases/Transformation`` while a market
activity resolves to ``material/Chemicals/Gases/Market``. Other background
profiles remain unchanged until an exact, separately attributed category
catalog is available.

Write SimaPro CSV
-----------------

.. code-block:: python

   output = inventory.write_csv("foreground-checked")

Output uses the SimaPro 9 CSV grammar: semicolon delimiters, CRLF records,
Latin-1 text, and the DEL character as the intra-cell paragraph marker used by
SimaPro exports. Characters outside Latin-1 are transliterated where possible
and otherwise replaced with ``?``. Product, waste-treatment, technosphere, and
biosphere rows use their distinct SimaPro column layouts; numeric exchange
values retain 15 significant digits. ``validate=False`` bypasses facade
structural/catalog preflight, but rendering and encoding still apply.

Canonical Brightway datasets normally use ``type="process"``; BrightPath
renders these as SimaPro ``Unit process`` records. Existing SimaPro process
identifiers are preserved when they use the observed eight-character prefix
and fifteen-digit suffix. Other canonical activity codes are converted to a
stable ``BRTPATH0`` identifier with a fifteen-digit suffix so that SimaPro does
not receive UUIDs or other unsupported identifiers.

Before importing a foreground CSV, enable the exact background library in the
target SimaPro project. A ``process not found`` message for an otherwise exact
ecoinvent link normally means that the corresponding library is unavailable
to that project rather than that the CSV exchange name is malformed.

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

The same context is valid for Brightway output. Format conversion alone does
not trigger background migration. An explicit background-migration operation
can use the heuristic ecoinvent 3.6–3.12 to UVEK 2025 compatibility resources.

CSV detection
-------------

The SimaPro adapter looks for bounded content evidence: export signatures,
format/separator declarations, process fields, and exchange sections. A
``.csv`` suffix alone is not evidence. When the application already knows the
source, pass ``FormatProfile("simapro_csv")`` or CLI
``--source-format simapro_csv``.
