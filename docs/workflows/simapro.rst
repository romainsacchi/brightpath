SimaPro CSV workflows
=====================

Load a SimaPro export
---------------------

SimaPro process names encode database-specific conventions, so the source
profile is required:

.. code-block:: python

   from brightpath import BackgroundProfile, SimaProInventory

   inventory = SimaProInventory.from_csv(
       "foreground.csv",
       background_profile=BackgroundProfile("ecoinvent", "3.10", "cutoff"),
       database_name="foreground-model",
   )

The reader parses SimaPro names into canonical ``name``, ``reference product``,
and ``location`` fields. It also preserves database, project, and process
parameters where the SimaPro format represents them.

Inspect parsed data
-------------------

.. code-block:: python

   for dataset in inventory.data:
       identity = (
           dataset["name"],
           dataset["reference product"],
           dataset["location"],
       )
       print(identity)

``.data`` returns a deep copy. Use :meth:`~brightpath.SimaProInventory.from_data`
to construct a changed inventory rather than mutating the returned copy and
expecting the original facade to change.

Validate SimaPro-specific requirements
--------------------------------------

.. code-block:: python

   report = inventory.validate(
       check_background_links=True,
       check_simapro_rendering=True,
   )
   for issue in report.issues:
       print(issue.severity, issue.code, issue.path, issue.message)

Rendering validation catches missing SimaPro categories, unsupported units,
unrepresentable exchanges, and other output-specific conditions. Validation
also reports mixed cut-off/consequential markers or a mismatch between name
markers and the selected ecoinvent profile.

Render without writing
----------------------

.. code-block:: python

   rendered = inventory.render()
   if rendered.has_errors:
       for issue in rendered.issues:
           print(issue.code, issue.message)
   else:
       print(f"Prepared {len(rendered.rows)} SimaPro rows")

This is useful for previews and intake services. Rendering is read-only.

Create SimaPro output from data
-------------------------------

Canonical data can be wrapped directly:

.. code-block:: python

   simapro = SimaProInventory.from_data(
       data,
       background_profile=BackgroundProfile("uvek", "2025", "cutoff"),
       database_name="uvek-foreground",
   )

For SimaPro output, each production exchange needs a ``simapro category``:

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

Write the same format
---------------------

.. code-block:: python

   output = inventory.write_csv("foreground-checked")
   # output ends with .csv and is an absolute pathlib.Path

SimaPro files are semicolon-delimited and written with Latin-1 encoding.
Characters outside Latin-1 raise ``SimaProSerializationError``. Passing
``validate=False`` bypasses the structural and catalog preflight, but rendering
and encoding requirements are still enforced.

UVEK is a background family
---------------------------

The same SimaPro facade works with UVEK:

.. code-block:: python

   uvek = SimaProInventory.from_csv(
       "foreground-uvek.csv",
       background_profile=BackgroundProfile("uvek", "2025", "cutoff"),
   )
   uvek.write_csv("foreground-uvek-roundtrip.csv")

UVEK selection changes name parsing and rendering. It does not trigger an
ecoinvent-to-UVEK migration.
