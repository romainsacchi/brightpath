Core concepts
=============

Four independent axes
---------------------

BrightPath models four pieces of information separately.

.. list-table::
   :header-rows: 1
   :widths: 24 34 42

   * - Axis
     - Examples
     - Meaning
   * - Software format
     - Brightway Excel, SimaPro CSV
     - File syntax and serialization rules
   * - Background family
     - ecoinvent, UVEK
     - The database supplying external links
   * - Background version
     - ecoinvent 3.10, UVEK 2025
     - The release whose identities must match
   * - System model
     - cut-off, consequential
     - The background modeling system

Changing one axis never implies changing another. ``to_simapro()`` changes
only the format view. ``migrate_background()`` changes only the background
profile and linked identities.

Operation matrix
----------------

.. list-table::
   :header-rows: 1
   :widths: 28 24 24 24

   * - Operation
     - File format
     - Background profile
     - Source object
   * - ``normalize()``
     - Preserved
     - Preserved
     - Unchanged
   * - ``validate()``
     - Preserved
     - Preserved
     - Unchanged
   * - ``to_simapro()`` / ``to_brightway()``
     - Changed explicitly
     - Preserved
     - Unchanged
   * - ``migrate_background()``
     - Preserved
     - Changed explicitly
     - Unchanged
   * - ``write_excel()`` / ``write_csv()``
     - Written explicitly
     - Preserved
     - Unchanged

Canonical inventory data
------------------------

Both facades use Brightway-style dictionaries internally. A minimal dataset
contains a unique ``(name, reference product, location)`` identity, a unit,
and exactly one matching production exchange.

.. code-block:: python

   data = [
       {
           "name": "foreground service",
           "reference product": "service",
           "location": "GLO",
           "unit": "unit",
           "exchanges": [
               {
                   "type": "production",
                   "name": "foreground service",
                   "reference product": "service",
                   "product": "service",
                   "location": "GLO",
                   "unit": "unit",
                   "amount": 1.0,
               },
               {
                   "type": "technosphere",
                   "name": "market for electricity, low voltage",
                   "reference product": "electricity, low voltage",
                   "location": "CH",
                   "unit": "kilowatt hour",
                   "amount": 2.5,
               },
           ],
       }
   ]

``InventoryDocument`` is the internal software-neutral owner of this data.
The public facades expose copies through ``.data``, ``.metadata``, and parameter
properties. Unknown dictionary fields are retained in memory. BrightPath's
Brightway writer uses tagged JSON values for nested fields that the generic
workbook layout cannot otherwise represent.

Supported formats
-----------------

.. list-table::
   :header-rows: 1
   :widths: 28 18 18 18 18

   * - Format
     - Load facade
     - Write
     - Convert
     - Upload analysis
   * - Brightway Excel
     - Yes
     - Yes
     - Yes
     - Yes
   * - SimaPro CSV
     - Yes
     - Yes
     - Yes
     - Yes
   * - Brightway CSV/TSV
     - No
     - No
     - No
     - Yes
   * - OpenLCA Excel
     - Planned
     - Planned
     - Planned
     - No
   * - ecospold2
     - Planned
     - Planned
     - Planned
     - No

Background support
------------------

Packaged reference catalogs cover ecoinvent 3.6 through 3.12 for cut-off and
consequential inventories, plus UVEK 2025 cut-off. Packaged migration rules
cover ecoinvent cut-off 3.5 through 3.12 in both directions. Catalog support
and migration support are separate: ecoinvent 3.5 migration rules exist, but a
3.5 validation catalog must be supplied by the user.

Consequential profiles can be parsed, formatted, and validated against their
catalogs, but background migration is currently cut-off only. Cross-family
ecoinvent/UVEK migration is not implemented.
