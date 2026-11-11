BrightPath documentation
========================

BrightPath reads foreground life-cycle inventories, validates and normalizes
their datasets, optionally migrates their background links, and writes an
explicit LCA software exchange format.

An inventory context has three independent components:

.. code-block:: text

   InventoryContext
   +-- FormatProfile
   +-- BackgroundContext
       +-- TechnosphereProfile (family, exact version, system model)
       +-- BiosphereProfile    (family, exact version)

Format conversion changes only ``FormatProfile``. Background migration changes
only the requested background profiles and linked identities. Neither
operation assumes the other.

The built-in registry currently provides Brightway Excel, Brightway block CSV,
Brightway block TSV, SimaPro CSV, process-only openLCA JSON-LD ZIP,
single-output ecoSpold1 XML, single-output ecoSpold2 packages, and ILCD 1.1
process packages.
OpenLCA Excel is reserved in the model but remains unregistered. Query the
installed capabilities with ``brightpath formats``.

Start with :doc:`getting-started`, then choose the task-oriented guide.

.. toctree::
   :maxdepth: 2
   :caption: User guide

   getting-started
   concepts
   workflows/cli
   workflows/brightway
   workflows/simapro
   simapro-stock-validation
   simapro-sodium-identity
   simapro-water-ions
   workflows/conversion
   workflows/ecospold1
   workflows/ecospold2
   workflows/ilcd
   workflows/native-fossilwater-quantity
   workflows/parameters
   workflows/migration
   workflows/ecoinvent-to-uvek
   workflows/uvek-export
   workflows/uvek-2026
   workflows/validation
   workflows/analysis
   limitations

.. toctree::
   :maxdepth: 2
   :caption: Maintainer guide

   architecture
   migration-resource-schema
   migration-audit
   uvek-cement-heat-review
   uvek-cement-treatment-review
   uvek-fuels-312-review
   uvek-fuels-39-endpoints
   uvek-fuels-39-silica
   uvek-fuels-39-molybdenum
   uvek-fuels-39-carbon
   uvek-fuels-39-material-markets
   uvek-fuels-39-oxide-services
   uvek-fuels-39-heat-recycling
   uvek-fuels-39-final-markets
   uvek-fuels-38-services
   uvek-fuels-38-nutrients
   uvek-packaging-hdpe
   uvek-fuels-38-lime
   uvek-fuels-38-chemical-stocks
   uvek-fuels-38-additional-chemicals
   uvek-fuels-38-compost
   recycled-paper-migration
   simapro-reference-evidence
   adr/index
   api

Indices
-------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
