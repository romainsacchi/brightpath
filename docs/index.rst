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
Brightway block TSV, and SimaPro CSV file adapters. OpenLCA Excel and ecoSpold2
are reserved in the model but remain unregistered and unimplemented. Query the
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
   workflows/conversion
   workflows/migration
   workflows/validation
   workflows/analysis
   limitations

.. toctree::
   :maxdepth: 2
   :caption: Maintainer guide

   architecture
   adr/index
   api

Indices
-------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
