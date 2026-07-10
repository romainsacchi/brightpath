BrightPath documentation
========================

BrightPath validates foreground life-cycle inventories, migrates their
background database links, and writes them in LCA software exchange formats.

The central rule is that **software format and background profile are separate**.
Loading Brightway Excel does not imply SimaPro output, and converting a file
format does not silently change ecoinvent versions.

.. code-block:: text

   source file + source background profile
                    |
                    v
             canonical inventory
                    |
          +---------+----------+
          |                    |
          v                    v
   format conversion    background migration
          |                    |
          +---------+----------+
                    |
                    v
               explicit output

Version 1.0 supports Brightway Excel and SimaPro CSV as full read/write
facades. It supports forward and reverse ecoinvent cut-off migration from 3.5
through 3.12, validates links against exact ecoinvent and UVEK catalogs, and
provides a separate analysis API for file-upload workflows.

Start with :doc:`getting-started`, then choose the guide matching your task.

.. toctree::
   :maxdepth: 2
   :caption: User guide

   getting-started
   concepts
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
