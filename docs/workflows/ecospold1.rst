ecoSpold1 single-output exchange
================================

Scope
-----

The ``ecospold1`` adapter reads and writes XML process files containing one or
more datasets. Each dataset must have exactly one production exchange and no
allocation records. Multi-output processes, elementary-flow definition files,
LCIA datasets, directories, and ZIP archives are not supported.

Metadata profile
----------------

ecoSpold1 requires structured provenance that is not normally present in a
Brightway, SimaPro, or openLCA description. Supply it as YAML or JSON:

.. code-block:: yaml

   schema_version: 1
   defaults:
     classification:
       category: Materials
       subcategory: Foreground processes
     time_period:
       start_date: 2020-01-01
       end_date: 2020-12-31
       data_valid_for_entire_period: true
     source:
       first_author: Smith J.
       year: 2020
       title: Foreground inventory study
       place_of_publication: Zurich
     data_entry_person:
       name: Jane Smith
       address: Example Institute, Zurich
       company_code: EXAMPLE
       country_code: CH
     data_generator_person:
       name: Jane Smith
       address: Example Institute, Zurich
       company_code: EXAMPLE
       country_code: CH
     copyright: false

Use ``datasets`` entries for exact overrides. A selector may use ``code`` or a
combination of ``name``, ``reference_product``, ``location``, and ``unit``:

.. code-block:: yaml

   datasets:
     - selector:
         code: foreground-2
       metadata:
         source:
           title: A dataset-specific source

Explicit overrides take precedence over metadata preserved from an ecoSpold1
import, which takes precedence over document defaults. Multiple matching
overrides are an error.

Command line
------------

.. code-block:: console

   brightpath convert-format foreground.xlsx foreground.xml \
     --source-format brightway_excel \
     --source-technosphere-family ecoinvent \
     --source-technosphere-version 3.10 \
     --source-technosphere-system-model cutoff \
     --source-biosphere-family ecoinvent \
     --source-biosphere-version 3.10 \
     --target-format ecospold1 \
     --ecospold1-metadata metadata.yaml \
     --policy strict

Run the same command with ``--dry-run`` to validate output metadata and
representability without creating the XML file.

Python
------

.. code-block:: python

   from brightpath.formats.ecospold1_metadata import EcoSpold1ExportProfile

   profile = EcoSpold1ExportProfile.from_file("metadata.yaml")
   options = {"metadata_profile": profile}
   conversion = pipeline.convert(
       document,
       "ecospold1",
       adapter_options=options,
   )
   written = pipeline.write(
       document,
       "foreground.xml",
       target_format="ecospold1",
       adapter_options=options,
   )

Description fields
------------------

BrightPath maps a dataset comment to ecoSpold1's general process comment. It
does not parse citations, authors, dates, addresses, or publication places from
that prose. Missing required structured metadata blocks export even under a
permissive conversion policy.
