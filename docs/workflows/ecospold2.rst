ecoSpold2 single-output exchange
================================

Scope and package shapes
------------------------

The ``ecospold2`` adapter reads XSD-valid activity datasets from one ``.spold``
file, a ZIP package, or a directory. It writes one ``.spold`` per activity and
a minimal ``MasterData/IntermediateExchanges.xml`` file. A ``.spold``
destination selects single-file output and therefore requires one dataset. A
``.zip`` destination produces a deterministic package. Python callers can also
select ``package_mode="directory"`` explicitly.

Each activity must have exactly one production exchange. Child activity
datasets, allocations, transfer coefficients, substitutions, multi-output
activities, LCIA datasets, and elementary-flow definition files are outside
the supported route. ZIP reads reject encryption, traversal paths, symbolic
links, duplicate entries, excessive expansion, and oversized members.

Metadata profile
----------------

Required administrative and temporal fields must be supplied as structured
YAML or JSON when they were not preserved from an ecoSpold2 import:

.. code-block:: yaml

   schema_version: 1
   defaults:
     time_period:
       start_date: 2020-01-01
       end_date: 2020-12-31
       data_valid_for_entire_period: true
     macroeconomic_scenario:
       name: Business as usual
       id: 11111111-1111-4111-8111-111111111111
     data_entry_person:
       name: Jane Smith
       email: jane@example.org
       id: 22222222-2222-4222-8222-222222222222
     data_generator_person:
       name: Jane Smith
       email: jane@example.org
       id: 22222222-2222-4222-8222-222222222222
     data_published_in: 2
     published_source_id: 33333333-3333-4333-8333-333333333333
     published_source_year: '2020'
     published_source_first_author: Smith J.
     copyright: false

Dataset-specific overrides use the same exact selectors as the ecoSpold1
profile: ``code``, ``name``, ``reference_product``, ``location``, and ``unit``.
An explicit override wins over imported ``ecospold2`` metadata, which wins over
document defaults. Multiple matching overrides are rejected.

Description fields are not metadata
-----------------------------------

BrightPath maps a canonical dataset comment to ecoSpold2's general activity
comment. It never tries to extract authors, email addresses, dates, scenario
names, or identifiers from that prose. Such extraction would be ambiguous and
not reproducible. Missing required structured fields are hard errors under both
strict and permissive policies.

UUID reference catalog
----------------------

ecoSpold2 represents linked activities, intermediate products, elementary
flows, units, and geographies with UUIDs. Foreground activity and product UUIDs
are preserved when present and otherwise generated deterministically. Links to
other foreground activities are resolved by exact canonical identity.

Every external technosphere or elementary-flow link needs an exact preserved
UUID or a catalog entry. BrightPath automatically loads UUIDs from its packaged
catalog for the document's exact technosphere version/system model and
biosphere version. An explicit ``--ecospold2-references`` catalog overrides
that default. BrightPath does not derive UUIDs from names or neighboring
ecoinvent versions and does not create substitute background entities. A
reference catalog looks like this:

.. code-block:: yaml

   schema_version: 1
   technosphere:
     - identity:
         name: market for electricity, medium voltage
         reference_product: electricity, medium voltage
         location: CH
         unit: kilowatt hour
       activity_id: 33333333-3333-4333-8333-333333333333
       flow_id: 44444444-4444-4444-8444-444444444444
   biosphere:
     - identity:
         name: Carbon dioxide, fossil
         categories: [air, urban air close to ground]
         unit: kilogram
       flow_id: 55555555-5555-4555-8555-555555555555

Command line
------------

.. code-block:: console

   brightpath convert-format foreground.xlsx foreground.zip \
     --source-format brightway_excel \
     --source-technosphere-family ecoinvent \
     --source-technosphere-version 3.10 \
     --source-technosphere-system-model cutoff \
     --source-biosphere-family ecoinvent \
     --source-biosphere-version 3.10 \
     --target-format ecospold2 \
     --ecospold2-metadata metadata.yaml \
     --policy strict

The packaged UUID catalogs have complete exact coverage for every packaged
ecoinvent profile: 3.6, 3.7, 3.8, 3.9, 3.10, 3.11, and 3.12 cut-off and
consequential where supported, plus the packaged 3.9.1 and 3.10.1 cut-off
profiles. The UVEK catalog has complete UUID coverage for its ecoinvent 3.10
biosphere; its separate UVEK technosphere still requires preserved UUIDs or an
explicit catalog for ecoSpold2 export.

Use ``--dry-run`` to resolve metadata and UUIDs, apply representability policy,
and XSD-validate all generated activity XML without writing the package.

Python
------

.. code-block:: python

   from brightpath.formats.ecospold2 import write_ecospold2
   from brightpath.formats.ecospold2_metadata import (
       EcoSpold2ExportProfile,
       EcoSpold2ReferenceCatalog,
   )

   profile = EcoSpold2ExportProfile.from_file("metadata.yaml")
   references = EcoSpold2ReferenceCatalog.from_file("references.yaml")
   write_ecospold2(
       document,
       "foreground.zip",
       metadata_profile=profile,
       reference_catalog=references,
   )

Brightway compatibility
-----------------------

The package layout uses ``datasets/`` and
``MasterData/IntermediateExchanges.xml``. The integration test passes the
generated datasets directory to ``bw2io.SingleOutputEcospold2Importer``, applies
its import strategies, and writes the foreground database in an isolated
Brightway project. Successful import does not waive the UUID requirement:
external flows must still exist in the target project's databases to link.
