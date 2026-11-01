Validate a target SimaPro substance list
======================================

SimaPro stock membership and background-database validity are separate checks.
Substance-list validation is optional. To enable it, choose the exact target
SimaPro version and provide its substance catalogue.
BrightPath never downloads a list or chooses another version automatically.
Programmatic exports without an explicit stock catalogue retain their existing
behaviour; they do not certify stock membership.

An external GLAD workbook can provide name, main-compartment and wire-unit
membership for its declared Professional nomenclature:

.. code-block:: python

   from brightpath.formats.simapro_stock import (
       SimaProStockCatalog,
       validate_simapro_stock_csv,
   )

   stock = SimaProStockCatalog.load_glad_xlsx(
       "20250804_CurrentSimaProSubstanceList_GLAD.xlsx",
       stock_version="10.2",
       complete=False,
   )
   report = validate_simapro_stock_csv(
       csv_path.read_bytes(), stock_catalog=stock, stock_version="10.2",
   )
   print(report.to_dict())

The loader selects the exact token ``Professional 10.2`` from
``SupportedNomenclatures``. It excludes ``Professional 10.20`` and other
nomenclatures, maps the published main-compartment labels to CSV section names,
and records the unmodified workbook's SHA-256. It introduces no substance-name,
chemistry or unit aliases. ``from_glad_rows`` accepts the same columns as
dictionaries when an application already owns the parsed metadata; provide its
source and ``source_sha256`` explicitly.

``complete=False`` is conservative: an observed entry may match, while a missing
entry is unknown. Set ``complete=True`` only when the supplied catalogue is
known to cover the declared sections completely. This flag is never inferred
from a row count or a release changelog. Unknown results block strict stock
validation; they do not prove that the substance is absent from SimaPro.

The 2025-08-04 workbook contains 70,865 entries across nomenclatures, including
36,769 tagged ``Professional 10.2``. It supplies no subcompartment relation.
Default membership therefore checks name/main/unit and reports that
subcompartments were not checked. It does not certify compatibility with SimaPro
10.4. Combining it with a partial 10.3/10.4 changelog does not create a complete
10.4 catalogue. BrightPath does not bundle or certify a complete SimaPro 10.4
substance list; supply a separately verified external catalogue for that version.

Select the receiving software version in the CLI
-----------------------------------------------

``convert-format --target-format simapro_csv`` exports normally when
``--simapro-version`` is omitted. No substance-list check runs, and a supplied
``--simapro-substance-list`` path is ignored. Normal format and background
validation still apply.

Supplying ``--simapro-version`` enables the additional check and requires a
matching ``--simapro-substance-list``. The version identifies
the receiving SimaPro software's substance list, independently of the
inventory's ecoinvent/UVEK background version and the CSV grammar version.

.. code-block:: shell

   brightpath convert-format foreground.xlsx inventory.csv \
     --target-format simapro_csv \
     --simapro-version 10.2 \
     --simapro-substance-list substances.xlsx \
     --report conversion-report.json

The workbook must contain the exact requested Professional version. A list for
10.2 cannot satisfy a request for 10.4. A supplied version without a reference
list produces a usage error. No version is inferred or replaced with another.
When validation is enabled, unknown or unmatched substances stop writing even
under a permissive conversion policy. ``--dry-run`` includes the same optional
substance check. When performed, the audit
report includes a separate ``simapro_substances`` stage with the requested
version, reference hash, actual wire cells and match results. No background
migration or automatic substance substitution is performed.

The same optional behaviour applies to ``migrate-background`` when it preserves
a SimaPro CSV source format. When enabled, the substance check runs against the
migrated output before writing, including during a dry run. Other output formats
do not accept these SimaPro-specific options.

Check or guard an actual export
------------------------------

.. code-block:: python

   report = inventory.validate_stock(
       stock_catalog=stock, stock_version="10.2",
   )
   path = inventory.write_csv(
       "inventory.csv", stock_catalog=stock, stock_version="10.2",
   )

The facade check returns an immutable core ``StageReport``. The write guard
checks the final rendered cells before creating the destination directory or
opening an existing file. For writes, omitting ``stock_version`` skips this
check, even if a catalogue or subcompartment-check option is supplied. Providing
``stock_version`` enables validation and requires a matching ``stock_catalog``.
The explicit ``validate_stock`` operation always requires a version and catalogue.
The low-level ``write_simapro_csv`` accepts these same
options, so adapter writes can enforce the check too. Background catalogue
injection remains independent through ``catalog_provider``.

``validate_simapro_stock_rows`` checks renderer rows after the writer's final
Latin-1/spreadsheet-cell transformation. ``validate_simapro_stock_csv`` checks
actual saved Latin-1 CSV cells unchanged. Neither mutates the inventory or
replaces names. Products, technosphere suppliers and parameters are outside the
substance-stock namespace and retain their separate validation rules.

Reports distinguish ``matched``, ``unmatched`` and ``unknown``. A missing
complete-catalogue entry is unmatched; incomplete coverage, an unverified
dimension, ambiguous stock identities or a version mismatch cannot pass.
Metrics record source provenance, requested/supplied versions, coverage,
checked dimensions and every actual substance row. A valid inventory with no
substance rows is reported as ``not_applicable`` and is not certified.

A catalogue with explicit subcompartment coverage can additionally be checked
with ``check_subcompartments=True``. An absent dimension is never interpreted as
an empty or unspecified subcompartment. Catalogue records and coverage can be
provided directly through ``SimaProStockSubstance`` and ``SimaProStockCatalog``.

Stock membership is a vocabulary check. It does not establish scientific
equivalence, authorize a generic-to-ionic substitution, validate the full CSV
grammar, or prove successful native application import, Monte Carlo sampling
or LCIA. Existing identity and representability guards still apply.
