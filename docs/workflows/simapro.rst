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

Water emission units and uncertainty
------------------------------------

The writer retains its existing water-emission convention of 1000 kg per m3.
For biosphere exchanges named ``Water`` outside natural resources, it converts
``cubic meter`` quantities to ``kilogram`` and scales the distribution together
with the amount. Normal standard deviations and uniform/triangular bounds are
scaled by 1000; lognormal scale is dimensionless and stays unchanged. Negative
amounts retain their sign. Exchanges already in kilograms and natural-resource
water are not converted.

Other units, unsupported water distribution types, and unevaluated water
exchange formulas raise a serialization error. Resolve such formulas explicitly
before export; the exchange writer currently emits numeric amounts, whereas
calculated parameter sections support formulas. Input inventories are unchanged.

An empty or ``unspecified`` biosphere subcompartment is written as a blank field.
Other unknown subcompartments still require an explicit mapping.

Regression tests parse the emitted uncertainty fields with Brightway's
`SimaPro CSV extractor <https://docs.brightway.dev/en/latest/_modules/bw2io/extractors/simapro_csv.html>`_,
which interprets normal uncertainty as squared standard deviation and lognormal
uncertainty as squared geometric standard deviation. This checks the numeric
representation; it does not substitute for importing a complete scenario into
SimaPro.

Waste signs and supplier categories
---------------------------------------------

For both ecoinvent and UVEK contexts, the reference flow in the ``Waste treatment``
section is written with a positive magnitude: both +1 and -1 become +1. The
quantity is preserved, so -2 becomes +2 rather than being replaced by one.
Links to waste suppliers in ``Waste to treatment`` are sign-reversed, not made
absolute: positive and negative exchanges remain distinguishable, including credits. Ordinary material and energy inputs
inside a waste process retain their signs. Normal and lognormal spread are
unchanged by sign reflection; distribution bounds are negated and exchanged,
and location/sign fields follow the reflected distribution.

The production ``simapro category`` determines the process section. For links
to included suppliers, that supplier's category also determines the exchange
section. Explicit external-exchange categories take precedence over the existing
name heuristic. Conflicting categories for the same included supplier identity
fail serialization. Generic Brightway ``type="process"`` does not override a
waste category. On import, explicit SimaPro process categories and exchange
sections take precedence over name heuristics.

Unsupported distribution types and unevaluated formulas on reflected export
exchanges fail explicitly. Import sign reflection preserves formulas by negating
them. For supplier links, +2 becomes -2 and -2 becomes +2.
External suppliers without
categories still use the existing name heuristic; this is not proof of their
classification in a target library.

Regression tests cover signed distributions and complete linked inventories in
cut-off and consequential contexts. The exported CSVs are read back through the
Brightway importer, and technosphere matrices and solved supplier demands are
compared. Native SimaPro import and LCIA validation remain necessary before a
complete Premise scenario uses this backend by default.

The category/section consistency requirement is also enforced by
`Brightway's SimaPro process parser <https://docs.brightway.dev/projects/bw-simapro-csv/en/stable/_modules/bw_simapro_csv/blocks/process.html>`_.

Classification-based waste inference
------------------------------------

Pass ``category_mode="infer_classifications"`` to ``render`` or ``write_csv``
to infer waste status from ISIC revision 4 and CPC metadata. Explicit production
``simapro category`` values remain authoritative, including native round trips.
Inferred processes use a generic ``Classified`` folder: folder naming is separate
from waste status. Included supplier classifications also control the section
and sign of links to those suppliers.

ISIC waste treatment/recovery and sewage sectors provide supporting evidence;
the actual reference product, unit and sign constrain inference. Recovered energy,
fertiliser, compost, biogas and dried poultry manure are ordinary products only
when their reference names, CPC codes and unit dimensions agree and their
reference amounts are positive. Negative recovered products and energy CPC codes
attached to kilograms of waste require review. A treatment activity name alone
does not justify classifying its allocated products as waste treatment. Unlisted
positive treatment products and services remain unresolved. CPC waste goods alone
do not imply waste treatment.
Ambiguous markets, incomplete classifications and conflicting evidence raise
``simapro_waste_unresolved`` errors. Review these cases and supply explicit
production categories before export. External links require explicit categories
if the supplier is absent; this mode never falls back to name keywords.

These are conservative inference rules, not an official ISIC-to-SimaPro mapping.
Their sources are the UN ISIC revision 4 classes 3700, 3821/3822 and 3830,
and CPC waste-treatment service groups 941/943. They do not establish classification
accuracy against the legacy Premise exporter, whose lookup is also a convention.

Classification definitions: `ISIC 3821 <https://unstats.un.org/unsd/classifications/Econ/Structure/Detail/en/27/3821>`_,
`ISIC 3830 <https://unstats.un.org/unsd/classifications/Econ/Detail/EN/27/3830>`_,
and `CPC 943 <https://unstats.un.org/unsd/classifications/Econ/Structure/Detail/EN/1073/943>`_.

The reference-product distinction follows
`ecoinvent's activity and product documentation <https://support.ecoinvent.org/activities-products>`_.

One reviewed exception identifies the Swiss ``treatment of effluent from nitrogen
trifluoride production, wastewater treatment, class 3`` dataset as waste treatment,
even with a positive reference quantity. It requires the matching effluent reference
product, cubic-metre unit, ISIC 3700 and CPC 39990. The audit rule is
``reviewed_nf3_effluent_treatment``; explicit SimaPro categories still take precedence.

Market supplier names
---------------------

Market names retain their complete activity text, including technology, period,
vehicle-size and scenario qualifiers. A market reference product does not replace
its activity name. Packaged global-market abbreviations apply only to unqualified
canonical market names. Before rendering, Brightpath rejects distinct included
supplier identities whose serialized labels collide after Latin-1 conversion and
case folding; no partial CSV is emitted. The same formatter names production
outputs and technosphere links.

Caller-supplied folder hierarchies
----------------------------------

Set dataset metadata ``simapro category path`` to a slash-separated folder path
(e.g. an ISIC division/group/class hierarchy). The writer applies it after resolving
the production category, preserving the product or waste-treatment category type.
It also applies to explicit production categories. Without this metadata, supplied
folders remain unchanged, and inferred categories use ``Classified``. Folder metadata
never resolves unknown waste status. Premise supplies the same hierarchy and fallbacks
as its openLCA export through ``assign_simapro_category_paths`` on its prepared payload.

The SimaPro ``Geography`` field defaults to the dataset's ``location`` (including
country, regional and global codes). Explicit ``geography`` or native SimaPro
``Geography`` metadata remains authoritative.

Default ecoinvent process display names follow Premise's legacy convention:
``reference product {location}| activity name | Cut-off, U`` (or ``Conseq, U``).
Explicit native ``Process name`` metadata or ``simapro process name`` remains
authoritative. This display field is separate from supplier-link labels.
