Limits and compatibility
========================

Version 1 compatibility
-----------------------

Version 1 intentionally removed the 0.x ``BrightwayConverter`` and
``SimaproConverter`` classes. They coupled source format, target format, and
background behavior. No compatibility wrapper restores that assumption.

The v1 ``BrightwayInventory`` and ``SimaProInventory`` facades remain, including
the technosphere-only ``BackgroundProfile`` argument. New application code
should use ``InventoryContext`` and ``InventoryPipeline`` to express exact
format, technosphere, biosphere, policies, and injected catalogs.

``brightpath.migrations.migrate_inventory`` is not a public v1 export. Its
legacy engine implementation remains internal for focused compatibility tests;
public migration uses the transactional executor or facade methods so source
and target validation cannot be bypassed.

Format boundaries
-----------------

* The built-in registry supports Brightway Excel, Brightway block CSV,
  Brightway block TSV, openLCA JSON-LD ZIP, SimaPro CSV, ecoSpold1 XML, and
  ecoSpold2 and ILCD 1.1 process packages as detection/read/write adapters.
  ecoSpold2 and ILCD also read package directories.
* Only Brightway Excel and SimaPro CSV have dedicated v1 facades. Brightway
  CSV/TSV use the generic pipeline.
* CSV suffixes are ambiguous. Detection inspects content and reports absent or
  tied evidence; callers should pass an explicit format when known.
* SimaPro CSV is semicolon-delimited and Latin-1 encoded. It has stricter unit,
  category, section, and field representation than the canonical inventory.
* OpenLCA Excel is a reserved identifier only. It has no registered adapter and
  does not appear in ``brightpath formats``.
* Custom format identifiers are valid canonical context values.
  ``InventoryDocument.inventory_format`` returns their string ID while
  retaining enum members for known legacy IDs. A custom format is executable
  only when its adapter is registered.
* Read capability requires ``can_validate_format``; write capability also
  requires ``can_preflight_conversion``. Registry construction rejects missing
  flags and non-callable hooks before capability discovery. Runtime hook
  failures and malformed reports fail safely.
* The ecoSpold1 adapter is limited to process datasets with exactly one
  reference output and no allocation records. It does not support multi-output,
  elementary-flow definition, LCIA, directory, or ZIP artifacts. Export
  requires structured source, person, classification, and time-period metadata;
  free-form comments are never interpreted as citations.
* The ecoSpold2 adapter is limited to activity datasets with exactly one
  reference output. Child datasets, allocations, transfer coefficients,
  substitutions, multi-output activities, LCIA datasets, and elementary-flow
  definition files are unsupported. Export requires structured temporal and
  person metadata. External technosphere and biosphere exchanges require exact
  ecoSpold2 UUIDs from preserved source fields or an explicit reference catalog.
* A generic adapter does not accept arbitrary format qualifiers. Its
  ``compatible_format_versions`` and ``compatible_dialects`` must explicitly
  admit them; built-in Brightway Excel admits only the ``bw2io`` dialect.
* BrightPath writes exchange artifacts. It does not install databases into
  Brightway, SimaPro, or another LCA application.
* A process-only openLCA JSON-LD package can reference entities already present
  in a target database. For UVEK 2025 with the ecoinvent 3.10 biosphere,
  BrightPath packages exact references for all 11,747 UVEK technosphere
  identities and 3,959 elementary-flow identities found in the
  inspected UVEK openLCA database build. The remaining 408 packaged ecoinvent
  3.10 biosphere identities are not present under the same UUID in that build;
  exporting one of them fails instead of creating an uncharacterized duplicate.
* Generated openLCA elementary-flow IDs distinguish compartments and
  subcompartments; generated product-flow IDs distinguish supplier locations.
  Explicit openLCA flow IDs and packaged reference IDs are preserved. Conflicting
  definitions of one flow ID cause export to fail instead of silently replacing
  the earlier definition. These generated IDs differ from earlier exports;
  re-export affected inventories and import into a fresh target database to avoid
  retaining obsolete synthetic flows. Compartments already lost in older exports
  cannot be reconstructed automatically. Distinct generated IDs alone do not
  establish compatibility with a target database's LCIA methods.

Local versioned ecoinvent method references
-------------------------------------------

Use an explicit local method mapping when exporting inventories intended for
a version-matched ecoinvent openLCA method package::

    from brightpath.formats.openlca_methods import OpenLCAMethodMapping
    from brightpath.formats.openlca_jsonld import write_openlca_jsonld

    mapping = OpenLCAMethodMapping(
        "path/to/ecoinvent 3.12 LCIA Methods 2025-12-01",
        "path/to/flows_biosphere_312.csv",
        biosphere_version="3.12",
    )
    coverage = mapping.audit(document)
    write_openlca_jsonld(document, "inventory.zip", method_mapping=mapping)

The package may be a directory or a ZIP with ``flows``, ``flow_properties``,
and ``unit_groups`` at its root. The source CSV uses Premise's headerless
five-column layout: name, compartment, subcompartment, unit, UUID, using commas
or semicolons. Neither
licensed method data nor a dependency on a local Premise checkout is bundled.
The caller supplies both files and an exact matching ecoinvent biosphere context;
the technosphere profile remains independent. With ``InventoryPipeline.write``,
pass the mapping through ``adapter_kwargs={"method_mapping": mapping}``.

The mapping accepts explicit ecoinvent versions from 3.5 through 3.12, including
patch releases such as 3.9.1; patch versions are never silently collapsed.
``biosphere_version`` defaults to 3.12 for existing callers. The caller is
responsible for choosing a matching package and CSV: shared flow UUIDs and entity
version fields cannot reliably establish the database release. Legacy JSON-LD
category objects and reference flags (as in the 3.6–3.8 downloads) are supported
alongside current JSON-LD fields. A ``.zolca`` file is a database backup, not
JSON-LD: restore it in openLCA and export the method package as JSON-LD first.

Import the original method package into the target openLCA database first.
Matched exchanges reference its existing flow, flow-property, and unit UUIDs;
they do not duplicate these definitions in the process package. UUID matching
is checked against source names, normalized compartments, and units. Four
known compartment-label correspondences are supported. Standard cubic metres
(including the source alias ``Sm3``) map to the package's ``m3`` label only for
the two specific ecoinvent natural-gas
and mine-gas flow UUIDs; this is not a general volume conversion. Exchange
amounts remain unchanged. Conflicting identifiers or incompatible units fail.

Local compatibility checks covered downloaded JSON-LD packages for 3.5, 3.6, 3.7,
3.8, 3.9.1, 3.10, 3.11, and 3.12. Matching against independent Premise source
CSVs produced these counts (unique source UUIDs / matched UUIDs): 3.7
4,329 / 3,610; 3.9.1 4,709 / 1,960; 3.10 4,362 / 3,579; 3.11
9,795 / 8,529; and 3.12 9,850 / 8,955. These are flow-reference checks, not
numerical LCIA comparisons in openLCA.

For 3.6 and 3.8, all 3,598 and 4,006 package flow/quantity definitions,
respectively, passed schema checks using package-derived CSVs. Those checks do
not establish compatibility with an independent inventory. Premise's 3.8 CSV
contained eight conflicting UUID aliases; the corrected table retains the
identities from the imported ecoinvent 3.8 XML biosphere definitions.

Conflicting package metadata normally raises an error. With the explicit
``conflict_policy="preserve"`` option, known source flows whose package name,
compartment, or unit differs are emitted as uncharacterized local flows under
separate deterministic UUIDs. Their names, amounts, units and compartments stay
unchanged. The coverage report records ``package_conflicts`` and marks affected
inventory entries as ``conflicting_method_package_flow``, including original
and exported UUIDs and both identities. This policy prevents an incompatible
package flow from supplying characterization merely because its UUID matches.
It does not accept contradictory source identities or malformed quantity
references. Review these exclusions before interpreting results; even spelling
differences remain excluded until independently verified. Premise uses this
policy for 3.8 and retains strict rejection for 3.12.

The converted 3.5 JSON-LD package contains 51 methods, 878 impact categories,
and 3,442 elementary flows. All flow/quantity definitions passed the same
package-derived checks, and an export referencing all 3,442 flows preserved
flow, flow-property, and unit UUIDs and exchange amounts. An independent 3.5
inventory mapping and numerical LCIA comparison remain untested. No loader
changes were needed for the converted package.

The inspected 2025-12-01 package matches 8,955 of the 9,850 source flows. Flows
absent from the package are retained with their source UUIDs and locally
created quantity definitions. Unknown source flows are also retained and
reported separately. Export writes ``inventory.biosphere-coverage.json`` with
inventory-specific missing-flow details. Presence in the package does not
imply a characterization factor in every method; absence does not imply no
environmental impact. This mapping does not supply external technosphere
provider IDs or validate openLCA calculation-engine results.

openLCA uncertainty conversion
------------------------------

Brightway lognormal ``loc`` and ``scale`` describe the underlying normal
distribution. JSON-LD export writes ``geomMean = exp(loc)`` and
``geomSd = exp(scale)``; import takes their natural logarithms. An explicit
``negative`` flag controls the sign of ``geomMean``. When the flag is absent,
export infers it from the exchange or parameter amount. The amount itself is
unchanged. Import restores ``loc = log(abs(geomMean))`` and the sign flag.
This applies to exchanges and process, database, and project parameters.

Both log-space parameters must be finite, with ``scale >= 0``. Missing values,
overflow, underflow to zero, zero geometric means, and geometric standard
deviations below one are rejected. Uncertainty types 0 and 1 export without a
distribution; unsupported types fail rather than silently losing uncertainty.
Older archives containing unconverted log-space values must be regenerated from
the original inventory: they cannot reliably be identified or repaired on import.

This conversion follows the `JSON-LD uncertainty field definitions
<https://greendelta.github.io/olca-schema/classes/Uncertainty.html>`_. It does not
establish Monte Carlo equivalence: the inspected `openLCA sampler
<https://github.com/GreenDelta/olca-modules/blob/efe43c244a988d584b5029e3d5f44c975fdecf1f/olca-core/src/main/java/org/openlca/core/math/rand/LogNormal.java>`_
subtracts half the log-variance from the underlying normal mean. BrightPath
does not compensate for that application-specific shift in the schema fields.
Likewise, retained lognormal minimum/maximum metadata does not establish that
openLCA applies Brightway's truncation semantics. Target-application stochastic
parity requires separate validation.

An explicit ``brightpath.formats.openlca_native_uncertainty`` helper supports
unbounded exchange lognormals for the verified openLCA 2.6.2 runtime. It validates
an independent Normal-parameter/exponential graph with a nominal gate, preserving
separate deterministic amounts and sampled centres. Callers must validate their
original document before using ``prepare(..., compatibility="openlca-2.6.2")``
and apply its specifications after rendering. The generic writer does not enable
this profile automatically. Bounds, active lognormal input parameters and
unverified formula dependencies are refused.
Power expressions are refused because native syntax and precedence differ.
The document reader verifies and
decodes recognized graphs; ``decode_native_uncertainty=False`` retains the raw
graph for inspection. Native schema-version-5 re-export containers are not
supported by this addition. No SimaPro sampling compatibility is implied.

Migration boundaries
--------------------

* Packaged ecoinvent cut-off technosphere edges cover 3.5→3.12.
* Packaged ecoinvent biosphere edges cover adjacent release series from
  3.5→3.6 through 3.11→3.12.
* Reverse routes are inferred from forward data and fail under strict policy.
  Permissive policy records the inference and all known losses.
* Forward resources can contain deletion rules. A route is not rejected merely
  because such rules exist: deletion policy is applied during execution only
  when a rule matches an inventory exchange. Strict policy then rolls back;
  permissive mode records the applied deletion as an explicit loss.
* Rules that change units without numeric conversion factors are skipped and
  reported. BrightPath never changes a unit while retaining an unconverted
  amount.
* Consequential version-to-version and cross-system-model migrations are
  unavailable. UVEK 2025/2026 cut-off release migrations have independent
  directional resources, with explicit unmatched identities, biosphere
  ambiguities, and missing unit factors. See :doc:`workflows/uvek-2026`.
* UVEK 2025 cut-off to ecoinvent 3.12 cut-off has an explicit directional
  resource, also reached by UVEK 2026 through the release bridge. Its 1,509
  handled supplier identities include the explicitly warned biowaste-collection
  omission and do not cover the whole UVEK 2025 catalog.
  10,238 identities remain explicitly unresolved and block conversion when used.
  The expansion from the 354 rules in ``1.0.0a10`` is included in ``1.0.0a11``.
  See :doc:`workflows/uvek-export` for proxy policies and generated recipes.
* Reverse industrial-gas burners use independently reviewed German allocated
  electricity/heat reconstructions. Blast-furnace gas excludes upstream fuel;
  coke-oven gas retains it so a separate fuel correction is applied once.
  RER-to-DE geography, allocation, infrastructure and emissions differences
  warn; one-MJ fuel balance does not establish LCIA equivalence.
* Reverse refinery-gas flaring independently matches a positive UVEK physical
  combustion service to the negative ecoinvent treatment reference with factor
  -1. Both exclude upstream fuel, flare infrastructure and energy credits;
  ecoinvent substitutes nine active emission exchanges for UVEK's ten. Different
  emission factors, speciation and plant representation warn; no equal impact
  score or general product-to-treatment conversion is established.
* Modern sweet-natural-gas flaring uses an independently established 36 MJ/m3
  reference conversion: -1/36 forward and -36 for the independently reviewed
  ecoinvent 3.12 cut-off reverse. Resource withdrawal remains in flaring.
  Pollutant coverage, oxygen balancing, nitrous oxide, waste heat and emission
  coefficients differ and warn; no equal LCIA score is established. Older
  sweet-gas and all sour-gas profiles retain pending boundary decisions.
* Same-region reverse ash-disposal proxies preserve kg and apply the independently
  checked -1 waste-reference factor. UVEK specifies dry ash; ecoinvent reports
  H2O as unavailable, so matching moisture is not established or inferred.
  Underlying infrastructure, emissions and fate models can differ and warn.
* Physical cut-off DDGS, grass-fibre and sorghum-grain product proxies preserve
  kg with allocation, technology, geography and moisture warnings. They do not
  establish equivalent nutrients or LCIA scores. Their market helpers retain
  the original freight and supply shares. Consequential nutritional omissions
  are retired with the unsupported cross-model route. All five physical cut-off feed markets have exact-profile approvals.
  Eight reverse physical-product correspondences preserve kg with independent
  inventory/reference reviews. Potato DDGS uses a CH-to-RoW proxy; no nutritional
  omissions or original forward market mixes are reconstructed.
* Exact-profile gas markets represent cut-off allocated products. Consequential
  treatment mixes are outside the supported conversion scope. Generic composed markets retain their
  inspected supplier and freight coefficients through existing component
  mappings. They inherit those mappings' approximations and do not reconstruct
  internal uncertainty or formulas. Only enumerated profiles are approved.
* Average wastewater sludge landfarming and its four regional markets use
  complete scoped helpers in six exact cut-off versions: 3.9, 3.9.1, 3.10,
  3.10.1, 3.11 and 3.12. The mechanical proxy is the complete Swiss vacuum-tanker
  spreading service, at the original volume per kg wet sludge; it excludes
  sludge production and field releases. Each helper retains its separate
  physical releases, including soil carbon only in 3.9/3.9.1. Three historical
  calcium, sodium and strontium labels use updated canonical names only where
  their published UUID, unit and receiving compartment are identical.
  Delivery retains original tonne-kilometres through complete heavy-road
  services and the exact Swiss electric/diesel rail shares. Geography, vehicle
  capacity, vintage, fuel and machinery-emission differences warn. Only the
  explicitly reviewed optional waste indicator may be omitted if unavailable;
  missing native physical releases refuse strict conversion, while explicit
  permissive conversion retains them unlinked with warnings. No incineration
  inverse or dry-mass, density or freight normalization is inferred.
* Fifteen allocation-boundary profiles in the actual ecoinvent 3.9.1 and
  3.10.1 cut-off databases extend existing exact-identity approvals. Four
  positive Recycled Content stocks contain only self-production +1 and reuse
  their existing allocation-only foreground suppliers. Eleven recovery
  handoffs contain self-production -1 and a single -1 kg input to a positive,
  production-only Recycled Content endpoint. These exact handoff exchanges
  are omitted with warnings and full original-exchange audit records.
  Source quantities, units, material-property values and uncertainty attributes
  were checked independently; nine 3.9.1 handoffs add documentary property
  comments and unused variable names. The allocated empty boundary does not
  quantify the impacts of physical recycling, processing or treatment.
  Foreground suppliers, caller uncertainty/formulas and separate emissions
  remain preserved. Every patch decision is enumerated against its actual
  source inventory; missing or different profiles require their own review.
* The older unspecified-road product in ecoinvent 3.6 through 3.10.1 also
  represents all-size lorry freight. Four regional markets use the complete
  RER heavy fleet in 26 independently inspected profiles, including both actual
  patch releases. Brazil starts at 3.8; earlier Brazilian profiles are not
  inferred. The correction retains 674 original signed freight quantities
  through 553 parent profiles and preserves 500 already-correct profiles.
  Regional weight-class shares, EURO standards, loading, fuel intensity and
  vintage remain screening approximations. Existing custom freight choices
  and all rail services remain independently scoped; no vehicle, payload or
  lifetime rescaling is introduced.
* Regional unspecified diesel-road markets in ecoinvent 3.11 and 3.12 use the
  complete RER heavy-lorry fleet, whose weight-class shares are based on Swiss
  2020 data. Regional fuel, load, duty, vintage and emission mixtures remain
  approximations. The Swiss rail market retains its original electric and
  filtered-diesel shares through complete Swiss rail services; energy intensity,
  shunting, maintenance, infrastructure and emissions still differ. The review
  preserves all signed tonne-kilometres through 424 exact parent profiles and
  retains independently scoped custom freight choices. It creates no individual
  steam-rail substitution, payload normalization or inferred reverse mapping.
* The ecoinvent 3.6–3.12 to UVEK 2025 route is a heuristic compatibility map,
  not an equivalence map. It always reports ``migration.heuristic_mapping`` and
  must be reviewed before assessment results are used.
* Approved refrigerant-venting mappings create foreground service activities
  with explicit air releases. Export the complete returned inventory, including
  these helpers. Their physical release model does not represent reclamation,
  destruction or new refrigerant manufacture. Native compartment approximations
  and excluded waste indicators warn and remain audited; missing profile
  decisions or declared native emissions block conversion.
* Approved produced-water discharge mappings preserve mass through direct UVEK
  service proxies, with no helper datasets. They substitute UVEK's pollutant
  concentrations and speciation, omit older source air/water-balance exchanges,
  and use offshore measurements for the onshore proxy. Information-loss warnings
  disclose these differences; toxicity and other LCIA scores are not equivalent.
  Only inspected cut-off source/profile pairs are executable.

Exact version boundaries
------------------------

The exact ecoinvent 3.10.1 consequential catalog contains 20,772 native activity
and reference-flow UUID pairs and 4,362 biosphere definitions. It is generated
from the locally installed exact patch database, rather than relabelling 3.10;
native activity UUIDs differ even though the technosphere identities agree.
Catalog availability does not provide a conversion between system models.

Technosphere migrations must preserve the system model, across database families
and releases. Ecoinvent consequential cannot migrate to UVEK cut-off, and a
composed route cannot bypass this restriction. Earlier consequential mappings,
including the 103 industrial-gas treatment recipes, have been removed from the
executable resource. Historical scientific evidence does not authorize execution.
Independently reviewed cut-off reverse combustion routes remain separate.

The five approved antifouling-paint emission omissions remove the associated
marine metal/organotin contributions with warnings and full original-exchange
loss records. This may understate ecotoxicity for ordinary releases and excludes
credits from avoided-release inputs. The former uranium-milling and tyre-wear
targets did not establish scientific equivalence. Paint manufacture, matching
foreground suppliers and separately listed emissions remain intact; no helper
dataset or zero-impact substitute is introduced.

The standing unsupported-industrial-WWT-residue policy also covers 226
independently inspected nonbiogenic sludge/ash/residue treatment and market
identities across 1,704 source/profile inventories. All direct treatment comments
document 0% biogenic carbon; this does not mean zero impact. Omitted contributions
include processing, metal/leachate releases, downstream residue handling, market
freight and associated recovery credits. Full original-exchange loss records and
warnings remain mandatory; no whole-class omission by keywords is inferred.

Inspected empty recovered-R134a suppliers under cut-off accounting use explicit
production-only foreground helpers. Their zero upstream burden is an approved
source allocation boundary; processing remains on the treatment-service helper.
All returned helpers must be included in downstream scoring and export. Missing
suppliers and other system models cannot inherit this empty-supplier handling.
The same independently inspected allocation pattern covers 34 explicitly named
positive-reference cut-off stock supplies across 168 source profiles. Nested use
requires exact direct-supplier evidence and matching root profiles. It does not
cover actual recycling or negative-reference empty treatment stages. A further
12 identities have only partial cut-off profile coverage; their unreviewed
counterparts remain blocked and visible among the 464 pending queue identities.

Exact patch versions are preserved. ecoinvent 3.10.1 can resolve to migration
series 3.10 for route planning, but it remains 3.10.1 in context and output.
Strict endpoint validation requires exact 3.10.1 catalogs; packaged 3.10
catalogs are not substituted.

Two distinct versions within the same migration series cannot be migrated
because no resource establishes their equivalence. Series resolution is a
resource lookup rule, not semantic version compatibility.

Validation boundaries
---------------------

Background validation performs exact identity matching, not fuzzy semantic
linking. Technosphere identity is name, reference product, location, and unit;
biosphere identity is name, categories, and unit. External foreground targets
must be declared explicitly.

Catalog absence and catalog integrity failures are reported separately from
unresolved links. A catalog is loaded only when links on its axis need it; an
inventory with no such links can report ``not_required``.

SimaPro parsing is an exception to lazy link-validation loading: its adapter
requires the exact biosphere catalog while reading so it can normalize flow
names against the declared profile. Direct facade reads use an explicit
provider or the application default; pipeline reads use the injected provider.

System-model catalog files may contain complementary biosphere shards. The
directory provider unions them only after validating every resource and a
common schema version; a corrupt shard invalidates the combined catalog.

Packaged biosphere rule sources are independently identifiable by a unique
``(name, categories, unit)`` tuple, so forward matching does not require UUIDs.
The exact catalog at each route step is still needed for partial targets,
reverse rules, and endpoint validation. A missing intermediate catalog can
therefore leave an otherwise resolvable multi-step replacement ambiguous under
the selected policy.

The packaged catalogs cover ecoinvent 3.6–3.12 and UVEK 2025/2026, while migration
resources begin at ecoinvent 3.5. Strict 3.5 execution therefore needs a custom
exact catalog provider.

Canonical and round-trip boundaries
-----------------------------------

Unknown fields are retained in canonical source namespaces, but a target file
can preserve only what its grammar supports. SimaPro cannot represent arbitrary
canonical metadata. Conversion and write reports identify known unsupported or
unused features, but no generic checker can prove semantic equality for an
uninterpreted vendor extension.

BW2IO ``input`` and ``output`` keys are a documented exception: they are
reconstructible graph metadata, so they do not count as information loss or
block a strict round trip.

Format validation checks intrinsic grammar only. Conversion preflight
exclusively owns representability, information-loss, and ambiguity policy;
post-conversion target validation cannot override those decisions.

Normalization and migration copy source data. Reports are immutable and JSON
serializable, but v1 facade ``ValidationReport`` and upload ``AnalysisResult``
remain mutable compatibility projections.

Data and licensing
------------------

Premise migration resources retain their declared attribution and CC-BY-4.0
metadata. BrightPath does not package complete proprietary ecoinvent
inventories.

The packaged identity-catalog manifest has status ``legal_review_required``.
It records integrity and provenance but is not a license grant. Redistribution
approval or separately licensed/local provider data is a mandatory gate before
a stable public release.

The packaged openLCA reference catalog has the same
``legal_review_required`` status. It contains identities and entity UUIDs, not
background exchange amounts or complete inventories. Its integrity manifest
and attribution live under ``data/export/openlca_references``.

ILCD process exchange
---------------------

ILCD supports one product reference output and explicit local quantitative
references. Core ILCD rejects selected-supplier loss; the openLCA dialect
preserves those links through exchange extensions and generates used references
from exact ecoinvent catalogs or the verified UVEK openLCA build. Unknown units,
uncovered identities, and different target UUID conventions need explicit mappings.
No EF-to-ecoinvent mappings
are bundled. Allocation, waste-treatment reference inputs, additional product
outputs, EPDs, eILCD models, parameter uncertainty, and unrepresentable uncertainty
precision remain explicit errors. Missing auxiliary source/contact XML is
preserved as an external reference with a warning. See :doc:`workflows/ilcd`.

The additional exact cut-off patch boundary review covers 40 positive allocation-stock
profiles and 21 recovery-handoff profiles in ecoinvent 3.9.1 and 3.10.1. Each
stock contains only self-production +1. Each handoff contains self-production -1
and one -1 kg input to a positive production-only recovered stock. Existing
stock helpers are reused; approved handoffs are omitted with warnings and the
complete original exchange in the audit report. The 136 source, core and
endpoint fingerprints are recorded in
``uvek_complete_patch_boundary_review.json``. These are exact-profile decisions;
they do not authorize empty negative-reference treatments, other patch versions
or cross-system-model conversion.

Cable treatment and older Swiss rail
------------------------------------

The scoped cable/rail review covers 34 exact cut-off source profiles and
321 parent profiles with independently inspected dependency quantities. Cable
treatment retains 0.18 kWh electricity, 4e-10 manual treatment facility units and
0.34 kg wire-plastic treatment per kg, plus the source wire-market freight. It
does not substitute an empty recovered-copper stock or add the collection
freight and waste heat present in UVEK's complete cable-disposal process.

For nine inspected Swiss cable profiles, the 0.00506014 kg fly-ash-extraction
portion uses UVEK's complete wire-plastic incineration process as an explicitly
approved screening proxy. Warnings disclose differences in ash treatment, metal
recovery, emissions and allocation; no additional recovery credit is added.
This approval does not cover other waste materials. Older Swiss freight-train
markets retain their original diesel/electric shares using complete native
services, including vehicles and tracks, with geography and technology warnings.
The review records 535 inventory and description fingerprints in
``uvek_cable_rail_review.json``; proprietary inventories remain private.
Unknown source profiles remain unresolved, strict information-loss policy may
reject these proxies, and no reverse or cross-system-model mapping is inferred.

Textile-waste disposal and markets
----------------------------------

The textile extension covers nine exact cut-off RoW unsanitary-landfill
profiles, five older global market profiles and four modern RoW market
profiles, including the actual 3.9.1 and 3.10.1 inventories. It retains 277
physical release/land-flow entries and 18 operating-service quantities. The
global markets retain their original Indian/RoW landfill shares and all four
freight quantities; the modern RoW markets retain their original road freight.
No controlled municipal landfill, jute-production stock, additional collection
freight, moisture conversion or recovery credit replaces that source boundary.
The operating services and freight retain their existing geography, technology,
fleet and vintage limitations; internal source formulas and uncertainty are
not reconstructed. Caller signs, uncertainty, formulas and separate emissions
remain preserved.

All 310 disposal and companion inventory fingerprints are pinned in
``uvek_uncontrolled_disposal_review.json``. Every original 217 disposal profile
remains unchanged. Canonical biosphere definitions are reused without new
chemical proxies. Native UVEK 2026 still lacks some physical flows: strict
coverage rejects these conversions transactionally, while explicit permissive
policy retains unresolved physical links with warnings, requiring manual
linking before scoring. Only the 12 already-classified accounting-indicator
occurrences in the added profiles may be omitted where unsupported; physical
releases are retained. No consequential or inverse mapping is inferred.

Swiss CEM II cement product delivery
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The CEM II/A and CEM II/B cement markets in Switzerland retain their original
factory-gate cement quantity and separate road and rail delivery in twelve
inspected cut-off profiles, covering ecoinvent 3.9, 3.9.1, 3.10, 3.10.1, 3.11 and
3.12. Complete native Swiss production proxies retain the matching EN 197-1
cement class; their embedded raw-material haulage does not replace product
delivery. Within-class mix, vintage, energy and allocation details can differ,
as can the reviewed freight fleet and rail technology. These are screening
correspondences rather than LCIA equivalence claims.

All caller signs, uncertainty, formulas and foreground ownership remain
unchanged. Exact source names, reference products, locations and units are
required: this review does not approve the same-named negative-reference
hard-coal-ash functions, other cement classes, missing older profiles or
consequential routes. The evidence is packaged as
``uvek_swiss_cement_market_review.json``. It does not authorize reconciliation
of inconsistent concrete-market formulas or any density or binder scaling.

CEM III/A concrete output grade
-------------------------------

The six inspected cut-off CEM III/A 25 MPa producer profiles (ecoinvent
3.9, 3.9.1, 3.10, 3.10.1, 3.11 and 3.12) use UVEK's primary-aggregate
NPK B concrete, whose published class is C25/30. The previous NPK A target
has class C20/25. This corrects the output grade of the existing whole-process
screening proxy; it does not reproduce the source concrete formulation.
The source uses about 316.59 kg cement per cubic meter and the target 280 kg.
Swiss geography, aggregate and binder suppliers, processing, emissions,
infrastructure and delivery differ. These differences remain information-loss
warnings, and strict policy requires explicit consent to the proxy.

The output remains one cubic meter, with no density, strength or cement-dose
rescaling. Caller-owned signs, uncertainty, formulas, foreground suppliers
and separate emissions remain intact. Exact source and native inventory hashes
are recorded in ``uvek_concrete_grade_review.json``. Uninspected profiles have
no fallback approval. This producer-only correction authorizes no concrete
market composition, other cement family, waste-treatment coproduct,
consequential route or reverse mapping.

Source gas-flare inventories and markets
----------------------------------------

Four flaring and single-provider-market identities are reviewed across the nine
installed cut-off versions. The older sweet-gas and all sour-gas flares retain
their complete inspected biosphere inventories in service helpers per MJ,
including source methane, nitrogen oxides and sulfur dioxide where present.
The source treatment reference is negative; a negative caller input becomes
positive demand for a helper with a positive service reference. Each market
retains its original negative handoff as one unit of its corresponding service.
No purchased gas, gas turbine, infrastructure or recovered-energy credit is added.

The eight previously approved modern sweet-gas producer/market profiles keep
their existing native flare target and energy/volume factor. They acquire no
source-inventory helper or duplicate emissions. The extension adds 28 source
helper profiles and retains 132 physical biosphere occurrences and their
marginal lognormal uncertainty fields. Source parameter formulas and shared
parameter correlations are not reconstructed. Caller signs, uncertainty,
formulas, foreground suppliers and separate emissions remain preserved.

Canonical biosphere labels follow eleven exact flow UUIDs and compartments.
For the same natural-gas resource UUID only, the old cubic-metre spelling is
represented as standard cubic metres without changing the coefficient. This
is supported by ecoinvent's published natural-gas reference conditions in
the version 3.0 change report (page 16, Table 4.1) and the old/new resource
property units in the version 3.9 change report (page 32, Table 12). It does
not authorize arbitrary gas-volume, heating-value or reservoir-state conversions.

Native UVEK 2026 still lacks three physical correspondences used by the new
helpers: the natural-gas resource, non-urban mercury and non-urban NMVOC.
Strict coverage refuses these conversions transactionally. Explicit permissive
coverage retains the unresolved physical links and warnings for subsequent
manual resolution; it does not discard or assign zero impact to them. All 37
source/native inventory fingerprints are recorded in
``uvek_remaining_gas_flare_review.json``. Missing exact profiles and conversions
between system models remain unsupported. No reverse route is inferred.


Precious-metal markets at the refinery gate
-------------------------------------------

Three precious-metals-in-anode-slime markets are reviewed in eleven exact cut-off
profiles across the nine installed ecoinvent versions. Their inputs are positive
refinery products, not waste services or empty recovered-content stocks. The
source markets contain only refinery supplier shares: no freight, operating
inputs or direct emissions are omitted. Generated market helpers retain every
original share as a separate input to the complete UVEK Swedish precious-metals
refinery process. Negative caller quantities remain negative product credits;
there is no service-sign flip, share normalization or added recovery credit.

The source and target describe the same multi-output secondary-copper refining
boundary and precious-metal coproduct. The earlier global-market proxy used
incoming electronics scrap at a preparation plant, which is a different gate.
The complete target is still a screening proxy: allocation, vintage, technology,
water definitions, regional electricity and LCIA equivalence are not established.
Swedish refining approximates RoW refining even though original regional shares
remain visible. Do not use common-emission ratios, metal prices, purity or copper
mass to rescale this product. Do not add source refinery operating inputs or
emissions to the complete target process.

The inspected market edges are deterministic, with no source parameters or
formulas. Internal refinery uncertainty and correlations are represented by the
target proxy rather than reconstructed. Caller uncertainty, formulas, metadata,
foreground suppliers, production and separate emissions remain preserved. The
review records thirty complete inventory/metadata fingerprints: eleven markets,
eighteen source refineries and the native target. Publisher inventory vectors
and comments remain private. No reverse correspondence or cross-system-model
conversion is inferred; missing exact profiles remain unreviewed.


Metalliferous hydroxide sludge at the cut-off boundary
------------------------------------------------------

The nine inspected ecoinvent cut-off versions of metalliferous hydroxide sludge,
Recycled Content cut-off, contain only self-linked production of +1 kg. Their
former target, fresh lithium hydroxide production, describes another material
and imports primary-production burdens or credits into an empty allocated stock.
The reviewed conversion now produces a dedicated production-only stock helper,
with explicit allocation warnings. The nine independently inspected stock-market
profiles also retain all four original freight services and one unit of the
allocated stock. The two exact patch markets have their own complete inventory
fingerprints; they do not inherit an uninspected base-version decision. Signed
quantities, credits, uncertainty, formulas, foreground suppliers and separate
emissions remain unchanged.

This boundary is specific to the exact positive-reference stock. It does not
zero an actual sludge treatment, recycling process, incinerator, ash disposal,
or negative-reference empty treatment stage. Processing and recovery outside
this stock retain their own burdens and reviews. Other versions, uninspected
patches and consequential contexts cannot inherit the decision. No reverse
correspondence or primary-chemical mapping is inferred. Complete original
vectors and comments remain private; the packaged review publishes only exact
inventory/metadata fingerprints and the observed allocation boundary.


Polypropylene open burning and wet open dumping
-----------------------------------------------

The eighteen inspected cut-off profiles of polypropylene open burning and wet
open dumping contain only direct biosphere exchanges. Source-inventory helpers
retain all 1,719 signed emissions, resource and land flows, with their marginal
lognormal uncertainty. These services use no invented fuel, transport, facility,
recovery credit or municipal-incineration substitute. The source reference is
negative; a negative caller waste input becomes positive demand for a helper
with production +1 kg. Avoided-treatment quantities retain their credit sign.
Caller formulas, uncertainty, foreground suppliers and separate emissions remain
preserved. The inspected source processes have no parameters or formulas.

Canonical labels use the original publisher UUID, unit and receiving compartment.
Three older nickel-ion groundwater occurrences use an existing, exact-release
same-CAS synonym review. This adds no new ion-to-element proxy or relocation.
The canonical inventory uses 209 distinct biosphere definitions. Current native
UVEK 2026 lacks 101 of those definitions; each source profile has 44--55 missing
native correspondences. Strict native coverage refuses transactionally. Explicit
permissive coverage retains the unresolved physical links and warnings. No
physical flow is omitted. Native water conversions already registered as m3 to
kg apply factor 1,000 to amounts and the corresponding lognormal location shift;
this does not rescale the waste moisture or treatment demand.

The complete source inventories and publisher comments remain private. Packaged
evidence contains fingerprints, canonical definitions and derived coefficients.
Only the nine enumerated cut-off versions are approved for each service; other
patches, consequential contexts and reverse correspondences are not inferred.
Swiss polypropylene incineration and fly-ash-extraction proxy decisions remain
separate from these complete source-inventory reconstructions.


Uncontrolled domestic-refrigerator dismantling in Ghana
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Nine inspected cut-off profiles (ecoinvent 3.6--3.12, including 3.9.1 and
3.10.1) reconstruct the Ghanaian uncontrolled-dismantling service per appliance.
Every profile retains five direct refrigerant/blowing-agent emissions and seven
material-treatment or recovered-material inputs. Across these profiles, all
40 numeric arithmetic formulas and the source marginal uncertainty are retained;
source activity parameters are absent. Missing lognormal sign flags are made
explicit from the signed quantity, including negative recovered-material credits.
Each source negative appliance reference converts with factor -1 to a generated
positive-reference service. This service does not substitute a complete controlled
European refrigerator-treatment inventory for uncontrolled emissions.

The existing directional component correspondences remain screening proxies:
their technology, geography, waste-market composition, transport and allocation
limitations are inherited and reported. The two recovered-material inputs use
already reviewed positive-reference cut-off stocks, preserving their signed
quantities. This does not authorize an empty physical treatment service.
Caller formulas, uncertainty, foreground suppliers and separately listed
emissions remain caller-owned. Only enumerated exact cut-off profiles are
reviewed; missing versions and cross-model routes are unsupported. Independent
reverse mappings remain unchanged. Native UVEK 2026 physical-flow gaps must
refuse transactionally under strict policy or remain unresolved with warnings
under explicitly permissive policy; no emission is silently omitted.


Refrigerant patch services and domestic-refrigerator markets
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The inspected 3.9.1 and 3.10.1 cut-off refrigerant services and markets retain
all source emissions, operating inputs, local parameters and formulas. Existing
independent decisions for older releases remain unchanged. New complete
controlled-dismantling and global used-appliance-market helpers cover nine exact
cut-off releases, retaining both controlled and uncontrolled Ghanaian disposal
shares and every transport input.

Across the 38 newly inspected profiles, the target recipes retain 59 direct
emissions, 226 technosphere inputs and 104 formulas. Four original parameter
definitions remain local to their two reclamation helpers. Fourteen fixed source
reference parameters retain the original negative per-kg waste coordinate; six
calculated emission parameters retain dependencies between source expressions.

Twenty-four hazardous-waste expressions require their inspected signed regional
allocation weights: three regional negative quantities sum to the negated common
expression. They agree with the local base-release inventories. Fifteen further
controlled-dismantling expressions require the existing negative waste-reference
coordinate; nine older occurrences are already consistent. Only formulas are
normalized at this stage. Stored signed quantities and marginal uncertainty
remain unchanged until each independently reviewed provider factor is applied.
No new probability-correlation model is inferred.
Source ``ABS`` function calls use canonical Python ``abs`` in target formulas,
with the original operands and source formula fingerprints retained. This keeps
the authored expressions evaluable by Brightway while preserving their response.

Existing electricity, hall-construction and maritime-freight endpoints are
independently checked against their positive source and native reference units.
All component proxy limitations remain, including geography, technology,
allocation and waste-market composition. Eighteen direct recovery-stock credits
retain their exact positive cut-off profile witnesses. Caller parameters,
formulas, uncertainty, foreground suppliers and separate emissions remain
caller-owned. Source full vectors and comments remain private. Only enumerated
exact cut-off profiles are authorized; native physical-flow gaps refuse strictly
or remain unresolved with warnings under explicitly permissive policy. No
physical emission omission or reverse correspondence is inferred.

The digester-sludge allocation review covers nine exact positive-reference
``digester sludge, Recycled Content cut-off`` profiles and thirteen material-market
profiles in ecoinvent 3.6--3.12, including independently inspected 3.9.1 and 3.10.1
patches. The stock contains only self-production +1. Its material supply and signed
credits use a production-only helper; the markets retain the original stock
quantity and all road/rail freight, including parent exchange uncertainty. The
former municipal-incineration target added a disposal burden or credit outside
this stock's allocation boundary. Actual sludge disposal, digestion, composting
and their physical releases are not zeroed or reconstructed by this correction.
Only the enumerated cut-off profiles are covered, and no inverse is inferred.
In particular, native UVEK biowaste digestion describes incremental burdens relative
to composting and excludes post-rotting. This stock correction does not approve
that process as a complete replacement for an ecoinvent digestion inventory that
includes post-composting.


Copper bottom-ash disposal
--------------------------

The Swiss copper bottom-ash market and slag-compartment treatment in ecoinvent
3.11 and 3.12 cut-off use reviewed helpers rather than horse-dung ash landfarming
or operating burdens alone. The treatment preserves both copper releases,
their receiving-water compartments, the original slag facility fraction and
operating service. The market also preserves its original road and rail
collection quantities through the separately reviewed complete freight services.
Negative waste references use factor -1; signed caller quantities, uncertainty,
formulas, metadata and separately listed emissions remain preserved.

Native UVEK 2026 uses the previously approved copper water-flow proxies, with
explicit warnings for chemical labels and the surface-water-to-unspecified
receiving-water approximation. Long-term groundwater remains long-term
groundwater. Only the two nonphysical waste inventory indicators may be omitted
when unavailable, with warnings. Physical copper releases are retained. Slag
operation, infrastructure and freight retain their documented background
approximations; equal LCIA scores are not established.

This ash-service review does not approve the separate waste-copper incineration
FAE boundary, recovered-energy allocation, additional source releases, consequential
conversion or an inferred reverse route. The independent UVEK-to-ecoinvent
resource is unchanged. Review evidence and exact component fingerprints are in
``brightpath/data/export/uvek_copper_ash_review.json``.


Canonical ecoinvent biosphere exports retain waste inventory indicators without
a reviewed SimaPro representation and currently refuse before writing a file.
They do not drop those indicators to force an export. This indicator representation
gap remains outstanding work.

Native UVEK 2026 copper-ash helpers can round-trip through SimaPro CSV, preserving
the exact ``groundwater, long-term`` compartment and all physical copper releases.
The native wire label is accepted only for a unique exact name, unit and
compartment identity in the selected native catalog. Unknown flows, ambiguous
identities, wrong units and use of that native token in a canonical ecoinvent
biosphere remain errors. Verified native names remain literal rather than
inheriting legacy ecoinvent aliases, including ``Ammonium ion`` versus ``Ammonium``.
This is a format representation, not a flow migration
or a receiving-water proxy. Positive generated treatment and transport services
must be exported as positive product/service references; native SimaPro waste
reference categories use a different reference coordinate. Brightway Excel
round trips also preserve the complete generated hierarchy.


Swiss waste-concrete landfill route
-----------------------------------

The Swiss waste-concrete market and inert-material-landfill treatment use
source-preserving helpers for ecoinvent cut-off 3.6, 3.7, 3.8, 3.9, 3.9.1,
3.10, 3.10.1, 3.11 and 3.12. The helpers retain the as-received waste basis,
one kilogram of generic landfill operation, the original facility share and
separate collection transport. They do not replace landfill disposal with
building-waste crushing/recycling or scale a five-percent-water concrete proxy
to the source's fifty-percent-water waste.

The complete generic UVEK operation and facility are screening correspondences.
The inspected source and target have matching land-use quantities and the same
documented facility capacity. Heat supply, construction materials, freight and
vintage differ; the native operation includes its own waste-heat release.
Native use and aftercare durations are not explicitly documented. No lifetime,
capacity, density or moisture scaling is inferred, and this scoped whole-service
review does not establish an individual heat or material substitution elsewhere.
These differences produce warnings and do not establish equivalent LCIA results.

Signed caller quantities, uncertainty, formulas and metadata remain preserved.
Only an unavailable nonphysical waste-accounting indicator may be omitted under
the previously approved warning policy; physical emissions must not be omitted.
No reverse rule or consequential route is inferred. Exact source, complete
component and freight fingerprints are recorded in
``brightpath/data/export/uvek_waste_concrete_inert_review.json``.


Older PET markets and polyester transfer
----------------------------------------

The RoW PET-waste markets in ecoinvent cut-off 3.6, 3.7, 3.8, 3.9 and 3.9.1
retain every inspected treatment share and separate road, rail, inland-barge and
container-sea tonne-kilometre input. Under the existing whole-freight screening
approval, complete regional rail fleets use the complete UVEK RER rail service,
including where the source mixture contains steam traction. This approximates
fleet technology and geography; it does not create an individual steam-to-diesel
or steam-to-electricity correspondence. The source steam process is not treated
as emission-free, and its coal and ash are not omitted from an otherwise
reconstructed steam inventory. Complete barge and container-ship proxies retain
their modes with their documented fuel, asset, port, load and vintage differences.
No treatment or freight share is renormalized and no extra infrastructure is added.

The ecoinvent 3.9 cut-off polyester-waste linking process has a positive one-kg
reference and eight negative demands for PET-waste markets. Its helper preserves
the positive caller coordinate and those eight treatment quantities; it is neither
fleece manufacture nor a zero-burden allocation stock. The four already reviewed
modern RoW PET-market helper bodies retain their exact previous definitions.

These exact background correspondences support UVEK 2025 and 2026 technosphere
profiles with the canonical ecoinvent biosphere. Under strict coverage, native UVEK 2026 still refuses
when a physical emission has no approved correspondence, preserving the source
transactionally. This review does not authorize a new chemical, receiving-water
or air-compartment proxy or omission of a physical release. Supported uncertainty,
formulas, caller metadata and separately listed emissions remain preserved.
Unknown profiles and consequential conversions remain unsupported; no reverse
correspondence is inferred. Exact full-vector and component fingerprints are in
``brightpath/data/export/uvek_older_pet_whole_freight_review.json``.
