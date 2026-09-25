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
  Brightway block TSV, openLCA JSON-LD ZIP, and SimaPro CSV as file
  detection/read/write adapters.
* Only Brightway Excel and SimaPro CSV have dedicated v1 facades. Brightway
  CSV/TSV use the generic pipeline.
* CSV suffixes are ambiguous. Detection inspects content and reports absent or
  tied evidence; callers should pass an explicit format when known.
* SimaPro CSV is semicolon-delimited and Latin-1 encoded. It has stricter unit,
  category, section, and field representation than the canonical inventory.
* OpenLCA Excel and ecoSpold2 are reserved identifiers/namespaces only. They
  have no registered adapter and do not appear in ``brightpath formats``.
* Custom format identifiers are valid canonical context values.
  ``InventoryDocument.inventory_format`` returns their string ID while
  retaining enum members for known legacy IDs. A custom format is executable
  only when its adapter is registered.
* Read capability requires ``can_validate_format``; write capability also
  requires ``can_preflight_conversion``. Registry construction rejects missing
  flags and non-callable hooks before capability discovery. Runtime hook
  failures and malformed reports fail safely.
* A generic adapter does not accept arbitrary format qualifiers. Its
  ``compatible_format_versions`` and ``compatible_dialects`` must explicitly
  admit them; built-in Brightway Excel admits only the ``bw2io`` dialect.
* BrightPath writes exchange artifacts. It does not install databases into
  Brightway, SimaPro, or another LCA application.
* A process-only openLCA JSON-LD package can reference entities already present
  in a target database. For UVEK 2025 with the ecoinvent 3.10 biosphere,
  BrightPath packages exact references for all 11,747 UVEK technosphere
  identities and 3,954 characterized elementary-flow identities found in the
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
* Consequential version-to-version, cross-system-model, and UVEK-version
  migrations are unavailable.
* UVEK 2025 cut-off to ecoinvent 3.12 cut-off has an explicit directional
  resource. Its 354 approved supplier identities do not cover the whole catalog:
  11,393 identities remain explicitly unresolved and block conversion when used.
  See :doc:`workflows/uvek-export` for proxy policies and generated recipes.
* The ecoinvent 3.6–3.12 to UVEK 2025 route is a heuristic compatibility map,
  not an equivalence map. It always reports ``migration.heuristic_mapping`` and
  must be reviewed before assessment results are used.

Exact version boundaries
------------------------

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

The packaged catalogs cover ecoinvent 3.6–3.12 and UVEK 2025, while migration
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
