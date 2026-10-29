Ecoinvent-to-UVEK conversion resource
==========================================

``brightpath/data/migrations/uvek/ecoinvent-to-uvek-2025.json`` maps ecoinvent
supplier identities to UVEK 2025 cut-off suppliers. Format conversion is a
separate operation. Biosphere migration uses the separate
``ecoinvent-to-ecoinvent-3.10-biosphere.json`` resource.

Strict policy refuses these heuristic compatibility resources by default with
``migration.heuristic_mapping``. After reviewing the evidence and limitations,
accept them explicitly while retaining other strict checks::

    from brightpath.core.policies import MigrationPolicy, PolicyAction

    policy = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN)
    converted = inventory.migrate_background(target_context, policy=policy)

For the CLI, add ``--allow-heuristic-mapping`` to ``migrate-background``. This
does not relax unresolved links, ambiguous rules, missing unit factors or exact
target validation. The report records the acceptance and heuristic limitations;
acceptance is not evidence of scientific equivalence. Reviewed UVEK-to-ecoinvent
exports use their independent directional resource and do not require this
heuristic permission merely because they cross database families.

Storage and loading
-------------------

Both UVEK directions use the named ``brightpath.migration`` schema, version 3.
It follows ecoinvent's ``replace``, ``disaggregate`` and ``delete`` operations,
with inline source and target identities, shared review records, and optional
foreground recipes. All 31,488 forward rules, confidence scores, amount factors
and mapping explanations are preserved. This file contains 104 shared reviews and uses
4,043 distinct UVEK suppliers; it contains no complete process inventories.

``load_uvek_technosphere_resource()`` verifies the packaged bytes and applies the
same schema validation used for the reverse direction. It returns the same v3
representation stored on disk. The deterministic writer uses one line per rule
to limit formatting overhead. See :doc:`../migration-resource-schema` for the
complete contract and upgrade instructions.

Top-level fields
----------------

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Field
     - Meaning
   * - ``name``
     - Resource identifier.
   * - ``schema``
     - Schema identifier, ``brightpath.migration``.
   * - ``schema_version``
     - JSON structure version, currently 3.
   * - ``description``
     - Purpose and limitations of the mapping.
   * - ``status``
     - ``active`` makes the resource available for loading.
   * - ``axis``
     - ``technosphere`` identifies process/supplier-link migration.
   * - ``quality``
     - ``heuristic`` identifies the overall approximate compatibility approach.
   * - ``source_profiles``
     - Explicit routing declarations, each with ``family``, ``version`` and
       ``system_model``. Inventory contexts retain exact patch versions.
   * - ``target_profile``
     - Destination ``family``, exact ``version`` and ``system_model``:
       UVEK 2025 cut-off.
   * - ``coverage``
     - Counts and scores describing the generated crosswalk.
   * - ``replace``
     - One-source-to-one-target replacement instructions.
   * - ``reviews``
     - Shared review records indexed by deterministic ``r-...`` identifiers.
   * - ``recipes``
     - Foreground helper definitions; empty in this direction.
   * - ``reverse_policy``
     - ``explicit_only`` requires an independently authored reverse mapping.
   * - ``disaggregate``
     - One-to-many rules; empty in this resource.
   * - ``delete``
     - Deletion rules; empty in this resource.

Generation inputs, methodology and source attribution are documented in
``brightpath/data/migrations/ATTRIBUTION.md``. The runtime file retains profile
declarations instead of duplicating input catalog filenames. Mappings establish
compatibility, not scientific equivalence.

``coverage.source_identities`` counts distinct source identities across all input
catalogs, and ``mapped_identities`` counts rules. ``target_identities`` counts
all available UVEK catalog identities, including unused targets.
``unresolved_identities`` is zero for the generated forward catalog crosswalk.
``mapping_methods`` counts rules by selection method. ``mean_confidence`` is the
arithmetic mean of their uncalibrated scores.

Rules, identities and review records
------------------------------------------

.. list-table:: Replacement rule fields
   :header-rows: 1
   :widths: 30 70

   * - Field
     - Meaning
   * - ``id``
     - Stable rule identifier for traceability.
   * - ``source``
     - Ecoinvent identity: ``name`` (activity), ``reference product`` (supplied
       product), ``location`` (geography), and ``unit`` (reference-flow unit).
   * - ``target``
     - Inline UVEK supplier identity with the same four fields as ``source``.
   * - ``mapping_method``
     - Procedure or evidence batch responsible for the selected target.
   * - ``confidence``
     - Compatibility score from matching or review, between 0 and 1. It is not
       a probability, statistical confidence level or proof of equivalence.
   * - ``conversion_factor``
     - Optional quantity multiplier; omitted means 1. The new exchange amount
       is the old amount multiplied by this factor.
   * - ``review_ref``
     - Optional reference to a record in ``reviews``.

Each review record has ``rationale`` (the decision explanation) and
``limitations`` (known proxy limitations). The complete decision history,
comparison revisions, candidate origins and sparse inventory evidence remain in
``brightpath/data/export/uvek_reviewed_overrides.json`` and the existing
``docs/uvek_*`` review documents. They are not duplicated in runtime rules.
The executable quantity multiplier remains ``conversion_factor`` (default 1).

Current coverage
----------------

All 31,488 distinct technosphere source identities have catalog-valid UVEK
targets, and all unit changes have explicit factors. Every identity in the 14
generation catalogs, plus the packaged 3.9.1 and 3.10.1 cut-off catalogs, is
represented. The following counts are identities per catalog, so their sum
exceeds the distinct combined count.

.. list-table:: Technosphere identities with a mapping / total identities
   :header-rows: 1

   * - Ecoinvent version
     - Cut-off
     - Consequential
   * - 3.6
     - 18,121 / 18,121
     - 16,459 / 16,459
   * - 3.7
     - 19,128 / 19,128
     - 17,328 / 17,328
   * - 3.8
     - 19,565 / 19,565
     - 17,653 / 17,653
   * - 3.9
     - 21,255 / 21,255
     - 18,856 / 18,856
   * - 3.9.1
     - 21,238 / 21,238
     - No exact packaged catalog
   * - 3.10
     - 23,523 / 23,523
     - 20,772 / 20,772
   * - 3.10.1
     - 23,523 / 23,523
     - No exact packaged catalog
   * - 3.11
     - 25,412 / 25,412
     - 22,241 / 22,241
   * - 3.12
     - 26,533 / 26,533
     - 23,191 / 23,191

The separate biosphere crosswalk covers all 11,470 distinct source identities:
4,362 are unchanged identities and 7,108 use heuristic matching to the ecoinvent
3.10 biosphere. Every biosphere identity in these same packaged catalogs is
represented. Target-link validation remains mandatory during conversion.

.. list-table:: Technosphere selection methods
   :header-rows: 1

   * - Method
     - Rules
     - Meaning
   * - ``heuristic_similarity``
     - 28,601
     - Deterministic similarity ranking with compatible units.
   * - ``curated_legacy``
     - 311
     - Legacy correspondence-table lookup.
   * - ``product_checked``
     - 35
     - Initial product-identity corrections.
   * - ``pairwise_review``
     - 2,315
     - Expanded product, fuel, technology and waste-role checks.
   * - ``followup_review``
     - 164
     - Subsequent scoped adjudication of alternatives.
   * - ``uvek_inventory_review``
     - 62
     - Seedling and CHP selections supported by sparse inventory evidence.

Technical mapping coverage is 100%, but scientific equivalence is not established.
Similarity-only rules account for 90.8% of sources. The mean confidence is
0.414702; 21,618 entries (68.7%) have scores below 0.5. Curated and reviewed
selections also include documented proxies. Inventory-backed evidence inspected
ecoinvent 3.10 cut-off; shared four-field identities inherit choices across
versions and system models without establishing allocation equivalence.

The crosswalk uses 4,043 of the 11,747 available UVEK targets. Many source
identities therefore converge on the same supplier. Planning reports
``migration.heuristic_mapping`` and execution validates the exact target links.
The cleanup changes storage only; it does not improve mapping quality or expand
supported background profiles. See :doc:`../limitations` and
:doc:`uvek-export` for the independent reverse-direction resource.
