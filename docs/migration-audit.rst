Migration regression audit
==========================

Purpose and scope
-----------------

Route availability is not a promise that every supplier can migrate. In
particular, reversing a many-to-one mapping may require a scientific decision
that the original mapping does not contain. The audit detects coverage changes
without adding proxies, weakening policies, or rewriting source inventories.

``scripts/audit_migration_coverage.py`` executes synthetic inventories against
the installed package's exact catalogs and migration engine. It covers:

* Every approved UVEK 2025 supplier rule against every packaged ecoinvent
  cut-off target catalog, including exact patch versions, complete composed
  routes, and generated recipe dependencies.
* Every identity in the UVEK source biosphere profile (ecoinvent 3.10) against
  those target biosphere versions. These probes isolate biosphere migration
  from technosphere conversion and do not require UUIDs.
* Static forward and reverse match collisions in all packaged ecoinvent
  technosphere and biosphere resources, including replacement/disaggregation
  overlaps, inherited biosphere fields, preferences, and compatibility proxies.

The probe policy matches CLIC's export policy: inferred reversal and documented
information loss produce warnings; ambiguous rules, missing links, explicit
deletion rules, unsafe technosphere unit changes, and invalid targets remain
errors. The existing engine separately permits documented omission of
unrepresentable biosphere unit changes; the audit reports those as
``omitted_with_warning``, never as fully supported probes. Target coverage must
be complete. Unavailable routes remain visible rather than being skipped.

Each successful probe is independently target-validated and checked for an
unchanged production exchange, finite numerical values, no undocumented
exchange omission, and same-context idempotence. Failed migrations must roll back with
structured errors. Successful output hashes include quantities, uncertainty,
metadata and generated helpers, so changes require review even if coverage
counts stay the same. These hashes detect drift; they do not independently
prove the scientific correctness of every coefficient or uncertainty model.

The audit uses packaged identities and synthetic amounts only, not proprietary
background inventories or a user's local Brightway installation.

Running and reading the audit
-----------------------------

From an installed source checkout:

.. code-block:: console

   python scripts/audit_migration_coverage.py \
     --workers 2 \
     --baseline docs/migration-audit-baseline.json \
     --output /tmp/migration-audit.json

The JSON report contains every probe identity and outcome, structured errors
for blocked identities, route-level warning/loss codes, and collision candidate
identities with their originating rules. Biosphere probes are batched for speed;
failed batches are bisected until individual blocked flows are identified.
Batch warning/loss codes are intentionally reported at route level, not
misattributed to individual flows.

``--workers`` optionally parallelizes independent routes. Results and baseline
fingerprints are sorted and independent of worker completion order.

``supported`` means the probe passed this policy and exact target validation;
it does not mean lossless or scientifically equivalent. ``blocked`` means the
engine safely refused it. ``omitted_with_warning`` identifies a biosphere probe
whose omission was explicitly reported by the engine. ``audit_error`` means an unexpected exception or a
broken invariant, and always fails the gate, even if present in a baseline.

Collision candidates marked ``review_required`` are static overlaps, not
necessarily runtime ambiguities: some may be disambiguated by the executor.
Conversely, a documented proxy only records the chosen policy; it does not
prove equivalence. All candidates remain visible in the report.

Release gate and baseline review
--------------------------------

Both CI and the release build install the newly built wheel and run the audit
outside the source checkout. The full JSON is uploaded as the
``migration-audit`` artifact, including on gate failure. PyPI and Anaconda
publishing depend on the gated build succeeding.

The checked-in ``docs/migration-audit-baseline.json`` records observed coverage
and fingerprints. Known blocked probes are an explicit backlog, not approved
conversions or ignored tests. The gate fails on any change to route coverage,
identities, output fingerprints, error signatures, warnings/losses, collision
candidates, resource manifests, or policy. Adding/removing targets or supplier
cases also requires review. Improvements intentionally require review too.

To update the baseline:

1. Run the audit with the existing baseline and retain its full report.
2. Inspect changed routes, blocked identities, output changes and collisions.
   Fix regressions; do not create arbitrary preferences just to make CI green.
3. Record the reason and evidence for accepted changes in the change description
   and add targeted tests. Scientific proxy decisions belong in the attributed,
   integrity-manifested migration resources, not in the baseline.
4. Only after review, replace the baseline with the report's ``baseline`` object
   and rerun the gate. There is deliberately no automatic bless/update flag.

Without ``--baseline``, the script writes a discovery report and exits nonzero
to require review. Unexpected loader errors also fail the command.

Limitations and next layers
---------------------------

Initial findings (2026-10-08)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The initial baseline executes 354 approved supplier rules and 4,362 source
biosphere identities against nine exact target versions: 42,444 probes.
Across this matrix, 40,915 probes retain their exchanges and validate, 1,504
are blocked, and 25 explicitly omit an unrepresentable biosphere exchange.
Counts are identity/target pairs, not distinct inventories or failure rates
for the CLIC collection. Warnings and scientific limitations still apply to
validated probes.

.. list-table:: Blocked probes and documented omissions by exact target
   :header-rows: 1

   * - Target
     - Blocked suppliers / 354
     - Blocked biosphere / 4,362
     - Biosphere omissions
   * - 3.6
     - 23
     - 483
     - 5
   * - 3.7
     - 21
     - 481
     - 5
   * - 3.8
     - 21
     - 337
     - 5
   * - 3.9
     - 19
     - 49
     - 5
   * - 3.9.1
     - 19
     - 49
     - 5
   * - 3.10
     - 1
     - 0
     - 0
   * - 3.10.1
     - 1
     - 0
     - 0
   * - 3.11
     - 0
     - 0
     - 0
   * - 3.12
     - 0
     - 0
     - 0

The initial supplier failure at 3.10/3.10.1 was
``Paper, recycling, with deinking, at plant`` (RER): its migrated supplier
``tissue paper production, recycled`` (RER) does not exist in those exact target
catalogs. It is corrected by the reviewed output-specific rename documented
in :doc:`recycled-paper-migration`, not by a similar-looking supplier guess.
Other suppliers in older targets still expose reverse-rule ambiguities and
missing catalog identities. The table above records the initial baseline.

The static scan records 483 collision candidates: 478 require further review,
four have an explicit reverse preference, and one has an explicit proxy.
These counts must not be interpreted as 483 demonstrated runtime bugs. The
11,393 UVEK suppliers without an approved initial mapping remain unresolved
and outside the approved-supplier execution matrix.

The audit also exposed a real reverse-biosphere crash when shared targets
contained list-valued categories. Structural comparison fixes that crash
without choosing among distinct targets. The initial baseline is taken after
this correction and contains no audit errors. Existing blocked conversions
and reported omissions are retained as known limitations, not scientifically
approved resolutions.

Remaining scope
~~~~~~~~~~~~~~~

After the reviewed tissue-paper correction, the source and independently
installed-wheel audits agree on all 42,444 probes: 40,922 supported, 1,497
blocked and 25 reported omissions. Exactly seven cases change, all the
same RER paper supplier changing from blocked to supported at 3.6, 3.7,
3.8, 3.9, 3.9.1, 3.10 and 3.10.1. The other 42,437 outcomes are unchanged,
including output hashes and every biosphere result. All 354 approved supplier
probes now pass at 3.10, 3.10.1, 3.11 and 3.12 under the declared policy.

The baseline update accepts only those coverage improvements, the changed
migration-manifest digest and collision candidate indices shifted by the
inserted rule. All 483 collision findings retain their identities, rule
content and dispositions; no new collision is introduced. The prior baseline
correctly rejected these changes before review. There are no audit errors.

This is exhaustive for the declared UVEK supplier/biosphere matrix, not for
every possible foreground inventory, uncertainty distribution, system model,
or arbitrary ecoinvent-to-ecoinvent supplier combination. Unreviewed UVEK
suppliers remain unresolved and their count is reported separately; a green
gate must not be presented as complete UVEK conversion support.

Format serialization, SimaPro Desktop constraints, native application imports,
and the full collection of published CLIC baskets require separate end-to-end
tests. A deployment should additionally preflight actual baskets and show
dataset-specific compatibility rather than treating a route as universally
exportable. This audit does not change CLIC's UI or publication workflow.
