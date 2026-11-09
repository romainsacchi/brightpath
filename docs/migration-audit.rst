Migration regression audit
==========================

Purpose and scope
-----------------

Route availability is not a promise that every supplier can migrate. In
particular, reversing a many-to-one mapping may require a scientific decision
that the original mapping does not contain. The audit detects coverage changes
without adding proxies, weakening policies, or rewriting source inventories.

The canonical ecoinvent 3.10 biosphere bridge is validated independently of
heuristic supplier consent. Its publisher UUID or reviewed chemical-synonym
evidence must preserve the source release, unit and receiving compartment.
Planning records ``migration.canonical_biosphere_identity`` with the scoped
coverage, including unresolved flows. Such unresolved physical exchanges still
cause execution to fail; the informational finding does not authorize a
technosphere correspondence or an inverse biosphere mapping.

The Premise electricity pilot has a separate, exact-profile material review in
``brightpath/data/export/uvek_pilot_material_review.json``. It corrects quicklime,
nylon 66, metallurgical silicon and water-based alkyd paint rather than using
limestone, nylon 6, electronic-grade silicon or solvent-based paint. Positive
source and native references were inspected independently. The paint source
uses a dry-paint coordinate; the native target is a 60% mixture, so its approved
factor is ``1/0.60``, applied to quantities, uncertainty and formulas together.
Other reviewed material factors are ``+1``. Supplier geography, producer/market
boundaries and inventory-vintage differences remain screening limitations.
The same review also corrects the desktop-PC power-supply fan from a printed
circuit board to the documented native fan, and corrugated boxes from testliner
base paper to complete European mixed-fibre single-wall packaging. Signed kg
are retained. The fan inventory excludes assembly, and the box configuration
is an explicit proxy; its complete converting inventory is used without adding
another board or converting burden. These two decisions are limited to the six
inspected cut-off source releases, without implicit 3.11 or patch approval.
Uninspected versions and patches have no implicit approval. These decisions do
not change the overall heuristic policy gate or independently authored reverse
routes; their collector/replay tests do not certify a prospective scenario.

Four further ecoinvent 3.6 cut-off municipal-incineration links use dedicated
glass, polyethylene, polypropylene and polystyrene suppliers, documented in
``brightpath/data/export/uvek_pilot_incinerator_endpoint_review.json``. Their
42 wet-composition entries and both heating values match native UVEK 2026.
The former construction-waste wrappers added transport, and the polypropylene
wrapper supplied polyethylene incineration. The corrected endpoints retain
signed wet kg with factor ``-1`` and preserve separately listed caller freight.
Shorter UVEK 2025 composition documentation and native inventory differences
remain warnings. Only the inspected 3.6 source profiles are reviewed; other
releases remain unresolved, without an inferred reverse correspondence.

The inspected bare mineral-wool landfill link also removes an erroneous
collection wrapper, as recorded in
``brightpath/data/export/uvek_pilot_bare_landfill_review.json``. Its source and
native terminal processes are the same ones traced inside the previously
reviewed collection service. The other inspected bare inert-waste landfill
already has the correct target. Preserve signed kg with factor ``-1``, retain
the bare landfill's own operations and internal logistics once, and add no
collection freight or dismantling. Native long-term leachate and composition
differences remain explicit screening warnings. The correction is gated to the
inspected ecoinvent 3.6 cut-off reference; other versions stay unresolved.

The pilot's delivered tap-water review is recorded separately in
``brightpath/data/export/uvek_pilot_tap_water_review.json``. Three markets across
four inspected source profiles use the complete native Swiss tap-water supply
as an explicitly geographic screening proxy. Both references are kg delivered
at the user, so signed quantities retain factor ``+1``. Source distribution
losses are not multiplied into the target demand: the native inventory already
includes its own abstraction, treatment, pumping, storage and network. Different
treatment mixes, water sources, network losses and regional water-stress results
remain limitations. This metadata review changes no raw migration rule.

Four inspected ecoinvent 3.9 waste markets have exactly one ordinary
municipal-incineration provider: European scrap copper, industrial-electronics
plastic, and European/rest-of-world unspecified rubber. Their separate review,
``brightpath/data/export/uvek_pilot_single_fate_incineration_market_review.json``,
matches wet composition, carbon-origin parameters, technology, ash/cement
quantities and recovery statements. Signed waste kg retain factor ``-1``.
The complete native service keeps its own allocation and internal logistics;
source-market collection freight is not reconstructed, and separately listed
caller freight is preserved. Delivery, auxiliary-energy and background
differences remain warnings. Mixed-fate plastic/oil markets and Swiss copper
with fly-ash extraction are excluded. No raw mapping rule is changed.

Six further exact-profile pilot correspondences are recorded in
``brightpath/data/export/uvek_pilot_matching_supply_service_review.json``:
charcoal, platinum, raw softwood board dried to u=10%, hydraulic excavation,
whole rail freight and petroleum-tanker freight. Positive references preserve
signed kg, m3 or net cargo tonne-km with factor ``+1``. Complete native material
and infrastructure/service boundaries were inspected; no extra drying,
combustion, excavation material, vehicle or recovery credit is added. Native
tanker size, fleet, machinery year, supply allocation and sparse wood
documentation remain warnings. Internal capacity, lifetime, density and heating
values are not demand conversion factors. Raw migration rules are unchanged.

The explicit lignite-scrubber approval is recorded in
``brightpath/data/export/uvek_pilot_lignite_scrubber_review.json`` for ecoinvent
3.6 only. The complete native wet scrubber supplies kg SOx retained at factor
``+1`` and 90% removal. Its warnings identify source disposal of 4.1 kg gypsum
and treatment of 0.0024 m3 wastewater absent from the native coproduct and
ash-wetting boundary, with unquantified score effects. Separate caller stack
emissions remain unchanged; no recovery or avoided-SOx credit is added. This
changes no raw mapping or independently authored reverse route.

``brightpath/data/export/uvek_pilot_fuel_heat_service_review.json`` records six
further exact-profile fuel and heat correspondences. Building-machine diesel
services preserve MJ of fuel combusted, while the three furnace-heat services
preserve MJ of useful heat. The source heavy-fuel-oil reference already applies
its inverse-efficiency scaling, and native heat wrappers contain their burner
factors internally; caller heat demand remains at factor ``+1``. Hard-coal stock
retains signed kg without an LHV factor. Complete native infrastructure, fuel
and residue handling remain included once, with technology/background warnings.
Generator reference ambiguity and direct gas-volume conventions are excluded.

The explicitly approved small-generator normalization is replayed from
``brightpath/data/export/uvek_pilot_generator_fuel_review.json`` for ecoinvent
3.6 only. Factor ``+2.853881278538814`` preserves the audited direct diesel draw:
0.0666667 kg per source labelled MJ versus 0.02336 kg per native fuel MJ.
Source-reference ambiguity, native 10 MW versus source 18.5 kW infrastructure
and unquantified inventory/score differences remain warnings. No electrical
efficiency, extra generator, heat credit or independent inverse is inferred.
The unresolved default prevents uninspected versions from inheriting the factor.

``brightpath/data/export/uvek_pilot_sanitary_inert_review.json`` corrects two
inspected sanitary-landfill endpoints (Swiss 3.6 and European 3.9). All 42
declared wet-composition entries match the complete native inert-material
sanitary service. Factor ``-1`` converts the source negative waste reference
to positive treatment demand without moisture, density or capacity scaling.
Sanitary sealing, leachate treatment and long-term releases remain included.
Twenty-one nominal releases match exact publisher coordinates; the inherited
native manganese definition differs and remains an explicit limitation.
Auxiliary treatment, energy, geography, vintage and unquantified score
differences are warnings. No new helper, recovery credit or inverse is added.

``brightpath/data/export/uvek_pilot_inert_composition_review.json`` adds two
exact RoW helpers: 3.9 zeolite treatment and the 3.12 inert-waste final-disposal
market. Complete reviewed generic operation and facility services retain the
source quantities, uncertainty and documented leachate inventory gap. No
composition-specific oil-catalyst or inert-disposal emissions are inserted.
The market retains all three original freight quantities separately through
the exact previously reviewed regional heavy-lorry proxies. Its nonphysical
waste indicator is retained in ecoinvent 3.10 biosphere; only when unavailable
may that indicator be omitted, with a warning and audit. Source negative waste
references require factor ``-1``. Geography, native background and data gaps
remain explicit; no moisture, capacity, recovery or inverse model is inferred.

``brightpath/data/export/uvek_pilot_european_scrap_steel_review.json`` preserves
the inspected 3.9 European market's landfill/incineration shares and both original
freight quantities. The complete ordinary steel incinerator matches all 42
declared composition entries and the inspected electricity, slag, facility and
internal freight amounts. Native water/oxygen coverage, manganese definitions,
geography and vintage remain explicit screening differences. The original
negative recovered-iron quantity points to the existing exact-profile,
production-only allocated stock helper, with its dependency and source-profile
approval hashes checked. No avoided-primary-steel credit is added. Source
uncertainty is retained and scaled with the deterministic treatment shares;
the Swiss fly-ash-extraction route is outside this decision.

``brightpath/data/export/uvek_pilot_uncontrolled_market_review.json`` preserves
all treatment shares in the inspected 3.9 DE/RoW mixed-plastic and 3.12 RoW
untreated-wood markets. Twenty-two source helpers retain every signed physical
biosphere exchange, full receiving compartment and marginal uncertainty from
open burning, open dumping and unsanitary landfilling. They reuse exact
publisher UUID/unit/context identities in ecoinvent 3.10; label/CAS metadata
differences are warned without a molecular factor or compartment inference.
The unsanitary branches retain the two previously reviewed complete operating
services. Controlled fractions use complete native processes with 44 matched
composition/heating-value coordinates, preserving native limitations and the
source wood thermal-output report's 3.5 versus native 3.49 MJ difference.
Collection remains separate and unnormalized, including individual country rail
amounts through the reviewed whole-fleet proxies. The wood process's nonphysical
waste indicator is retained once outside the native controlled fraction.
Paperboard, fly-ash-extraction and other source profiles remain outside this scope.

``brightpath/data/export/uvek_pilot_teg_supply_review.json`` preserves the exact
3.6 RER triethylene-glycol market's virgin and recovered quantities, with its
original road, rail and barge delivery. The virgin share uses the complete
native ethylene-oxide-derived TEG product. The recovered share retains the
source's positive production-only cut-off reference; its same activity UUID
also has a negative-litre waste-treatment reference with active electricity,
infrastructure and residue-treatment burdens. This evidence distinguishes the
allocated product handover from a missing treatment inventory. No recycling
burden, avoided virgin-product credit or native mass-allocation model is added.
The source rail service is represented by native rail, rather than the erroneous
aircraft endpoint in its raw child heuristic. All three collection suppliers
match previously reviewed source hashes. Quantities, signs, marginal
uncertainty and caller formulas remain; native allocation, precursor/energy,
emission coverage, geography and vintage differences remain warnings. Only the
two inspected positive ecoinvent 3.6 cut-off references are admitted.

Five further whole-function matches are documented in
``brightpath/data/export/uvek_pilot_matching_legacy_supplier_review.json``:
natural barite, 35% scrap/65% pig-iron castings, one 10 MWe turbine-construction
unit, the four-tank/16000 m3 storage module and the same 40 W solar-circulation
pump. Actual specifications and production/disposal boundaries were inspected
before reusing the standing matching-material/function/unit approval. Signed kg
and manufactured-module units retain factor ``+1`` without capacity or lifetime
scaling. Converter steel with extra hot rolling and lubricating-oil additive
formulation differences are excluded. Raw mapping rules remain unchanged.

``brightpath/data/export/uvek_pilot_grid_converter_review.json`` subsequently
corrects the inspected 3.6 converter low-alloyed steel target to the existing
native converter/casting process without rolling, and the 3.8 European
medium-voltage target to the complete ENTSO-E grid. Standing inspected
same-function and geographic proxy approval applies. Signed kg/kWh keep factor
``+1``; native grid losses, infrastructure and SF6 exchanges remain internal.
Country coverage, trade, market delivery, vintage and imported biosphere
differences remain warnings. No new helper, manufacturing stage, reverse rule
or cross-system-model route is introduced. Authoring replay retains unresolved
defaults outside the two exact cut-off profiles.

``brightpath/data/export/uvek_pilot_metal_chemical_supply_review.json`` corrects
the inspected 3.6 generic copper market to the existing complete regional
primary/secondary supply. The packaged 3.6-to-3.7 rename establishes the cathode
product identity, while fresh inspection covers the current inventory and all
23 positive-reference suppliers. Its source hash differs from the historical
cathode-review row; no equality with that older inventory is asserted.
The same evidence inspects unchanged complete methanol supply and an allocated
indium-rich residue from zinc production. The latter is a residue product,
not zinc metal or waste-treatment demand. Signed kg keep factor ``+1``;
production shares, allocation, delivery, geography and vintage remain warnings.
Only the copper rule is rewritten in the raw resource. Other exact admissions
are controlled by Premise's approval ledger. No new helper, recovery credit,
empty stock or inverse route is inferred.

``brightpath/data/export/uvek_pilot_wastewater_endpoint_review.json`` corrects
only the inspected 3.6 Swiss class-2 wastewater treatment to its existing native
counterpart without the additional residential sewer network in the old target.
The treatment class, 71,100 population-capacity equivalents and declared COD
match. Keep the native treatment plant and internal sewer grid; the old and new
targets' direct biosphere coordinates agree within each native release.
The -1 m3 waste / +1 m3 service references require factor ``-1`` without volume
or capacity scaling. Complete-process energy, chemical, residue/ash-recovery,
biosphere and vintage differences remain warnings. No new helper, internal
inventory reconstruction, credit, inverse or cross-system-model route is added.
Uninspected profiles retain an unresolved reference-review default.

``brightpath/data/export/uvek_pilot_wood_steel_freight_review.json`` corrects
three suppliers for four inspected cut-off profiles. The 3.6/3.9 wet-chip market
uses complete native RER forest/sawmill and hardwood/softwood supply, retaining
kg dry matter without moisture or yield scaling. The 3.12 EAF/AOD stainless
market uses native EAF 18/8 without the previous target's converter share and
hot rolling. The 3.6 >32 t EURO4 lorry uses the complete native 32–40 t diesel
freight fleet. Its long-haul/regional/urban and EURO-standard suppliers retain
fuel, vehicle lifecycle, roads, maintenance and emissions. Signed kg/tkm keep
factor ``+1``; exact source supply shares, steel details, freight class/standard,
payload, geography, delivery and vintage differ and remain warnings. No helper,
quantity multiplier, extra processing stage or inverse is introduced. Other
profiles retain unresolved reference-review defaults.

``brightpath/data/export/uvek_pilot_secondary_aluminium_pcb_review.json``
corrects the inspected positive 3.6 secondary cast-billet supplier to native
old-scrap refining/alloying/casting, preserving signed kg at factor ``+1``.
It also inspects the unchanged 3.9 PCB preparation service with opposite
waste/service references and factor ``-1``. Allocation, electricity and
facility quantities match; native freight and waste heat remain warned
differences. The source prepared-electronics co-product is a verified positive
production-only cut-off stock. No downstream metallurgy, recovery credit,
helper or empty treatment is introduced. Only the aluminium rule is rewritten;
exact scientific admissions remain in Premise's ledger.

``brightpath/data/export/uvek_pilot_generic_alumina_review.json`` inspects
the unchanged 3.6 positive oxide co-product and 3.12 oxide-market rules.
Both provide generic Al2O3 stock in kg; the original EAA report confirms
calcined oxide as the native output basis. Signed demands retain factor
``+1`` using the complete native extraction/calcination inventory once,
without a hydroxide-feed multiplier or additional calcination. Specialized
purity, phase, particle size and application-grade equivalence are not
established. Co-product allocation, supply mix, delivery and vintage
differences remain warnings. Exact-profile admissions remain in Premise's
ledger; no raw rule, helper, inverse or broader profile approval is added.

``brightpath/data/export/uvek_pilot_inert_landfill_review.json`` reconstructs
only the inspected 3.6 Swiss bare steel/glass landfill services from the
complete generic operation and facility already reviewed for concrete.
The native components retain their inventories, including heat and freight
differences; the facility has the same documented 450,000 m3 capacity.
Signed waste demands use factor ``-1`` against positive service helpers.
The source's missing leachate modelling remains a warned data gap, without
substituting the old native endpoints' different waste compositions. No
collection wrapper, moisture/density/lifetime factor or recovery credit is
added. Two rules gain two helper recipes and one shared review; all earlier
recipes and reviews remain unchanged. Exact source-profile gates, caller
ownership, formulas, uncertainty and separately listed emissions are retained.
Premise pins the two exact admissions; other source versions remain unresolved.
The maintainer generator and historical system-model comparison share
``apply_pilot_reviews`` so both compare the same independently reviewed
corrections. This retains exact executable-rule and helper-coefficient
preservation checks while accounting for later approved supplier/factor changes.

``brightpath/data/export/uvek_pilot_unalloyed_cast_steel_review.json`` inspects
the unchanged 3.9 global unalloyed-steel market rule. Its two actual source
producers and the complete native converter process end at steelmaking and
casting, without rolling. Signed stock kg retain factor ``+1``. Pig-iron/scrap
mix, gas handling, energy, residues, allocation, delivery, geography and vintage
differences remain warnings. No extra rolling, steelmaking or recovery credit
is added. Exact-profile scientific admission remains in Premise's ledger;
other profiles, specialized grades and inverse routes are not inferred.

``brightpath/data/export/uvek_pilot_low_alloyed_cast_stock_review.json``
reconstructs only the inspected 3.6/3.9/3.12 low-alloyed-steel market profiles
from complete native casting and whole-freight components. All 25 source
producer references are positive; their declared endpoints end at casting,
including the Austrian record whose narrative discusses later rolling.
The helpers preserve source BOF/electric shares and all four original market
delivery quantities. They use the existing native low-alloyed converter and
Swiss electric processes as scoped whole-manufacturing geographic proxies,
retaining native plant, alloy, scrap, gas, allocation and vintage differences.
The old native whole-market target's hot rolling and fixed furnace split
are not added. Whole freight retains its fleet, vehicle, infrastructure,
fuel and emissions boundaries at the original net transport quantities.
No yield, allocation, grade, payload, distance or capacity factor is inferred.
Raw child bindings are drift witnesses; the independently reviewed furnace
choices are explicit inside this parent and do not rewrite global child rules.
Only deterministic repeated furnace suppliers are combined; each helper has
two furnace totals and four freight exchanges with their original uncertainty.
One parent gains three profile-specific recipes and one shared review; older
rules, recipes and reviews remain unchanged. Other versions remain unresolved.

Exact rule approvals
--------------------

``MigrationPolicy.approved_compatibility_rules`` optionally carries SHA256
fingerprints for independently reviewed operations. Default strict behavior is
unchanged. With a nonempty set, the planner defers the technosphere resource
decision to execution, which checks every matching external replacement,
deletion and disaggregation before materializing helpers or changing exchanges.
An unapproved match rejects the complete migration transactionally, including
when other matches are approved. Overlapping candidates each require approval.
Owned foreground links are excluded from this background-rule check.

``brightpath.background.rule_approvals.compatibility_rule_fingerprint`` binds
an approval to the exact source and target technosphere profiles, resource,
operation, complete rule, shared reviews and referenced helper-recipe closure,
including profile-specific target recipes. Changing a supplier, factor, review,
helper inventory, dependency or profile invalidates the fingerprint. Computing a fingerprint is not evidence
of scientific approval. Native UVEK 2026 forward conversion uses the UVEK 2025
compatibility edge followed by its release bridge; approval scopes refer to
the actual edge. Successful matches retain their fingerprints in reports.
Biosphere, reference-sign, ambiguity, information-loss, unit, system-model and
source/target-link checks remain independent. Broad heuristic warning consent
continues to be a separate policy; it cannot bypass a supplied approval set.

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
errors. The explicit information-loss warning policy permits documented omission
of unrepresentable biosphere unit changes; strict policy rejects the omission
and rolls back. The audit reports accepted omissions as
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

Native UVEK 2026 routes have a separate audit entry point. It discovers 90
independent-axis task/policy combinations from the current exact catalogs:
release bridges, incoming ecoinvent process mappings, approved outgoing process
mappings and both biosphere directions. Each runs under strict policy and under
the explicit information-loss, inferred-reverse and heuristic warning policy::

   python -m scripts.audit_uvek_2026_routes --list-routes
   python -m scripts.audit_uvek_2026_routes \
     --workers 2 --baseline docs/uvek-2026-audit-baseline.json \
     --output /tmp/uvek-2026-audit.json

Discovery without a reviewed ``--baseline`` deliberately exits nonzero. The
historical native baseline contains 90 tasks and 892,550 probes: 443,401
supported, 449,099 blocked and 50 explicitly omitted under the warning policy.
There are no audit errors. Counts repeat source identities across policies,
directions and exact profiles; they are not a global mapping-coverage score.
Both CI and publication compare the installed wheel against this baseline and
upload the full audit report. A resource change during a run aborts it. The legacy
baseline remains independent, allowing its existing probe outcomes to be
compared without mixing them with the larger native-route inventory.

Every release-bridge and incoming process task probes its full source catalog;
every biosphere task does the same. Outgoing process tasks probe all 1,424
approved native supplier identities. The other 10,523 native identities are
reported separately with their count and a stable identity digest. They are
outside the approved export cohort, not successful conversions or skipped
runtime checks. Current discovery excludes incoming and outgoing consequential
routes from executable probes and records both directions as explicit planning
findings. It contains 76 compatible-model tasks. Historical native counts above
predate the system-model restriction and must not be treated as current route
coverage.

Current native audit discovery includes approved omission rules in this cohort.
The biowaste-collection case is reported as ``omitted_with_warning`` and retains
its explicit loss audit; the historical result counts below predate that decision.

Selected native-route results are:

.. list-table:: Supported probes by policy
   :header-rows: 1
   :widths: 55 15 15

   * - Independent route
     - Strict
     - Explicit warnings
   * - UVEK 2025 process to 2026 (11,747 sources)
     - 11,742
     - 11,746
   * - UVEK 2026 process to 2025 (11,947 sources)
     - 11,742
     - 11,746
   * - Approved UVEK 2026 process to ecoinvent 3.12 (1,424 sources)
     - 1,237
     - 1,424
   * - Native biosphere to ecoinvent 3.12 (2,679 sources)
     - 1,789
     - 1,935
   * - Ecoinvent 3.10 biosphere to native (4,362 sources)
     - 1,297
     - 1,297
   * - Ecoinvent 3.12 biosphere to native (9,850 sources)
     - 0
     - 1,471

The release-bridge difference is the four reviewed CH/RER wind revisions; the
remaining washing-machine unit change remains blocked under both policies.
The native-to-2025 direction additionally has 200 identities without a release
correspondence. The ecoinvent 3.12 reverse biosphere route requires acceptance
of inferred reversal. Incoming ecoinvent process routes require heuristic
acceptance and remain mechanically validated heuristic conversions.

For focused iteration, use ``--route`` with an exact task name returned by
``--list-routes``. A subset report is marked partial and cannot pass the release
gate, even against an identical partial baseline. These probes retain the same
rollback, exact-target validation, finite-quantity and idempotence checks as the
legacy audit. Synthetic amounts are used throughout; inventories are not
redistributed. Mechanical success of a heuristic crosswalk remains distinct
from a scientifically reviewed correspondence.

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

Both UVEK directions now use the named ``brightpath.migration`` schema,
version 3. The ecoinvent-style operation lists, inline supplier identities and
shared review references are validated by the same packaged JSON Schema and
semantic checks. Upstream ecoinvent rules and attribution remain unchanged and
normalize when loaded. Resource and schema-definition digests are included in
the baseline and migration fingerprint.

The schema upgrade preserves all 42,444 reverse-route case outcomes, output
hashes and collision findings in source and wheel audits. Baseline review
accepts only the migration-manifest change and the added schema-definition
digest. Existing unsupported cases remain blocked; neither mapping confidence
nor numerical recipes are adjusted to make the audit pass.

The forward crosswalk accounts for all 30,732 cut-off supplier identities in the
packaged source catalogs, including explicitly pending scientific decisions.
Executable rules are tested against those exact catalogs with complete target-link
coverage. Historical testing exposed an existing amount-factor bug: uncertainty locations,
spreads, bounds and formulas previously remained unscaled. Unit-factor execution
now shares the reverse exporter's scaling routine. Unsupported distributions
roll back with structured errors, including under permissive policy. All
mapping scores, factors, targets and evidence remain unchanged; the seven
existing helper outputs retain their pre-upgrade hashes.

Runtime metadata cleanup subsequently removes duplicated catalog filenames,
generation-method descriptions and evidence hashes from UVEK resources. Forward
reviews retain rationale and limitations; comparison revisions, activity codes
and candidate histories stay in the authoring ledgers. The tissue-paper
supplement likewise keeps rationale and its evidence link, with detailed review
identifiers in its dedicated document. Only the migration-manifest digest changes
in the audit baseline; operational rules, coverage and helper outputs are preserved.

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

Alpha 10 resource expansion
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The UVEK 2026 catalog and four directed release resources change the migration
and reference-catalog manifest digests. An installed-wheel audit retains all
42,444 established UVEK 2025/ecoinvent case outcomes, output fingerprints,
route warnings and losses, policies, and all 483 collision findings. The
reviewed baseline change accepts only those two manifest digests; coverage
remains 40,922 supported, 1,497 blocked and 25 explicit omissions.

The matrix above still describes the UVEK 2025/ecoinvent export routes. Native
UVEK 2025/2026 routes are additionally exercised by the source test suite and
installed-package smoke checks, including every safe process correspondence,
the reviewed factor-of-1 unit labels, biosphere correspondences, uncertainty,
idempotence, and rollback for unsupported links. Their separate partial
coverage is documented in :doc:`workflows/uvek-2026`; it is not counted as
complete support by this regression matrix.

Alpha 11 publisher-correspondence expansion
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The publisher-table review in :doc:`workflows/uvek-export` adds 1,070
reference-function correspondences and retains all original 354 rules,
reviews, factors and supporting recipes. The approved supplier matrix grows
to 1,424 cases per target, adding 9,630 successful probes across the nine exact
cut-off target profiles. UVEK 2026 reaches every added source through its
explicit 2025 bridge; separate source tests and installed-wheel checks execute
the complete new cohort under strict policy from both UVEK releases.

The expanded audit contains 52,074 probes: 50,552 supported, 1,497 blocked and
25 explicitly omitted with warnings. Comparing cases by ID with the alpha 10
audit verifies that all 42,444 pre-existing outcomes are identical, including
their output hashes, errors and every biosphere case. The policy and all 483
collision findings remain unchanged. There are no audit errors.

The reviewed baseline accepts the changed migration-manifest digest, the new
supplier cases and their output hashes, and the reduced count of unresolved
UVEK 2025 suppliers (10,323). It does not relax any policy, reclassify an old
blocker or approve any additional biosphere mapping. These are metadata-backed
reference-function correspondences, not claims of unchanged inventories,
allocation or LCIA results. Native UVEK 2026 retains 10,523 process identities
without an approved export correspondence and its separate biosphere limits.
