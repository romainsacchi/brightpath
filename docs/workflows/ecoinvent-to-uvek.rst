Ecoinvent-to-UVEK conversion resource
=====================================

BrightPath relinks foreground inventories between **ecoinvent cut-off** and
**UVEK cut-off** across supported releases. It rejects changes between cut-off
and consequential models before choosing direct or composed technosphere routes.
This also applies to routes through UVEK 2026. Permissive and warning policies
cannot enable a cross-model conversion. Format conversion and biosphere
migration remain independent operations.

The executable forward resource is
``brightpath/data/migrations/uvek/ecoinvent-to-uvek-2025.json``. It contains only
cut-off source profiles for ecoinvent 3.6 through 3.12. Independently supported
patch-release handling preserves the exact input version; it does not authorize
uninspected profile-specific helpers. UVEK 2026 uses its explicit release bridge.

Accepting heuristic mappings
----------------------------

The forward resource is a compatibility aid, not a claim of scientific or LCIA
equivalence. Strict policy refuses it with ``migration.heuristic_mapping``.
After reviewing the evidence and limitations, accept that finding explicitly::

    from brightpath.core.policies import MigrationPolicy, PolicyAction

    policy = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN)
    converted = inventory.migrate_background(target_context, policy=policy)

For the CLI, add ``--allow-heuristic-mapping`` to ``migrate-background``. This
does not relax unresolved suppliers, missing unit factors, target validation or
system-model compatibility. Approved omissions and helpers can require separate
information-loss acceptance. Reports preserve the source exchange and explain
excluded contributions; omission never means zero impact.

Treatment references and signed quantities
-------------------------------------------

An ecoinvent treatment supplier can have a negative reference production amount
while its UVEK counterpart has a positive service reference. An inspected
correspondence therefore uses a negative conversion factor: negative waste inputs
become positive treatment demands, and positive avoided-treatment inputs become
negative service credits. BrightPath never applies an absolute value to exchanges.
Recovery credits to positive product references retain their own signs.

Reference signs alone do not establish a matching function. Every retained
profile-specific approval binds the inspected inventories and suppliers. Caller
quantities, supported uncertainty, bounds and formulas are scaled consistently.
Foreground suppliers, production exchanges and separately listed emissions remain
under caller ownership. An error rolls back the complete conversion.

Separately listed emissions
----------------------------

The canonical ecoinvent biosphere bridge used with UVEK 2025 selects each
correspondence from the declared source biosphere release. It requires the same
publisher flow UUID, physical unit and complete receiving compartment. Two
explicitly reviewed older groundwater-ion synonyms have different UUIDs but
matching CAS numbers, units, compartments and publisher synonym evidence.
Version-specific decisions distinguish reused manganese labels; an activity in
kilo Becquerel cannot become a mass in kilograms.

The bridge accounts for 11,470 distinct source name/context/unit identities:
5,390 have at least one verified release correspondence and 6,080 have none.
Coverage is selected per release, so a verified correspondence in one release
does not authorize another. Unsupported chemical identities, receiving contexts
or units cause ``migration.biosphere_identity_unresolved`` and roll back the
whole conversion, including under permissive policy. No emission is deleted or
replaced by a guessed chemical or compartment.

Verified renames retain signed quantities, formulas and uncertainty fields.
Publisher flow identifiers are updated together with the target name, unit and
compartment. The two synonym decisions are recorded in
``brightpath/data/export/uvek_canonical_biosphere_identity_review.json``;
``scripts/generate_uvek_canonical_biosphere.py`` reproduces the bridge from the
packaged catalogs. Native UVEK biosphere mappings have their own independently
reviewed coverage and are not extended by this canonical bridge.

Current coverage and pending reviews
-------------------------------------

The cut-off resource accounts for 30,732 source identities: 30,475 replacement
rules and 257 explicit omission rules. Profile-specific approved omissions can
also be selected from replacement rules. The 2,923 retained helper recipes are
reachable from cut-off rules; unused consequential helpers have been removed.

There are 221 identities with pending cut-off decisions: 197 wholly pending and
24 partially approved across versions. They remain blocked with
``migration.reference_flow_sign_unresolved``, including under permissive policy.
The machine-readable queue is ``docs/uvek_forward_review_queue.json``. A pending
counterpart cannot inherit another version's scientific approval.

Five further complete waste-market functions
--------------------------------------------

Five markets retain their complete original mixtures in sixteen exact cut-off
profiles: biowaste and textile waste in India, damaged glazed clay roof tile in
Ecuador, and used metallurgical coke and graphite-forming pitch/coke waste in
China. All sixteen use one wet kilogram of the independently reviewed,
material-specific uncontrolled-disposal helper and the original road tonne-km.
Negative market and child reference signs are corrected independently.

These markets omit no component and invent no recovery credit. Complete RER
average-fleet road services approximate source size/EURO/geographic fleet
shares, with fuel, vehicles, maintenance and infrastructure retained.
Original 3.10.1 inventories are checked separately; no 3.9.1 or other missing
profile inherits approval. Existing reverse road and disposal-operating
correspondences are checked independently. Native physical-flow completeness,
accounting indicators and all earlier land-class scopes remain unchanged.

Regional PET-waste market compositions
--------------------------------------

Forty-two regional PET-waste markets retain their inspected treatment and
freight mixtures in 373 exact cut-off profiles from 3.6 through 3.12, including
original 3.9.1 and 3.10.1 inventories. Incoming negative waste demand becomes
positive demand for a generated service; each original treatment fraction
retains its own reference-sign correction. Open dumping, unsanitary landfill
and open burning use the independently reviewed complete source inventories,
including physical emissions and operating services. No market component is
omitted or normalized.

Dedicated CH PET municipal-incineration and sanitary-landfill services retain
the same 0.2%-water material and wet-kilogram basis. Geography, gas capture,
leaching, emission controls, residue treatment, recovered energy and allocation
can differ; no LCIA equivalence or reconstructed recovery credit is claimed.
The legacy ``xx`` PET-landfill name is an actual inspected disposal service.
Two independently checked patch landfill links apply only inside these market
helpers; the global patch landfill decisions remain unresolved.

All original positive road tonne-km in the approved profiles remain. Complete
native services include fuel/energy, vehicles, maintenance and infrastructure.
The native average road fleet approximates source size,
EURO-class and geographic shares, with explicit warnings. Rail, barge and
container-ship services were also inspected independently; their older RoW
market profiles retain positive steam rail and all original freight quantities.
Those five older RoW profiles remain blocked, including under
permissive policy; no coal/ash omission or steam-to-diesel proxy is approved.
Nine Swiss market profiles with fly-ash extraction use the separate explicit
proxy approval documented below; this 42-market review retains its original
scope.
Existing independent reverse freight mappings are checked, with no inferred
reverse PET-treatment correspondence. Native biosphere completeness and earlier
flow-proxy approvals retain their separate scopes.

Complete freight services and packed stone wool
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Reviewed global freight correspondences preserve tonne-kilometres and distinguish
light-commercial vehicles, heavy lorries, rail and inland barges. Light-commercial
freight uses the complete Swiss 3.5-tonne light-duty fleet; unspecified heavy-diesel
freight uses the complete European diesel fleet across weight classes and delivery
cycles. Rail and barge correspondences use complete transport services, including
vehicle production, maintenance, disposal and infrastructure through their linked
inventories. They replace inappropriate vehicle classes or operation-only targets.
Signed demand and genuine avoided-freight credits retain factor +1. No payload,
vehicle-kilometre, passenger-kilometre or lifetime conversion is inferred.

These are geographic fleet proxies. Country, load, fuel, EURO standard, duty,
vintage and emissions mixtures differ. The Swiss light-duty mix includes diesel,
hybrid, gas and battery-electric vehicles; the source generic light-commercial
fleet uses petrol/diesel assumptions. European heavy-diesel class weights follow
Swiss 2020 shares. Global rail traction can include steam, whose share is not
reconstructed by the European fleet proxy. No individual steam operator is
remapped by this review. Existing independently scoped freight reconstructions
retain their own reviewed mixes. Parent inventories retain every original freight
coefficient, other input, genuine credit and separate emission.

Nine exact cut-off profiles of the global packed stone-wool market now retain
both original producer shares and all four delivery-freight coefficients. The
complete Swiss packed rock-wool factory approximates each CH/RoW producer; the
former glass-wool target supplied a different material. The packed and unpacked
factory modules split manufacture between linked inventories, so using unpacked
wool alone omits part of the required boundary. Both inspected packed factories
consume one kilogram of their own unpacked wool and the matching LDPE packing
quantity. Add no second factory, wool or packing step. The documented 100 kg/m3
is a packing assumption, not a density or thermal-performance conversion.

Reports disclose factory geography, energy, binder, raw materials, infrastructure,
vintage, allocation and performance differences. Signed kilograms, caller
uncertainty, formulas, foreground ownership and separate emissions are preserved;
internal market uncertainty and formulas are not reconstructed. The full helper
graph must be included in scoring and exports. Only enumerated cut-off profiles,
including inspected 3.9.1 and 3.10.1 cases, are supported.

Two independently reviewed reverse correspondences preserve factor +1: the native
Swiss light-duty fleet maps to the ecoinvent 3.12 cut-off Swiss light-commercial
fleet market, and native Swiss packed rock wool at plant maps to the Swiss packed
stone-wool factory. The latter adds no global-market delivery freight. Neither
selection reconstructs the original forward global supply mixture. Cross-model
conversion remains unsupported. Historical consequential recipe contents are
retained only as provenance; scoped cut-off recipe identifiers keep those records
separate without changing their contents.

Bulk copper supply
~~~~~~~~~~~~~~~~~~

The global cathode-copper market supplies bulk copper metal. Its former UVEK
``Cathode`` target supplies a lithium-ion battery electrode containing NMC material
and aluminium. Nine inspected cut-off profiles, including 3.9.1 and 3.10.1, now
use the complete ``Copper, at regional storage`` supply with factor +1 per kg.
Signed quantities, uncertainty, formulas, foreground suppliers and separate
emissions are preserved. This nonempty metal supplier is not an empty stock.

Reports identify a screening proxy: UVEK represents a European supply based on
German 1994 primary/secondary copper and import/logistics data. Grade/purity,
supplier mix, vintage, recycling allocation and impact equivalence are not
established. The native secondary-copper documentation recommends a more detailed
analysis when secondary copper materially affects results. No purity, yield or
concentrate-mass scaling is inferred; all native refining, allocation and transport
components remain. The independently authored reverse copper-supply proxy to
ecoinvent 3.12 cut-off is retained unchanged. Consequential routes remain excluded.

Allocated copper stock and ship-disposal dependencies
-----------------------------------------------------

Nine independently inspected cut-off profiles of sorted, pressed copper scrap
marked Recycled Content cut-off contain only self-linked production +1 kg.
They now use an explicit production-only allocated material stock. Their former
UVEK target is economically allocated copper recovered from PV-panel recycling,
with six nonzero operating inputs, including collection and processing transport.
Those burdens do not belong to the empty source stock. Positive demand must not
acquire them, and negative stock quantities must not create a recycling credit.

All signed copper quantities remain unchanged. The exact stock boundary is also
retained at 42 occurrences in five existing ship-disposal functions and one
copper-scrap market, with dependencies propagated through three vessel markets.
All 63 parent source inventories match their existing fingerprints. Original
processing burdens, freight and other independently reviewed recovery credits
remain separate. Export and score the complete returned graph. Each use of the
allocated stock produces an explicit warning; this records an allocation
boundary and makes no claim that actual recycling is impact-free.

Only the enumerated original cut-off profiles, including independently inspected
3.9.1 and 3.10.1 stock profiles, apply. This does not authorize negative-reference
empty treatment stages or recovered-energy stocks. No inverse correspondence
from physical PV-panel recycling to this empty stock is inferred. The former
native recovery process remains outside independent reverse coverage; an owned
allocated-stock helper retains its original graph through reverse migration.
These repairs affect previously mapped prerequisites and preserve the original
pending queue at 221 identities.

European PET market groups
--------------------------

The Europe-without-Switzerland and RER PET market groups now retain all 333
original child-supply weights across eighteen inspected cut-off profiles from
3.6 through 3.12, including the original 3.9.1 and 3.10.1 patch databases. The
first group keeps all 35 regional markets; RER keeps that nested group and the
original Swiss share. The groups add no freight, loss, treatment or recovery
operation. Every child retains its complete already reviewed treatment and
transport graph, including the separately approved Swiss PET FLUWA profiles.
Shares are neither rounded nor renormalized. Source reference -1 kg becomes
helper reference +1 kg with caller factor -1; original negative child inputs
become positive demand for their complete reviewed helpers.

These groups replace unrelated fleece-production heuristics. They inherit all
child technology, geography, allocation and biosphere limitations. They authorize
no new FAE technology proxy, recovery credit, omitted branch, native physical
flow, energy allocation, cross-system-model route or inverse treatment mapping.
Native physical gaps remain subject to the existing coverage policy; complete
helper graphs must be exported and scored together. Reverse conversion still
requires independently reviewed PET-treatment correspondences and refuses
unsupported suppliers transactionally. Two original queue identities are resolved.

Swiss PET markets with fly-ash extraction
-----------------------------------------

Nine original Swiss cut-off PET-market profiles from 3.6 through 3.12,
including 3.9.1 and 3.10.1, use the explicitly approved complete UVEK PET
municipal-incinerator proxy at the original incineration fraction. Both source
and target treat PET with 0.2% water per wet kilogram. The native service does
not model the source FLUWA extraction and recovered zinc-bearing hydroxide
sludge. Reports warn about ash treatment, recovery, emissions, energy and
allocation differences; no extra recovery or avoided-production credit is
added and no scientific or LCIA equivalence is claimed.

The original open-dump and open-burning fractions keep their unchanged complete
source-inventory helpers. Original road tonne-km remain. Later source profiles
also retain rail tonne-km and the exact diesel/electric weights using complete
CH diesel-with-filter and electric-with-shunting services. Fuel/energy,
vehicles, maintenance and infrastructure are included; fleet, filter,
shunting, vintage and geographic differences remain explicit. No fraction or
freight quantity is normalized, omitted or rescaled using capacity or fuel.

Approval applies only to these nine Swiss PET-market profiles. It creates no
global FAE-incinerator correspondence and authorizes no biowaste technology,
native physical-flow proxy, negative-empty allocation or new reverse rule.
Independent reverse freight suppliers are checked; the native PET incinerator
still has no approved reverse correspondence, so reverse conversion refuses
that link without mutating the input. Earlier native completeness and
land-class scopes remain unchanged.

Uncontrolled disposal and source reference signs
-------------------------------------------------

Thirty-six open-dump, unsanitary-landfill and open-burning identities have
complete reconstructions across 217 exact cut-off profiles. They retain 21,844
nonzero biosphere entries and 270 original operating-service inputs in 124
distinct helpers. Complete uncontrolled source inventories supply their own
material-specific releases and fate. They do not inherit controlled landfill or
incinerator inventories, additional collection freight or recovery credits.

Eleven original ecoinvent 3.9 profiles have reference production +1 kg. Their
caller factor remains +1; the other 206 references are -1 kg and use factor -1.
A negative caller quantity against a positive reference remains a credit.
Missing 3.9.1 counterparts cannot inherit those original 3.9 approvals.

The two operating proxies preserve complete diesel-combustion service per MJ
and skid-steer excavation per cubic metre. Fuel supply, lubricants, machinery,
oil disposal and operating emissions remain embedded in those services.
Reports disclose the source global mix versus Swiss/RER supply and the
Swiss 2015 diesel fleet/filter assumptions versus the source year-2000 basis.
No additional diesel purchase, soil-mass conversion, capacity normalization or
excavated-material disposal is inferred. Both reverse operating suppliers were
checked independently; no inverse uncontrolled-treatment correspondence is
inferred.

Canonical ecoinvent biospheres retain every reported entry. For native UVEK
2026, 134 physical-flow definitions remain unmapped across 213 source profiles;
four profiles have no physical-flow gaps. Default coverage and unresolved-link
policies roll back incomplete results. An explicit permissive policy can retain
unlinked physical exchanges with warnings and incomplete coverage; these require
manual linking before scoring. Only the three marked waste accounting
indicators may be omitted where unavailable, under the existing warned
indicator policy. This review extends neither native chemical proxies nor the
36-context residual-landfill land refinement.

Scoring and exporting helpers
------------------------------

Approved helpers represent inspected treatment, processing or supplier assemblies.
Include **all returned datasets** in scoring and export, including nested helpers.
Supply shares, freight and signed credits retain their reviewed coefficients;
BrightPath does not renormalize around an omitted contribution. Internal source
parameter formulas and uncertainties are not reconstructed unless an approval
explicitly provides them.

Explicit empty cut-off stock helpers preserve documented allocation boundaries.
They do not imply that actual recycling, waste treatment or a negative-reference
empty process has no burdens. Missing suppliers cannot inherit a zero-burden
approval. Material-specific, geographic, moisture and process approximations
remain visible in information-loss reports.

Steel-residue markets and recovery handoffs
-------------------------------------------

Seven steel-residue market identities have 56 inspected cut-off profiles in
3.7--3.12, including original 3.9.1 and 3.10.1 databases. Thirty-six profiles
compose the original landfill fractions and complete road, rail and barge
services. Twenty profiles containing unresolved clinker treatment or positive
steam freight remain pending; neither component is omitted or replaced here.

Eighteen exact recovery handoffs are omitted inside the approved market
profiles under the existing user approval. Each inspected handoff is a renaming
activity into empty positive cut-off recovered stock. Warnings and packaged
evidence retain the original signed coefficient and exact profile. All other
quantities remain unchanged: the retained landfill fraction is not normalized
to one kg and no recovery credit is invented. Four independently inspected
patch handoffs are scoped inside these helpers; their direct global mappings
remain pending rather than inheriting a patch fallback.

Four waste-specific landfill treatments are reconstructed across 32 exact
profiles. They preserve their own releases, cement stabilization and hydrated-
cement disposal, with the reviewed full construction/operation suppliers and
material proxies. They replace the earlier direct mapping to a different waste
where applicable. Three existing market compositions now use these complete
children and matching freight services. Original electric/diesel traction and
regional weights remain explicit, without substituting coal freight, aircraft,
operation-only services or steam traction. The earlier composition decisions
remain historical evidence and do not authorize current conversion.

Native UVEK 2026 generic dump-site transformation remains a separate biosphere
gap for the 16 modern landfill profiles that report it. Earlier profiles can
use the existing native correspondences without that refinement. The earlier
36-context native land-class refinement is not extended. Canonical ecoinvent biosphere conversion preserves the generic
land flow; unsupported physical native flows still reject conversion under the
default coverage policy. Independent reverse supplier checks do not imply an
inverse for an omitted recovery handoff or a reconstructed source market.

Packaging-paper sludge landfarming
----------------------------------

The dedicated landfarming identity has four inspected cut-off profiles in
3.10--3.12, including the original 3.10.1 database. Its source reference is
-1 kg of wet sewage sludge containing 97% water. The complete helper has a
+1 kg reference and uses factor -1 for caller quantities. It retains all 39
reported biosphere exchanges and exactly 0.00097087 m3 of vacuum-tanker service
per kg, without density, moisture or dry-mass normalization.

The complete Swiss spreading service includes fuel, tractor, tanker, shed,
pumping and field spreading. Both source and target explicitly exclude sludge
production and sludge releases; the helper therefore retains those releases
separately. The Swiss proxy replaces the source global service mix. Reports
also disclose the native supplier's chromium-VI and waste-heat differences from
the inspected Swiss ecoinvent supplier. These whole-background differences do
not authorize relabelling the sludge's separately reported chromium-III release.
No fertilizer credit, manure supply, collection freight, extra landfill or
wastewater-treatment burden is added. Only the marked waste inventory indicator
may be omitted where unsupported, with a warning.

UVEK 2025/2026 technosphere conversions can preserve every physical release in
canonical ecoinvent biospheres. Native UVEK 2026 soil-ion and groundwater-volume
correspondences remain a separate biosphere review; unresolved physical flows
reject and roll back conversion under the default coverage policy. No new native
chemical proxy is implied by approving this complete technosphere helper.

Landfill-leachate sludge landfarming
------------------------------------

Twenty-two sewage-sludge functions at 97% water have 82 individually inspected
cut-off profiles in 3.10, 3.10.1, 3.11 and 3.12. These replace legacy heuristic
matches to unrelated dry-waste disposal. Complete helpers retain all 2,368
source biosphere entries and their full receiving contexts, plus exactly
0.00097087 m3 of whole vacuum-tanker spreading service per wet kg. No moisture,
dry-mass, fertilizer, fuel or machinery rescaling is applied. A source reference
of -1 kg becomes a helper reference of +1 kg, with caller factor -1 applied once.

The already reviewed Swiss spreading service includes machinery, fuel, pumping,
field operation and their releases. Its geographic, fleet, capacity and vintage
differences from the source global service mix are reported. Sludge-specific
metals, nitrogen, salts, carbon and water releases remain separate, including
chromium III to agricultural soil. Native spreading combustion emissions do not
authorize relabelling those sludge releases. The independently reviewed reverse
spreading supplier is checked separately; no inverse sludge mapping is inferred.

Canonical ecoinvent biosphere identities remain independent of native UVEK flow
coverage. Default conversion rejects missing physical native flows. Explicitly
permissive conversion keeps those exchanges unlinked, warns about incomplete
coverage, and requires manual mapping before scoring. Only the exact
non-hazardous waste accounting indicator may be warning-omitted if unavailable.
This review approves no new physical proxy or recovered-energy allocation.
Only the enumerated source profiles apply; other patches and consequential
routes remain unsupported. These prerequisite repairs do not reduce the
the original pending identities.

Industrial sludge landfarming
------------------------------

Seven hard-, medium-, soft-fibreboard and particle-board sludge identities have
42 inspected cut-off profiles in 3.9--3.12, including original 3.9.1 and 3.10.1
inventories. Complete helpers retain every wastewater-specific and geographic
release. A -1 kg reference of wet sludge containing 97% water becomes a +1 kg
service with factor -1 for caller quantities. The reported 0.00097087 m3 of
vacuum-tanker service per kg stays unchanged; no density, moisture, dry-mass or
fuel-based rescaling is inferred.

Both fossil and biogenic CO2 remain explicit. Geographic evaporation and
groundwater quantities, nitrogen, phosphorus, metals and heat retain their
source values. The seven 3.9/3.9.1 inventories also report agricultural-soil
carbon; retain it only in those profiles. Later inventories omit that flow,
so no carbon stock or substitution credit is introduced for them.

The existing complete Swiss spreading-service proxy replaces the global
service mix with warnings about regional diesel, equipment, shed supply,
mixing, capacity, vintage and emissions. Older markets contain Quebec and RoW
suppliers; Quebec uses a larger tanker, mixing and higher fuel consumption.
Source and target spreading functions exclude sludge production and sludge
releases. Retain the sludge releases separately and avoid a second machinery,
collection, wastewater-treatment or fertilizer credit. Preserve sludge
chromium-III soil releases independently of the native equipment service's
chromium-VI combustion-air inventory.

Only the explicitly marked non-hazardous-waste inventory indicator may be
omitted where unsupported, with a warning. Physical native-biosphere gaps
remain unresolved under the default coverage policy; canonical ecoinvent
biosphere conversion retains the original releases. The full Swiss reverse
spreading correspondence is independently inspected and preserves service
volume; an inverse waste-treatment correspondence is not inferred.

Industrial-waste treatment markets
-----------------------------------

Fourteen residue and wet-sludge markets have 84 inspected cut-off profiles in
3.9--3.12, including the original 3.9.1 and 3.10.1 databases. Each source has a
-1 kg waste reference, one -1 kg treatment input, no direct biosphere releases,
and positive road freight. A +1 kg helper and caller factor -1 retain one kg of
the independently reviewed waste-specific landfill or landfarming child.

Keep every original freight row, including the three country contributions in
RoW markets. Their sum remains 0.077 tonne-km per kg of waste. Do not infer a
new distance, moisture correction or regional weighting. The complete UVEK RER
fleet service includes vehicle manufacture, maintenance and roads; it replaces
the source freight fleet with explicit regional, EURO-class, technology and
vintage warnings. Its four size classes do not reconstruct the source fleets.
The independently inspected reverse supplier is a complete ecoinvent freight
market; it does not imply an inverse for these waste-market compositions.

The children retain their existing source releases, material inputs, spreading
service and inventory-indicator policies. No physical release or treatment
fraction is omitted. Eight residue-market graphs support the existing native
UVEK 2026 biosphere correspondences; six sludge-market graphs retain explicit
physical biosphere gaps and reject conversion under the default coverage
policy. Canonical ecoinvent biosphere conversion preserves all these releases.
No new chemical proxy or land-class refinement is authorized by these markets.

Further cut-off waste-market compositions
-------------------------------------------

Fourteen further steel-residue and packaging-paper-sludge markets have 100
inspected cut-off profiles in 3.7--3.12, including original 3.9.1 and 3.10.1
inventories. Seventy-six profiles compose existing independently reviewed
waste-specific landfill or landfarming children at their original fractions.
Negative source references use caller factor -1 and positive helper references;
each waste child retains its independently verified sign and original mass.

Seventy-two exact renaming handoffs into empty positive cut-off recovered stock
are omitted with signed-quantity and profile warnings, including 18 independently
inspected patch chains. All other treatment and freight coefficients stay
unchanged. No fraction is normalized around an omitted handoff, and no recovered
product or avoided-production credit is created. Global patch omission rules do
not inherit these parent-scoped approvals. The existing younger BOF-dust market
approvals and all previously generated treatment helpers remain unchanged.

Complete road, diesel/electric rail and barge proxies retain original tonne-km
and country/traction weights, with fleet, geography, infrastructure, fuel,
shunting, filter, vintage and loading limitations. Full source road operations
were inspected through 492 operating inventories. A matching complete native
container-ship service and its independent reverse correspondence are also
inspected; the older market routes containing that service remain pending when
another freight component is unresolved. No service is scaled using fuel use,
ship capacity, distance or equipment lifetime.

Twenty-four older profiles retain positive steam freight and remain blocked,
including under permissive policy. Neither steam-to-diesel substitution nor
omission of coal-ash treatment is approved. The separate proposal for empty
negative-reference mineral co-processing is not applied by this batch.
Native physical biosphere gaps and the earlier 36-profile landfill refinement
retain their own scopes; these market compositions authorize no new flow proxy.

Sewer throughput per cubic metre
---------------------------------

The rural and urban sewer construction and market services have 24 inspected
cut-off profiles across 3.9--3.12, including original 3.9.1 and 3.10.1 databases.
They are already normalized per cubic metre of wastewater conveyed. Complete
helpers retain their material supply, machine use, energy, end-of-life demands
and separate air/water releases, without pipe-length, lifetime or capacity
scaling. They provide sewer infrastructure; wastewater treatment remains separate.

Full material suppliers and disposal services retain their disclosed geographic,
technology, market/freight and treatment-mix approximations. The exact patch
polyethylene-waste groups preserve their reported regional shares through
existing country-market correspondences, without extending the global rules to
an unreviewed patch. Water releases retain the reported cubic metres in canonical
exports; native UVEK uses the existing compartment-preserving 1000 kg/m3
correspondences with warnings. All returned nested helpers must be scored/exported.
No real release is omitted, and no reverse sewer-throughput correspondence is
inferred from these forward helpers.
Fifteen existing reverse supplier choices were checked independently. Six native
suppliers still lack approved reverse correspondences: the four disposal services,
global medium-voltage electricity and emulsion PVC. A complete helper graph that
contains them remains blocked on UVEK-to-ecoinvent conversion, including under
permissive policy; quantities and datasets are rolled back together.

Residual-material landfill helpers
-----------------------------------

Twenty-five residual-landfill identities have 142 independently inspected profiles
across cut-off 3.7--3.12, including original 3.9.1 and 3.10.1 databases. Helpers
retain waste-specific releases, stabilization cement, separate hydrated-cement
disposal and the source operating and construction quantities. Two active
blast-furnace-slag profiles in 3.9 have positive reference amounts: these retain
factor +1 and their treatment burdens. The other 140 references use factor -1.

Modern inventories keep their expanded construction and operation inputs. Scoped
proxies use refinery bitumen for construction pitch, normal ready-mix concrete
for 25 MPa concrete, full PVC resin from regional storage, and the existing RER
heavy-fuel-oil industrial-heat proxy for the qualified non-natural-gas heat
commodity. Pipe extrusion remains separate. Reports disclose feedstock,
allocation, strength/mix, fuel/technology, market, geography and delivery
differences. These helper-specific choices do not change global correspondences.

The approved native UVEK 2026 land refinement applies only to 36 profiles of nine
known residual-landfill helpers. Generic ``Transformation, from dump site``
becomes the native residual-material-landfill class at the unchanged signed
5.7143e-5 square metres per kg. Canonical ecoinvent biospheres retain the generic
label. The native operation's 6.25e-5 area is not a conversion factor; no
occupation-year conversion is inferred. Every refinement reports an
information-loss warning. Unrelated foreground land exchanges cannot inherit it.

Generated helpers retain their creation biosphere profile so later migration can
verify their identity against the current packaged recipe and replay its scoped
bindings. Stale identities require regeneration. Strict information-loss policy
rolls back the operation. Only explicitly approved waste/carbon inventory
indicators may be omitted with warnings when absent from the target; actual
emissions remain. Independent reverse component correspondences retain their own
reference signs and do not establish an inverse for a complete landfill helper.

Sewage-sludge digestion and allocated gas
------------------------------------------

The negative sludge reference supplies a treatment service per cubic metre of
sludge. Its helper retains the inspected plant, electricity, heat, chemicals or
lubricating oil and direct emissions with factor -1. Source 3.6 additionally
retains carbon-dioxide resource uptake. The separate positive biogas reference
has no active allocated operating burdens in these cut-off profiles; earlier
zero-valued biosphere records are retained and later production-only profiles
use explicit allocated-stock helpers. Both branches belong to the same original
activity in each inspected version. No gas-production burden or recovery credit
is added to the gas stock, and sludge volume is never equated to gas volume.

All nine versions (3.6--3.12, including 3.9.1 and 3.10.1) were inspected directly.
The two patch contexts are explicitly declared; other correspondences still
require their own exact profile decisions and cannot inherit a base-version
review. Component technology, supply and geographic approximations are reported.
The 3.12 waste-disposal inventory indicator retains its original quantity where
the target biosphere supports it; otherwise only that indicator is omitted with
a warning. Methane, carbon dioxide, resource uptake and operating inputs remain.
The indicator is never used to infer a sludge-volume or gas-yield conversion.
The independent reverse operating reconstruction is described in
:doc:`uvek-export`.

The CH and RoW sewage-sludge markets are independently inspected aliases of
these full treatment services in all nine exact versions. Each market has a
-1 m3 sludge reference and only one -1 m3 input to the same region's digestion
service; there are no other exchanges, emissions or freight. Conversion reuses
the exact child helper with factor -1, including its operating burdens, direct
releases and information-loss warnings. Demanding the market and direct service
together shares the helper while preserving both demands. No extra market
helper, gas production, recovery credit, dry-mass conversion or sludge-to-gas
volume equivalence is introduced. The former biogas-product target is replaced
by the complete sludge-treatment function. Independent reverse component rules
now cover the matching CH plant and seven regional electricity suppliers, with
explicit plant-background and electricity-system warnings. No inverse
sludge-market correspondence is inferred.

Slag-landfill construction and operation
----------------------------------------

Nine construction and operation suppliers use UVEK's dedicated slag compartment.
The former sanitary-landfill suppliers describe a different facility: its capacity
is 1.8 million cubic metres, while the inspected slag compartment holds 375,000
cubic metres at 1,500 kg/m3. One facility unit remains one facility unit; no
capacity rescaling is applied. Operating demand remains per signed kilogram,
separately from infrastructure and waste-specific emissions.

Both imported references are positive. This includes the UVEK operating supplier,
whose native reference is an input waste flow: the mapping retains factor +1.
It does not inherit the sign convention of a negative-reference waste treatment.
The Swiss proxy preserves the identified function and unit while substituting
its own material, heat, electricity, freight and water inventory. Warnings name
these differences, including the source facility's direct water releases absent
from the UVEK facility. Caller-owned emissions remain unchanged. Equal LCIA
scores are not established.

Nine exact cut-off releases 3.6 through 3.12, including 3.9.1 and 3.10.1,
are reviewed; other uninspected versions receive no inherited approval. Existing reverse rules independently
select Swiss slag-landfill construction and operation and remain unchanged.
This review does not authorize whole ash-treatment or leachate-treatment mappings.

Waste-specific bottom-ash landfill services
-------------------------------------------

Forty-nine bottom-ash identities use generated treatment helpers for 200 exact
cut-off profiles across ecoinvent 3.9, 3.9.1, 3.10, 3.10.1, 3.11 and 3.12.
A negative incoming exchange against the source's -1 kg reference becomes a
positive demand for a +1 kg service; positive credits and zero quantities
retain their corresponding signed meaning. No absolute-value, moisture,
dry-mass, carbon-content or facility-capacity scaling is applied.

Each helper retains the source's own releases and original regional operating
weights, totalling one kg of operating burdens and 1.7778e-9 slag-compartment
facility units per kg ash. The facility fraction is retained as reported,
without replacing it by a newly calculated reciprocal capacity. The separately
reviewed Swiss operation and facility replace regional background supplies;
heat, freight, water, electricity and vintage differences remain approximations.

These services start after ash has been generated. They add no municipal
incineration, upstream wastewater treatment, original-waste disposal or recovery
credits. Ordinary MSWI ash, fly-ash-extraction variants and WWT-derived ash keep
their individual release inventories. For example, polyethylene bottom ash
retains 0.0046932 kg long-term groundwater zinc and 1.3971e-7 kg surface-water
zinc per kg ash, rather than inheriting a generic ash composition.

Native UVEK 2026 uses the explicitly approved water proxies with chemical and
receiving-water warnings. Reported waste/carbon inventory indicators remain
where the requested biosphere supports them; only the three specifically marked
indicators may be omitted with warnings when unavailable. Actual emissions are
retained. Caller uncertainty, formulas, foreground suppliers and separate
emissions remain protected; internal background formulas and uncertainty are
not reconstructed.

The independent reverse supplier mappings still select Swiss ecoinvent slag
operation and construction. Generated graphs with the canonical ecoinvent
biosphere can use those reverse links while retaining their helpers. A complete
native-biosphere graph has a known reverse limit: 28 of the water-flow targets
lack independently reviewed correspondences. Those routes fail transactionally;
no generic-element-to-ion inverse is inferred from the forward approval.

Hot-rolled stainless material
-----------------------------

The global hot-rolled 18/8 stainless market and RoW producer use UVEK's
``Chromium steel 18/8, at plant`` material. The former ``Sheet rolling, chromium
steel`` target was a cold-rolling service with only loss-replacement stock.
The material target supplies one kg of stainless stock and already includes
hot rolling; BrightPath preserves signed kg with factor +1 and adds no further
rolling demand. Geographic, supply-mix, freight and production differences are
reported as approximations. Twenty-three inventory and description fingerprints
bind the seven inspected cut-off versions, their RER producer and both UVEK
process boundaries. The independently inspected reverse material rule already
selects the RER hot-rolled producer and remains unchanged.

Sewer infrastructure capacity classes
-------------------------------------

Sewer construction matches the documented capacity class rather than numbers
shared by otherwise different activity titles. The five ecoinvent network designs
map to UVEK sewer classes 5, 4, 3, 2 and 1 respectively for the networks named
``1.6E8l/year, 6 km``, ``1E9l/year, 30 km``, ``5E9l/year, 110 km``,
``1.1E10l/year, 242 km`` and ``4.7E10l/year, 583 km``. The descriptions establish
the person equivalents and unrounded lengths. Construction and market exchanges
retain **one kilometre as one kilometre**, including negative construction credits.
The total network length and annual throughput are not conversion factors.

These fifteen mappings retain the 100-year lifetime and renovation after
70 years. UVEK substitutes Swiss materials, energy, disposal and emission models;
the source global supply mix and RoW construction geography are not reconstructed.
Information-loss warnings disclose these differences. The five independently
inspected reverse correspondences already select the matching CH constructions
and are unchanged. The newer rural/urban services per cubic metre of conveyed
wastewater require their separate boundary review; they cannot inherit this
kilometre-based correspondence.

Reverse conversion and audit history
------------------------------------

UVEK-to-ecoinvent conversion uses the independently reviewed directional cut-off
resource described in :doc:`uvek-export`. A forward approximation or omission
does not establish an inverse. Both directions reject cross-system-model routes.

Historical scientific reviews remain available for traceability. The authoring
ledger includes observations of both ecoinvent models so existing scientific
fingerprints can be reproduced. The generator publishes only the compatible
cut-off projection; historical consequential decisions cannot execute.

.. toctree::
   :hidden:

   ../history/uvek-reviews-before-system-model-scope

Bottom-ash markets and freight
-------------------------------

Forty-two bottom-ash market identities use helpers for 176 independently
inspected cut-off profiles across the same six exact releases as the ash
services above. Preserve one kg of the matching ash-treatment service and all
source freight in ton-km. A negative market reference becomes positive service
demand with factor -1; caller credits retain their signed meaning. These
markets have no direct biosphere exchanges. Their child helpers keep the
waste-specific releases, facility and operating demand.

Road inputs use the independently inspected complete RER diesel lorry fleet
average. This proxy uses Swiss 2020 size-class shares; geography, load, vehicle
classes, EURO standards, duty and vintage differ from source supplies. CH rail
markets retain 0.0412324596234946 diesel-with-particle-filter and
0.958767540376505 electric-with-shunting shares using complete UVEK services.
Both rail services include vehicles, maintenance, disposal and track
infrastructure; no extra operating or infrastructure burdens are added.
Regional ash markets retain 0.02577 road ton-km and 0.00356 rail ton-km per kg;
other inspected markets retain 0.077 road ton-km, including their original split
inputs. No freight normalization, kg-to-ton conversion or LCIA equivalence is
inferred. Freight choices are scoped to these helpers and do not replace the
generic freight mappings used elsewhere.

The independent reverse review adds the complete Swiss electric rail service
to ecoinvent 3.12 cut-off with factor +1 and explicit energy, vintage and asset
warnings. Existing road and diesel rail correspondences are unchanged. Complete
canonical-biosphere helper graphs can reverse; the known native-water reverse
limitations above still apply. Evidence comprises 231 exact inventory and
boundary fingerprints in ``uvek_ash_market_review.json``.

Waste markets with reviewed landfill children
---------------------------------------------

Eleven electric-arc-furnace dust/slag and post-incineration residue market
identities have 54 inspected cut-off profiles. Forty-six complete profiles reuse
their exact reviewed residual-landfill children, retaining waste-specific releases
and operating/construction burdens. Source references are -1 kg; market helpers
use +1 kg with factor -1. Caller signs and each child reference factor remain
independent. No upstream treatment, alternative fate or recovery credit is added.

The helpers retain original road, rail and barge ton-km. Road demand uses the
complete RER fleet proxy. Rail demand preserves its source traction shares:
India has 0.644291081477659 electric and 0.355708918522341 diesel; Europe without
Switzerland has 0.6 electric and 0.4 diesel; Swiss markets retain their inspected
0.958767540376505 electric and 0.0412324596234946 diesel shares. Complete CH
services include shunting, rolling stock, maintenance and track infrastructure.
The complete RER barge service includes vessels, ports and canals. No second
operation, assets, capacity scaling or mass/distance conversion is added.
Geography, gauge, filters, fleet/class composition, vintage, fuel, load, energy
and background differences remain proxy warnings. Global freight rules and
independently reviewed reverse freight correspondences are unchanged.

Nine identities are fully resolved. Two dust-market identities retain eight
older pending profiles containing unresolved clinker co-processing; some older
global routes also include steam traction. These profiles cannot inherit a later
version's approval, omit the unresolved fraction or renormalize landfill shares.
Only the enumerated profiles can execute, including under permissive policy.

Nested helpers retain packaged review metadata for approved optional indicators
even when they have no contextual land binding. On later migration to native
UVEK, only those indicators may be omitted with explicit warnings when absent
from the target. Real emissions remain. Stale helpers and strict information-loss
policy fail transactionally; caller-owned indicators receive no new omission.

Chemical-plant heat and retained make-up water
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The inspected ``heat, from steam, in chemical industry`` product supplies heat
energy in MJ. It differs from a physical steam feed measured in kg. Four exact
market/factory identities across nine cut-off releases, including 3.9.1 and
3.10.1, use a generated helper with one MJ of the complete UVEK
``Heat, unspecific, in chemical plant`` service per MJ requested. This replaces
the rounded 0.36 kg/MJ steam links and also completes the former direct MJ link.

The original UVEK heat definition explicitly excludes water input under its
closed-loop assumption. Each helper therefore retains the inspected ecoinvent
make-up-water supply and both physical water releases separately. Per MJ this
is approximately 0.061818 kg of make-up water, with 9.272727e-6 m3 released to air
and 5.254545e-5 m3 to water. Every original water-provider share is retained
separately, including the seven RoW shares. Do not replace it with the fresh-steam
water input, normalize the country shares or add the original fuels/electricity
again: the complete native heat service already includes energy generation.

Reports disclose a screening proxy for fuel and electricity mix, geography,
combustion efficiency, assets, allocation and vintage; impact equivalence is not
established. Native water supplies retain their geographic and technology proxy
limitations. The canonical physical water identities, compartments, units and
amounts remain explicit. Unsupported physical flows must be reported as
unresolved rather than omitted, including on native UVEK 2026 routes.

Factor +1 preserves caller signed MJ, uncertainty, formulas, metadata, foreground
suppliers and separately listed emissions. Internal source uncertainty and
formulas are not reconstructed. Export and score every returned helper. The
unit-share markets are flattened only after confirming their complete local
producer share and absence of other active exchanges in each exact release.
Missing profiles, consequential inventories, allocated chemical co-product steam,
physical kg-steam and individual steam transport cannot inherit this review.

The reverse native MJ heat identity remains unsupported pending an independent
water-boundary review. No inverse, water cancellation or change to the separate
existing kg-steam reverse correspondence is inferred.

Allocated aluminium and iron stocks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Two explicit ``Recycled Content cut-off`` stocks supply aluminium scrap prepared
for melting and sorted, pressed iron scrap. All nine inspected cut-off releases,
including the actual 3.9.1 and 3.10.1 databases, contain exactly one self-linked
production of +1 kg and no other exchanges. Their allocation boundary is retained
through production-only foreground suppliers, with factor +1 and an explicit
allocation-only warning. This does not assert zero impacts for actual scrap
collection, preparation, recycling or treatment.

The former native suppliers contain real preparation inputs. Those burdens must
not be added to an empty allocated stock. Nineteen existing helper bodies also
use those native suppliers, but their exact source inputs represent different,
nonempty preparation processes; those bodies remain unchanged. This correction
uses source identities rather than replacing every occurrence of a native name.

Preserve signed kg, uncertainty, formulas, caller metadata, foreground suppliers
and separately listed emissions. Export and score every returned helper. Only
the eighteen exact profiles are reviewed. Missing profiles, consequential supply,
negative empty treatment stages and recovered-energy allocation cannot inherit
this decision. Native preparation suppliers do not acquire an inferred reverse
mapping to an empty stock.

Exact source-input omissions in a reviewed recipe
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A nonempty recipe can declare ``source_input_omissions`` for individually
reviewed source exchanges that are absent from its derived inventory. Each
entry records the exact source technosphere profile and activity, source
inventory SHA-256, original exchange position, complete supported exchange
identity/quantity/uncertainty, and rationale. The schema requires an explicit
verified direct cut-off correspondence to that source activity/profile, and
rejects repeated original positions, zero-valued entries and a shared direct
recipe owner. This is not a general small-exchange threshold or an unavailable
supplier fallback.

Materialization emits a separate ``migration.recipe_source_input_omission``
issue and loss for every declared exchange. Its quantity is stated per one
recipe reference unit, independently of caller demand. The immutable report
and the generated helper's ``brightpath_conversion`` metadata retain the audit.
Strict information-loss policy reports an error; warning policy retains the
loss. The complete omission metadata participates in recipe and generated-helper
identities. Caller-provided metadata cannot grant such an omission.

The exact 3.9 cut-off global HDPE market now uses this mechanism for four
explicitly user-approved positive recyclate entries totalling
1.173141240030732e-13 kg/kg. Two original virgin producer entries and three
original freight entries remain separate, with quantities and uncertainty
unchanged and no share normalization. The omitted recycling production is
nonempty; neither an empty native recycling handover nor a virgin-product
replacement represents it. Its impact difference is unquantified. All other
supplier, geography, cumulative-inventory and native biosphere limitations
remain warnings. Other source versions and other omissions are not approved.

Pilot natural-gas heat markets
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The exact RoW natural-gas heat markets from ecoinvent 3.9 and 3.12 cut-off
retain their 43 original supplier entries individually, including coefficients,
order and marginal uncertainty. Native complete heat-output services replace
the source production models per positive MJ of heat. Conversion factor +1
preserves signed caller heat demand and credits. No extra fuel-efficiency,
distribution-loss or network factor is applied: the native services already
contain their production inventories and allocation.

Separate furnace, low-NOx, modulating and condensing technologies remain
separate. Engine heat uses the inspected exergy-allocated native capacity
proxies, including the closer 1 MWe lean-burn endpoint. Utility CHP uses native
country suppliers where available, US for US subregions and ENTSO-E otherwise,
with explicit technology, capacity and geographic warnings. The native RU
identity and upstream burner specify RU but descriptive text says RO; retain
the links and expose the contradiction rather than silently relabelling it.

The packaged review records source/native inventory and metadata witnesses,
heat-output and upstream-burner bases, the native bundle pin and geographic
conflicts. Native internal inventories and uncertainty replace source internal
models; allocation, performance, heat quality, geography, vintage and score
differences remain unquantified. The 2,991 upstream native biosphere exclusions
remain explicit. Only these two market profiles qualify; other source releases,
standalone components, reverse routes and system-model changes remain outside
this decision.

Pilot mixed-fuel heat market
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Only the encountered ecoinvent 3.9 cut-off RoW other-than-natural-gas heat
market receives this scope. Preserve all 188 supplier entries separately,
including their original coefficients, order and marginal uncertainty. The
positive heat reference gives factor +1; negative caller heat credits remain
negative. Add no district network, efficiency factor, normalized shares or
co-output credits.

Complete native heat services cover the reviewed furnace and CHP branches.
Explicit screening differences include wood CHP capacity and vintage,
industrial propane versus a smaller LPG boiler, coal tar versus heavy fuel oil,
straw versus industrial wood chips, and pulp-mill/black-liquor recovered heat
versus wood-chip CHP heat. These replace source production and allocation
models; neither matching MJ units nor a similar name establishes equivalence.
Feedstock, performance, emissions, allocation, geography and uncertainty
differences have unquantified score effects.

Seventy-five allocated-burden helpers keep a one-MJ heat reference. Gas and coke
combustion use audited source fuel demand; refinery gas uses a documented mass
normalization. Bagasse/oil CHP and EAF heat use individually verified source
heat-to-companion inventory ratios to weight complete native donors. Those
ratios are allocation proxies, not energy-unit conversions or physical steel
purchases. Native donor upstream boundaries and allocation differ, including
agricultural/refinery/distillery burdens in bagasse electricity and absent
upstream blast-furnace-gas production. Missing oil geographies use the actual
historical UCTE fleet. No invented supplier, thermal efficiency or recovery
credit repairs these differences.

Twelve source heat outputs contain only positive production. Their market shares
use the actual native positive recovered-heat output, also production only and
documented as zero allocation to energy and full allocation to waste disposal.
This preserves the source allocated-heat boundary with waste/feedstock and
geographic warnings. It does not imply zero physical combustion, qualify an
empty waste-treatment process or introduce a generic empty-provider fallback.
All 188 market entries remain; there are no source omissions or empty helpers.
Six municipal recovered-heat wrappers also use the native plant-gate heat
output, with source routing and allocation differences explicit.

The packaged review retains source/native witnesses, individual allocation
proofs, exact scope and limitations. The native bundle's 2,991 upstream
biosphere exclusions persist. Other versions, standalone producers, reverse
routes and system-model changes do not inherit this review.
