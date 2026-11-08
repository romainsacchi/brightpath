Historical UVEK mapping reviews before system-model restriction
================================================================

.. warning::

   This is an archival record of scientific reviews before 2026-11-06.
   All consequential-to-UVEK routes, decisions and helpers described here have
   been retired. Current conversion supports compatible cut-off models only.
   Historical counts and approved-profile lists below do not describe runtime
   coverage. See :doc:`../workflows/ecoinvent-to-uvek` for current behavior.


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

Copper and steel sheet-rolling markets map to the corresponding RER rolling
services for the same metal. The silica-sand market maps to ``Silica sand, at
plant`` in Germany. These choices correct three legacy assignments to different
commodities. They retain the geographic and market-versus-production limitations
of the heuristic route; no global supply-mix or grade equivalence is implied.
The same choices apply when composing the route to UVEK 2026. Quantities,
uncertainty parameters and the original source context are preserved.

Waste-treatment reference signs
--------------------------------

Ecoinvent treatment suppliers commonly have a negative reference production
amount; the installed UVEK 2025 Brightway background uses positive service
production amounts. An ecoinvent negative waste input must therefore become
a positive UVEK treatment-service input when the supplier roles agree.
This is a reference-coordinate conversion, not an absolute-value operation:
positive avoided-treatment exchanges become negative service exchanges, and
negative recovered-product inputs retain their legitimate credit sign.
Independently reviewed waste-to-recovered-product credits also retain their
incoming sign and use an explicit positive quantity factor, as described below.

The evidence-backed audit corrects 4,236 background-supplier rules with explicit negative
``conversion_factor`` values. It inspects single, self-linked reference
production exchanges in all 14 installed ecoinvent 3.6--3.12 cut-off and
consequential backgrounds and UVEK 2025, and checks the native UVEK waste/product
roles against the independently verified openLCA reference catalog. Names alone
never establish reference signs. Factors apply even when source and target units
are identical; supported uncertainty, bounds and formulas are scaled with them.

Another 595 mappings remain wholly pending and blocked with
``migration.reference_flow_sign_unresolved``, including under permissive policy.
A further 123 identities have exact-profile approvals and explicitly pending
counterparts: 121 market/processing assemblies and two
sweet-gas flaring references. The queue
therefore retains 718 identities with pending decisions. A sign flip cannot
establish functional equivalence. Unknown versions, system models and uninspected
patch profiles cannot inherit an exact-profile helper approval. The original
foreground and its production exchanges remain unchanged; failures roll back the
conversion.

Of the 196 identities with conflicting reference signs, 195 vary by system model.
Their sign reviews now contain separate ``profiles`` entries for every inspected
exact source version and system model. The remaining ``market for coal tar``
case also changes sign between consequential versions and now has its own scoped
approval. Runtime selects only an exact family/version/system-model match, on
an operation-local copy of the rule. Missing decisions, including unreviewed patch
versions for this cohort, remain blocked without falling back to another profile.

Approval of profile-specific handling does not approve the supplier substitutions.
Three Swiss paper-disposal mappings now have independent approval as described
below. Five feed markets retain their consequential nutritional omissions and now
have independent physical cut-off product-market approvals across all 35
observed cut-off profiles. Other supplier substitutions require their own scientific evidence. For example,
a negative consequential reference for recycled plastic can still be classified
as an allocatable product;
its sign alone does not establish a waste-treatment function. Independently
approved replacement entries must provide a verified review and an explicit signed
``conversion_factor``; approved profile omissions instead declare deletion and
reference a shared omission review. Until approval, global and selected profile dispositions
remain unresolved, even under permissive policy. Profile-selection infrastructure
alone never establishes scientific equivalence.

Swiss paper disposal: approved profile-specific treatment proxies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The 2026-11-05 approval covers three exact CH, kilogram waste references:
newspaper municipal incineration, newspaper sanitary landfill, and packaging-paper
municipal incineration. Their targets are the corresponding CH UVEK disposal
services: newspaper at 14.7% water (``bafu-341912`` incineration and ``bafu-602962``
landfill), and packaging paper at 13.7% water (``bafu-445807`` incineration).
All three target inventories contain active treatment inputs and emissions and
have production +1 kg and verified native WASTE_FLOW input references.

All three source identities have production +1 kg in ecoinvent 3.9 cut-off, so
their approved factor is +1. The inspected consequential identities have
production -1 kg: 3.6--3.9 for both incineration services and 3.6--3.12 for
newspaper landfill, requiring factor -1. These are 18 exact source/profile
approvals. The positive-reference cut-off inventories are not empty placeholders.
The audit records these mappings separately as ``approved_by_profile``. Global
sign reviews remain unresolved until runtime selects the exact verified profile;
uninspected versions/models, including patch profiles, retain their blocker.

These are disclosed screening proxies, not equal-score claims. Composition,
treatment technology, emissions, allocation and background supplies may differ.
The landfill warning explicitly states that newer consequential electricity and
heat substitution credits are not reproduced by UVEK. Incineration warnings
disclose approximated energy recovery, emission controls and residue treatment.
The existing information-loss policy applies: acceptance yields a warning and
loss record; an error policy rolls back conversion. Exchange amounts, supported
uncertainty and formulas follow the selected factor; production, caller-owned
data and recovered-energy reference products remain unchanged. The UVEK 2026
bridge carries the approved conversion without inferring reverse rules.

Coal tar: approved positive-reference production proxy
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The 2026-11-04 scoped review approves ``market for coal tar`` (coal tar, GLO,
kilogram) to ``Tar, at coke plant`` (GLO, kilogram) with factor +1 for all seven
inspected cut-off releases (3.6 through 3.12) and consequential 3.10, 3.11 and
3.12. Consequential 3.6, 3.7, 3.8 and 3.9 retain negative references and use the
separately approved omissions below. Uninspected patch profiles
also remain blocked. Negative exchanges in approved positive-reference profiles
keep their negative amounts and uncertainty; no absolute-value operation is used.

This is an approved approximate production proxy, not a market-equivalence claim.
The inspected UVEK source allocates 4.1% of coke-plant burdens to tar using energy
allocation. Market coverage and ecoinvent consequential modelling are not
reproduced. Applied conversions report ``migration.technosphere_information_loss``;
callers must explicitly accept that loss as well as the heuristic mapping, for
example ``MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN,
on_information_loss=PolicyAction.WARN)``. The runtime and scientific review evidence
are recorded independently, and both resource regeneration and audit replay retain
the approved scope.

The subsequent 2026-11-05 approval covers ``treatment of coal tar, in industrial
furnace 1MW`` and ``market for coal tar``, GLO, kilogram, only in consequential
3.6--3.9: eight exact source/profile pairs. Both reference amounts are -1 kg.
Each market supplies 100% furnace treatment plus train/lorry freight. Each
treatment has active combustion emissions and a heat substitution credit of
26.6 MJ/kg. UVEK tar production cannot represent that function, and the
inspected catalog has no complete matching coal-tar furnace-treatment service.

Those exact profiles now omit the external input and continue with
``migration.technosphere_omission`` warnings. Combustion emissions, operating
burdens, heat substitution credits and additional market freight are excluded;
score effects are method-dependent and unquantified. The complete original
exchange remains in the loss report. Production, matching foreground suppliers
and separate foreground biosphere exchanges are preserved. Generic deletion or
information-loss error policies cannot block these approved warnings; heuristic
route permissions still apply and unrelated errors roll back conversion. The
UVEK 2026 bridge preserves the omissions and findings.

The market keeps its ten approved production-profile replacements with factor
+1. The direct treatment has only four reviewed omission profiles. Missing
profiles, including unreviewed patch profiles, fail closed without fallback.
The cached resource is unchanged: execution selects the exact profile on an
operation-local resource and uses either a replacement or an approved deletion.
Former targets/factors, all four direct-treatment and fourteen market
observations remain audited; no reverse omission is inferred.

Steelmaking recovery handoffs: approved profile-specific omissions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The next 2026-11-05 approval omits the RER/RoW basic-oxygen-furnace-dust
handoffs to recovery. Independently inspected similar cases extend this to
sixteen exact kilogram identities in ecoinvent 3.7--3.12, both system models:
192 source/profile pairs. These are renaming activities that change waste into
recyclable material, covering:

* Basic oxygen furnace dust, sludge, slag and secondary-metallurgy slag.
* Blast furnace dust and sludge.
* Electric arc furnace slag and secondary-metallurgy slag.

Every inspected handoff has reference production -1 kg, one -1 technosphere
input to its own recovered-material supplier, and no active direct biosphere
exchanges. All 96 cut-off observations terminate at a +1-reference process with
no other active exchanges. All 96 consequential observations lead through
positive-reference recovery markets to supplementary-cementitious-material
markets, carrying recovery credits. Version/geographic market mixes vary.

Their former UVEK targets supply economically allocated slag production:
``Basic oxygen furnace slag, at plant`` (``bafu-607456``), ``Blast furnace slag,
at plant`` (``bafu-167904``), and ``Electric arc furnace slag, at plant``
(``bafu-339275``), all RER/kg with native PRODUCT_FLOW output and production +1.
Their documented allocation factors are 0.0077, 0.015 and 0.0052 respectively.
A matching material name does not make allocated production equivalent to a
waste-to-recyclable handoff or its consequential recovery credit.

The exact inspected profiles now omit the external handoff and continue with
``migration.technosphere_omission`` warnings. Consequential recovery credits are
excluded and score effects remain method-dependent and unquantified. The empty
cut-off chain does not establish zero impact for steelmaking, waste disposal,
transport or recycling as a whole. Complete original exchanges, including
amounts, uncertainty, formulas and caller metadata, remain in loss reports.
Production, matching foreground suppliers and separate foreground inputs and
emissions are preserved. No physical landfill or recycling treatment is invented.

These are profile-specific decisions inside the existing rules, with their prior
targets/factors retained for audit. Two handoffs were explicitly approved and
fourteen independently inspected similar cases auto-approved. Missing profiles,
including 3.6 and unreviewed patch versions, remain blocked without fallback.
Generic deletion/information-loss error policies cannot block these approved
warnings; route permissions still apply and unrelated errors roll back conversion.
The UVEK 2026 bridge retains the omissions and reports. Reverse decisions remain
independent, and no omission is inferred for other steelmaking, material-market,
disposal or recycling processes.

Metal recovery credits: approved positive quantity factors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The subsequent 2026-11-05 approval preserves the zinc-concentrate credit from
GLO ``metalliferous hydroxide sludge to market for zinc concentrate``. Similar
independently inspected zinc-scrap and bronze-scrap credits are auto-approved.
These three kilogram identities exist only in the inspected consequential
ecoinvent 3.6--3.12 profiles: 21 source/profile pairs. Each source has reference
production -1 kg, one negative input to a positive-reference product market,
and no other active technosphere or biosphere exchanges.

.. list-table::
   :header-rows: 1
   :widths: 30 45 25

   * - Source reference
     - Credited UVEK product
     - Positive factor
   * - Metalliferous hydroxide sludge, GLO/kg
     - Zinc concentrate, at beneficiation, GLO/kg (``bafu-370641``)
     - 0.0622641509433962
   * - Post-consumer zinc scrap, GLO/kg
     - Zinc, primary, at regional storage, RER/kg (``bafu-236764``)
     - 1
   * - Post-consumer bronze scrap, GLO/kg
     - Bronze, at plant, CH/kg (``bafu-368900``)
     - 1

These conversions change a waste-service reference into its recovered-product
credit, rather than substituting a treatment provider. The explicit
``role_conversion: recovery_credit`` review preserves the incoming sign. For
example, -10 kg sludge becomes a credit of -0.622641509433962 kg UVEK zinc
concentrate; positive avoided-credit exchanges remain positive. A treatment-sign
inversion would turn the credit into a burden. No extra metal-content or moisture
scaling is inferred. Supported uncertainty, bounds and formulas follow the factor.

The replaced suppliers were lithium hydroxide, zinc concentrate used as zinc
metal, and a manufactured bronze contour. All three new UVEK products have
production +1 kg, native PRODUCT_FLOW output and non-empty production inventories.
These remain screening proxies: source global market mixes, transport and
consequential supply models are not reproduced. Concentrate grade, technology
and allocation can differ. Primary zinc uses a European regional-storage supply
mix. The Swiss bronze provider assumes 95% copper/5% tin and documents poor data
quality. The sludge source already lacks Waelz recovery burdens; this conversion
does not add physical recovery processing to any of these credit-only references.

The existing information-loss and heuristic policies apply. Accepting the proxy
produces warnings/loss records; strict information-loss policy rolls back the
operation. Production, matching foreground suppliers and other foreground
exchanges remain intact. The UVEK 2026 bridge preserves the credited quantities.
Missing exact profiles, including cut-off and unreviewed patch versions, remain
blocked. No reverse reconstruction of waste from primary metal is inferred.

The authoring collector independently checks source/market reference amounts,
the exact credited supplier and its amount, and absence of extra processing or
biosphere exchanges. Changed quantities, suppliers or profile coverage require
a new scientific review. The audit retains prior mappings and 21 sparse quantity
observations with per-profile integrity digests. Evidence files used with
``--evidence`` must include this functional quantity evidence as well as reference
signs; older sign-only snapshots cannot reapprove the credit rules.

Generic pesticide credit: auto-approved single-product substitution
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The same documented single-product-credit authorization independently covers
``dichloropropene to generic market for pesticide, unspecified`` / GLO / kg
only in consequential ecoinvent 3.6--3.9: four source/profile pairs. Each has
production -1 kg, one -1 kg input to ``market for pesticide, unspecified`` /
GLO / kg, market production +1, and no other active inputs or direct emissions.
The documented function is a generic pesticide credit, not chemical manufacture
or pesticide application.

The unrelated ``Diode, unspecified, at plant`` target is replaced by RER/kg
``Pesticide unspecified, at regional storehouse`` (``bafu-285276``), with factor
+1 and an explicit ``recovery_credit`` role conversion. The positive-reference
native PRODUCT_FLOW provider has a non-empty pesticide-production inventory.
Negative inputs retain a 1 kg pesticide credit per kilogram dichloropropene;
positive avoided-credit inputs retain their positive sign.

Warnings disclose differences in the generic production/formulation mixture,
technology, toxicity inventory, geography and market freight/consequential model.
No ingredient-specific toxicity equivalence is asserted and no application
emissions are added. Other foreground exchanges, including separately listed
chemical emissions, remain intact. The existing information-loss policy applies.
Later releases, cut-off and unreviewed patch profiles remain blocked. All four
quantities are independently collected and audited, bringing the inspected
single-product-credit total to 25 pairs across four identities. The UVEK 2026
bridge preserves the proxy; no reverse reconstruction is inferred.

Nutritional feed substitution: approved omissions and independent product supply
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The subsequent approval omits five GLO/kg nutritional linking references only
in consequential ecoinvent 3.6--3.12: DDGS, fodder yeast, grass fibre, sugar beet
pulp and sweet sorghum grain. Their 35 source/profile pairs credit the generic
gross-feed-energy market (MJ) and, where present, the 100%-crude-protein feed
market (kg). These are substitution quantities per kilogram of source, not a
validated mass conversion to a particular UVEK crop.

.. list-table::
   :header-rows: 1
   :widths: 45 30 25

   * - Source coproduct
     - Gross energy credit (MJ/kg)
     - Crude-protein credit (kg/kg)
   * - DDGS
     - 18.009
     - 0.38
   * - Fodder yeast
     - 18.814
     - 0.4876
   * - Grass fibre
     - 17.954
     - None
   * - Sugar beet pulp
     - 4.23
     - 0.0225
   * - Sweet sorghum grain
     - 16.68924
     - 0.34542

All inspected links have production -1 kg, no direct biosphere exchanges and
only negative nutritional-market inputs. UVEK crop/product-mass proxies do not
establish equivalent gross-energy and protein substitution. Conversion now
omits the external link and continues with ``migration.technosphere_omission``
warnings about excluded nutritional contributions and their associated credits.
Score effects remain method-dependent and unquantified. Grass-fibre warnings
identify its energy-only basis. No arbitrary single-nutrient or crop-mass scaling
is introduced, and the omission does not establish zero impact for feed production.

Independently inspected similar cases auto-approve the five GLO coproduct markets
in the same seven consequential versions: another 35 source/profile pairs.
Each has production -1 kg and supplies exactly -1 kg of its own linking activity,
with no direct biosphere releases. DDGS, yeast and sorghum markets add train/lorry
freight; grass fibre adds lorry freight; beet pulp has none. Warnings disclose
excluded additional market freight only where present. The complete batch
therefore covers 70 consequential omission decisions across ten identities.

The same five markets have production +1 kg in cut-off backgrounds. Their
physical-product supply requires independent review; the omission decision does
not apply to it. DDGS, grass fibre, sweet-sorghum grain, fodder yeast and sugar-beet pulp now have
35 separately approved cut-off market compositions, described below.
Missing patch/profile decisions also fail closed. Reinspection preserves
independent product approvals rather than replacing them with omissions.

Full original exchanges, including quantity, uncertainty, formula and caller
metadata, remain in loss reports. Production, matching foreground suppliers,
separate transport and other inputs/emissions are preserved. Approved omissions
warn despite generic deletion/information-loss error policies; route permissions
still apply and unrelated errors roll back conversion. UVEK 2026 retains these
omissions and reports. Reverse mappings remain independent.

Normalized nutrient references: approved omissions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Inspection of the related references found 95 further unsupported nutritional
substitutions: 45 gross-feed-energy references measured in MJ and 50 feed
references measured in kilograms of 100% crude protein. Their former heuristic
targets supplied industrial heat, fuel, crop mass, crude oil or an unspecified
whey protein concentrate. Unit factors of one do not establish equivalent
nutritional function. Thermal energy is not nutritional gross energy; a kilogram
of crop or unspecified concentrate is not a kilogram of 100% crude protein.

Standing approval for similar cases omits these exact references in 692 inspected
source/profile pairs. All have positive production references: this decision
addresses unsupported nutritional equivalence, rather than a sign inversion.
Warnings disclose excluded nutritional supply contributions and any associated
substitution credits for positive, negative and zero incoming quantities. Effects
on LCIA scores remain method-dependent and unquantified; omission does not imply
zero impact. Complete original exchanges remain in loss reports, and matching
foreground suppliers, production and separate inputs/emissions are preserved.

The fixed source/prior-target/profile whitelist is packaged as
``data/export/uvek_feed_nutrient_omission_review.json``. Runtime matching uses exact
identities and profiles, without keyword-based deletion or missing-profile
fallback. Ordinary coproduct-mass supplies, including the five cut-off markets
above, remain outside this approval. Reverse conversion remains independent.

Refrigerant venting: approved physical release recipes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Five previously blocked GLO/kg references describe venting of used R12, R134a
and R600a, including historical hyphenated names. Their 42 exact source/profile
pairs in ecoinvent 3.6--3.12 cut-off/consequential release one kilogram of the
identified refrigerant to air per kilogram vented, with no active processing
inputs. Their former R134a-manufacturing target represented a different function
and, for R12/R600a, a different chemical.

The approved model creates a foreground venting-service activity with positive
production and the correct one-to-one biosphere release. A factor of -1 converts
the source negative waste-reference coordinate: a -1 kg treatment input becomes
+1 kg of service and +1 kg of emission. An avoided-venting exchange retains the
opposite effect. This is a physical mass-balance model, without a claim of equal
LCIA scores. Consumer uncertainty, formulas, metadata and complete original
exchanges remain in immutable change reports. Matching foreground suppliers and
their production/emissions remain protected. Export the complete returned
inventory, including generated activities.

Six recipes distinguish the three chemicals with and without the additional
3.12 waste-disposal inventory indicator. Exact profile targets select the
appropriate variant. Preserve that indicator when supported by the target
biosphere; otherwise emit ``migration.recipe_input_omission`` and retain the
excluded coefficient in the loss report, even under generic deletion/loss error
policies. The air release is never optional.

Brightway Excel preserves the supported legacy indicator. SimaPro currently has
no reviewed wire representation for ``Non-hazardous waste disposed``; exporting
a 3.12-derived legacy-biosphere helper to that format therefore fails rather
than silently dropping it. This does not block background migration. Older
venting profiles without that indicator and the native UVEK 2026 path can be
exported to SimaPro after supplying the required production categories, as for
other foreground datasets.

Exact recipe-local bindings name the native UVEK 2026 air flows for CFC-12,
HFC-134a and isobutane. Isobutane's urban-air source compartment becomes native
unspecified air with ``migration.recipe_biosphere_approximation`` warnings and
loss records. These explicit approved approximations warn under strict loss
policy; unavailable declared emission targets fail even under permissive policy.
No global biosphere crosswalk or reverse rule is inferred from these recipes.

``data/export/uvek_refrigerant_venting_review.json`` binds source identities,
prior targets and all inspected inventory fingerprints. Authoring rejects
changed inventory contents, signs or profile coverage; runtime rejects missing
profiles and generated-activity collisions. Unrelated errors roll back the whole
conversion. Refrigerant markets, reclamation, destruction and equipment disposal
have different treatment boundaries and remain outside this approval.

Refrigerant final disposal: approved incineration and leakage proxies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Five GLO/kg final-disposal references for R12, R134a and R600a, including older
hyphenated names, have independent approval in 42 exact source/profile pairs.
Four formerly pointed to R134a manufacturing; the older R600a reference pointed
to cable treatment. The latter already had a coordinate sign correction, which
did not establish a matching disposal function. It now belongs to this recipe
cohort rather than the background-supplier sign-correction count above.

Per kilogram accepted, each generated positive-reference disposal service
contains 0.999 kg of ``Disposal, hazardous waste, 25% water, to hazardous waste
incineration`` (CH, ``bafu-165198``), plus 0.001 kg of the identified refrigerant
released directly to air. This preserves the inspected source leakage assumption;
it does not establish actual destruction efficiency. The original source already
uses average hazardous-waste incineration. The approved substitution replaces
its regional supplier mix with the Swiss UVEK provider, without moisture or
heating-value scaling.

Six recipes distinguish the three chemicals with and without the 3.12 waste
indicator. Exact profile decisions apply factor -1 to consumer exchanges, retaining
avoided-disposal signs, uncertainty, formulas and full original exchanges. The
source/profile whitelist and exchange-record fingerprints are retained in
``data/export/uvek_refrigerant_disposal_review.json``. Missing or changed profile
evidence requires review. Reinspection retains the historical factor of the older
R600a proxy; existing venting rules and independent reverse mappings are unchanged.

``migration.technosphere_information_loss`` warnings and loss records disclose
the incineration proxy's geography, average waste composition, plant operation,
emissions control, residues, energy recovery, allocation and background differences.
Refrigerant-specific destruction performance, fluorine/chlorine balances and LCIA
equivalence are not established or quantified. Strict information-loss policy
blocks this proxy; accepting losses permits conversion with warnings, including
under ``ALLOW``. Other unrelated failures still roll back the whole conversion.

The venting cohort's approved native biosphere bindings, isobutane compartment
warning and optional indicator handling also apply to these recipes. Source
foreground emissions and matching local suppliers are preserved. Native UVEK 2026
release conversion also migrates the generated helper's incineration supplier.
Collection, reclamation, refrigerant treatment markets and their freight remain
separate functions. The documented legacy SimaPro indicator limitation remains.

Produced-water discharge: direct service proxies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Four GLO/kg source identities cover onshore petroleum/natural-gas produced-water
discharge and offshore petroleum produced-water discharge, each as a direct
service and a market. All 56 inspected ecoinvent 3.6--3.12 cut-off/consequential
source/profile pairs use production -1 kg. Every market supplies exactly -1 kg
of its corresponding discharge service, without additional active inputs or
emissions. UVEK's ``Discharge, produced water, onshore`` (GLO) and
``Discharge, produced water, offshore`` (OCE) have production +1 kg and native
``PRODUCT_FLOW`` output references representing discharge services.

The approved direct mappings use factor -1: an incoming -1 kg becomes +1 kg of
the corresponding UVEK service. Positive avoided-discharge inputs retain their
opposite direction. Preserve water mass without concentration-based scaling;
no foreground helper, additional treatment plant or recovery credit is added.
Only the enumerated exact profiles are approved. Missing or patch profiles
remain blocked. Source and target inventory fingerprints are retained in
``data/export/uvek_produced_water_review.json`` and checked during audit replay.

These proxies substitute UVEK's emission inventories, with policy-controlled
``migration.technosphere_information_loss`` warnings and exchange-path loss
records. The differences can materially affect toxicity and other scores:

* Ecoinvent 3.10--3.12 dissolved organic carbon is 0.02003 kg/kg, compared with
  UVEK's 0.000295 kg/kg. No DOC-based impact multiplier is applied.
* UVEK uses chromium VI where ecoinvent 3.9--3.12 uses chromium III.
* Older ecoinvent 3.6--3.8 air and water-balance exchanges are absent from these
  UVEK inventories.
* UVEK extrapolates offshore measurements to onshore surface-water discharge.
  Its onshore chloride amount is 0.071775 kg/kg, compared with 1.2e-7 kg/kg in
  ecoinvent 3.10--3.12 onshore discharge.

LCIA equivalence is not established or quantified. Both UVEK suppliers model
discharge after pre-treatment; their inventories contain 55 active water-emission
exchanges and no technosphere inputs. Existing foreground suppliers, production
and separately listed emissions remain intact. Reverse mappings are independent.

Antifouling-paint emissions: unsupported marine releases
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The approved omission covers five exact kilogram source identities: the GLO
``market for antifouling paint emissions`` and the PE/RoW direct Cu-based and
Sn-based emission services. All 70 inspected source/profile pairs across
ecoinvent 3.6--3.12 cut-off/consequential have production -1 kg. Every market
supplies 50% copper-based and 50% tin-based services, through PE/RoW suppliers,
with no other active exchanges. Direct services contain marine biosphere
releases, with an additional disposal indicator in 3.12.

The former market target represented uranium milling. The four direct targets
represented lorry tyre wear to air, groundwater and soil. Their previously
verified reference signs established coordinates only; these are scientifically
inappropriate counterparts for marine paint releases. No dedicated UVEK
antifouling-emission supplier was established. The audit retains their original
targets, factors and sign-review statuses, and binds replay to sparse inventory
fingerprints in ``data/export/uvek_antifouling_omission_review.json``.

Conversion omits these five exact external exchange identities with
``migration.technosphere_omission`` warnings, including when ordinary deletion
or information-loss policies are set to error. Each loss record retains the
complete original exchange, amount, sign, uncertainty, formula and caller
metadata. Duplicate and zero-valued exchanges receive individual records.
No helper or zero-impact emission service is introduced.

The associated marine metal and organotin releases are absent from the target
dataset. Ordinary releases may therefore have understated ecotoxicity impacts;
positive avoided-release inputs lose their associated credits. Score effects
are method-dependent and unquantified. Paint-manufacturing inputs, matching
foreground suppliers, production and separately listed foreground emissions
remain intact. Other mapping failures roll back all changes. The UVEK 2026
bridge retains the omissions and their audit; reverse rules are independent.

R134a reclamation: processing, recovery credits and cut-off products
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The reclamation approval covers three exact source identities and 21 profiles:
14 waste-treatment service profiles and seven positive-reference recovered-product
profiles under two historical product names. All previously pointed to primary
R134a manufacturing, which does not preserve their allocation boundaries.

Per kilogram of used refrigerant accepted, the positive-reference service helper
contains 0.0466666666666667 kWh of GLO medium-voltage grid electricity, 0.1 kg of
the approved CH average hazardous-waste incineration provider, and 0.001 kg of
direct HFC-134a air release. The incineration input uses the background provider
directly, preserving the separately recorded leakage quantity. Cut-off profiles
have no recovered-product substitution credit. Consequential profiles additionally
retain -0.899 kg of RER primary R134a supply as a documented proxy for the credited
market, whose inspected primary-production mix is 25% RER and 75% RoW.

Separate service recipes preserve these system-model differences. Their consumer
factor of -1 converts the negative source waste-reference coordinate while
retaining the credit's own negative sign. Net consequential results can legitimately
be negative after recovery credits; no absolute-value correction is applied.
Electricity, treatment, geography, allocation, credited-market transport and marginal
supply differences warn under the existing information-loss policy. Equivalent
LCIA scores are not established or quantified.

The seven inspected cut-off recovered-product suppliers have one +1 kg production
exchange and no active processing inputs or biosphere exchanges. They map to a
production-only foreground supplier with factor +1, preserving material quantities,
uncertainty and formulas. Processing burdens remain on the separate treatment
service. ``migration.zero_burden_supplier`` warnings identify this observed
allocation boundary, without recording an information loss or implying zero impact
for reclamation, other recycled products or uninspected source profiles. Source
inventories and all seven credited primary markets are fingerprint-bound in
``data/export/uvek_r134a_reclamation_review.json``.

Helpers use the inspected stored, allocated inventory amounts. Source background
formulas and activity parameters are not reconstructed; formulas on the caller's
exchanges remain preserved. Inactive zero-amount source rows do not introduce
additional processing burdens into the recovered-product supplier.

Empty recipes require ``zero_burden: true``, an explicit reason and evidence, and
verified exact positive-reference cut-off profile decisions. Missing providers,
consequential product profiles and unreviewed patch versions cannot inherit zero
burdens. Matching nonempty foreground suppliers retain their own inventories.
Unrelated failures roll back processing and product-supplier changes together.
The complete returned inventory, including both service and recovered-product
helpers, must be included in scoring and exported. These production-only helpers
round-trip through Brightway Excel and SimaPro; no disposal inventory indicator is
introduced into the reclamation model.

Industrial gas combustion and energy recovery
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The approved industrial-gas batch covers 69 blast-furnace-gas and 34 coke-oven-gas
power-plant services, each in the seven inspected ecoinvent 3.6--3.12 consequential
profiles: 103 identities and 721 exact source/profile decisions. Source reference
production is -1 MJ. UVEK combustion has a positive per-MJ reference and native
product-flow metadata; generated treatment helpers explicitly preserve its
physical combustion function and the independently reviewed energy credits.

Each helper contains 1 MJ of the matching RER UVEK gas-combustion process and the
inspected negative electricity/heat credit quantities, converted through existing
UVEK energy correspondences. Shared target suppliers are summed without changing
their physical units. Every source supplies approximately 0.145 MJ heat credit;
electricity ranges from 0.0333333 to 0.1477778 kWh/MJ for blast-furnace gas and
0.0333333 to 0.1347222 kWh/MJ for coke-oven gas. Regional supplier mixes can differ.
For example, German blast-furnace-gas treatment retains -0.1013889 kWh electricity
and -0.145 MJ heat per MJ accepted.

The UVEK coke-oven burner includes a deterministic 1 MJ upstream coke-oven-gas
production input. The helper subtracts that exact supplier quantity to isolate
waste-gas combustion. This is a boundary correction, not another recovered-fuel
credit. The 2026 burner was independently checked to retain the 1 MJ fuel input.
The blast-furnace burner has no such upstream fuel input and receives no fuel
correction. No additional direct emission recipe is added on top of combustion.

The helpers are screening proxies: regional combustion, cooling, infrastructure,
emissions, internal uncertainty and parameterization differ. Existing UVEK energy
links approximate consequential marginal supplies. Information-loss warnings
disclose these limits. Caller-owned signs, uncertainty, formulas, local suppliers
and separate emissions remain intact. Net impacts can legitimately be negative;
do not take their absolute value. Export and score the complete returned inventory,
including generated helpers. Only exact consequential profiles are approved;
cut-off product references, gas markets and missing/patch profiles remain separate.

Sparse source fingerprints, derived UVEK credit inputs, 140 energy-correspondence
bindings and three primitive fingerprints are retained in
``data/export/uvek_industrial_gas_review.json``. Identical recipes are reused across
profiles: the 103 mappings add 117 definitions, instantiated only on demand.
No complete source inventories are published. Reverse resources are independent.

Related nonbiogenic industrial wastewater residues
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Standing approval for unsupported CRT wastewater residues also covers the
independently inspected matching LCD, solar-collector and other nonbiogenic
industrial wastewater chains. This batch enumerates 198 exact treatment/market
identities and 1,520 source/profile inventories. All 760 direct treatment
inventories document 0% biogenic carbon. Markets supply 100% treatment of their
own residue plus freight, without active direct biosphere exchanges. Most
identities occur in ecoinvent 3.9--3.12, cut-off and consequential; sixteen
PET-pelletising/polyester-recycling identities occur only in 3.11--3.12.

The industrial chains include LCD/backlight, tube-collector, PV-cell, wafer,
liquid-crystal, glass, concrete, black-chrome-coating, ammonium-paratungstate,
PET-pelletising and polyester-recycling production. No dedicated complete UVEK
counterpart for these residue-specific disposal routes was established.
Manufacturing, mechanical screen recycling, municipal/paper or other-industry
sludge, and standalone landfill-operation components do not establish the same
industrial sludge/ash fate model. Earlier sign verification of related direct
services does not approve those scientific correspondences.

The exact external exchanges are omitted with warnings and full original-exchange
loss records. Excluded contributions include treatment operation, metal/leachate
releases, downstream residue handling, market freight and associated recovered
energy/fertilizer credits. Score effects are method-dependent and unquantified;
0% biogenic carbon is not a zero-impact claim. Preserve foreground suppliers,
production and separately listed emissions. No moisture scaling, generic keyword
omission or helper is introduced. The 2026 bridge preserves the omissions and
reverse resources are independent. Exact source fingerprints, prior targets and
the standing approval basis are retained in
``data/export/uvek_wwt_residue_omission_review.json``.

A further 28 exact pending identities, covering 184 profiles, use the same
inspected nonbiogenic omission policy for boiler condensate, mineral-oil-storage
rainwater, heat-carrier liquid and wastewater/landfill-treatment residues.
Their direct-treatment comments explicitly report 0% biogenic carbon; markets
supply 100% treatment of the same residue plus freight. The combined review
enumerates 226 identities and 1,704 source/profile inventories. Former targets
include unrelated manufacturing, fertilizer and material supplies. Full
contributions and credits are omitted with warnings and original-exchange loss
records; separately listed foreground exchanges remain intact.

Gas-market boundaries and composed market helpers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Two independently inspected gas markets have 28 exact-profile decisions.
Consequential markets mix the previously approved gas-treatment helpers at
their inspected country shares. Negative inputs to negative-reference
treatment suppliers represent demand for treatment. Their combustion and
recovered electricity/heat credits remain in the component helpers.

Cut-off blast-furnace gas instead supplies one MJ of its allocated coproduct,
which has only a positive production exchange and no active inputs or releases.
The market has no other active inputs or releases. Its helper retains the
caller quantity with an explicit zero-burden allocation warning. This is an
inspected supply boundary, not missing data. Cut-off coke-oven gas supplies a
nonempty allocated product and uses one MJ of UVEK coke-oven-gas production,
with a warning for allocation and producer-mix differences. Unreviewed or
patch profiles cannot inherit either boundary.

Another 372 markets, covering 2,929 approved source/profile inventories, are composed
from their existing resolved component correspondences. Each inspected market
has a unit-magnitude reference, active technosphere inputs only, and no direct
biosphere or unsupported exchange types. Helpers retain the supply mix and
freight coefficients without renormalizing them; component reference-sign
factors distinguish treatment demand from recovered-product credits. No
component omission or missing-provider fallback is used. Explicitly inspected
production-only cut-off stocks retain their source allocation boundary as
described below; this does not assign zero to actual treatment or recycling.

Composition inherits the existing component mappings' heuristic, technology,
geography and allocation approximations; resolved signs do not establish
scientific or LCIA equivalence. Internal component uncertainty and formulas
are not reconstructed. Caller amounts, signs, uncertainty and formulas remain
preserved, with full original-exchange change records. Information-loss warnings
require an explicit policy; all generated helpers must be scored and exported.
Exact inventory hashes and component-rule/recipe hashes guard regeneration in
``data/export/uvek_gas_market_review.json`` and
``data/export/uvek_market_composition_review.json``. Reverse counterparts require
independent approval; the composition does not make unresolved UVEK suppliers
reversible.

The same composition method covers 106 processing references across
817 approved source/profile inventories. These sources have active
technosphere inputs only, no direct biosphere or unsupported exchanges, and a
single self-linked production reference of magnitude one. Examples include
wood-residue handling, dismantling and material sorting. The helpers preserve
all inspected processing inputs, freight and recovered-product credits through
existing component correspondences, with each provider's sign factor applied
independently. Negative product credits remain negative; negative inputs to
negative waste-service references represent positive treatment demand.

No new process primitive, component omission or inferred empty supplier is substituted. Component
technology, composition, geography and allocation approximations remain explicit;
internal uncertainty and formulas are not reconstructed. Net scores can remain
negative when recovery credits exceed processing burdens. Caller quantities,
uncertainty, formulas, foreground production and separate emissions are preserved.
``data/export/uvek_processing_composition_review.json`` retains exact source and
component-rule/recipe fingerprints. Only the enumerated profiles are approved.

Physical cut-off feed coproducts
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

DDGS, grass fibre and sweet-sorghum grain now have separately reviewed physical
cut-off supplies. Nine source suppliers across 63 exact cut-off profiles use
six feedstock/technology-specific UVEK product inventories. Five DDGS suppliers
formerly pointed to ethanol output from the same or another crop; they now use
the matching corn, rye or potato DDGS coproduct. Grass fibres retain their
separate digestion and fermentation routes. Both sorghum inventories document
9.1% grain moisture. Every inspected source and target has a single positive
one-kg product reference. Factor +1 preserves signed quantities, supported
uncertainty and formulas. No physical coproduct is treated as ethanol, waste
treatment or a generic nutrient-market reference.

The three market helpers retain all 21 inspected cut-off supply and freight
compositions without omission, normalization or nutritional scaling. Their
21 consequential counterparts retain the previously approved nutritional
omissions, original review references and loss warnings exactly. The three
market identities therefore leave the pending queue. The separate beet-pulp/
whey-yeast review below resolves the other 14 cut-off market profiles while
preserving their consequential omissions. Unknown and patch profiles remain blocked.

The product proxies inherit allocation, energy, agricultural inputs, geography,
water/emissions and background differences. Ecoinvent describes DDGS with 90%
dry matter; UVEK states a wet-basis kg without an independently quantified
identical moisture fraction. No dry-matter conversion, equivalent feed energy,
protein value, insulation performance or equal LCIA score is inferred. Exact
inventories and boundary comments for 111 source/target/market records are
bound in ``data/export/uvek_cutoff_feed_product_review.json``. Every generated
market helper must be included in scoring and export. Six independently
reviewed reverse physical-product correspondences preserve one kg and its sign
through the matching ecoinvent 3.12 cut-off coproduct. Five retain the region;
Swiss potato DDGS uses the inspected RoW process with a geography warning. They
do not reconstruct original market mixes or reverse the nutritional omissions.
See :doc:`../workflows/uvek-export` for the independent review and limitations.

Beet-pulp and whey-yeast physical products
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Seven further coproduct suppliers cover 35 observed cut-off profiles. The older
``beet sugar production`` names occur through 3.10; the ``sugar beet processing``
names occur in 3.11--3.12. They retain the existing Swiss sugar-refinery pulp
supplier. Source and target refinery-pulp comments specify 25.6% dry matter.
Beet-ethanol pulp retains the existing same-product refinery-pulp correspondence
with an explicit primary-process/allocation warning: the source separates pulp
during beet extraction, but its fermentation route differs and identical
moisture is not independently quantified. Whey-fermentation legacy comments
explicitly identify the fodder-yeast coproduct as yeast paste alongside ethanol
and protein concentrate; both source and target are allocated physical products.

Both markets now have 14 independent physical cut-off compositions, retaining
every original supply share and the yeast market's freight. Their 14
consequential nutrient omissions retain the original review records and warnings.
Together with DDGS, grass fibre and sorghum, all five cut-off markets have 35
approved physical profiles; all 70 consequential nutritional omissions across
links and markets remain unchanged. No nutritional equivalence, arbitrary
moisture conversion or equal LCIA score is inferred. Allocation, old Swiss
backgrounds, Irish whey technology, energy, upstream collection, crop inputs,
water/emissions and geography remain explicit proxies. Missing and patch
profiles stay blocked.

The review now binds 176 inventory/comment records for 16 suppliers, five
markets and eight UVEK products. Thirty-seven new supplier/product records also
bind the formal ``product_information`` metadata used for the product boundary.
Every earlier product, market recipe and reverse rule remains unchanged.
Two separately inspected reverse correspondences match Swiss refinery pulp to
ecoinvent 3.12 cut-off Swiss sugar-beet-processing pulp, and Swiss whey-yeast
paste to its Swiss fodder-yeast reference. Both preserve kg with factor +1 and
proxy warnings; neither reconstructs a market mixture or omitted nutrient credit.

Physical cut-off ground-granulated blast-furnace slag
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Four physical product suppliers, in IN, RoW, US and ZA, have 28 independently
inspected exact cut-off profiles. Their positive one-kg references differ from
the negative-reference raw-slag treatment functions. Each physical product now
uses one kg of the UVEK ``no burdens`` operating product plus the source's exact
raw-slag quantity through its already reviewed cut-off supply-market helper.
Those market helpers retain the source's zero-production-burden Recycled
Content stock and original freight. The older GLO raw market is used through
3.9; the observed regional markets are used from 3.10. No transport coefficient
or raw-slag quantity is rounded or normalized.

The former ``with burdens`` target adds economically allocated slag production
and extra haulage. Independent comparison of both UVEK inventories finds the
same operating and direct-emission coefficients, with only raw allocated slag
and its extra haulage added in the with-burdens variant. The no-burdens variant
still contains processing energy, water, emissions, infrastructure and disposal.
Its operating technology approximates the four regional ecoinvent processes;
heat/electricity mixes, yield, moisture, granulometry, transport and background
models differ. No cement-strength, material-performance or LCIA equivalence is
claimed. Caller signs, supported uncertainty/formulas, foreground suppliers and
separate emissions are preserved; internal uncertainty/formulas are not rebuilt.

The review binds 47 independently re-read inventory/comment/product-information
records: 28 products, ten raw markets, seven empty positive cut-off stocks and
both UVEK variants. Two supplementary native-2026 inventory observations
confirm positive one-kg references, no raw-slag supply in the no-burdens process,
and 1.0014 kg raw slag in the with-burdens process. Every prior raw-market and
stock rule/recipe is unchanged.
Only the seven observed cut-off versions receive product helpers. Unknown
patches and consequential product fallbacks remain blocked. The 38 inspected
negative-reference slag-treatment profiles remain unapproved; their granulated
slag or supplementary-cementitious-material recovery credits require a separate
boundary review. All returned foreground helpers must be scored and exported.

An independently inspected reverse helper represents UVEK's no-burdens product
using the US cut-off product, subtracting its exact raw-slag input. The UVEK
with-burdens variant remains unsupported in reverse. Neither direction infers
a treatment or recovery-credit inverse or reconstructs a source geography. Complete
helper-hierarchy reverse checks retain existing unsupported truck/train links
and roll back transactionally; the standalone operating-product correspondence
does not claim a complete inverse for the original inventory.

Feedstock-specific esterquat credits
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Two esterquat supply markets have 28 independently inspected exact-profile
compositions. Their previous all-tallow target did not represent the observed
feedstock mix. Cut-off supply contains 0.883720930232558 kg of coconut/palm-kernel
esterquat and 0.116279069767442 kg of tallow esterquat per kg. Consequential
supply is entirely coconut/palm-kernel esterquat. Each helper retains the
original regional freight coefficients, including unrounded shares and repeated
rows, through the existing component mappings. No component is omitted or
renormalized. Signed recovery credits, supported uncertainty and formulas are
preserved. A consequential esterquat credit therefore cannot spuriously cancel
a tallow-based operating primitive.

Both source and UVEK production providers have positive one-kg product
references. The inspected coconut/palm-kernel processes consume 0.3 kg of each
oil; the tallow processes consume approximately 0.6 kg of tallow. Forty-two
exact source-provider inventories and two UVEK production inventories are bound
in ``data/export/uvek_esterquat_market_review.json``. Changed or missing provider
inventories and native product roles require another review. Market inventories
and exact component-rule bindings are retained in the market-composition review.
These helpers inherit technology, allocation, feedstock, regional supply and
freight approximations; equal LCIA scores and exact marginal production are not
established. Only enumerated profiles are approved. The negative-reference
tallow-treatment services remain pending, and neither UVEK production endpoint
has an independently approved reverse correspondence. No inverse is added.

Refinery-gas flaring
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Two exact GLO/MJ refinery-flare references cover the direct flaring service and
its market across 28 inspected source/profile inventories. UVEK's matching
``Refinery gas, burned in flare`` process has positive reference production and
a native product output representing the combustion service. Factor -1 converts
the source negative waste reference to that positive service coordinate while
preserving the caller's energy quantity, uncertainty and formulas. No helper or
heating-value conversion is used. Every inspected market supplies only the same
flaring service, without freight or additional emissions.

Both inventories cover direct combustion emissions and exclude flare
infrastructure, purchased fuel, upstream fuel production and energy-recovery
credits. UVEK substitutes its own emission inventory: ten active biosphere
exchanges versus nine in each inspected source. Pollutant coverage, emission
factors, speciation and plant/geographic representation can differ; climate and
toxicity equivalence are not established. Separately listed foreground emissions
remain unchanged. Warnings disclose these limitations. Exact source and target
fingerprints are recorded in ``data/export/uvek_refinery_flare_review.json``;
missing and patch profiles remain blocked. Natural-gas flares, turbines and fuel
production are outside this approval. A separate independent reverse review now
matches the UVEK flare to the ecoinvent 3.12 cut-off direct treatment with factor
-1 and emission-coverage warnings. Market-only supply is collapsed to that direct
service; no general inverse is inferred.

Sweet-natural-gas production flaring
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The direct GLO/MJ sweet-gas flare and its market have 16 approved exact profiles:
ecoinvent 3.9--3.12 cut-off and consequential. They use UVEK's dedicated
``Natural gas, sweet, burned in production flare`` service, whose reference is
one cubic metre. The inspected source withdraws 1/36 standard cubic metre of
natural gas per MJ; UVEK withdraws one standard cubic metre and releases 36 MJ
of waste heat per reference cubic metre. These inventories independently
establish the 36 MJ/m3 reference conversion. Factor -1/36 preserves the incoming
energy quantity while changing the negative waste-reference coordinate to the
positive service coordinate. This replaces the former gas-turbine suggestion.
The market supplies only -1 MJ of its matching flare, without freight or other
active exchanges. No helper or purchased-fuel correction is required.

Modern source comments assign CH4, NOx and SO2 to the main oil/gas production
activities. Neither inspected flare contains those flows, and both retain the
flared gas's resource withdrawal. That withdrawal is not purchased-fuel
production and must not be removed. Both flare inventories lack technosphere
suppliers, infrastructure and energy credits. Full parent production inventories
are not claimed equivalent; separately recorded foreground emissions are retained.

UVEK substitutes nine active biosphere exchanges for the source's seven/eight.
Emission coefficients and coverage differ: source oxygen balancing is absent
from UVEK, while UVEK includes nitrous oxide and waste heat. CO2 and NMVOC
coefficients also differ slightly. Norwegian observations, global extrapolation,
time and volume-reference conventions remain screening approximations, with
explicit warnings and no claim of equal LCIA scores. Quantities, supported
uncertainty and formulas scale together. Native UVEK 2026 is reached through its
explicit release bridge, including the ``normal cubic meter`` reference alias.

The 12 older sweet-gas profiles in 3.6--3.8 remain pending because they retain
methane and NOx inside the flare. All sour-gas profiles remain pending, including
their SO2 emissions. Unknown and patch profiles cannot inherit this approval.
``data/export/uvek_natural_gas_flare_review.json`` binds the source and target
inventory bytes and activity comments. The exact UVEK service has a separately
inspected reverse correspondence to ecoinvent 3.12 cut-off with factor -36;
no other inverse is inferred.

Legacy electric-arc-furnace waste landfill services
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Three exact CH/kg services in ecoinvent 3.6 cut-off and consequential describe
residual-material landfilling of alloyed EAF dust, unalloyed EAF dust and unalloyed
EAF slag. The former allocated slag-production suppliers are replaced by UVEK's
dedicated disposal services for those waste types. Source reference production
is -1 kg; the UVEK services have +1 kg production and independently verified
native waste-input references. Factor -1 preserves the incoming mass and
treatment-demand direction. These direct mappings need no helper.

Dust composition differs: the source documents H2O 105410 ppm (10.541%), while
the UVEK names specify 15.4% water. Source slag moisture is unavailable; UVEK
specifies 0%. The original kg is preserved without moisture or dry-mass scaling.
Both alloyed-dust processes include cement stabilization and hydrated-cement
disposal; UVEK adds road and rail freight absent from the source service.
Foreground transport remains unchanged, so callers must assess potential overlap.
The target substitutes its own metal concentrations, speciation, leaching,
short- and long-term releases, infrastructure and background inventories.
Toxicity and other LCIA equivalence are not established. Warnings disclose these
limitations; uncertainty, formulas, foreground suppliers and separate emissions
are preserved. ``data/export/uvek_eaf_landfill_review.json`` records six exact
source/profile fingerprints and three target fingerprints. Other versions,
patch profiles, alloy grades, waste types, markets and reverse routes receive
no approval from this batch.

Dedicated LCD treatment and freight modes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Five LCD service references cover 50 inspected source/profile inventories:
CH/RoW manual dismantling, GLO mechanical treatment and CH/RoW municipal
incineration of LCD shredder residues. Each uses its dedicated UVEK LCD service.
Source reference production is -1 kg; target +1 kg production represents a
native waste-input service. Factor -1 preserves incoming wet mass, uncertainty
and formulas. Two related manual-dismantling suppliers are corrected alongside
three originally pending service identities.

The mechanical proxy includes manual dismantling before shredding/separation.
Ecoinvent 3.6--3.9 describes both steps; 3.10--3.12 describes a four-step
mechanical procedure without a separate manual-service supplier. Those newer
profiles use the previously approved broader display-treatment boundary and
warn about added manual burdens and overlap with separately modelled manual
treatment. Foreground exchanges remain unchanged.

The incineration proxy describes LCD shredder residues with 1.95% rubber and
predominantly electronic plastics. Composition, metal speciation, leaching,
emission controls, fly-ash extraction, recovery, allocation and consequential
credits can differ; the source-specific credit model is not reconstructed.
Wet mass is preserved without moisture, heating-value or device-count scaling.
Toxicity and other LCIA equivalence are not established. Whole LCD modules,
glass and CRT coatings retain separate correspondences. Exact source and target
fingerprints are retained in ``data/export/uvek_lcd_treatment_review.json``.
Missing and patch profiles remain blocked; reverse routes remain independently
unresolved rather than inferred.

The LCD market helper retains 23% manual and 77% mechanical treatment across
14 profiles, together with all four freight quantities. Its freight review
also identified three existing carrier mappings that substituted lorry
transport for barge or rail. Those three mappings now use whole-life RER barge
or rail services across 12 inspected source profiles. Both references are
positive one-tkm services, so factor +1 preserves signed demand and avoided
transport credits. Global fleets are approximated by RER; country weights,
loading, energy, infrastructure, emissions and fleet technology differ and warn.

All 71 affected composed inventories were re-reviewed transitively across 256
source/profile inventories. Raw source quantities, treatment shares and recovery
credits are preserved. Source fingerprints, new component bindings and recipe
dependencies were revalidated; superseded unused recipes were removed. The
market and processing review files retain the re-review history, linked to
``data/export/uvek_modal_transport_review.json``. Existing stale-binding guards
remain enforced.

Dedicated regional and legacy vehicle disposal
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Seven additional identities use dedicated waste services across 32 inspected
source/profile inventories. Three RoW legacy EAF dust/slag references, present
only in 3.6, use the corresponding Swiss services: alloyed dust, unalloyed dust
and unalloyed slag remain distinct. Source dust documents 10.541% water versus
15.4% in UVEK; slag water content is unavailable versus the target's 0% label.
The RoW PET sanitary-landfill service, inspected across 3.6--3.12, uses the
published CH PET endpoint, retaining its legacy ``xx`` name. Both specify
0.2% water. This replaces fleece manufacture rather than introducing a new
plastic-treatment approximation.

Three legacy vehicle references, present only in 3.6 and 3.7, use dedicated RER
scooter-glider shredding, scooter-powertrain manual dismantling and passenger-car
engine shredding. References are per kg; the documented example vehicle or
component weights do not scale demand. Native UVEK waste inputs and positive
self-linked kg production were checked independently. All seven conversions
apply factor -1 without moisture or dry-mass scaling. Regional technology,
freight, cement, allocation, recovery credits, gas capture, leaching and release
models differ and warn; source-specific recovery credits are not reconstructed.
Foreground suppliers, separate emissions and separately listed credits remain
unchanged. Exact-profile inventory guards reject stale or uninspected inputs.
None of these seven reverse correspondences is inferred or newly enabled.

Five newly complete markets add 16 inspected profiles to the composed-market
cohort: legacy alloyed EAF dust, unalloyed slag and the three vehicle components.
Their helpers preserve all source treatment shares and freight through frozen,
resolved directional component mappings, without renormalization or omission.
``data/export/uvek_dedicated_disposal_review.json`` records the seven direct
approvals; the market-composition review records the five derived assemblies.

Spent Formox catalyst carrier
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A separate review adds the RER spent Formox carrier landfill across six exact
3.10--3.12 cut-off/consequential profiles. UVEK calls the dedicated CH process
``Disposal, catalyst base CH2O production, 0% water, to residual material landfill``.
Both descriptions identify the matrix remaining after metal recycling from
formaldehyde-production catalysts. It replaces the former steam-production
proxy. Source references are -1 kg; independently checked native UVEK waste
inputs have +1 kg production, so factor -1 preserves signed waste demand.

The source's water content is unavailable, while the UVEK label specifies 0%.
Preserve the original wet kg without dry-mass or moisture scaling. Both functions
exclude additional cement solidification. The modern source has 24 active
biosphere releases versus eight in UVEK; speciation, leaching, hydrology,
time horizons, infrastructure and CH/RER geography differ and warn. Neither
emission-coverage nor toxicity equivalence is claimed. No catalyst manufacture,
metal recovery or steam-production credit is introduced. Foreground suppliers,
separate credits, production and emissions remain unchanged. Reverse conversion
of this UVEK endpoint was independently checked as unsupported; no inverse is
inferred from the forward correspondence.

The RER market adds six profiles to the composed-market cohort. Its helper
retains 0.9999999998340274 kg of treatment and 0.077 tkm of lorry freight without
rounding the treatment share to one or renormalizing. The dedicated-disposal
review now records two explicit evidence batches: the original seven references
with 32 profiles and this one reference with six. Existing supplier bindings and
helper definitions remain unchanged.

Inspected cut-off stock allocation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Thirty-four explicitly named ``Recycled Content cut-off`` suppliers are
production-only stocks across all 168 installed source/profile pairs. Each
contains exactly one self-linked production exchange of +1 and no other
exchanges. Their former UVEK targets included production and disposal processes
with different allocation boundaries. Substituting a disposal service could turn
a negative input of an unburdened material into a false avoided-disposal credit.

Each exact inspected profile now uses a separate production-only foreground
supplier with factor +1 and a ``migration.zero_burden_supplier`` warning. This
preserves the observed stock allocation; actual recycling, processing and
treatment remain separate. Signed material quantities, uncertainty, formulas,
foreground suppliers, production and separately listed emissions are retained.
The stock evidence and fingerprints are in
``data/export/uvek_cutoff_stock_review.json``. Negative-reference empty treatments,
consequential supplies, missing inventories and uninspected patch profiles cannot
inherit this approval.

Composition dependencies on these suppliers require an explicit direct-rule ID,
exact source profile and profile-review hash. The schema checks every containing
helper, including indirect dependencies, against the same source profile. Every
changed frozen component binding was re-reviewed against its original inventory;
52 existing parent identities were refreshed. All coefficients and credits retain
their original quantities. Annotation fields belong to migration resources and
are removed from rendered helper exchanges. Nested stock use also emits the
allocation warning and marks its production-only helper.

An additional 118 identities have 630 approved exact-profile assemblies: 114
markets with 612 profiles and four processing references with 18 profiles.
Their other 672 observed source/profile pairs remain explicitly unresolved, with
no target or conversion factor. These are partial approvals, so all 118 remain
in the review queue. The full market cohort has 372 identities, 2,929 approved
profiles and 646 pending profiles; processing has 106 identities, 817 approved
profiles and 62 pending profiles. Unknown profiles and blocked counterparts fail
transactionally. Every returned helper must be exported and scored. Reverse
background correspondences remain independently reviewed; retaining a generated
stock as foreground does not approve an inverse background mapping.

Sawing-slurry treatment in ecoinvent 3.6
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Four exact profiles cover RER and RoW in cut-off and consequential 3.6. They
replace the unrelated agricultural slurry-spreading correspondence with
complete technosphere-only treatment compositions. The caller supplies litres
of received sawing slurry; the helper reference remains one litre. The old
0.001 litre-to-cubic-metre factor belongs to the rejected spreading target and
is removed. Its exact value remains bound in authoring evidence, so a changed
prior correspondence requires reinspection.

Every original electricity, chemical-plant infrastructure and downstream waste
coefficient is retained through its reviewed directional mapping. Both
consequential profiles retain the original negative silicon-carbide and
triethylene-glycol recovery credits. The cut-off treatment profiles do not gain
those credits. Positive-reference chemical-production and electricity suppliers
were independently checked, as were the source's existing silicone-factory
proxy and UVEK's matching chemical-plant description. Electricity mix, regional
representation, waste composition, routes, allocation and displaced-product
supply remain proxies; no equal LCIA score is asserted. Internal component
uncertainty/formulas are not reconstructed, while caller uncertainty/formulas,
foreground ownership and separate emissions remain preserved.

Two independently inspected global market profiles retain their unrounded RER
and RoW shares of 0.500000000000001 and 0.499999999999999 litres respectively.
All six returned parent compositions and their nested helpers must be included
in scoring and export. These are six resolved profile decisions across three
still-partial identities: the 36 observed newer counterparts remain blocked
because their sludge recovery/co-processing/landfill components are unreviewed.
Unknown patch versions remain blocked. No reverse correspondence is inferred;
the independently generated reverse resource is unchanged. Whole-helper reverse
checks find unsupported component suppliers under permissive policy and fail
transactionally, preserving the complete foreground hierarchy.

Remaining review queue
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

After these approvals, 718 identities retain pending decisions in eight screening
families: 595 are wholly pending and 121 assemblies have approved
exact profiles with other observed profiles still pending. Two sweet-gas flaring
references have approved modern profiles and pending older counterparts. The standalone feed-credit and
direct-release families are resolved. This is a first grouping by source metadata,
not eight scientific approvals or a prediction of additional user decisions. Existing approvals can
cover recurring cases after inspection; new modelling choices require separate
review. Scientific inspection may split or regroup these families. The number
of additional approval requests is currently undetermined.

.. list-table::
   :header-rows: 1
   :widths: 75 25

   * - Screening family
     - Pending identities
   * - Ash, sludge and leachate treatment
     - 176
   * - Other industrial waste markets and chemical treatment
     - 111
   * - Model-dependent recycled materials and coproduct allocation
     - 140
   * - Mineral recovery and cement/concrete co-processing
     - 179
   * - Equipment, vehicle and electronics end-of-life
     - 12
   * - Energy recovery and combustible byproducts
     - 16
   * - Material sorting and recycling services
     - 50
   * - Other waste disposal and its markets
     - 34

The :download:`machine-readable queue <../uvek_forward_review_queue.json>`
records each pending rule exactly once and binds the count to the resource hash.
It distinguishes wholly pending identities from partially approved ones, so a
valid consequential omission does not hide a pending cut-off supply decision.
Regenerate it with ``python scripts/group_uvek_review_queue.py`` after changing
mapping dispositions. Its labels never authorize a runtime conversion.

Li-ion battery hydrometallurgical treatment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The 2026-11-04 approval replaces the graphite-anode manufacturing proxy for the
exact ``treatment of used Li-ion battery, hydrometallurgical treatment`` /
``used Li-ion battery`` / GLO / kilogram source identity. Its approved target is
``Disposal, Li-ions batteries, hydrometallurgical`` / GLO / kilogram. Inspected
source reference production is -1 across the 14 source profiles; the UVEK
reference production is +1 and its verified native reference is a waste input.
The explicit factor -1 converts waste demand into positive treatment-service
demand, and converts avoided treatment in the opposite direction without taking
absolute values of exchanges or impacts. Supported uncertainty and formulas
follow the same scaling.

This approval is confined to the waste reference. The same-named activity's
recovered cobalt and lithium-carbonate reference products keep their own mappings.
The inspected UVEK inventory (``bafu-272203``) allocates 100% of the process to
disposal. Recovery yields, inventory coefficients, background supply chains and
consequential credits may differ. The declared proxy limitations use the same
information-loss policy as coal tar; strict callers must explicitly accept the
loss as well as the heuristic mapping. The independent reverse-direction
resource is unchanged. Audit replay and mapping generation apply this exact
approved target correction before checking its source/target binding.

Li-ion battery pyrometallurgical treatment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A separate 2026-11-04 approval replaces the anode-manufacturing proxy for
``treatment of used Li-ion battery, pyrometallurgical treatment`` /
``used Li-ion battery`` / GLO / kilogram with
``Disposal, Li-ions batteries, pyrometallurgical`` / GLO / kilogram. Reference
production is -1 across the 14 inspected source profiles and +1 in UVEK, with a
verified native waste input. The explicit factor -1 preserves treatment demand,
avoided-treatment signs, supported uncertainty and formulas. No recovered-product
reference is included in this approval.

The inspected UVEK provider (``bafu-265762``) allocates 100% of the process to
disposal. Recovery, allocation, background and consequential-credit limitations
are disclosed through the existing information-loss policy. The independent
reverse mapping remains unchanged. This approval covers only the exact waste
reference; unknown targets require renewed review. Later approvals are separate.

Similar-treatment auto-approval and battery markets
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The 2026-11-04 approval accepts ``market for used Li-ion battery`` / GLO /
kilogram to ``Disposal, Li-ions batteries, mixed technology`` with factor -1.
Both inspected mixes contain 50% hydrometallurgical and 50% pyrometallurgical
treatment across all 14 ecoinvent source profiles. Four additional freight
supplier functions in each source market are absent from the UVEK mix; this
market-level transport omission is explicit information loss.

The user's subsequent similar-case authorization also covers two independently
inspected NiMH waste identities: ``treatment of used Ni-metal hydride battery,
pyrometallurgical treatment`` and ``market for used Ni-metal hydride battery``,
both GLO / kilogram. Both replace the unrelated Li-ion NMC111 hydroxide proxy
with ``Disposal, NiMH batteries`` / GLO / kilogram and factor -1. Across all 14
source profiles, the reference is classified Waste with production -1. Each
NiMH market supplies 100% pyrometallurgical treatment and four freight functions.
The UVEK service has production +1, uses pyrometallurgical technology, and
allocates 100% of the process to disposal. The market proxy excludes that extra
freight; the direct treatment proxy retains separate recovery and allocation
caveats. Both disclosures use the existing information-loss policy.

Auto-approval is limited to inventory-backed agreement of waste function,
chemistry, treatment technology/mix, geography, unit and reference direction.
This battery batch does not approve geographically different proxies, different
waste/material roles, undocumented unit conversions, different treatment mixes
or technology changes. Recovered-product references and the independent reverse
resource remain outside this batch. A subsequent geographic-proxy approval is
described below.

PET incineration: approved geographic treatment proxies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The subsequent 2026-11-04 approval replaces the fleece-production proxy for
``treatment of waste polyethylene terephthalate, municipal incineration`` /
``waste polyethylene terephthalate`` / kilogram, in RoW and GLO, with
``Disposal, polyethylene terephtalate, 0.2% water, to municipal incineration`` /
CH / kilogram. The factor is -1. RoW is inspected in ecoinvent 3.6--3.9 and GLO
in 3.10--3.12, for both cut-off and consequential. All 14 source references are
classified Waste with production -1. The UVEK supplier (``bafu-514203``) has
production +1 and a verified native waste-input reference.

The inspected source and target describe municipal incineration of PET in
communal waste, 0.2% water, with upper/gross heating value 23.13 MJ/kg. Older
RoW documentation explicitly describes Swiss 2010 technology; newer GLO
documentation says treatment-technology parameters are not geography-specific.
This supports a screening proxy, without establishing equal inventories or
impact scores. Reports explicitly disclose the geographic approximation and
possible differences in energy recovery, emission controls, residue treatment,
allocation and background supply chains. Callers must accept information loss
as well as the heuristic mapping. No LCIA difference has been quantified.

The similar-case authorization permits independently checked geographic proxies
only where waste material, treatment technology, moisture/quantity basis and
reference direction agree. Executable corrections are enumerated by exact
source and approved target; there is no automatic label-based fallback. Audit
regeneration rejects changed source-profile coverage. The current geographic
batch contains only these two PET rules. PET dust is covered by the separate
composition-proxy approval below; landfill treatment and recovered-energy
references remain outside both approvals.
The independent reverse resource is unchanged.

PET dust and LDPE fraction: approved incineration composition proxies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The 2026-11-05 approval replaces two manufacturing targets with explicit
municipal-incineration services. These exact RER, kilogram waste references
have production -1 in all ten inspected source/profile combinations:

* ``treatment of waste polyethylene terephthalate, dust, municipal incineration``
  / ``waste polyethylene terephthalate, dust`` in ecoinvent 3.11--3.12,
  cut-off and consequential. The former fleece-production target is replaced
  by CH ``Disposal, polyethylene terephtalate, 0.2% water, to municipal
  incineration`` (``bafu-514203``). Source recycling dust has 2.7027% water and
  gross heating value 37.1 MJ/kg, versus 0.2% and 23.13 MJ/kg in the proxy.
* ``treatment of LDPE in waste fresh milk carton (chilled), post-industrial,
  municipal incineration`` / ``LDPE in waste fresh milk carton (chilled),
  post-industrial`` in ecoinvent 3.10--3.12, both models. Auto-approved after
  independent inventory inspection under the similar-incineration authorization,
  this replaces an industrial-machine manufacturing target with CH ``Disposal,
  polyethylene, 0.4% water, to municipal incineration`` (``bafu-275730``).
  Source documentation identifies the fraction as 100% LDPE, with 13.775% water
  and gross heating value 38.79 MJ/kg, versus 0.4% and 42.82 MJ/kg in the proxy.
  This is the LDPE fraction, not an entire composite carton.

Both UVEK services are non-empty, with production +1 kg and independently
verified native WASTE_FLOW input references. Factor -1 converts reference
coordinates while preserving wet waste mass: -10 kg becomes +10 kg of treatment.
No heating-value or moisture scaling is applied. Supported uncertainty and
formulas follow the factor; caller-owned data and production remain unchanged.

Every conversion discloses the accepted composition and RER-to-CH approximation,
possible differences in trace pollutants, emissions, controls, residue handling,
energy recovery, allocation and background supplies. Consequential electricity
and heat substitution credits are not reproduced by the UVEK services. These
are screening proxies; LCIA equivalence is not established or quantified.
The existing information-loss policy applies: acceptance yields warnings and loss
records; an error policy rolls back the operation. The UVEK 2026 bridge preserves
the converted suppliers and their warnings. Original targets and factor history
remain in the audit, and changed source/profile coverage fails regeneration.

Similar-case approval is applied only to the exact inspected identities. It does
not automatically approve CRT coatings, mixed WEEE residues, industrial sewage
sludge, whole cartons, aluminium/polymer composites, ash disposal or recovered
electricity/heat reference products. The reverse resource is not inferred from
these decisions.

CRT coatings and shredder residues: approved incineration proxies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The subsequent 2026-11-05 approval covers CRT coating powder, with independently
inspected similar CRT-incineration cases auto-approved. Twelve exact kilogram
rules cover 100 source/profile pairs across ecoinvent 3.6--3.12, cut-off and
consequential. Every source has a single self-linked reference of -1 kg.

* Coating/luminophore treatment uses CH ``Disposal, coatings in CRT screens,
  to municipal waste incineration`` (``bafu-269681``). Direct sources are CH
  and RoW ``municipal waste incineration`` and CH ``municipal incineration with
  fly ash extraction`` in 3.6--3.9, plus GLO ``municipal incineration`` and CH
  ``municipal incineration FAE`` in 3.10--3.12. The two CH/RoW coating markets
  are covered in all seven releases. This is seven coating rules and 64
  source/profile pairs.
* Mechanical-treatment residue uses CH ``Disposal, residues, mechanical
  treatment, CRT screen, in MSWI`` (``bafu-377324``). Direct sources are CH/RoW
  ``municipal waste incineration`` and CH ``municipal incineration with fly ash
  extraction`` in 3.6--3.9, plus GLO ``municipal incineration`` and CH
  ``municipal incineration FAE`` in 3.10--3.12. These are five residue rules and
  36 source/profile pairs.

Six previously blocked correspondences targeted CRT-screen manufacturing.
The older CH residue service with fly-ash extraction instead targeted zinc-only
car-shredder-residue disposal; that supplier is corrected too, retaining the
existing factor -1. Five other executable correspondences already used the
matching disposal services and now receive the same scoped review and warning
handling. Full original target/factor history remains in the authoring audit.

Both UVEK targets have production +1 kg, verified native WASTE_FLOW input
references, active treatment inputs/emissions, and no negative technosphere
supplier credits. All twelve rules use factor -1 and preserve wet waste mass,
supported uncertainty and formulas without heating-value or moisture scaling.
The two markets each supply exactly 100% incineration of the same coating and
have no active direct biosphere releases. Their additional market-level freight
is explicitly omitted: two freight suppliers for CH and three, eight or nine
for RoW depending on profile. Separately listed foreground freight is preserved.

Warnings disclose CH geographic proxies, emission controls, fly-ash extraction,
ash treatment, leaching, metal speciation, long-term metal releases, recovery,
allocation and background differences. Source-specific extraction/leaching and
credit models are not reconstructed; toxicity-score equivalence is not asserted.
Coating reviews explicitly retain the inconsistent legacy LCD-use wording: the
CRT name, luminophore synonym and active silver, strontium, sulfate and zinc
flows support the CRT interpretation. This documentation ambiguity is disclosed
and is not treated as independent proof of equal inventories or impacts.

Existing information-loss policy applies: acceptance yields warnings and loss
records; an error policy blocks and rolls back conversion. Production, matching
foreground suppliers and caller-owned data are preserved. The UVEK 2026 bridge
carries the reviewed suppliers and warnings. Glass, whole-screen dismantling,
sludge, ash-disposal and recovered-energy reference products remain outside
this approval; reverse correspondences are independently authored.

CRT-glass processing: auto-approved service and market proxies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The same similar-CRT-treatment authorization independently covers two further
GLO, kilogram waste references in all seven ecoinvent 3.6--3.12 releases and both
models: ``treatment of glass from used cathode ray tube`` and ``market for glass
from used cathode ray tube``. These 28 source/profile pairs have reference -1 kg.
Their screen-manufacturing targets are replaced by GLO ``Disposal, treatment of
CRT glass`` (``bafu-285219``), a non-empty positive-reference service with verified
native WASTE_FLOW input metadata. Factor -1 preserves waste mass.

The source describes CRT-glass crushing, washing and sorting. Both source and
proxy document a multi-output process producing panel, funnel and mixed glass,
with 100% allocation to disposal. This supports the functional correspondence;
recovery yields, composition, infrastructure, energy, downstream coating disposal,
allocation, transport and consequential credit models remain approximations.
Downstream metal speciation, leaching and long-term releases are not independently
validated and toxicity-score equivalence is not asserted. Recovered-glass product
references remain separate.

All fourteen market inventories supply exactly 100% of this processing service
plus four freight suppliers, without active direct biosphere releases. The proxy
omits that additional market freight and retains its own treatment-internal
transport model. Direct-treatment reviews disclose transport differences without
claiming a market-freight omission. Caller-owned data, matching foreground links,
production, supported uncertainty and formulas follow the same protections as the
CRT incineration batch. Accepted loss produces warnings and loss records; error
policies roll back conversion. The 2026 bridge preserves these findings and no
reverse rule is inferred. Glass-cullet landfill and whole-screen dismantling are
outside this approval.

Manual CRT dismantling: auto-approved kilogram-based services
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Two further inspected correspondences replace screen-manufacturing targets with
CH ``Dismantling, CRT screen, manually, at plant`` (``bafu-448178``). They cover
CH manual treatment of used CRT displays in all seven ecoinvent 3.6--3.12 releases
and RoW manual treatment in 3.10--3.12, both models: 20 source/profile pairs.
Source production -1 kg becomes the proxy's +1 kg service through factor -1.
The proxy has a verified native WASTE_FLOW input and eight active technosphere
inputs. Both functions manually dismantle used screens into fractions; no
kilogram-to-device conversion is needed.

Warnings disclose Swiss transfer coefficients, geographical approximation,
dismantling performance, recovery yields, fraction composition, downstream metal
fate, allocation, background supplies and credit-model differences. Original
waste mass is preserved; no additional mechanical stage or market freight is
inferred. Source-specific credit models and toxicity equivalence are not
reconstructed or asserted. Existing information-loss policy, foreground/source
protection, uncertainty/formula scaling and the 2026 bridge apply as above.

The subsequent approvals below separately cover the broader mechanical
treatment boundary, its effect on the whole-screen market, and CRT-cullet
and production-frit landfill.

Mechanical CRT treatment: approved broader boundary and market mix
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The subsequent 2026-11-05 approval covers GLO, kilogram ``treatment of used
cathode ray tube display, mechanical treatment`` in all seven ecoinvent
3.6--3.12 releases and both models. Its screen-manufacturing target is replaced
by GLO ``Dismantling, CRT screen, mechanically, at plant`` (``bafu-201280``), with
factor -1. All fourteen inspected sources have reference -1 kg and describe a
four-step shredding/separation procedure without a manual-dismantling service
input. The positive-reference UVEK service explicitly describes manual
dismantling before shredding/separation. The accepted warning states that this
broader boundary may add or double-count manual dismantling where it is already
represented. Foreground treatment exchanges are preserved.

The similar-case authorization also covers the GLO, kilogram ``market for used
cathode ray tube display`` in the same fourteen profiles. Every source market
routes 23% of mass to manual dismantling and 77% to mechanical treatment, plus
four freight inputs, without active direct biosphere releases. UVEK ``xx Disposal,
CRT screen, 17 inches, to WEEE treatment`` (``bafu-352244``, CH, unit) supplies the
same 23%/77% mix through the reviewed kilogram-based services. Its two active
inputs total 19.9 kg of treatment per reference unit and it has no other active
inputs. Factor ``-1/19.9`` therefore preserves both source mass and route shares:
-19.9 kg becomes +1 unit. This is the reference mass of this exact mix, not an
assumed weight for all CRT displays. The mixed provider retains the approved
broader mechanical boundary. Its additional source-market freight is omitted;
the source manual-treatment geography (CH through 3.9, RoW from 3.10) is
approximated by the proxy's CH manual service.

The two existing CH/RoW CRT-shredder-residue markets are independently inspected
and retain their CH disposal supplier and factor -1. All 28 source/profile
inventories supply 100% incineration of the same residue plus freight only.
Warnings now explicitly omit additional market freight: two CH suppliers and
three, eight or nine RoW suppliers depending on profile. This aligns their
disclosures with the already reviewed direct residue-incineration services.

All four rules disclose performance, recovery yields, fraction composition,
downstream metal fate, allocation, background and credit-model uncertainty;
equal LCIA/toxicity results are not claimed. The existing loss policy applies.
The whole-screen reference-mass factor scales amounts, supported uncertainty,
bounds and formulas together; invalid mass bases or changed quantity units fail
authoring. Production and caller-owned input are preserved, unrelated errors
roll back conversion, and the 2026 bridge retains these suppliers and warnings.
Original suppliers/factors and all 56 source/profile observations are audited.
Reverse rules are independently authored. CRT-glass landfill is covered by the
separate approval below; sludge, ash disposal and recovered-material reference
products remain outside these approvals.

CRT cullet and production frit: approved landfill screening proxies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The next 2026-11-05 approval covers lead-containing CRT-glass cullet landfill and
its market, with independently inspected similar panel/mixed-glass and
production-frit cases auto-approved. Ten exact kilogram rules cover 120
source/profile pairs. Seven screen/frit-manufacturing correspondences are
replaced by disposal services; three existing matching frit treatments retain
their supplier/factor and gain consistent scientific disclosures.

The six GLO cullet rules cover direct ``inert material landfill`` and markets
for panel glass, lead-containing glass and mixed glass in all seven ecoinvent
3.6--3.12 releases, cut-off and consequential (84 source/profile pairs). They use
CH ``Disposal, glass, 0% water, to construction waste landfill`` (``bafu-506481``)
and factor -1. The original waste mass is preserved without composition or
leaching-based scaling.

This is an explicit composition and emission-boundary approximation. All source
cullet landfills document that direct leachate emissions are excluded as
negligible. The inspected pre-3.12 inventories have no active direct biosphere
exchanges; 3.12 adds only ``Non-hazardous waste disposed``. The UVEK ordinary-glass
landfill includes generic-glass metal releases, including chromium, copper, lead
and zinc. These are proxy releases, not a reconstruction of the actual cullet's
metal content. Warnings disclose this difference, CH geography, composition,
hydrology, landfill design, metal speciation, infrastructure and background
uncertainty. Generic direct metal contributions may be introduced where absent
from the source landfill inventory; equal LCIA/toxicity results are not claimed.

The four production-frit rules use CH ``Disposal, frit for CRT tube production,
to residual material landfill`` (``bafu-466727``), with factor -1. Direct sources
are CH/RoW in 3.6--3.9 and GLO in 3.10--3.12; the GLO market spans all releases,
both models (36 source/profile pairs). These functions treat CRT-production frit
and include residual-landfill metal releases. This is distinct from cullet's
excluded-leachate model. Source-specific solidification, composition, hydrology,
short/long-term leaching, metal speciation and background differences remain
disclosed, without a claim of toxicity equivalence.

Every inspected market supplies 100% landfill of its own waste plus freight,
with no active direct biosphere releases. Older frit markets mix CH/RoW residual
landfill; newer ones use GLO treatment. Market proxies omit the additional
freight: four suppliers through 3.9 and one from 3.10. Direct-treatment warnings
do not claim a market-freight omission. Separately listed foreground transport,
emissions, production and matching foreground suppliers are preserved.

Both target services have native WASTE_FLOW input references, production +1 kg,
and active treatment inventories. Supported uncertainty, bounds and formulas
follow the factor. The existing information-loss policy applies: acceptance
produces warnings/loss records; error policy rolls back conversion. The UVEK
2026 bridge retains the services and findings. Original targets/factors and all
120 observations remain in the replayable audit. Reverse rules are independent;
Recovered-material references are outside this approval. CRT-production
wastewater residues have the separate omission approval below.

CRT-production wastewater residues: approved omissions with warnings
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The subsequent approval omits the 75%-water CRT-production wastewater sludge
sanitary-landfill service and its market. Independently inspected similar cases
extend this to 18 exact kilogram identities across ecoinvent 3.9--3.12, cut-off
and consequential: 144 source/profile pairs. They comprise four 70%-water sludge
incineration services/markets, two 75%-water sludge landfill services/markets,
four 97%-water sludge landfarming services/markets, and eight wastewater-derived
bottom-ash/residue landfill services/markets. All products identify wastewater
from CRT production; this is an exact identity list, not a keyword-based rule.

No inspected UVEK supplier establishes a suitable complete treatment model for
these materials. The previous proxies supplied frit, municipal or paper sludge
treatment, iron-chloride-production sludge, mechanical CRT-screen residue
treatment, or a process-specific landfill component. Fourteen former mappings
had compatible waste-reference coordinates, which did not establish functional
equivalence; four crossed product/waste roles and were blocked. All 18 now have
approved omission rules instead of executable replacements. All 40 direct
sludge observations explicitly specify zero biogenic carbon. The 72 market
observations supply 100% treatment of their own waste plus freight and contain
no active direct biosphere exchanges.

Conversion omits these external technosphere/substitution inputs and continues
with ``migration.technosphere_omission`` warnings. This excludes their treatment
burdens, associated credits and market freight, including route-specific
emissions, landfill gas handling/recovery or landfarming effects. Score changes
are method-dependent and unquantified; omission does not establish zero impact
or equivalent treatment. Each loss record retains the complete original
exchange, including amount, uncertainty, formula and caller metadata. Production,
matching foreground suppliers and separately listed foreground biosphere
exchanges remain intact. The underlying background databases are unchanged.

Generic deletion/information-loss error policies cannot block these approved
warnings. Route-level heuristic permissions still apply, and unrelated errors
roll back conversion. The UVEK 2026 bridge retains the omissions and reports.
Original targets/factors, native roles and all 144 sign observations remain in
the replayable audit. Reverse conversions require independent decisions;
no reverse omission is inferred from this approval.

Wastewater treatment: approved service and boundary proxies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The subsequent 2026-11-04 approval accepts the RER hard-fibreboard wastewater
treatment proxy and authorizes independently inspected similar cases. This
batch covers 11 exact treatment identities: hard fibreboard and medium-density
board in RER/RoW, and particle board/particleboard in RER. Each retains the
existing UVEK supplier for its own effluent type, in RER and cubic meters, and
applies factor -1. No wastewater type is substituted for another. RoW-to-RER
geography is a disclosed approximation under the earlier geographic-proxy
authorization. Two separately inspected older markets qualify as described below.

The inspected UVEK providers are ``bafu-193650`` (hard fibreboard),
``bafu-414420`` (medium-density board) and ``bafu-397133`` (particle board).
All have positive reference production and active class-1 sewer and wastewater
treatment infrastructure. Their native openLCA references are PRODUCT_FLOW
outputs: here the product is a volume of treatment service, not recovered
material. The audit accepts this classification only for the enumerated,
inventory-inspected approvals, retaining the native metadata unchanged. Other
waste/product mismatches remain blocked; native product status or a treatment
label alone never authorizes inversion. The reverse resource is independent.

All available exact source profiles were inspected: older capacity-5E9l/year
identities occur in 3.6--3.8, and wastewater-treatment identities in 3.9--3.12,
in both system models. The particle-board spelling is version-specific. Their
references are Waste, production -1 cubic meter. Older sources already include
sewer inputs; their reports say that both source and proxy include sewering.
Newer sources place sewering in upstream markets. Their reports explicitly
warn that the UVEK service adds sewer infrastructure and can double-count it
where represented separately. No sewer burden is silently subtracted.

Plant capacities, treatment-stage mixes, wastewater composition, pollutant
removal, sludge fate, energy recovery, allocation and background supplies may
differ. Class-1 UVEK infrastructure documents 47.1 million cubic meters/year;
this does not reproduce either the older source capacity or newer regional
treatment mixes. LCIA differences have not been quantified. These screening
proxies require explicit information-loss acceptance as well as heuristic
mapping acceptance. Audit regeneration rejects changed signs, native reference
roles, source-profile coverage and supplier identities. Partially untreated
wastewater markets received a separate subsequent approval described below.
PET/textile effluents and other material substitutions remain pending review.

The similar-case authorization also covers the two GLO markets for hard-fibreboard
and medium-density-board wastewater in 3.6--3.8, both system models. Every
inspected market supplies 100% treatment from two geographic suppliers for the
same effluent and has no active direct biosphere emissions. Their older suppliers
already include sewer infrastructure. These markets retain the same effluent's
RER class-1 service with factor -1, explicitly disclosing replacement of the
global supplier mix and the treatment-model limitations. The wastewater batch
therefore initially contained 13 exact rules. Newer RER/RoW markets include
partially untreated wastewater and direct pollutant releases; they were outside
that older-market approval and required the separate decision below.

Wastewater markets: approved omission of untreated discharges
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The subsequent 2026-11-04 approval accepts five exact market proxies: hard
fibreboard and medium-density-board wastewater in RER/RoW, and particle-board
wastewater in RER. All five exist in ecoinvent 3.9--3.12, cut-off and
consequential. Read-only inspection of all 40 source/profile combinations
confirms negative cubic-meter Waste references, active sewer inputs, treatment
of the same effluent, and direct emissions associated with untreated wastewater.
Treatment shares are stable across these inspected profiles: about 87.3% in RER
and 41.3% in RoW, with about 12.7% and 58.7% untreated, respectively.

Each proxy retains its matching effluent's RER UVEK class-1 service and applies
factor -1 to the full source volume. One cubic meter of source market becomes
one cubic meter of UVEK treatment. The factor converts reference coordinates;
it does not multiply by the source treated fraction or reproduce its mixed
treated/untreated system. The UVEK proxy represents 100% treatment and omits the
source background market's untreated-discharge emissions. This may understate
water-pollution impacts and overstate treatment burdens. Both sides include
sewer infrastructure, but regional supply and sewer mixes, treatment performance,
sludge fate, energy recovery and allocation may differ. LCIA equivalence has not
been established or quantified.

Every conversion explicitly reports the omitted untreated emissions and changed
treatment coverage via ``migration.technosphere_information_loss``. Strict
callers must accept this loss as well as the heuristic mapping. Positive avoided
treatment exchanges retain their opposite direction, supported uncertainty and
formulas are scaled, and caller-owned data is preserved. Separately represented
foreground pollutants remain unchanged; no raw-discharge inventory is copied
into the resource or synthesized during conversion.

These wastewater approvals cover 18 exact identities with matching effluent
types. The separate composition-proxy approval below covers selected plastic
effluents. A cubic-meter UVEK service alone does not approve an unrelated
wastewater. Source-profile coverage, native roles, factors and exact supplier
bindings remain checked. The independent reverse resource is unchanged.

Industrial wastewater: approved generic composition proxies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The subsequent 2026-11-04 approval accepts generic municipal wastewater treatment
for PET-pelletising wastewater and independently inspected similar plastic
effluents. Six exact RER / cubic-meter identities are enumerated: treatment and
market references for PET pelletising, PET recycling and PP recycling. All exist
in ecoinvent 3.11 and 3.12, both system models. Read-only inspection confirms
negative Waste references in all 24 source/profile combinations. The target is
``Treatment, sewage, to wastewater treatment, class 1`` / CH / cubic meter
(``bafu-299668``), with positive production and a verified native waste-input
reference. Its documentation reports COD 0.1556 kg/m3.

The source treatment documentation reports COD 0.373 kg/m3 for PET pelletising,
0.213 for PET recycling and 0.425 for PP recycling. These are disclosed composition
differences, not scaling factors: one cubic meter of source wastewater becomes
one cubic meter of generic service, with factor -1. Pollutant concentrations,
fossil/biogenic carbon fate, sludge treatment, removal performance, energy
recovery, allocation and background supplies may differ. No impact equivalence
is established or quantified and no source-specific pollutant recipe is copied.

Direct treatments place sewering in their upstream market, so their warnings
disclose the proxy's additional sewer infrastructure and possible double counting.
The markets include sewer inputs and supply about 87.3% treatment; their warnings
retain the separately approved change to 100% treatment and omission of untreated
discharges. All six preserve source volume, supported uncertainty, formulas,
caller data and separately represented foreground emissions. Strict callers must
accept the composition/geographic/boundary information loss as well as heuristic
mapping. Landfill leachates and direct untreated-disposal activities are outside
this approval.

The same-case authorization also covers four modern textile references: treatment
and markets in RER/RoW, in 3.11--3.12 and both system models. Inspection confirms
municipal wastewater treatment for these sources, COD 0.93446 kg/m3 (about six
times the proxy), and 35% biogenic carbon in the source wastewater. Their warnings
disclose the larger composition mismatch and regional/Swiss treatment-model
differences. Markets retain the full-treatment approximation and omitted direct
discharges, with about 87.3% source treatment in RER and 41.3% in RoW. Older
fully untreated textile direct-disposal activities and markets remain outside
this authorization.

The generic registry therefore covers ten exact identities and 40 inspected
source/profile combinations. Six previously blocked rules replace particle-/soft-
fibreboard effluent targets. Four already executable mappings (PET/PP recycling
treatments and the two RoW textile references) are aligned with the same approved
class-1 service. Their signed factor stays -1. Audit regeneration retains the
original factor history when replaying an approved supplier change, and baseline
checks protect unrelated mappings. The independent reverse resource is unchanged.

Fully untreated textile disposal: approved omission with warning
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Six exact legacy correspondences are explicitly omitted instead of mapped to
NF3 or municipal wastewater treatment: direct disposal and its markets for
textile-production wastewater in GLO, BD and IN, in cubic meters. Read-only
inspection covers 30 source/profile combinations: GLO direct disposal in
3.6--3.11, GLO markets in 3.6--3.10, and BD/IN direct disposal and markets in
3.12, both system models. Direct disposal has sewer inputs and direct emissions
but no treatment supplier; each associated market supplies only direct disposal.

These are ``delete`` rules with ``approved_omission: true``, stable identifiers
and shared rationale/evidence. Only matching external technosphere/substitution
exchanges are omitted. Dataset headers, production exchanges, foreground links,
other exchanges and separately listed biosphere emissions are preserved.
Matching uses the existing unit aliases (including ``m3`` for cubic meter);
another quantity basis such as liters does not match these rules.
Exact source-catalog validation remains independent: normalize raw unit aliases
before that validation, or explicitly disable source validation.
Each omission emits ``migration.technosphere_omission`` at warning severity and
records a loss containing the original source exchange, amount, uncertainty,
formula and other metadata, original path, rule, resource and review evidence.
Migration metrics count the removed exchanges as ``technosphere_deletions``.
The omitted impacts are absent from the target inventory and can understate its
score; no source pollutant recipe is reconstructed or substituted.

Approved omissions continue conversion regardless of generic ``on_deletion`` or
``on_information_loss`` settings and still warn under permissive/allow policies.
This behavior is scoped to the six audited rules. Ordinary deletions retain
``on_deletion`` handling; other unresolved mappings, validation errors and the
route's heuristic-mapping permission retain their existing policies. Execution
remains transactional: any unrelated blocking error restores the complete source
inventory. Applying the migration again does not repeat an already removed
exchange. The UVEK 2026 bridge preserves warnings and losses. The independent
reverse resource cannot recreate omitted exchanges.
A separately approved reverse biowaste-collection omission is documented in
:doc:`../workflows/uvek-export`; neither direction infers omissions by inverting the other.

The authoring audit retains the six former replacement records for historical
comparison and deterministic regeneration, while the runtime resource contains
only their deletion rules. Changed sources or omission reviews fail replay.

Industrial biowaste composting: approved omission with warning
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Two further approved omissions cover ``treatment of biowaste, industrial
composting`` / ``biowaste`` / CH or RoW / kilogram in ecoinvent 3.6--3.12,
cut-off and consequential. Their former target, ``Biowaste, at collection
point`` / CH / kilogram, supplies collected material rather than composting.
The apparent alternative, ``Composting organic waste`` / RER / kilogram
(``bafu-509472``), is an empty process with only a reference exchange. Its
documentation explicitly attributes this to a cut-off at recycling/composting.
It cannot supply the treatment burdens contained in the ecoinvent inventories.

Conversion omits these two external inputs and continues with
``migration.technosphere_omission`` at warning severity. The warning names the
excluded energy, machinery, infrastructure, residual-waste handling and
emissions including methane, nitrous oxide and ammonia. Score changes are
method-dependent and unquantified. The complete original exchange remains in
the loss report; caller-owned data, foreground suppliers and separately listed
emissions are preserved. Generic deletion/loss policies cannot silence or block
these approved warnings; unrelated errors still roll back conversion. The UVEK
2026 bridge preserves the omission and its audit. Neither a zero-burden
composting substitute nor a reverse reconstruction is introduced.

The audit retains the two historical collected-material targets and all 28
source/profile sign observations. Other biowaste markets, dumping, cement-kiln
treatment and ash disposal are outside this approval.

The audit also preserves 416 forward correspondences where both imported
reference amounts are positive despite differing native flow classifications.
These need no sign inversion. Their existing heuristic scientific limitations
remain; coordinate agreement is not evidence of equivalent treatment functions.

``brightpath/data/export/uvek_reference_flow_sign_review.json`` records per-rule
source-profile sign observations, native roles, original and reviewed factors,
exact source/target bindings, and source coverage digests. It retains original authoring inputs and derived helper recipes for replay,
without complete source inventories. Both resource generators replay this review after their existing
scientific mapping decisions and reject stale targets or factors.

Heuristic generation orders equal-frequency token shortlists and floating-point
summations deterministically for both technosphere and biosphere candidates.
``brightpath/data/export/uvek_heuristic_candidate_baselines.json`` retains nine
historical technosphere authoring inputs affected by the former hash-dependent
shortlist. This is an authoring baseline, not scientific approval or an executable
migration. It restores only exact source/target candidate identities and scores
before independently reviewed overrides and the unchanged bound sign audit.
It cannot add a sign decision, helper, omission, review flag or profile approval.
Changed fingerprints, source identities, methods, units or catalog membership
fail validation; all rows are checked before any is changed. New candidate
bindings still fail the independent sign audit. Historical runtime suppliers,
helper recipes, factors, uncertainty handling and unresolved states are preserved.

The complete forward technosphere resource is reproduced byte-for-byte across
Python hash seeds. The biosphere rule identities, targets and scores are also
unchanged; its legacy packaged storage and schema-3 generated storage differ in
representation only. Scientific proxy limitations still apply to every retained
heuristic candidate, including candidates subsequently superseded by a reviewed
helper or blocked by reference-role checks.

To replay the checked-in evidence and refresh integrity metadata::

    python scripts/review_uvek_reference_signs.py
    python scripts/generate_migration_manifest.py

To repeat the inventory inspection against the same trusted local Brightway
projects, add ``--brightway-root /path/to/Brightway3`` to the first command.
This reads installed SQLite databases without modifying projects. Brightway's
locally stored pickle metadata must be trusted. The audit does not revalidate
scientific proxy equivalence or automatically recalculate downstream scores.

Storage and loading
-------------------

Both UVEK directions use the named ``brightpath.migration`` schema, version 3.
It follows ecoinvent's ``replace``, ``disaggregate`` and ``delete`` operations,
with inline source and target identities, shared review records, and optional
foreground recipes. Its 31,488 source dispositions comprise 31,231 replacements
and 257 unconditional approved omissions. Another 123 replacements contain 962
exact-profile omission decisions. Four additional replacements contain 25
independently reviewed recovery-credit decisions. Five replacements target six
physical venting-service recipes with 42 exact-profile decisions. Another five
target six disposal-service proxy recipes, with 42 more profile decisions. Three
reclamation/product mappings add two service recipes and one production-only
supplier across 21 profiles. Four direct produced-water discharge mappings add
56 exact-profile decisions. Two refinery-flare service mappings add another 28;
two sweet-natural-gas flare references add 16 modern profiles and retain 12
pending older counterparts; three legacy EAF landfill services add six. Five LCD service mappings add 50
inspected source/profile decisions and three modal transport proxies add 12.
Eight regional/legacy vehicle and catalyst-carrier disposal mappings add 38 exact-profile decisions.
Five antifouling-emission omissions cover 70 inspected
source/profile pairs. The 103 industrial-gas service mappings add 117 recipe
definitions and 721 exact-profile decisions. The industrial WWT-residue omissions
cover 226 identities and 1,704 source/profile inventories. Gas markets add 28
exact-profile decisions; other composed markets add 2,929 approved profiles and processing
compositions add 817. Thirty-four allocated stock supplies add 168 exact-profile
decisions. Nine physical cut-off coproduct suppliers add 63 exact-profile
reviews; the later beet-pulp/whey-yeast batch adds 35 supplier profiles. Five
associated markets add 35 physical supply compositions while retaining 35
already approved consequential market omissions. Four physical cut-off slag
products add 28 inspected profile helpers. This file contains 625
shared reviews, 2,819 helper definitions and 4,012 distinct direct UVEK suppliers,
excluding recipe primitives. Its venting
coefficients follow complete release and mass conservation; no complete source
inventories are published.

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
     - Six venting, six disposal, two reclamation, one recovered-product and 117 industrial-gas helper definitions.
   * - ``reverse_policy``
     - ``explicit_only`` requires an independently authored reverse mapping.
   * - ``disaggregate``
     - One-to-many rules; empty in this resource.
   * - ``delete``
     - The 257 unconditional approved omissions; exact-profile omissions are selected separately.

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

All 31,488 distinct technosphere source identities have a catalog-valid UVEK
target or an explicit approved omission, and all unit changes have explicit factors. Every identity in the 14
generation catalogs, plus the packaged 3.9.1 and 3.10.1 cut-off catalogs, is
represented. The following counts are identities per catalog, so their sum
exceeds the distinct combined count.

.. list-table:: Technosphere identities with a rule / total identities
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
     - 27,873
     - Deterministic similarity ranking with compatible units.
   * - ``curated_legacy``
     - 311
     - Legacy correspondence-table lookup.
   * - ``product_checked``
     - 35
     - Initial product-identity corrections.
   * - ``pairwise_review``
     - 2,314
     - Expanded product, fuel, technology and waste-role checks.
   * - ``followup_review``
     - 163
     - Subsequent scoped adjudication of alternatives.
   * - ``uvek_inventory_review``
     - 62
     - Seedling and CHP selections supported by sparse inventory evidence.
   * - ``reviewed_venting_recipe``
     - 5
     - Exact-profile physical venting services with explicit air releases.
   * - ``reviewed_refrigerant_disposal_recipe``
     - 5
     - Exact-profile hazardous-incineration proxies with explicit refrigerant leakage.
   * - ``reviewed_r134a_reclamation_recipe``
     - 1
     - Reclamation processing with system-model-specific recovered-product credits.
   * - ``reviewed_r134a_cutoff_product_recipe``
     - 2
     - Inspected empty cut-off recovered-product suppliers with preserved material quantities.
   * - ``reviewed_cutoff_stock_recipe``
     - 34
     - Exact positive production-only stocks retain the inspected cut-off allocation boundary.
   * - ``reviewed_industrial_gas_recipe``
     - 103
     - Consequential gas combustion, explicit energy credits and corrected fuel-supply boundaries.
   * - ``reviewed_gas_market_recipe``
     - 2
     - Exact cut-off allocated supply and consequential gas-treatment mixes.
   * - ``reviewed_market_composition``
     - 372
     - Inspected market supply and freight coefficients through resolved component mappings.
   * - ``reviewed_cutoff_feed_market_recipe``
     - 5
     - Physical cut-off coproduct supplies with independently retained consequential nutritional omissions.
   * - ``reviewed_processing_composition``
     - 106
     - Complete technosphere-only processing inputs and recovery credits through resolved components.

Technical mapping coverage is 100%, but scientific equivalence is not established.
Similarity-only rules account for 88.9% of replacements. The mean confidence is
0.429543; 20,830 replacement entries (66.7%) have scores below 0.5. Curated and reviewed
selections also include documented proxies. Inventory-backed evidence inspected
ecoinvent 3.10 cut-off; shared four-field identities inherit choices across
versions and system models without establishing allocation equivalence.

The direct crosswalk uses 4,013 of the 11,747 available UVEK targets; recipes also
reference inspected background primitives. Many source
identities therefore converge on the same supplier. Planning reports
``migration.heuristic_mapping`` and execution validates the exact target links.
The cleanup changes storage only; it does not improve mapping quality or expand
supported background profiles. See :doc:`../limitations` and
:doc:`../workflows/uvek-export` for the independent reverse-direction resource.
