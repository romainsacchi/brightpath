Export UVEK inventories to ecoinvent
====================================

BrightPath provides a reviewed, directional technosphere route from UVEK 2025
cut-off to ecoinvent 3.12 cut-off, composed with ecoinvent reverse migrations for
3.6, 3.7, 3.8, 3.9.1, 3.10.1 and 3.11 cut-off. This does not invert the heuristic
ecoinvent-to-UVEK mapping or claim scientific equivalence between databases.

Older targets require ``on_inferred_reverse=PolicyAction.WARN`` as well as
``on_information_loss=PolicyAction.WARN``. Ambiguous suppliers and missing links
remain errors. Route availability is not a promise that every inventory converts.
In the archived CLIC CdTe basket, targets 3.10.1 through 3.12 pass; 3.6 through
3.9.1 currently encounter an ambiguous reverse mapping for waste incineration.

Patch routes preserve shared identities and validate against exact catalogs.
The 3.10/3.10.1 identity sets match; background coefficients and LCIA results do
not. The 3.9.1 catalog removes 17 supplier identities and adds nine biosphere
identities relative to the packaged 3.9 catalog. Removed suppliers fail validation,
even under permissive policy. ``docs/patch-catalog-comparison.json`` records counts
and digests; ``scripts/build_patch_catalogs.py`` reproduces this comparison.

The 3.12 unspecified-organic-chemical market maps backwards to the older generic
organic-chemical market as an explicit, policy-controlled proxy. Its composition
is different; the change is not an equivalence claim. The evidence and rationale
are recorded in the packaged compatibility resource and conversion report.

Generated helpers start in the reviewed recipe's 3.12 background and are migrated
with the rest of the converted inventory when an older target is requested.

.. code-block:: python

   from brightpath import (
       BackgroundContext, BiosphereProfile, BrightwayInventory,
       FormatProfile, InventoryContext, TechnosphereProfile,
   )
   from brightpath.core.policies import MigrationPolicy, PolicyAction

   source = InventoryContext(
       FormatProfile("brightway_excel"),
       BackgroundContext(
           TechnosphereProfile("uvek", "2025", "cutoff"),
           BiosphereProfile("ecoinvent", "3.10"),
       ),
   )
   inventory = BrightwayInventory.from_excel("uvek-inventory.xlsx", context=source)
   converted = inventory.migrate_background(
       BackgroundContext(
           TechnosphereProfile("ecoinvent", "3.12", "cutoff"),
           BiosphereProfile("ecoinvent", "3.12"),
       ),
       policy=MigrationPolicy(on_information_loss=PolicyAction.WARN),
   )
   converted.write_excel("ecoinvent-inventory.xlsx")

The warning policy explicitly accepts documented proxies, not unresolved
suppliers. Strict policy rejects proxy use. Missing directional rules remain
errors, including under permissive diagnostic policy. An unused unresolved
catalog entry does not prevent conversion of an otherwise covered inventory.

Coverage and review
-------------------

The independent reference-flow sign audit verifies 44 existing negative
conversion factors from positive UVEK treatment services to negative-reference
ecoinvent treatment suppliers. These factors already handled the reverse
direction correctly and remain unchanged. The former ``Biowaste, at collection
point`` to ``market for biowaste`` correspondence is now an explicitly approved
omission, described below. Its product/waste role mismatch is not resolved by
inverting its quantity or substituting a treatment market. Existing scientific
reviews remain recorded independently of the sign audit.
Three process-specific burden correspondences have positive imported references
on both sides; their existing factors and quantity direction are preserved.
The 1,516 authored source dispositions comprise 1,515 replacement/helper rules
and one approved omission, subject to their other policy and validation checks.
The release bridge carries these checks into conversions from UVEK 2026.
See :doc:`ecoinvent-to-uvek` for the evidence, reproduction commands and forward
direction limitations. Reversible treatment correspondences have round-trip
tests; no general inverse is claimed for lossy or many-to-one mappings.

This resource uses the same named ``brightpath.migration`` schema, version 3,
as the forward ecoinvent-to-UVEK crosswalk. Replacement rules live in ``replace``;
the approved omission lives in ``delete``. Direct targets are inline identities,
helper targets use ``recipe_ref``, and evidence
is shared through ``review_ref`` and ``reviews``. Factors use
``conversion_factor``. Review classifications and evidence are retained; a
selection confidence is not a quantified measure of LCIA equivalence. See
:doc:`../migration-resource-schema` for the full contract.

All 11,747 packaged UVEK 2025 identities have a disposition: 1,236 reviewed
correspondences, 268 documented proxies, eleven public supporting recipes, one approved omission, and
10,231 unresolved identities. All 1,516 approved source identities are reachable
from UVEK 2026 through its explicit release bridge. Most of the catalog still needs substantive
directional evidence review. The unresolved disposition is not a scientific
review or approval of its suggested candidates.

Version ``1.0.0a11`` expands coverage from 354 to 1,424; ``1.0.0a10`` contains
the initial 354 rules. No existing rule, proxy or supporting recipe was replaced.

Independent sewage-gas operating reconstruction
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

UVEK's ``Biogas, from sewage sludge, at storage`` retains operating burdens per
cubic metre of gas. Its independently authored helper uses ecoinvent 3.12
cut-off plant infrastructure, generic inorganic chemicals, Swiss low-voltage
electricity and industrial natural-gas heat, retaining UVEK's own methane and
carbon-dioxide releases. Industrial heat uses Europe without Switzerland as a
geographic proxy. These differences are reported; equal LCIA scores are not
established. The empty allocated ecoinvent biogas branch is not an operating
substitute, and gas volume is not converted to sludge volume. Supported older
ecoinvent targets still require acceptance of the reported reverse release
bridges and exact final-catalog validation. The forward cut-off sludge and gas
allocation boundaries are described in :doc:`ecoinvent-to-uvek`.
This operating recipe does not authorize a general inverse of the forward
sludge helpers: each supplier inside a generated helper still needs its own
reviewed reverse correspondence. For example, the standalone UVEK digester
plant and several regional low-voltage supplies remain outside this reverse
catalog's approved coverage.

Independent reverse gas-combustion reconstruction
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Four later independently reviewed correspondences extend that 1,424-source
baseline. The RER UVEK blast-furnace-gas and coke-oven-gas burners are represented
by helpers combining allocated German ecoinvent 3.12 cut-off electricity and heat
outputs. Per MJ of burner service, the blast-furnace helper uses 0.101388888888889
kWh plus 0.145 MJ of heat; the coke-oven helper uses 0.11 kWh plus 0.145 MJ of heat.
Each weighted combination consumes one MJ of gas in the inspected inventories.

The blast-furnace helper additionally subtracts one MJ of gas supply because
the UVEK burner excludes upstream fuel production. The coke-oven helper retains
fuel production because its UVEK burner includes it. A separately recorded
negative coke-gas input, including the forward waste-gas helper's correction,
therefore cancels purchased fuel exactly once. Caller quantities, their signs,
uncertainty and formulas remain unchanged; separately listed energy credits
remain separate.

The other two correspondences represent GLO coke-oven-gas supply with the
ecoinvent GLO coal-gas supply market and RER industrial heavy-fuel-oil heat with
the matching Europe-without-Switzerland furnace service. All four require the
information-loss policy to allow or warn about documented proxies. Warnings
identify the geographic, allocation, technology and emissions differences;
these reconstructions do not establish LCIA equivalence. Include both forward
and reverse generated helpers in scoring and exports. Independent inventory
fingerprints and boundaries are retained in
``brightpath/data/export/uvek_reverse_industrial_gas_review.json``; the earlier
publisher baseline remains protected by its original fingerprint.

A further 37 matching-region high-voltage electricity supply proxies cover the
regional energy credits used by the gas helpers. Both source and target have a
positive one-kWh reference and the same region; each ecoinvent endpoint is the
unique high-voltage market or market group for that region. Their supply mixes,
imports, losses, infrastructure and emissions differ and warn. These screening
proxies do not establish marginal-generation equivalence. They were inspected
independently and retain both directional inventory fingerprints. Earlier
correspondences and helper outputs remain unchanged.

Independent physical coproduct supply
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Six exact positive-kg UVEK product references have independently inspected
correspondences to ecoinvent 3.12 cut-off allocated coproducts. They preserve
one kg, the incoming sign, supported uncertainty and formulas with factor +1.
The activity name does not determine the product: DDGS maps to the allocated
DDGS reference of an ethanol-production activity, not its ethanol output.

.. list-table::
   :header-rows: 1
   :widths: 42 58

   * - UVEK product and region
     - Ecoinvent 3.12 cut-off product and producer
   * - Corn DDGS, US
     - DDGS from maize-ethanol production, US
   * - Rye DDGS, RER
     - DDGS from rye-ethanol production, RER
   * - Potato DDGS, CH
     - DDGS from potato-ethanol production, RoW
   * - Grass fibres at digestion, CH
     - Grass fibre from biogas production, CH
   * - Grass fibres at fermentation, CH
     - Grass fibre from ethanol production, CH
   * - Sweet-sorghum grain, CN
     - Sweet-sorghum grain from cultivation, CN

Five retain the named region. The catalog has no Swiss potato-DDGS supplier;
that correspondence explicitly warns about the RoW proxy. Economic allocation,
drying/process energy, agricultural inputs, infrastructure, freight and
water/emissions differ. UVEK DDGS is per wet-basis kg; an identical moisture
fraction to ecoinvent's 90% dry matter is not independently quantified. Both
sorghum inventories document 9.1% grain moisture. No arbitrary dry-matter,
nutritional or market-mix reconstruction is introduced, and equal LCIA scores
are not established. The proxy and information-loss policies apply.

The six pairs were selected and read independently of forward mapping inversion;
all have single positive one-kg references. Their source/target inventory and
boundary-comment hashes are bound in
``data/export/uvek_cutoff_feed_product_review.json``. The strict batch projection
protects the original publisher baseline. UVEK 2026 reaches these products
through its explicit release bridge. These correspondences do not reconstruct
the many-to-one forward supplier geography or original market mixtures, and do
not undo approved nutritional omissions. Foreground suppliers, production and
separately recorded emissions remain protected.

Swiss beet-pulp and whey-yeast products
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Two further independently inspected positive-kg correspondences extend the
physical-product batch. UVEK Swiss sugar-refinery pulp maps to the Swiss
``sugar beet processing`` / ``sugar beet pulp`` reference in ecoinvent 3.12
cut-off. Both refinery-pulp comments specify 25.6% dry matter. UVEK Swiss whey
yeast paste maps to the Swiss ``ethanol production from whey`` / ``fodder yeast``
reference; legacy co-production descriptions explicitly identify yeast paste.

Both preserve one kg, incoming signs, supported uncertainty and formulas with
factor +1. Allocation, energy, crop inputs, whey collection, water/emissions,
infrastructure and older background models differ. The Swiss whey names can
represent Irish technology, and identical yeast-paste dry matter or nutrient
values are not independently quantified. No moisture or nutritional conversion,
original market mix or omitted credit is reconstructed. The proxy policy warns
or rejects consistently with the other product correspondences.

The first six-rule batch remains protected byte for byte. The strict legacy
projection accepts only the complete earlier six or complete extended eight
rules, and validates their exact independent evidence. Native UVEK 2026 reaches
both products through the explicit release bridge.

Ground-granulated slag without upstream raw-supply burdens
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The UVEK RER ``Ground granulated blast furnace slag, no burdens, at plant``
product has an independently reviewed positive-kg reverse helper. It uses one
kg of ecoinvent 3.12 cut-off US ground-granulated-slag production and subtracts
that process's exact 1.0011225250000004 kg input from the US raw-slag market.
The raw market supplies zero-production-burden cut-off stock plus freight.
Removing that input retains processing while excluding raw supply and haulage,
matching the inspected UVEK boundary. Processing itself retains energy, water,
emissions, infrastructure and disposal burdens.

Independent read-only checks of both native-2026 processes confirm the same
raw-supply boundary: zero raw-slag input for no-burdens and 1.0014 kg for
with-burdens, each with a positive one-kg reference.

The US process is an explicit technology/geography proxy for the RER product.
Yield, energy mix, gas units, material grade, moisture, granulometry, emissions,
water, transport and background models differ; no equal material performance or
LCIA score is inferred. Caller quantities, signs, supported uncertainty/formulas
and foreground ownership are preserved. Native UVEK 2026 reaches this source
through the explicit release bridge. All generated helpers must be scored and
exported. The strict publisher-baseline projection validates the new rule,
recipe and review exactly before removing this independent addition.

The UVEK ``with burdens`` variant adds allocated raw-slag production and extra
haulage. It remains unsupported in this reverse route; a zero-production-burden
cut-off stock cannot reproduce that allocation by an inferred product inverse.
Negative-reference slag treatment and constrained supplementary-cementitious-
material credits are outside this physical-product review. Complete forward-generated
helper hierarchies also retain existing reverse freight gaps: independent 3.6
and 3.12 checks, in both releases, fail transactionally on unsupported truck or
freight-train suppliers. The new operating-product rule does not approve those
transport inverses or hide a partially converted hierarchy.

Independent refinery-gas flaring service
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The exact GLO/MJ ``Refinery gas, burned in flare`` reference has an independently
reviewed reverse correspondence to ecoinvent 3.12 cut-off's
``treatment of waste refinery gas, burned in flare``. UVEK's positive native
product output describes a physical combustion service; ecoinvent's waste
treatment reference is negative. Factor -1 changes reference coordinates while
preserving the incoming MJ magnitude, uncertainty, formulas and caller metadata.
No helper or heating-value conversion is required.

Both processes cover direct flare emissions and exclude upstream fuel, flare
infrastructure and recovered-energy credits. Ecoinvent substitutes nine active
biosphere exchanges for UVEK's ten. Emission factors, pollutant coverage,
speciation and underlying plants differ; climate, toxicity and other LCIA
equivalence are not established. The documented proxy warns and preserves
separately listed foreground emissions. Exact source and target fingerprints
are retained in ``data/export/uvek_reverse_industrial_gas_review.json`` as a
separate complete extension batch. It does not approve natural-gas flares or
other product-to-treatment conversions. Forward flaring rules and the reverse
endpoint were reviewed independently; their round trip preserves quantities.

Independent sweet-natural-gas production flaring
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The exact GLO cubic-metre ``Natural gas, sweet, burned in production flare``
service independently maps to ecoinvent 3.12 cut-off's negative-MJ treatment of
sweet natural gas in a production flare. Factor -36 preserves one cubic metre
as 36 MJ while changing reference coordinates. Both inspected inventories
withdraw one standard cubic metre of natural gas for that quantity. Resource
withdrawal remains in flaring; neither process contains purchased fuel,
infrastructure or energy credits. This is an explicit directional review,
not an automatically inverted forward mapping.

Ecoinvent substitutes eight biosphere exchanges for UVEK's nine. It includes
oxygen balancing and omits UVEK's nitrous oxide and waste heat; CO2/NMVOC
coefficients and observation periods also differ. Neither flare contains CH4,
NOx or SO2; modern ecoinvent comments account for these in main production.
Full parent production inventories and LCIA scores are not claimed equivalent.
Warnings retain those limitations. Caller signs, supported uncertainty,
formulas and separately recorded emissions are preserved. Native UVEK 2026's
``normal cubic meter`` identity reaches this rule through the explicit release
bridge. Exact inventories and activity comments are bound in
``data/export/uvek_natural_gas_flare_review.json``. Older sweet-gas and all
sour-gas profiles remain outside this review.

Same-region ash-treatment proxies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Twenty-three independently inspected ash services have explicit reverse rules:
11 hard-coal-ash residual-material-landfill references and 12 lignite-ash
opencast-refill references. Each endpoint retains the source region, ash type,
kg quantity and disposal route. UVEK has a positive Brightway production with a
native waste-input reference; ecoinvent has a negative treatment reference, so
the factor is -1. Caller uncertainty and formulas scale with that sign.

UVEK specifies 0% water; the inspected ecoinvent documentation reports H2O as
unavailable. The mapping preserves kg without moisture normalization and warns
that identical moisture is not established. Underlying Swiss infrastructure,
ash composition, controls, emissions and long-term fate models can also differ.
These documented proxies require an information-loss policy. Separate source
and target fingerprints are recorded in
``brightpath/data/export/uvek_reverse_ash_treatment_review.json``. Whole-batch
guards retain the earlier publisher baseline; no inverse is inferred.

Biowaste collection: approved reverse omission
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``Biowaste, at collection point`` / CH / kilogram is a positive-reference
material supply. The inspected UVEK 2025 source (``bafu-405466``) has two active
collection/transport suppliers and a ``Carbon dioxide, in air`` resource uptake.
The former ecoinvent 3.12 cut-off candidate, ``market for biowaste`` / CH /
kilogram, has a negative Waste reference and supplies a mix of composting,
anaerobic digestion, incineration and transport. These functions are different.

The approved reverse conversion omits this external input and continues with
``migration.technosphere_omission`` at warning severity. The warning explicitly
states that collection transport and atmospheric CO2 uptake contributions are
also omitted. Their effect on the score is method-dependent and has not been
quantified; this is not a burden-free or scientifically equivalent substitute.
The full original exchange, amount, uncertainty, formula and other metadata are
retained in the loss report. Separately listed foreground uptake/emissions,
production exchanges and foreground biowaste suppliers are preserved.

The rule is an audited ``delete`` entry with ``approved_omission: true`` and
shared rationale/evidence. It always warns regardless of generic deletion or
information-loss policies. Ordinary deletions remain unsupported in the reviewed
recipe exporter. Unknown suppliers and unrelated errors retain their existing
behavior and roll back the complete operation. The omission is carried through
the UVEK 2026 release bridge and supported older ecoinvent routes. Repeated
conversion does not recreate or remove the exchange again.

The historical publisher correspondence and reference signs remain in the
authoring audit; the resource no longer advertises the treatment-market target.
Coverage explicitly counts the omission. Regeneration preserves the review and
does not restore the old substitution. The independent ecoinvent-to-UVEK
biowaste mapping is not changed or automatically omitted by this decision.

The runtime resource stores approved rules, required supporting recipes, and
coverage counts. Unresolved identities are the source catalog entries without an
approved rule. The generator lists them with reasons and candidates in a separate
full-catalog review, using ``docs/uvek_export_review.json`` by default or the
explicit ``--review-output`` path. Regenerate that local review when resources
change; it is not a packaged runtime input. Unlisted suppliers still fail
conversion under every policy. The seven public recipes contain quantities
needed to generate foreground helpers; they are not background inventory dumps.

The initial approved evidence comes from the IEA PVPS T12-33:2026 conversion
ledger and public supporting recipes, plus three explicitly documented freight
proxies. European and Swiss weight-specific freight may fall back to a European
unspecified-size diesel fleet market; payload, vehicle size and Swiss geography
are not reproduced. Those coarse decisions are warnings, not equivalent matches.

``scripts/generate_uvek_export.py`` regenerates the resource, catalog review,
and integrity manifest from ``docs/uvek_export_evidence.json`` and
``docs/uvek_export_overrides.json``, supplemented by
``docs/uvek_export_lineage_evidence.json``. Optional explicit ``--pv-*`` inputs rebuild
the evidence from local source artifacts. Neither execution nor ordinary resource
generation needs a local premise checkout. Candidate suggestions from the
ecoinvent-to-UVEK mapping never become approved rules automatically.

Publisher correspondence review
--------------------------------

The additional 1,070 correspondences use ecoinvent's public
`Correspondence File for Intermediate Exchanges <https://support.ecoinvent.org/ecoinvent-version-3.0>`_.
This is a BrightPath-authored reference-function crosswalk, not a
publisher-certified UVEK-to-ecoinvent mapping. The workbook describes the legacy
ecoinvent nomenclature; matching a UVEK label to it does not establish common
dataset UUIDs, inventory coefficients, allocation or LCIA results.

An addition must satisfy every check:

* Source activity and product labels both match the legacy product label, using
  only case and whitespace normalization. Unit and geography must match exactly.
* Exactly one publisher row marks the reference product with ``x``. Multi-output
  rows and every flagged change are excluded, including reference quantity, unit,
  geography, deletion, input/output role and replacement changes.
* Exactly one ecoinvent 3.12 cut-off entry matches the published activity and
  product names, geography and unit. Its reference-product UUID must be unchanged.
* Labels flagged as waste, recycling or secondary-material cases remain deferred for additional
  sign and system-boundary review, even if their metadata otherwise matches.
* Existing reviewed sources take precedence and remain byte-equivalent after
  projecting the original rule values and reviews. No forward heuristic mapping
  is inverted, and no geographic fallback or unit factor is inferred.

The :download:`authoring ledger <../uvek_export_lineage_evidence.json>` records the exact publisher row, catalog digests, workbook
digest and excluded-cohort counts. Its 1,070 new rules execute under strict policy
from both UVEK releases while retaining uncertainty and caller-owned input. A
technosphere-only conversion preserves its explicitly selected biosphere; full
native-biosphere conversion still has the separate limits in :doc:`uvek-2026`.

To reproduce the review from the public workbook::

    python scripts/generate_uvek_export.py \
      --lineage-workbook /path/to/correspondence_file_intermediate-exchangesv2.2_to_v3.0_20130904.xlsx \
      --review-output /tmp/uvek-export-review.json

The workbook SHA-256 must be
``cd598b9c357a1c0f55ca5e00fc9861a753cbdce772a07dd43e8acffcb8f2b959``.
The workbook and proprietary inventories are not packaged. Subsequent runs can
replay the checked-in evidence without downloading it again; stale catalogs,
changed original rules and inconsistent evidence are rejected.

The distinction between mapping coverage and scientific equivalence also follows
the `LCA wiki mapping guide <https://github.com/sentier-dev/lca-wiki/blob/main/core/use-cases/map-flows-between-nomenclatures.md>`_
(source IDs ``ecoinvent-kb-glossary`` and ``lca-wiki-editorial``).

Generated foreground helpers
----------------------------

One-to-many recipes create export-only foreground datasets. The original datasets
remain first in ``converted.data``; required helpers are appended in stable order.
Consumers keep their original quantity and uncertainty on the helper exchange.
Recipe quantities and uncertainty are recorded inside the helper. Helpers are
deduplicated, carry deterministic codes and ``brightpath_conversion`` provenance,
and are validated against the target profile. Applications must export the entire
returned inventory, not only ``converted.data[0]``.

Original dataset identities, production exchanges, and foreground links remain
unchanged. Migration works on a copy and rolls back on error. Changed units or
waste signs require explicit factors, including consistent uncertainty scaling.
Only uncertainty types 0 through 5 support non-unit scaling; other cases fail
rather than preserving inconsistent distribution parameters.

Reports record each applied rule and helper, proxy limitations, source/target
catalog digests, and resource provenance. The public
``brightpath.background.migration_resource_fingerprint()`` identifies the engine
revision and resource manifest for application caches. Increment the execution
revision when changing migration semantics without a package-version change.

Only foreground inventories are exported. Users must install their licensed
ecoinvent 3.12 cut-off background separately. Regional water detail already lost
during source normalization is not reconstructed by this conversion.

PV regression baseline
----------------------

The source-backed 317-dataset PV draft converts to 322 datasets, including five
required helpers. All 1,577 original foreground links remain unchanged, and all
7,334 resulting exchanges pass the checked Excel identity/amount/uncertainty
round trip. CLIC exports the complete package as Brightway Excel, SimaPro CSV,
openLCA JSON-LD and Brightway JSON 1.1.

After expanding supporting recipes, 286 of the original 317 datasets match the
previously prepared ecoinvent workbook's aggregated exchange identities and
amounts. The remaining 31 differ in exactly two documented groups: 19 freight
supplier choices affected by the canonical UVEK proxies, and 12 aqueous copper
flow labels (``Copper ion`` rather than ``Copper``). Both copper names exist in
the target catalog; their characterization equivalence is not established by
link validation. No values were adjusted to force agreement. This comparison
does not establish LCIA equivalence or validate the scientific proxy assumptions.

The complete CH electric rail-freight correspondence includes shunting, rolling
stock and track infrastructure on both sides. It preserves ton-km with factor
+1 and warns about different energy coefficients, vintage, assets and background
supplies. It is independently reviewed; it does not invert an ash-market helper.

Sewage-digestion component suppliers
-------------------------------------

Eight independently inspected supplier correspondences support complete
forward-generated digestion helper graphs. The CH plant keeps the matching
material quantities and 30-year building / 10-year machine lifetimes, with
factor +1 per plant unit. UVEK's explicit construction freight and disposal
and ecoinvent's material-market / waste-service conventions differ; no capacity,
sludge-volume or gas-yield scaling is applied.

AU, NZ and RU low-voltage electricity use the same countries' complete markets.
RAF, RAS, RLA and RNA use Africa, Asia, Latin America/Caribbean and Northern
America market groups, whose contained markets include delivery and losses.
Preserve signed delivered kWh and the caller's original regional quantities.
Do not add network infrastructure, SF6 emissions, voltage transformation or a
second loss multiplier. Supply composition, trade, loss allocation, regional
aggregation and vintage remain explicit background approximations. These rules
were reviewed independently of the forward digestion-market aliases; complete
helper graphs can reverse without assuming an inverse sludge-to-gas relation.
The review binds 80 source, supplier and target inventory fingerprints.

Independent residual-landfill construction and material suppliers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Seven separately inspected supplier correspondences cover complete hydraulic
excavation, Swiss quarry-gate round gravel and sand, allocated Swiss/European
refinery bitumen, complete RER PVC supply, and Swiss delivered tap water. Signed
kg or cubic metres retain factor +1. Quarry outputs avoid adding a delivery
market; refinery outputs retain allocated petroleum processing and crude supply.
PVC remains complete resin rather than a polymerization-only service. Delivered
water retains treatment and network burdens without a mass-to-volume conversion.
Individual proxy warnings describe allocation, conflicting quarry-share
annotations, machine/emission vintage, geography, material supply and delivery
boundaries. These independently authored mappings do not reverse the forward
landfill recipes or infer an inverse from generic native water-emission labels.
