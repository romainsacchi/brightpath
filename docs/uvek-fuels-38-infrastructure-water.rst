Fuel infrastructure and deionised-water review
==============================================

Three inspected ecoinvent 3.8 cut-off construction references use complete
native UVEK installations as geographic screening proxies. One source unit
remains one complete installation, with signed caller quantities, uncertainty
and formulas preserved:

* Ethanol fermentation plant: 90,000 tonnes per year, 20-year lifetime.
* Organic-chemical storage: four 4,000 m³ tanks, 16,000 m³ total, 50-year lifetime.
* Biomass synthetic-gas plant: 5 MW thermal output, 50-year lifetime.

Capacity and lifetime describe the installation; they are not conversion
multipliers. These mappings retain native construction and dismantling once.
They do not add plant operation, divide by lifetime or claim score equivalence.
Geography, materials, allocation, internal uncertainty, native biosphere
exclusions and vintage remain explicit limitations. The gasifier is a Swiss
pilot-derived design, and the tank model is a weak-data fuel-storage-derived
approximation.

The separate deionised-water check confirms kg water and ion-exchange treatment,
with a finite native LCI. Its exact 3.8 reference-sign profile remains pending;
this review does not authorize that mapping or substitute ultrapure water.

The packaged ``uvek_fuels_38_infrastructure_water_review.json`` contains derived
reference facts and source witness hashes. Raw licensed inventories remain
private. Existing migration rules and native inventories are unchanged. Passing
these component checks does not qualify the complete fuel update or export.

Separate water profile qualification
-----------------------------------

The later ``uvek_fuels_38_water_profile_review.json`` resolves that pending
reference-sign decision for **ecoinvent 3.8 cut-off only**. The market, its
producer and native ion-exchange water each declare positive one kg. The added
profile therefore uses factor +1, retaining signed caller kg and statistics.
The historical inspection packet remains unchanged. Earlier 3.9 and 3.10
selected decisions are preserved; uninspected releases remain refused.

The native Swiss process is a geographic and raw-water-quality screening proxy
for Europe without Switzerland. It does not reproduce source regeneration,
utility or wastewater inventories and is not a claim of ultrapure-water
performance. No density, purity, concentration or extra treatment factor is
introduced. Original native upstream biosphere exclusions and unquantified
score differences remain explicit warnings. No runtime Python module changes
are needed; the authoring utility replays only the exact profile extension.
