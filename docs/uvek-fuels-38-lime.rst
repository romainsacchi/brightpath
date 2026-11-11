Inspected 3.8 lime delivery
==========================

The RoW lime market is qualified only for ecoinvent 3.8 cut-off. Its legacy
quicklime description conflicts with the actual uncalcined limestone-grinding
inventories. The previous hydraulic-lime match is replaced by a delivery helper
that retains all three producer shares and three freight rows separately,
including signed statistics, source order and uncertainty. Nothing is omitted
or renormalized.

Two grinding branches use complete native Swiss milled limestone as a screening
proxy. The zinc-mining co-product uses a separate allocated-mining helper:
0.59 * (0.117 / 0.38) kg of native Swedish contained-Zn beneficiation per kg of
source co-product. All 263 nonproduction source coordinates support the price
ratio; 262 lognormal coordinates retain scale and pedigree with the appropriate
log-location shift. This does not assert native uncertainty equivalence.

Warnings retain the source naming inconsistency, geography, ore, allocation,
energy, fleet, infrastructure, internal uncertainty and vintage differences.
There is no added metal recovery credit, calcination, purity multiplier or
chemical mass conversion. Score effects remain unquantified.

Frozen evidence is packaged in
``export/uvek_fuels_38_lime_review.json``. The authoring replay rejects changed
parents, helpers or retained statistics. Native single-supplier and flattened
whole-delivery LCIs are finite; these checks do not prove a complete fuel-sector
update or export. Raw licensed source inventories remain private.
