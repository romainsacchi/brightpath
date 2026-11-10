Oxide delivery and six native services
=====================================

Nine independently inspected ecoinvent 3.9 cut-off correspondences extend
UVEK fuel preparation. They introduce six oxide delivery helpers and direct
native matches for high-voltage electricity, useful boiler heat and four
waste-treatment services. No runtime Python change, physical source-input
omission, new zero-burden supplier or recovery credit is introduced.

Alumina and copper/zinc oxides
-----------------------------

All 35 market/import input identities, units and quantities independently match
the older reviewed native delivery bindings. Uncertainty, formulas, pedigree,
bounds and parameters are read anew from the actual 3.9 inventories.

European metallurgical alumina retains four domestic/import shares and four
market delivery rows. Its three import helpers remain distinct, each with one
kg of complete native alumina production and four original freight entries.
International import shipping and market-level delivery are separate stages.
Native production already includes bauxite extraction, hydroxide production,
calcination and upstream raw-material transport; these are not added again.

Copper oxide retains two producers and four delivery modes, using complete
native solid copper(II) oxide production. Zinc oxide retains its two producers
and four delivery modes, using complete native ZnO production. Neither maps to
the metal or receives a contained-metal, oxidation-yield or purity multiplier.
Native feedstocks, grades, process emissions, allocation, geography, internal
freight, uncertainty and vintage remain screening limitations with unquantified
score effects. These proxies do not reproduce the source production inventories.

Grid, boiler and treatment references
------------------------------------

* RER high-voltage electricity uses the complete native grid per kWh, retaining
  native signed exchanges, losses, network infrastructure and grid emissions.
  The native 2023 country/generation/import mix replaces the source historical
  regional mix. No new voltage branch, loss factor, clipping or IAM algorithm
  is introduced; shared Premise updates continue to act on native suppliers.
* Condensing, modulating natural-gas boiler heat above 100 kW uses complete
  native useful heat per MJ, including fuel, boiler, operating electricity and
  direct emissions. Source Europe-without-Switzerland is geographically proxied
  by native RER. No second calorific, efficiency or capacity factor is applied.
* Polypropylene incineration uses one kg wet waste. Both inventories describe
  H2O 159000 ppm, 15.9% water by wet mass, and the same heating-value coordinates.
  Source properties give 0.841 kg dry matter and 0.159 kg water per kg wet waste.
  The separate ``water content`` value is water/dry matter, not wet-water share.
  Negative source kg becomes positive native service kg with factor -1.
* Brick collection/final disposal uses one kg inert waste, with the same
  documented 1600 kg/m3 density. The source collection process includes its
  final-disposal market input, so the complete native chain is used once.
  Factor -1 changes reference convention; no density or extra-landfill factor
  is introduced.
* Dry mineral-wool landfill uses one kg dry waste and a complete native
  construction-waste landfill service, factor -1. Swiss site, sealing, leachate,
  land-use, infrastructure and vintage assumptions proxy the European source.
* Limestone residue is a composition/moisture proxy: source has 0.5% water by
  wet mass, while native explicitly states H2O 50000 ppm, 5% water. The original
  wet-waste kg remains the service basis, with factor -1. A dry-load ratio is not
  inferred to add target waste/water mass. Dry loading, paper-residue composition,
  transport, landfill, leaching, geography and age differences are warned, with
  unquantified score effects. Source moisture is not silently relabeled as 5%.

All direct matches retain complete native inventories. Separately listed caller
emissions remain separate; source process exchanges are not added on top of the
native treatment, grid or boiler inventory. Signed caller quantities, formulas
and uncertainty are preserved with their reviewed reference factors.

Preserved approvals and verification
-----------------------------------

The earlier 3.8 oxide selected rules/shared reviews and all six earlier delivery
recipes are unchanged, as is the 3.6 polypropylene selected rule. Four full-rule
fingerprints change when their new exact 3.9 profiles are added. Premise refreshes
only these fingerprints after proving unchanged selected behavior, reviews and
complete legacy helper closures. No other release/model approval is inferred.

Evidence is frozen in ``data/export/uvek_fuels_39_oxide_services_review.json``.
Licensed raw inventories/comments remain private; public data contains derived
native recipes, source identities, reference/moisture facts and inventory hashes.
Maintainer replay rejects drift before mutation. Historical authoring tests
restore their own earlier profile tables only after proving unchanged selected
behavior; current complete replay and new-profile tests cover the extension.

Strict fresh/cached conversion, signs, uncertainty, oxide shipping, moisture
facts, old-profile preservation, native LCIs, complete replay, wheel contents
and actual licensed preparation are recorded in Premise's companion receipt.
A finite native LCI or accepted correspondence does not qualify a prospective
fuel update or database export.
