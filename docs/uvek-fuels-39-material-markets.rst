Eight material markets for UVEK fuel preparation
===============================================

The inspected ecoinvent 3.9 cut-off markets retain all 22 source producer rows
and 29 delivery rows, without merging or normalizing them. Complete native
production stocks replace the individual source production inventories as
screening proxies; those source production technologies and emissions are
not claimed to be reproduced. Original market quantities, uncertainty,
formulas, bounds and pedigree remain. The eight helpers introduce no source
input omission, recovery credit or new zero-burden supplier.

The corrections and limitations are:

* Perimeter-insulation polystyrene uses complete native expanded-polystyrene
  slab. Source density/conductivity are 33 kg/m3 and 0.033 W/mK; native values
  are 30 kg/m3 and 0.035-0.04 W/mK. Stock mass is retained without a density
  multiplier or a claim of equal insulation service per area.
* Chromium-flake source product information names chromium(III) oxide, while
  its actual wet-process producers describe dark-red flakes and use the same
  1.34 kg sodium dichromate / 1 kg sulfuric acid inputs as native CrO3 production.
  This matches the recorded production stock, with an explicit source-label
  inconsistency warning. It does not establish caller intent or authorize a
  general Cr2O3-to-CrO3 substitution. No oxidation-state multiplier is inferred.
* Magnesium oxide uses the complete native MgO process, including its recorded
  fossil-CO2 emission. Native mining/processing approximations, grade and
  incomplete emissions remain limitations; no theoretical calcination correction
  is added on top of that complete inventory.
* Class-1 nickel retains eight producer entries and four freight modes, but
  each producer uses the native global 1994 Class-I mix. Modern production,
  co-product and recycled routes are replaced, not reproduced. Native nominal
  purity is 99.5% versus source 99.70% or better. Native documentation warns
  against relying on this generic model where nickel impacts are important;
  sensitivity/detail review remains necessary in those cases. No purity factor,
  additional refining or copper/recycling credit is invented.
* Chemical-factory stock uses **Facilities, chemical production, per kg**,
  rather than a complete chemical plant per unit. Nine independent source/native
  quantity comparisons agree, including both steels, combined concrete,
  electronics, solder, inorganic chemicals, rock wool, electricity and heat.
  The former plant-unit candidate contains 12.6 million kg of facilities plus
  buildings and disposal. No kg-to-plant, lifetime or capacity multiplier is
  appropriate. Native heating and internal transport still differ. Separate
  unit-based organic-chemical-plant correspondences are unchanged.
* Molecular iodine uses complete native I2 stock per kg, with production-route,
  geography, age, energy, allocation and emissions limitations. No solvent or
  atom/molecule multiplier is introduced.
* Crushed limestone for mill uses the matching complete Swiss process per kg.
  Shared annual-capacity documentation does not justify a second infrastructure,
  capacity or lifetime rescaling. Dust, energy and infrastructure differences
  remain explicit.
* Flexible polyurethane retains the source 20% MDI / 40% high-density TDI /
  40% low-density TDI entries and nine freight rows. Native flexible PUR is a
  TDI/polyol formulation. Its use for all three entries is a whole-product
  screening proxy: MDI chemistry, viscoelastic performance, density grades,
  additives, blowing agents, losses, emissions and waste fates are not preserved.
  No extra source processing inventory or density/isocyanate multiplier is added.

Geographic, vintage, allocation, delivery, internal-uncertainty and biosphere
limitations have unquantified score effects. Source caller emissions listed
separately remain separate. Raw licensed inventories and comments remain
private; frozen evidence contains identities, hashes, basis comparisons and
derived native stock/delivery recipes.

The existing 3.12 nickel behavior is preserved exactly. Only its new 3.9 profile
selects a market-delivery helper; the previous global rule, selected 3.12 rule,
native target, factor, shared review and warnings stay unchanged. Because the
approval fingerprint binds the full multi-profile rule, Premise refreshes that
one existing fingerprint after checking this equality; it does not reapprove
other correspondences or waive their checks.

Evidence lives in ``data/export/uvek_fuels_39_material_markets_review.json``.
Maintainer replay checks exact parents, source/profile references, transport
bindings, preserved nickel behavior and recipe collisions before mutation.
The historical deferred-material replay test restores its original nickel
stage only after proving unchanged 3.12 behavior; current complete replay and
new 3.9 conversion tests separately retain the new profile.

Strict fresh/cached migration, quantity/uncertainty retention, native-matrix
LCIs, complete generator replay, wheel contents and licensed preparation are
recorded in the companion Premise verification receipt. A passing helper LCI
or accepted correspondence does not qualify prospective fuel transformation
or export. No runtime Python module change is required.
