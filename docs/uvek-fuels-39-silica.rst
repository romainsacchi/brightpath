Activated silica and reviewed native ingredients
===============================================

The inspected ecoinvent 3.9 cut-off activated-silica process makes a dry,
chemically precipitated product. Mined silica sand does not provide that
production process. This review supplies one explicit inventory helper per kg
of activated silica, using native UVEK ingredients and the previously reviewed
steam and wastewater helpers. It applies to UVEK with the ecoinvent 3.10
biosphere; it does not approve another source release or a consequential route.

What the reconstruction retains
-------------------------------

* All 14 source input rows, in order, including the separate RER/RoW silicate
  purchases, acid, heat, electricity, infrastructure, water and wastewater.
* All five direct water emissions/resource withdrawals, with the same publisher
  UUIDs, compartments, units and signed quantities in the canonical 3.10 catalog.
* All 19 lognormal distributions and their recorded statistics/pedigree fields,
  plus four local parameters. No coefficient renormalisation or new chemical
  yield, concentration, neutralisation or water-balance correction is inferred.
* Four originally negative wastewater demands become positive treatment-service
  inputs through the already reviewed factor -1. The 13 existing steam and
  wastewater helper recipes remain unchanged and are independently fingerprinted.

The existing native wastewater model includes empty recovered-electricity and
recovered-heat suppliers. Their documentation explicitly allocates 100% of waste
incineration burdens to disposal and 0% to energy. These two suppliers remain
only inside the unchanged, fingerprinted native wastewater helper. They are not
new zero-inventory substitutes, new zero-burden recipes or extra recovery credits.

Three ingredient decisions
-------------------------

The RER solid-silicate market now uses complete furnace-process solid sodium
silicate instead of 80% spray powder. Primary source/native inventories agree on
the sand, soda and steam quantities per kg. The shared erroneous phosphate prose
and generic chemical metadata do not justify a different physical product or a
concentration/hydrate multiplier.

The global organic chemical-plant market uses one complete native plant unit,
without division by its lifetime or annual chemical output. The RoW sulfuric
acid market uses one kg of complete native 100%-active acid. Its liquid title
does not justify adding solvent or concentration scaling. This primary-acid
proxy replaces a source portfolio that includes recovered/co-product supplies.

Native geography, heat-network versus industrial-heat infrastructure, chemical
grade/modulus, allocation, vintage, market delivery, internal uncertainty and
biosphere coverage differ. Score effects are unquantified. Retaining the source
coefficients and parameters is not a new stoichiometric or LCIA-equivalence claim.
Ingredient decisions inside the closed helper do not implicitly approve those
source suppliers for other foreground inventories or source versions.

Verification and scope
----------------------

The frozen evidence is ``uvek_fuels_39_silica_review.json``. It contains the derived
native recipe, reference/retention checks and primary inspection hashes, not
original proprietary source inventory files or comments. A maintainer-only
replay stage checks parents, exact ingredient bindings and existing helper
closures before mutation. No runtime Python module is changed.

The endpoint/storage/silica group passes 65 tests and an additional strict-target
check passes independently. Premise's fuel-mapping group passes 41 checks, with
extra full-row statistics comparisons passing in both tools. All 575 prior
Premise scopes retain their original fingerprints. Full generation is
byte-identical and the wheel contains the exact review, resource and manifest.

A linear expansion of the helper and all frozen children has 38 native supplier
demands and 130 distinct direct biosphere identities. All direct biosphere
identities match the installed 3.10 biosphere. The calculation against the actual
BAFU matrix produces finite supply and combined inventory results for 1 kg.
No Brightway database is written; the source activity count and modified
timestamp remain unchanged. These are inventory-preparation and native-matrix
checks, not a successful prospective fuel-sector update or export.

The reference-sign regression suite passes all 733 tests. Its historical
observation/count test now replays the original 996-rule sign/material stage
without changing any count, sign or quantity assertion. A separate full replay
compares the complete current runtime resource with every frozen authoring
stage. The old stale-count failure is repaired without skipping tests, relaxing
validation or substituting the historical artifact for runtime coverage.
