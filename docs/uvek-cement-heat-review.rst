European gas heat for UVEK cement preparation
============================================

The inspected ecoinvent 3.9 cut-off market for natural-gas district/industrial heat
in Europe without Switzerland supplies useful heat at the plant. Its former
heuristic selected native district heat at the consumer, introducing a network
and delivery boundary that the source market does not contain.

The exact-profile replacement composes the source market from four complete
native useful-heat services. It retains the inspected aggregate technology
quantities from all 54 positive supplier references without normalization:

* Conventional and combined-cycle large CHP use native ENTSO-E gas-CHP heat.
* The 1 MWe lean-burn engine uses native exergy-allocated 1 MWe engine heat.
* Ordinary industrial furnaces use the native 1 MW industrial-furnace service.
* Low-NOx industrial furnaces use the corresponding native low-NOx service.

All source and inspected native references are positive one-MJ heat outputs.
Native allocated fuel demand is retained upstream, not inverted into an efficiency
or substituted for useful-heat output. No district network, extra loss factor,
added pumping or co-product credit is introduced. The original caller's signed
quantity, formula and uncertainty are retained.

This is a screening reconstruction. Native process size, cycle, heat quality,
allocation, geography, vintage and inventory limitations differ. Source countries
are represented through aggregate technology quantities and explicit native
ENTSO-E/CH/RER proxies; country-specific inventories and internal source parameters
or uncertainty are not reconstructed. Impact equivalence is not established.

The frozen evidence and derived target coefficients are in
``brightpath/data/export/uvek_cement_european_ng_heat_review.json``. The existing
native gas-heat review supplies the independently reviewed service correspondences;
new primary metadata/inventory hashes and reference checks bind this source case.
Full original source inventories remain outside the repository.

Only ecoinvent 3.9 cut-off is admitted. Other releases, consequential inventories
and inverse conversion are not inferred. The normal migration engine handles the
recipe; only mapping data and its maintainer replay step change. Premise must
approve the resulting exact rule/recipe fingerprint independently. This resolves
one preparation prerequisite, not the whole cement transformation.

Verification
------------

The new mapping and existing gas-heat tests pass (36 checks), as do the resource
schema, integrity manifest and generator regressions (101 checks). Replaying the
full generator produces byte-identical technosphere and canonical-biosphere
resources. The built wheel contains the new evidence, generated recipe and
matching integrity manifest. All 530 previously approved Premise rule/recipe
fingerprints remain valid; the new parent still needs its own scoped approval.
