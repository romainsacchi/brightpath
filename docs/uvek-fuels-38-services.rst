Native services for ecoinvent 3.8 fuel preparation
=================================================

Sixteen independently inspected cut-off correspondences cover three electricity
grids, four freight services, five fuel/combustion services and four water or
irrigation services. Fourteen existing Brightpath rules remain byte-identical.
Two incorrect combustion matches change for the exact 3.8 cut-off profile only.
No runtime Python module, alternative sector engine, source-input omission or
recovery credit is introduced.

Grids, freight and delivered stocks
----------------------------------

Indonesian medium-voltage, United States low-voltage and ENTSO-E low-voltage
supply use their complete native grids per delivered kWh, retaining native
voltage topology, losses, upstream generation/imports and infrastructure. Native
historical mixes replace source historical mixes; shared Premise transformations
continue to handle scenario changes. No additional loss multiplier is applied.

General inland barge freight uses native European barge service per tonne-km
for European and rest-of-world source markets. It is not tanker or sea freight.
European rail uses complete native rail freight; European onshore petroleum
pipeline uses complete native crude-oil pipeline freight. Fleet, geography,
loading, traction, product/viscosity, network and vintage limitations are explicit.
No distance, payload, empty-return or pumping-energy multiplier is inferred.

Complete native regional light fuel oil retains refinery supply, distribution,
storage and evaporation/effluent treatment. It replaces the source market-group
portfolio; source internal freight is not added again. Complete Swiss tap water
replaces the source rest-of-world delivered-water mix per kg, including its own
network and treatment model, without an extra loss or water-balance correction.

Fuel combustion corrections
---------------------------

* Average native diesel building-machine combustion retains fuel, machine,
  lubricant, used-oil treatment and direct emissions per MJ fuel energy.
* Agricultural diesel replaces the former particle-filter-specific match with
  complete average native non-road building-machine combustion. This remains
  an explicit fleet/technology proxy for agricultural equipment, not a claim
  that tractor infrastructure, field operations or exhaust performance agree.
* Native gas-motor combustion for storage retains gas supply, motor stock and
  emissions per MJ fuel. No useful heat/electricity output credit is added.
* Petrol combustion replaces refinery-gas flaring. Actual native petrol supply
  is 0.02447 kg per MJ; its description identifies gasoline-powered four-stroke
  tamping/vibratory machinery. Inherited diesel wording and the particle-filter
  title are retained as documented inconsistencies. Engine, load, emissions,
  controls and vintage differ from the source machinery model. No corrective
  efficiency, fuel ratio or filter-reduction factor is invented.

The petrol process exists in native UVEK 2026 but not in the intermediate 2025
catalog. The existing migration recipe mechanism therefore reconstructs its
complete inventory: four native technosphere inputs and 21 direct biosphere
rows, with signed amounts, uncertainty and publisher flow UUIDs preserved.
No fictional 2025 catalog entry or relaxed intermediate validation is needed.
The native processes used upstream already exist in the intermediate catalog.
This helper is not a reconstruction of the source ecoinvent engine inventory;
it reproduces the selected native screening proxy.

Irrigation reference and water boundary
--------------------------------------

Three source markets (rest-of-world, France and United States) use complete
Swiss sprinkler irrigation per cubic metre supplied to the plant. Native
sprinkler efficiency is documented as 75%; source surface/drip/sprinkler services
state 45%, 90% and 75%. The complete source technology mix is replaced by the
native sprinkler process, with geography, pumping head, energy and hardware
warnings. Both source and native descriptions leave supplied-water and
infiltration/evaporation flows to crop datasets. Separately listed crop flows
remain unchanged. No efficiency ratio, extra abstraction, water return or
water-balance correction is added.

Verification and limits
-----------------------

The frozen ``data/export/uvek_fuels_38_services_review.json`` records exact source
and producer witnesses, native reference hashes, decisions and compiled rules.
Replay validates all sixteen correspondences, rejects drift, preserves fourteen
existing rules, and adds only the complete native petrol helper. Premise approval
fingerprints authorize only the inspected 3.8 cut-off context, preserving older
approvals and their original evidence.

Strict linking, signed quantities, formulas, normal/lognormal uncertainty and
fresh/cached CLIC conversion are tested. Native LCIs and original source-data
non-mutation are checked independently. Native petrol reconstruction is compared
against the installed native matrix. That matrix stores coefficients at float32
precision: the control uses the same precision, while the recipe retains original
nominal values. This distinguishes import rounding from changed inventory and
keeps the original comparison tolerance.

These are complete native functional proxies. Source internal market shares,
production inventories, process emissions and internal uncertainty are replaced,
not reconstructed or added again. Geographic, technology, allocation, delivery
and vintage differences have unquantified score effects. Mapping acceptance and
finite native LCIs do not qualify prospective fuel transformation or export.
Premise's companion receipt records exact test and licensed preparation results.
