# Eight further heat dependencies: delivery and reference bases

`brightpath/data/export/uvek_heat_38_closure_review.json` pins the independently
inspected ecoinvent 3.8 cut-off cases, native UVEK 2025/2026 references and derived
helper contents. It closes only this observed cohort; later dependencies and
public heat operationalization require further checks.

Four delivered-material markets and the compressor market are reconstructed with
eight new helpers and 53 retained source exchanges. Producer shares, separate
freight entries, numeric formulas and supported uncertainty remain unchanged.
Native complete production services replace source production as disclosed
screening proxies. Their internal inventories are retained once; source raw
material/fuel/emission inventories are not added on top. No source/native database
is edited, no shares are normalized, and no source exchange is omitted from these
market/import reconstructions.

The European alumina market keeps all three import processes separately. Each
import helper retains one kg of production plus its original road, rail, barge
and ocean freight. The market also retains its four finished-product delivery
entries. Import and market delivery are distinct documented layers. The RoW
alumina market retains all six original production shares and every delivery
entry. Native production includes bauxite digestion/hydroxide and calcination;
source regional IAI and native European EAA technology, energy, caustic reference
basis, residues, allocation, vintage and uncertainty differ.

The zinc-oxide market retains both source producer shares and all four freight
modes. Both source producers explicitly document iron scrap as a proxy for
zinc-scrap processing at 0.84526 kg/kg oxide; original native XML and the actual
scrap-processing supplier confirm this approximation. It is not literal
iron-to-zinc conversion, and no invented zinc-metal input is added. Native
production has 36.428 MJ heat and 0.33333 kWh electricity per kg versus 8 MJ and
0.416 kWh in the source. Complete native ZnO is a disclosed process proxy;
there is no energy-based product-mass scaling. The copper-oxide market likewise
retains two producer shares and four delivery modes, using complete native
copper(II) oxide. Its documented 98% yield is already inside the process and
is not another multiplier. Incomplete water/solid-waste/emission models remain
limitations for both oxides.

Complete native rail, lorry fleet, barge and ocean-container services preserve
source tonne-km at factor +1. These are geographic/fleet/vintage proxies. Native
RER lorry fleet shares derive from Switzerland in 2020; rail traction, shunting,
loads, fuels, ports/canals, infrastructure and emissions differ. Native barge
transport has no separate bilge-oil treatment exchange. Finished-product delivery
is additional to native production's upstream raw-material freight.

The compressor market retains its two source shares as separate inputs to the
complete native 300 kW, nominal 4,600 kg compressor. Source electronics use two
0.1255 kg PCB inputs; native uses 0.251 m². This reference-basis difference is an
explicit complete-equipment proxy limitation, with no invented kg/m² factor or
claim of equal board area/mass. Native alloy, machining, freight, disposal and
uncertainty remain complete. The uncoated-glass market has one producer share and
no delivery inputs; use complete native float glass, warning about soda
hydrate/powder and gas reference bases, waste treatment, background and internal
uncertainty. Retain the documented tin/lead-versus-particulate double-counting
limitation rather than silently cancelling emissions.

Gypsum uses the complete native construction-waste landfill as a technology and
vintage proxy. Actual wet-water fractions are 0.194 in the source and 0.193990
in native; preserve wet kg with the source −1/native +1 sign conversion once.
The old inert landfill omits leachate as negligible, whereas native includes
short- and long-term gypsum-specific fate through 60,000 years. This changed
emissions boundary remains explicit; no wet/dry or hydrate multiplier is inferred.
Associated cardboard, reinforcement or fibres are outside the gypsum reference.

The existing steam rule and all its recipes remain byte-for-byte unchanged.
Independently verify its effective 3.8 target: one MJ native chemical heat,
which explicitly excludes make-up water, plus all seven source tap-water entries
and both exact air/water releases. The base rule's default helper belongs to a
different profile. Neither the source's 2.75 MJ/kg steam documentation nor an old
rounded 0.36 kg/MJ factor applies to this native MJ reference. Do not add source
fuel/electricity on top of complete heat, omit water or reuse another version's
water balance.

The standing autonomous modelling approval covers these explicit screening
choices. Exact-profile approvals, warnings and provenance remain required.
Native biosphere exclusions, geography, vintage, internal uncertainty and
unquantified score effects remain visible. No inverse or cross-system-model
approval, LCIA equivalence or impact completeness is claimed. Public heat remains
gated until its full transformation, relinking and export verification pass.
