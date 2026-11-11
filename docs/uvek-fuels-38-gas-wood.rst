Vehicle gas and delivered wood chips
===================================

Two exact ecoinvent 3.8 cut-off profiles retain signed caller quantities,
uncertainty and formulas at factor +1. Existing selected decisions for other
versions remain unchanged.

Vehicle gas remains kg gas. Both station descriptions identify an initial
medium-pressure network of 0.1–1 bar; this is not evidence of final vehicle
delivery pressure. The complete Swiss station includes upstream gas,
infrastructure, electricity and gas-loss emissions once. No additional
compression, heating-value or density factor is introduced. Global sourcing,
gas quality, losses, technology and vintage remain screening differences.

Wet wood chips remain kg dry matter. The new helper uses one kg of the complete
native European forest/sawmill production mix plus the source market's original
0.105 tonne-km road-delivery leg, with its marginal statistics retained. Native
direct delivery is zero. Native species, forest/sawmill shares and original
coefficients remain unchanged without normalization; they replace the source's
nine producer shares. Forestry allocation, geography, land use, moisture,
upstream emissions and internal uncertainty are not reconstructed.

The freight amount is already stated per kg dry wood. It is not recalculated
from native moisture or an invented wet-payload correction. Its original
background formula depends on source parameters, so only its hash is retained;
that parameter context and correlations are not transplanted. Caller formulas
remain preserved. No extra forestry, sawmill, chipping, drying or combustion
process is added. The generic European road service remains a fleet proxy.

Native biosphere exclusions and unquantified score effects remain explicit
limitations. The packaged ``uvek_fuels_38_gas_wood_review.json`` binds reference
facts, unchanged older selections, the freight row and witness hashes. Raw
licensed vectors and background formula strings remain private. Two weighted
native LCIs are finite. Component checks do not establish a complete fuel
update/export. No runtime Python module changes are needed.
