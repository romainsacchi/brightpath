# Sodium metal and potassium-chloride reference bases

The inspected ecoinvent 3.10 cut-off sodium process produces one kg sodium metal.
Sodium chloride is its feedstock, not its product. There is no native elemental
sodium counterpart in the reviewed UVEK 2026 catalog.

The new sodium helper retains the complete already allocated electrolysis and
purification inventory: four upstream inputs and five direct canonical ecoinvent
3.10 biosphere flows. All source coefficients, supported uncertainty and formulas
are retained. Complete native chemical-plant infrastructure, medium-voltage grid
and dry salt supply are screening proxies. The existing exact 3.10 chemical-heat
helper retains its original make-up water and water releases; those are distinct
from the sodium process's direct flows. Allocation is not applied again, and no
chlorine coproduct or avoided-production credit is invented. Source electricity
portfolio and salt-market producer/delivery mixes are not reconstructed by the
upstream whole-service proxies. Their boundaries and score effects remain limited.

The KCl case has a different reference issue. The actual source producer states
one kg potassium chloride with 60% K2O; the native service references one kg K2O
and states 60% K2O content. The inspected conversion is therefore 0.60 native
reference units per signed source kg. The native 1.67 kg KCl statement is rounded,
and is not used as an exact reciprocal. Generic source market text also says
NPK 0-0-50; that inconsistency remains disclosed. The actual producer basis
governs this exact-profile decision. Pure-KCl stoichiometry or another grade
cannot inherit it.

The complete native regional-storehouse KCl inventory replaces the source producer
and market as a screening proxy. Its energy, mining/concentration, tailings,
infrastructure and delivery remain once. Source freight is not added on top of
that delivered boundary. Industrial-grade versus fertiliser-oriented descriptions,
technology, geography, allocation, internal uncertainty and vintage differences
remain explicit; chemical-grade or LCIA equivalence is not established.

`uvek_heat_salts_review.json` records both corrections and the four upstream
qualifications, source/native hashes, canonical flow definitions, helper bindings
and limitations. Only inspected ecoinvent 3.10 cut-off is newly qualified. Caller
signs, formulas and supported uncertainty are preserved or scaled consistently
with the documented KCl factor. Other releases, reverse routes and conversions
across system models cannot borrow these decisions.
