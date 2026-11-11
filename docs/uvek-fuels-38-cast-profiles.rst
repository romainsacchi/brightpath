Cast-stock profile qualification
===============================

Two exact ecoinvent 3.8 cut-off profiles gain positive-reference sign reviews.
Signed caller kg, uncertainty and formulas retain factor +1. Existing helper
inventories and selected decisions for other versions remain unchanged.

The low-alloyed market's nine deterministic producer entries have no formulas.
Their converter/electric route totals match the existing cast-stock helper
within numerical precision: 0.7421076548969346 converter and
0.2578923451030648 electric steel. Its four final-delivery quantities and
marginal distributions also match. The existing helper is reused unchanged,
without normalization or extra rolling. Country-specific producer entries are
aggregated into European converter and Swiss electric route proxies. This
preserves route-level quantities, not the nine original production inventories,
country procurement, emissions, allocation or internal uncertainty.

Unrolled chromium 18/8 stock uses complete native European electric-furnace
steelmaking and casting, rather than the finished hot-rolled steel mix.
Original global producer shares and final delivery are not reconstructed;
native scrap/feedstock transport is not equivalent to final delivery. A
specific alloy chemistry or mechanical equivalence is not established.

All selected native steel inventories have positive one-kg references and no
direct rolling service. No second rolling, source-energy graft, density,
alloy-content, yield or recovery-credit adjustment is introduced. Geography,
technology, allocation, vintage and native biosphere exclusions remain explicit
limitations, with unquantified score effects.

The frozen ``uvek_fuels_38_cast_profiles_review.json`` binds source witness
hashes, route/freight conservation facts, unchanged helper closure and prior
profile selections. Raw licensed inventories remain private. Two weighted
native LCIs are finite; component qualification does not establish a complete
fuel-sector transformation or export. No runtime Python module is changed.
