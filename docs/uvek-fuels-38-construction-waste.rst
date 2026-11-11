Construction-waste treatment signs
=================================

Two independently inspected ecoinvent 3.8 cut-off services use a negative
one-kg waste reference, while native UVEK supplies a positive one-kg treatment
service. The existing conversion factor is -1: a negative incoming waste
exchange becomes a positive treatment demand.
A positive source coordinate changes sign consistently. Formulas and supported
uncertainty are scaled with the exchange; normal bounds reverse order and
lognormal magnitude parameters remain while the sign changes.

Bulk iron excluding reinforcement uses the complete native building-waste
sorting service: handling machinery, sorting electricity and transport to
dismantling facilities. It is not steel production or a zero-burden recycling
handover. Reinforced concrete uses the complete dismantling and final-disposal
service, documented as 97% inert material and 3% steel. Neither mapping adds
recovered-metal credits, a second handling/disposal operation, a composition
split or a density conversion. Both retain kg waste.

Native Swiss technology is an explicit geographic and vintage proxy for
Europe without Switzerland. Machinery, utilities, emissions, allocation,
internal uncertainty, background coverage and native biosphere exclusions
remain limitations; score differences are unquantified. Separate caller
transport, operations and emissions remain unchanged.

The packaged ``uvek_fuels_38_construction_waste_review.json`` binds source and
native reference signs, scalar boundary facts and witness hashes. Raw licensed
inventories remain private. No migration rule, native inventory or runtime
Python module changes. Component qualification does not establish a complete
fuel-sector transformation or export.

Independent calculations in the installed ecoinvent and UVEK databases confirm
positive treatment activity for source demand -1 and native demand +1. Both
matrices balance to their requested reference demands and have finite LCIs.
This checks reference-coordinate meaning; it does not compare LCIA scores or
guarantee that every impact category has a positive net result. Source
inventories remain unchanged and the regular BAFU project is restored.
