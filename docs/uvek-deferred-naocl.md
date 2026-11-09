# Dry-active sodium hypochlorite reconstruction

The exact ecoinvent 3.6 and 3.9 cut-off markets reference one kilogram of
**active NaOCl**, supplied in a nominal 15% solution. UVEK's complete native
hypochlorite process captures off-gas chlorine and has different precursor
requirements; it is not interchangeable with the inspected virgin-chlorine
production route.

The helpers retain the source producer and market structure, using complete
native chemical and utility suppliers. Per active kilogram, production requires
0.952447874110337 kg liquid chlorine and 1.07441948482084 kg active NaOH,
0.0281504 kWh electricity, 1.0488 MJ natural-gas heat, 0.585396 MJ other heat
and 4e-10 organic-chemical-plant units. The legacy native 50%-NaOH reference is
also an active-substance kilogram. There is no factor-two caustic correction,
6.6667 dilution multiplier, invented carrier-water exchange or off-gas credit.

[Ecoinvent v2 methodology, section 4.5.8](https://support.ecoinvent.org/hubfs/Knowledge%20Base/Database/Releases/overview_methodology_ecoinvent%20version%202.pdf)
defines the legacy dissolved-chemical reference by active substance.
The [v3 guideline](https://support.ecoinvent.org/hubfs/Knowledge%20Base/Database/Fundamentals/dataqualityguideline_ecoinvent_3_20130506.pdf)
clarifies that convention through the explicit “without water” wording.

The helpers retain three direct emissions in the canonical ecoinvent 3.10
biosphere: chloride (0.476223937055169 kg), sodium (0.30883719149539 kg),
and reaction water (0.000241806230380622 m³). Source and canonical definitions
have matching publisher UUIDs, units, roles and compartments. Their metadata
hashes are recorded. A nominal active-reactant mass-balance diagnostic closes
to floating-point roundoff; it does not establish a carrier-water balance or
joint stochastic mass conservation. Water exchanges remain in source m³.

Original road and rail quantities and supported coefficient uncertainty remain
separate. The unreviewed generic-rail → aircraft candidate is rejected locally:
complete native RER rail freight supplies each original tonne-kilometre.
Caller signs, quantities, formulas and supported uncertainty use factor +1.
Only the two inspected releases are qualified; no inverse or cross-system-model
correspondence is inferred.

This remains a screening reconstruction. Native chemical technologies,
allocation, geography, vintage and internal uncertainty differ. Source mixed
heat is represented by complete native fuel-oil and gas heat services; delivered
heat and factory capacity/lifetime boundaries differ. The original background's
2,991 biosphere exclusions remain explicit. Score effects are unquantified.

The hash-pinned review records source/native inventory evidence, six canonical
biosphere witnesses, rejected freight correspondence and exact helper inputs.
The compiler checks parent, component and helper drift atomically. Regression
checks exercise positive/negative/zero caller amounts, lognormal/normal
uncertainty, both source releases and refusal of uninspected releases.
