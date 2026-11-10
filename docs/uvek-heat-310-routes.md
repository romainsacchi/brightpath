# Exact 3.10 native process-boundary corrections

`brightpath/data/export/uvek_heat_310_routes_review.json` pins three independently
inspected ecoinvent 3.10 cut-off corrections for native UVEK 2026 via the 2025 edge.
Source/native inventory and metadata hashes, rejected candidates and upstream
witnesses are retained. Raw licensed inventories remain outside Git.

Electric secondary low-alloyed cast steel now uses the complete native electric
un/low-alloyed steel process. The previous candidate mixed 37% electric with 63%
converter steel and added hot rolling. The selected native inventory includes
scrap, furnace operation and casting, with its own actual yield, energy and residue
treatment. No converter iron or downstream hot rolling is added. Grade, alloying,
energy, geography and allocation differences remain warnings.

Biomass-gasification methanol now uses native methanol from synthetic gas at the
plant gate. Independent upstream inspection confirms wood-derived syngas and equal
fixed/fluidized gasifier shares in both databases. The earlier regional-storage
candidate added downstream distribution, storage and losses. These are excluded by
selecting the complete plant-gate process, without modifying its inventory. Pilot
scale, catalysts, water and generic calorific-metadata inconsistencies remain
limitations. No density, heating-value or gas-composition correction is inferred.

The >32 tonne EURO IV freight market now uses the European 32–40 tonne diesel
fleet service, whose actual route mix is 80% long-haul, 10% regional and 10% urban.
The previous all-size Swiss fleet included smaller vehicles. The selected complete
service retains its own vehicle, road, fuel and wear boundaries. Capacity upper
bound, emission standard, fleet vintage, loading and route mix remain screening
approximations, not an exact EURO IV counterpart. No payload or emission multiplier
is introduced.

All three preserve signed physical caller amounts, formulas and supported
uncertainty at factor +1 in the same unit. Only the exact inspected 3.10 cut-off
profile is executable; other releases and consequential routes cannot inherit the
corrections. Portable shared reviews retain scientific limitations and unquantified
score effects. No LCIA equivalence or impact completeness is claimed.

The authoring stage validates the entire batch before mutation, rejects changed
rules/evidence and supports idempotent replay. Generated resources, manifests,
installed packages, previous approvals, original electricity callers and native
finite LCIs are checked separately. These changes do not enable public heat updates.
