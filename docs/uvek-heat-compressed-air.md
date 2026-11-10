# Delivered compressed-air screening mix

The exact ecoinvent 3.9 cut-off market for compressed air at 1000 kPa gauge uses
two equal shares: average and optimised small installations. The earlier
compressor-only candidate did not include their distribution boundary.

The reviewed helper now supplies one declared m³ using the two complete native
UVEK supply-network services at 10 bar gauge and below 30 kW, retaining the two
0.5 shares separately. Actual source/native references are positive one m³.
Signs, caller formulas and supported uncertainty remain unchanged at factor +1.
No pressure, density, ideal-gas or heating-value conversion is applied.

Source and native documentation cite Steiner and Frischknecht's 2007 ecoinvent
report 23. Its chapter 10 uses normalised-volume operating data and distinguishes
compressor generation from delivered supply. The installed metadata does not
independently state exact reference temperature/pressure conditions. This is a
lineage-based volumetric screening proxy; exact condition equivalence is not
claimed. The review links the author report reproduced by a third-party host;
its contents were cross-checked against the original native XML and source data.

The native models remain complete and unchanged. Their generation requirements
are 1.5 and 1.05 per network output; source operating and compressor coefficients
differ. Do not silently repair those factors, add source electricity on top of
the complete native services, or multiply caller demand by a guessed loss rate.
Piping, generation, electricity sourcing, compressor lifetime, delivery, internal
uncertainty and vintage differences remain warnings. Native biosphere exclusions
also remain a limitation, and score effects are unquantified.

The average native service's name begins with `xx` in the original archive and
both release catalogs. Its reference and network scope are explicit, but the
archive does not explain the prefix. Keep the label literally and disclose its
unexplained marking; do not interpret it as a quality certification.

Only the inspected 3.9 cut-off profile is newly qualified. Other releases,
reverse routes and conversions across system models cannot inherit the review.
The maintainer utility checks the review hash, previous rule and helper before
mutation. This qualification does not enable Premise's public heat update.
