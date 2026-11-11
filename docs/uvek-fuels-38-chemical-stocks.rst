Inspected 3.8 chemical-stock proxies
===================================

Four exact ecoinvent 3.8 cut-off markets are reviewed against existing complete
native inventories: European and rest-of-world hydrochloric acid, global
methanol, and global hexane. The migration rules themselves remain unchanged.
Premise authorizes only the independently inspected profile with explicit
limitations. Signed caller quantities, formulas and uncertainty remain intact.

HCl references one kg active acid in both databases. The native 30% solution
name does not justify a 0.30 or inverse-dilution factor. The native 50/50
synthesis/Mannheim mixture replaces source supplier and co-product allocations;
it does not reconstruct the original source market or its final delivery.

Methanol uses complete native Swiss regional storage, including native transport,
storage emissions and infrastructure once. Its sourcing differs from the global
source market, including a small coal Fischer-Tropsch co-product branch. No
claim of fuel-route or transport equality is made.

Hexane uses complete native molecular-sieve naphtha separation. It is an
at-plant screening proxy for the delivered source market. Native feedstock
transport does not reproduce source final delivery; that missing boundary and
its unquantified score effect are explicit limitations. Source and native
production shares, allocation, geography, energy, internal uncertainty and
vintage differ for all these proxies.

The complete native inventories replace the source market models; original
internal freight and producer shares are not preserved or separately added.
This is not an exchange-by-exchange reconstruction, and no equivalent impact
score is claimed. Three unique native supplier LCIs are finite and the source
database is unchanged. Evidence and warnings are packaged in
``export/uvek_fuels_38_chemical_stocks_review.json``.

Sodium methoxide is excluded: its native Brown-Schlesinger inventory does not
represent the source market's majority direct-production route. It needs a
separate modelling review. That later decision is documented in
:doc:`uvek-fuels-38-additional-chemicals`, with an explicit technology-proxy warning.
