# Final observed 3.10 heat-cohort decisions

`brightpath/data/export/uvek_heat_310_closure_review.json` records three exact
source-profile decisions, with source/native inventory hashes, rejected candidates,
component references and explicit scientific limitations. Raw licensed inventories
remain outside Git. This closes an observed cohort only; later cohorts and public
heat integration need separate verification.

Primary aluminium uses the complete native European primary solid-metal inventory.
Source domestic/import producers and their ingot-production inventories were
inspected. Native casting consumes 1.019 kg of primary liquid aluminium per kg
solid; that internal loss is already included. Source production mixes, dross
allocation, purity assumptions, electricity and delivery differ. No additional
electrolysis, casting, yield, purity or recycling multiplier is introduced.

The copper decision is explicitly a material screening proxy. The observed PEM
stack consumes 0.01979 kg of anode supply and has no separate refining input.
Native global primary copper replaces the earlier Latin-American candidate, but
supplies refined/cathode copper and includes additional refining plus an SX-EW
share. Grade, smelter, co-product and process-boundary differences remain warnings
with unquantified effects. No guessed refinery subtraction or purity adjustment
is made; this does not establish equivalence for downstream refining services.

The condensate market retains its 0.4126619233000178 m³ treated fraction, separate
0.1723 and 0.8277 m³ rural/urban collection entries, and all 17 direct untreated
emission flows with exact source amounts, compartments and uncertainty. The
untreated-water flow remains its literal 0.587241188 m³; no water balance is
renormalised. The existing negative waste-reference conversion factor is retained
exactly once.

Three linked recipes represent this service:

- A treatment copy retains all 26 non-sewer technosphere inputs and 50 canonical
  biosphere exchanges of native UVEK 2026 class-2 condensate treatment.
- A collection service retains the native deterministic allocation of
  1.6832×10⁻⁷ km sewer per m³. Both original source collection entries call this
  service separately at their unchanged volumes.
- The market retains the treated share and all direct untreated fates.

Only the embedded sewer link is moved out of the treatment copy. Its literal
exchange and component witnesses are retained in the review and Premise audit.
There is no independent negative uncertainty cancellation, duplicated sewer demand,
new source-input omission, invented density or geometry-derived conversion.

Native three-stage treatment replaces the source regional stage portfolio. Native
sewer geometry and volume allocation replace source rural/urban engineering.
Sludge, energy, residues, auxiliaries, pollutant fate, geography and internal
uncertainty therefore differ. Actual native quantities remain unchanged before
multiplication by the source treated share; no recovery or avoided-energy credit
is added. Native biosphere exclusions remain explicit.

Only independently inspected ecoinvent 3.10 cut-off is newly qualified to UVEK
2026, through the existing 2025 edge. Caller signs, formulas, supported uncertainty
and ownership remain intact. Other releases and consequential routes receive no
new approval. Finite LCI does not prove LCIA equivalence or impact completeness.

The numerical component check compares on all 4362 canonical biosphere rows,
including source discharge definitions unused by the native-only matrix. Native
processed coefficients exactly match float32-rounded audited exchange values.
Recipes retain the full raw values. The raw-recipe versus processed-background
LCI comparison differs by 1.58×10⁻⁸ relative L2 and 4.19×10⁻⁷ maximum absolute;
an exploratory 10⁻⁸ bound therefore failed and remains recorded. Independent
coefficient identities explain that difference to below 1.24×10⁻¹⁷ after accounting
for processing precision. Neither rounding the recipe nor LCIA equivalence is
inferred from that check.
