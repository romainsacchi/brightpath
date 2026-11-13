# European municipal-waste disposal portfolio

The exact ecoinvent 3.8 cut-off rule `bp-168324086066bce9` now uses a reviewed
collection-and-treatment portfolio instead of one Swiss incinerator. The source
is `market group for municipal solid waste`, municipal solid waste, Europe
without Switzerland, kg. Other releases, consequential and reverse conversions
are outside this approval.

The frozen review is
`brightpath/data/export/uvek_mining_38_msw_portfolio_review.json`. Its maintainer
replay validates the public calculation, canonical biosphere dependency, original
parent, dependent recipes and exact cut-off-stock approvals before mutation.
The normal generator replays it after the polyacrylamide review.

## Physical boundary

- Keep all 30 original terminal supplier shares: 20 incinerators, one sanitary
  landfill, four unsanitary landfills, four open dumps and one open-burning
  process. Collapse only deterministic repeated paths to the same original
  supplier. The original 36-market graph has 125 terminal paths.
- Keep all 35 collection-freight entries. Sum deterministic parent activity
  first, multiply the original tkm by that activity, and shift lognormal location
  by its logarithm. Retain scale, pedigree and other supported uncertainty.
  The complete native RER fleet-average lorry service is a geographic/fleet
  screening proxy, with no added distance, payload or fuel correction.
- Do not normalize the terminal treatment activity sum
  `0.9999998698370718` to one, invent a missing treatment route or apply a moisture
  factor. The helper supplies one kg of treatment service; its caller factor is
  **−1 exactly once** because the source waste reference is −1 kg.
- Use the complete native Swiss incinerator only for the original incineration
  routes, and complete native sanitary landfill only for that original route.
  Retain native internal residues, energy, infrastructure and allocation once.
  Country, composition/trace-metal, technology, recovery, gas capture, leachate,
  energy, uncertainty and vintage differences are warnings. No separate source
  credit or disposal burden is added to these whole-function proxies.
- Keep the nine uncontrolled-treatment cases distinct, using the independently
  calculated [public treatment models](uvek-msw-public-calculation.md). All 1,008
  biosphere exchanges retain their calibrated quantities, compartments and
  marginal uncertainty. They are not replaced by controlled incineration or
  engineered landfill.
- Each unsanitary landfill retains 0.00031708 m³ of native RER skid-steer
  excavation and 0.046738 MJ of native CH average building-machine diesel
  combustion, with the public model's matching uncertainty. Retain both listed
  operating services; do not add further fuel or infrastructure.
- Open burning retains −0.017098 kg iron and −0.00083857 kg copper scrap outputs,
  including their uncertainty and negative signs. Reuse the two existing exact
  positive-reference production-only cut-off-stock helpers. Their source
  records were rechecked as production-only. This preserves an observed
  allocation boundary; it does not make physical recovery burden-free or add
  avoided virgin-production credits.

There are ten new recipes: nine treatment helpers and the portfolio. Together
with the two existing cut-off stocks, migration returns all twelve owned helpers.
The stocks are explicit, fingerprinted dependencies, not inferred empty suppliers.
The public model's fixed-case pedigree assumptions and cross-exchange correlation
limitations remain visible. Full source/native inventory or LCIA equivalence and
impact bounds are not claimed.

## Verification and remaining sector work

Signed migration tests cover caller quantity, formula, normal/lognormal
uncertainty, all nested inputs, freight, cut-off bindings, atomic replay and
refusal of another source release. Actual CLIC harmonization and adaptation into
the installed UVEK 2026 project give finite positive and negative LCIs for all
12 helpers, with source metadata unchanged. Exact-profile warnings survive cache
reuse. The full packaged scope ledger has 1,244 valid approvals.

A private real REMIND SSP2-NPi 2050 mining run now completes inventory preparation
and the shared sector update. It is **not a qualified mining export**. Graph
validation reports a component whose 232 electricity product labels differ from
the canonical baseline because mining still uses the default ecoinvent family.
A private diagnostic copy normalized through the existing UVEK alias function
has no new cycle, with quantities and supplier keys unchanged. This observation
is not a validation suppression or a runtime fix.

The resulting inventory also has no `market for sulfidic tailings` datasets.
Three imported negative-reference treatments have 36 regional clones, but native
market/fate coverage and the family/filter handoff remain to be completed.
Do not open the public mining gate on the strength of inventory preparation,
helper LCIs or those clones alone.
