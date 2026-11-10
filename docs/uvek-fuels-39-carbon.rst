Granular activated carbon and ash treatment
==========================================

The inspected ecoinvent 3.9 cut-off granular-carbon market must not map to
carbon black, carbon fibre or mercury-bearing spent-carbon disposal. Its
reviewed reconstruction keeps four original supply entries: 31.6% virgin
production and 68.4% reactivation, each with separate RER and RoW suppliers.
All four original market freight entries remain unchanged.

The four production/reactivation helpers retain every original nonproduction
row, all 288 direct biosphere exchanges and twelve local parameters, including
parameter uncertainty, pedigree and units. Together they retain 331 lognormal
exchanges and two deterministic spent-carbon inputs. Reactivation has a
positive product reference despite its treatment name. Its complete operating
inventory is retained separately from the existing, genuinely allocation-empty
spent-carbon stock and that stock market's freight.

Virgin production keeps three kg of coal feedstock per kg of product and all
original country rows. Complete native regional-storage supplies replace
mine-only matches, with explicit grade, moisture, ash, delivery, geography,
allocation and vintage limitations. No energy-content or carbon-content
multiplier is inferred, and separately recorded source emissions remain.
Native electricity, useful heat, infrastructure and water remain complete
reviewed utility inputs; no additional combustion stage is added.

All direct biosphere entries retain their publisher UUID, compartment, unit
and signed quantity against ecoinvent 3.10. Six updated labels per producer
account for 24 name changes across the four inventories. These are publisher
identity matches, not ionic-mass conversions or substance proxies.

Ash service and allocation
--------------------------

The Swiss ash market sends 46.9906787% to five production-only, zero-price
cement receivers and 53.0093213% to incineration with fly-ash extraction. The
RoW market sends 21.1641095% to 46 allocation-empty cement/concrete/brick
receivers, 77.9874560% to residual-material landfill, 0.6408694% to sanitary
landfill and 0.2075652% to incineration. Every nonempty receiving-process row
and all eleven freight rows remain separate. Disposal shares are not
renormalized after removing empty receivers.

Exactly 51 source receiving inventories have no nonproduction exchanges and
negative-one-kg references. Their omission preserves their zero source LCI;
it is not an added treatment, allocation credit or arbitrary zero-inventory
substitute. Each omission records its source identity, signed quantity,
uncertainty, index and inventory hash. The existing recipe-omission mechanism
places these records in helper provenance and migration warnings. Premise
requires their exact reviewed approvals; a broad proxy approval cannot
silently permit them. Brightpath's positive-reference zero-stock validation
is unchanged.

The ash helpers provide positive treatment services. Negative source ash
inputs convert with factor -1, with uncertainty and formulas scaled
consistently. Original positive freight remains positive. Native Swiss
incineration lacks the source Swiss fly-ash extraction and recovered residue;
this is an explicit screening limitation, without an added recovery credit.
Native regional landfill and transport proxies have documented geography,
technology, fleet, allocation, vintage and uncertainty differences. Score
changes remain unquantified.

Reproducibility and scope
-------------------------

Frozen evidence, derived recipes and exact reference-profile reviews live in
``data/export/uvek_fuels_39_carbon_review.json``. Raw licensed inventories and
comments remain outside the package. Maintainer replay rejects changed
parents, component correspondences, existing stock/heat/rail closures,
evidence and recipe collisions before mutation. Existing non-3.9 profile
entries on the RoW ash rule are preserved; no new consequential or other
release approvals are inferred. No runtime Python module change is required.

Strict conversion, signed uncertainty/formula retention, complete market and
ash fate composition, explicit omission provenance and refusal without its
separate approval are tested. Native-matrix calculation and licensed fuel
preparation are recorded separately by the companion Premise review receipt.
A finite helper LCI or accepted preparation correspondence does not qualify
a prospective fuel transformation or database export.
