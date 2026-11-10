# Complete native services for four heat dependencies

`brightpath/data/export/uvek_heat_38_services_review.json` pins the inspected
source 3.8 and native UVEK 2025/2026 reference inventories. These correspondences
are complete-service screening proxies, scoped to ecoinvent 3.8 cut-off. All
references are +1 in the same physical unit, so signed caller quantities,
formulas and supported uncertainty are preserved at factor +1. No native
inventory is edited, no extra helper is created, and other profiles cannot
inherit these approvals.

- **Inland barge transport:** use the complete European diesel barge service per
  tonne-km. The source RoW service is itself extrapolated from the Rhine fleet,
  but fleet/load/lifetime, canal and port allocation, diesel use, emissions and
  internal uncertainty differ. The source explicitly treats bilge oil; native
  transport has no separate bilge-oil treatment exchange. This changed waste
  boundary remains an explicit limitation. No payload, distance, fuel or lifetime
  multiplier and no additional source diesel/emissions are introduced.
- **Deionised water:** use complete Swiss ion-exchange water per kg. Both describe
  counterflow regeneration, comparable conductivity/silica and feed hardness,
  while excluding reverse osmosis, distillation, cartridges and very large units.
  Native delivered tap water replaces the source's direct raw-water abstraction
  and wastewater/sludge model. Electricity, regenerants, solution bases, water
  returns, pollutant fate, geography and vintage differ. This does not qualify
  ultrapure electronic-grade water; no density/purity or water-balance correction
  is inferred.
- **Organic chemical plant:** use the complete native European installation per
  unit. Source and native building/facility coordinates and total land quantities
  agree, and both derive from two German sites. Native industrial land is split
  into classes, and background supply, disposal and uncertainty differ. Preserve
  the whole plant unit; do not infer chemical output or a new lifetime/capacity
  factor from its name.
- **Liquid storage installation:** both references represent four 4,000 m³ tanks,
  16,000 m³ total, with a documented 50-year life. Preserve one complete reference
  unit. The source's 4,000 kg paint uses a without-solvent basis while native
  uses 4,000 kg of 60% solvent mixture. That coating-basis difference remains an
  explicit complete-installation proxy limitation; it is not an equal dry-paint
  claim or an application of the separate water-based paint correction. Native
  refinery bitumen, freight, disposal, background supply and uncertainty also
  differ. No paint, density, throughput or tank-count multiplier is invented.

The standing autonomous modelling approval covers these disclosed proxies.
Warnings retain the changed boundaries, native biosphere exclusions and
unquantified score effects. Neither LCIA equivalence nor impact completeness is
established. Public heat support remains gated until the remaining mappings,
transformation, relinking and real export verification pass.
