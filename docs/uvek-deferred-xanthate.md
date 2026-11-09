# Active sodium ethyl xanthate synthesis

The inspected ecoinvent 3.12 cut-off market supplies one kilogram of pure sodium
ethyl xanthate. A generic organic-chemical dataset is an inappropriate counterpart.
The derived native graph retains both original producer entries, their precursor
and utility coefficients, supported uncertainty, four direct flows per producer,
and every original market freight leg. Caller active kilograms and metadata use
factor +1. Only this exact source release is qualified.

The [Kunene thesis, Table 14, printed page 65](https://open.uct.ac.za/server/api/core/bitstreams/80bfc837-7adf-4c4f-bdce-b2cc4dc82595/content)
independently supports carbon-disulfide/ethanol/caustic synthesis and the nominal
reactant proportions. Its liquid-plant model does not prove dry-phase equivalence
or validate the source's additional generic waste and water assumptions. No
phase correction, extra drying process or recovery credit is invented.

Complete native carbon disulfide, fossil ethylene-hydration ethanol and active-
reference caustic supply the reactants. Legacy dissolved-chemical references
represent active substance under the [official v2 methodology, section 4.5.8](https://support.ecoinvent.org/hubfs/Knowledge%20Base/Database/Releases/overview_methodology_ecoinvent%20version%202.pdf).
There is no automatic concentration multiplier. The incorrect biomass-ethanol
candidate is rejected. The source RoW ethanol market includes coal/Fischer–Tropsch
supply: replacing that contribution with complete native ethylene hydration is an
explicit fossil-feedstock/technology/allocation screening proxy, not a geographic
match or claim of equivalent impacts.

Use native country/regional medium-voltage grids where available. The 22
remaining country entries use the complete global grid with explicit warnings;
they represent approximately 0.7132% of RoW producer electricity. Preserve each
original coefficient and its uncertainty, without renormalizing or inheriting a
Swiss-only candidate. Native grid mix, losses and infrastructure remain complete.

Existing reviewed hazardous-waste markets and their full helper closures retain
incineration shares and freight. Negative source treatment references become
positive service demand. Complete native unpolluted wastewater treatment is an
explicit plant-size/network/sludge/geography proxy. Waste moisture/composition,
chemical technologies, market versus at-plant delivery, equipment capacity and
lifetime, allocation, geography, vintage and native internal uncertainty differ.

Nitrogen release, evaporated water, discharged water and cooling-water withdrawal
remain separate. Their source and canonical ecoinvent 3.10 publisher UUIDs,
compartments, units and roles match; metadata hashes are recorded. No net-water
collapse, invented dilution, removal credit or source-exchange omission is used.
The original background's 2,991 biosphere exclusions remain explicit; effects on
scores are unquantified.

The packaged review contains derived native coefficients, correspondence and
hash witnesses. Raw licensed source inventories, source archives and private
reference snapshots remain outside Git. Three new helpers reuse nine existing
reviewed waste helpers. Fingerprint checks freeze the full closure, native input
bindings and parent rule. Tests cover signs, caller metadata, reactant totals,
water compartments, absence of biomass substitution, exact-release refusal,
cache behavior and atomic authoring replay. Native LCI and full public scenario,
export and readback provide separate numerical/integration evidence.
