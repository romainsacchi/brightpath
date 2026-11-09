# Concrete-waste markets with nonempty processing

The exact ecoinvent 3.9 cut-off markets for unreinforced concrete waste have
negative one-kilogram references. Generated treatment services have positive
references; caller conversion is therefore -1, preserving treatment demand,
formulas and supported uncertainty. Other releases and inverse conversions are
not qualified by this review.

The Swiss market retains sorting/recycling/collection shares of 0.28/0.62/0.10.
The European market retains 0.506227554061282/0.202491021624513/
0.291281424314205 respectively. Every original transport coefficient remains.
The three processing branches each consume 0.0437 MJ of machine-burned diesel
and emit 1.66e-5, 8.35e-5 and 6.34e-5 kg in the three source particulate-size
classes. Recycling is nonempty; an empty recycling handover would lose burdens.
Canonical ecoinvent 3.10 PM definitions retain source publisher UUIDs, units,
compartments and roles, with metadata hashes recorded in the review.

Sorting retains 0.0037 kWh low-voltage electricity, 0.000505 m³ hydraulic
excavation, 1e-10 sorting-facility units and the source 0.4 kg generic-inert-waste
route. The European electricity group uses the complete RER grid, rather than
the narrower CH-only heuristic candidate, with explicit mix/geography/vintage
limitations. Source machine/equipment and facility coefficients are retained;
there is no invented capacity, lifetime or equipment multiplier.

Generic Swiss inert waste uses its existing reviewed allocated clinker handover
and freight. The European generic-inert market is separately qualified: retain
0.4351742655843124 sanitary-landfill demand and the audited 0.5648257344156875
production-only allocated clinker handover, plus original road/rail freight.
The handover has no nonproduction exchanges in the inspected allocated source.
Its exact signed exchange remains in a fingerprinted omission audit; no landfill,
fuel burden or recovery credit is invented for it. This is not a physical claim
that kiln treatment is burden-free.

Collection retains one kilogram of the actual inert-final-disposal market.
The Swiss market retains its landfill service and both freight legs; the European
collection uses the existing reviewed RoW market and treatment reconstruction.
Complete native sanitary and construction-waste landfills are explicit screening
proxies. Native residue management, operation/infrastructure, allocation,
technology, geography, vintage and internal uncertainty differ. All proxy
limitations and the original background's 2,991 biosphere exclusions remain
visible; score effects are unquantified.

The review adds ten helpers and qualifies three exact rules (the two concrete
markets and their European generic-inert dependency). Existing helper definitions, shared reviews and unrelated rules remain unchanged. Source-input audits are bound to their own direct
verified correspondence; the Premise ledger admits only the literal matched
audits, without relaxing omission policy.

Regression checks cover source immutability, positive/negative/zero callers,
normal/lognormal uncertainty, all three processing shares, retained PM and diesel,
helper coefficients, warnings/cache behavior, uninspected-release refusal and
atomic authoring replay. Native LCI, public encrypted scenario/export, readback
and final goal packaging provide separate integration evidence.
