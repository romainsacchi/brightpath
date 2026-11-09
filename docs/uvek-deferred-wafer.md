# Fabricated-wafer functional proxy

The inspected ecoinvent 3.12 cut-off market and UVEK's complete logic-type wafer
process both explicitly reference one square metre of **fabricated** wafer.
The mapping uses the complete native process at factor +1 per source m², retaining
the caller's signs, formulas and supported uncertainty. It does not convert
through wafer mass, thickness, count, fabrication yield or energy demand.
Original market road and rail delivery remain separate, additional to the native
factory's internal chemical freight, with geographic/fleet/vintage warnings.

The native process includes substrate, layering, patterning, doping and heat
treatment, chemicals and gases, energy, emissions, water/waste treatment and
infrastructure. It represents a newer logic fabrication route. The source's
circuit type is unspecified. Memory fabrication also exists, but there is no
verified source logic/memory share, so no mixture is invented. Complete logic
fabrication is a representative non-memory screening proxy, not verified subtype
or process-generation equivalence.

Material differences are substantial. Source documentation represents European
production around 2000; the native model uses Ireland 2017 information and Asian
energy supply. Documented wafer masses are 0.7 versus 1.68 kg/m². Actual source
energy is 27,600 kWh and 58,959 MJ per m²; native logic fabrication uses 17,500 kWh
and 14,760 MJ. These differences are retained with explicit warnings, without
an invented mass, energy, substrate or yield compensation. Chemicals, gases,
emissions, effluent, waste management, throughput, infrastructure, allocation,
vintage and internal uncertainty also differ. The host SiC module already uses
a generic silicon-wafer source model: this mapping does not establish a
SiC-specific manufacturing inventory.

The helper preserves complete native fabrication and its reference area.
No raw-silicon substitution, bare wafer, new die cutting/packaging burden, avoided
emission or recovery credit is added. The original background's 2,991 biosphere
exclusions and unquantified score differences remain visible. Only the inspected
source release is qualified; no inverse or cross-system-model correspondence is
inferred.

The hash-pinned review records source/native reference and inventory witnesses,
the inspected memory alternative, rejected assumptions and the two retained
market-delivery legs. Regression tests cover reference area, caller uncertainty,
freight, exact-release refusal, cache/warning behavior and atomic replay. Native
LCI, public encrypted scenario/export and actual readback provide separate
numerical/integration checks. Final goal packaging verification remains required.
