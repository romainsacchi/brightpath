# Heat boundary corrections and superseded lime approval

`brightpath/data/export/uvek_heat_38_corrections_review.json` records five corrected
rules across nine exact profiles. Four are new 3.8 heat dependencies. Five older
3.6–3.10 Swiss loose-milled-lime profiles supersede an erroneous quicklime approval.
All source and native inventory witnesses are pinned; raw licensed inventories
remain outside Git.

The legacy lime product description/CAS says quicklime, but the inspected loose
Swiss inventories consume one kg of crushed limestone and only grinding inputs.
Ecoinvent 3.12 explicitly identifies this same inventory as milled limestone and
models quicklime separately with calcination. Native limestone reproduces the
characteristic crushing/milling boundary; native quicklime adds a kiln and direct
calcination emissions. The earlier quicklime match was wrong. Correct those five
profiles to native milled loose limestone, keeping signed kg and formulas unchanged.
The source semantic inconsistency remains a warning; genuine named quicklime rules
and native calcination processes remain unchanged.

The 3.8 packed-lime process also draws on uncalcined limestone milling. Use complete
native milled packed limestone, replacing the hydrated-lime candidate. Its native
packing/heat supply and the source market/mine-co-product mix differ. No CaO,
CaCO3 or Ca(OH)2 stoichiometric, yield or purity conversion is introduced.

The hard-coal market now uses complete UCTE regional-storage supply instead of an
Australian mine-gate process. This restores a delivered-fuel boundary, including
the native supply mix, transport, losses and handling, with explicit geographic,
quality, energy-content and route differences. Preserve physical kg; do not add
source delivery on top or infer an energy-equivalent mass factor.

Both wastewater sources describe class 4, 5320 PCE and 0.1556 kg COD/m³. Use native
class 4 with matching nominal capacity/COD basis, replacing class-1/class-2
candidates. This does not establish identical full influent or removal efficiency.
Keep complete native treatment, sludge, sewage collection and infrastructure once;
retain the existing −1 waste-reference factor once. No PCE or annual-capacity
multiplier or added recovery credit is introduced.

Premise archives the entire earlier lime operation and fingerprints as inactive
superseded history, while activating the scientifically corrected scopes. This is
a new scientific decision, not a structural fingerprint refresh. Original review
bytes remain unchanged. An earlier authoring-stage replay recognises only the
frozen superseding rule and cannot silently restore the quicklime mistake. Changed
source, target or profile content remains refused.

Validation covers signed normal/lognormal coordinates, source ownership, refusals,
atomic authoring, legacy material callers, cache warnings, inactive old approvals,
installed packages and signed finite LCIs. A fresh installed-wheel public default
biomass/electricity run is exported and read back. Its four affected foreground
lime links switch from quicklime to limestone with bit-identical physical amounts;
all 55 source pins and nine roots remain intact. Results can change because the
material boundary is corrected; no LCIA equivalence or characterized effect is
claimed. Native background databases remain unchanged. Public heat remains gated
pending its remaining mappings and integration/export validation.
