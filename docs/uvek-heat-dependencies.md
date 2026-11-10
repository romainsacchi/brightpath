# Qualified heat dependencies

Eight exact ecoinvent 3.9 cut-off correspondences are qualified in
`brightpath/data/export/uvek_heat_dependency_batch_review.json`:
acrylonitrile, acrylic acid, dimethyl sulfoxide, methyl acrylate, potassium
permanganate, deionised water, ammonium bicarbonate and generic Swiss wastewater
treatment. Source producers and native UVEK 2025/2026 references were inspected.

The seven kg products use complete native plant inventories as screening
proxies. Caller mass, formulas and supported uncertainty remain. Market supplier
weights and final delivery are not reconstructed; weak native emission data,
production/allocation differences and geographic/vintage limitations remain
explicit. No purity, concentration, density or yield factor is invented.

Wastewater retains its existing factor **−1**, exactly once, for a negative source
reference and positive native treatment reference. This preserves the service
supply coordinate for both treatment demand and credits. The earlier Premise
frontier note incorrectly inferred an inherited +1 factor; sign handling was
already present. This work independently qualifies the functional proxy.

Source COD is 0.42534 kg/m³ versus native 0.1556 kg/m³. The native class-1 plant,
sewer, sludge and emission model differ from the source at-gate service. Preserve
volume without multiplying the whole inventory by a COD ratio or inventing a
reuse/avoided-emission credit. The complete native boundary remains, with
unquantified score effects and no LCIA-equivalence claim.

All qualifications are restricted to the inspected exact source profile. They
do not authorize unknown releases, inverse or cross-system-model requests.
Resource replay is fingerprint-bound, idempotent and validates the whole batch
before mutation. These are heat dependencies; they do not establish a complete
Premise heat-sector implementation.
