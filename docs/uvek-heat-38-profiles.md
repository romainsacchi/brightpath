# Exact 3.8 profile reviews for existing heat counterparts

`brightpath/data/export/uvek_heat_38_profiles_review.json` pins independent source
3.8 cut-off inventories, producer references and both native releases for eight
existing correspondences. It qualifies coal-ash residual landfill, RoW rail freight,
global medium-voltage grid, cast aluminium alloy, cast iron, Swiss normal concrete,
European-without-Switzerland tap water and European low-voltage grid. Raw licensed
inventories remain outside Git.

Only concrete needs a new exact-profile entry. Its earlier 3.6 projection, shared
review and complete target/helper closure remain identical. Seven other raw rules
remain unchanged. The ash reference already requires factor −1: source is −1 kg
waste, native is +1 kg service, and actual source properties explicitly state zero
water. Keep that sign conversion once. All other caller reference factors remain +1.
Caller signs, formulas, supported uncertainty and physical units are preserved.

These remain complete native functional proxies, with differences logged:

- RoW rail's diesel/electric/steam portfolio becomes the native European
  electric/diesel service, including its shunting, vehicles and tracks.
- Grid portfolios, voltage/loss topology, infrastructure, trade and vintage differ;
  actual native coefficients are retained despite contradictory loss prose.
- Cast alloy's primary/secondary shares change; no pure-metal or recycling credit
  is inferred. Cast iron retains the complete native foundry boundary.
- Concrete changes CEM II/A–B at 200 kg cement/m³ to native CEM I at 300 kg/m³,
  with different performance and delivery. No density or cement-content scaling.
- Tap water uses the reviewed complete Swiss geographic proxy. The source network,
  treatment mix and supply loop are replaced; no extra loss or water-balance factor.
- Ash disposal retains the native waste composition and landfill model, with
  geography, operating allocation and short/long-term emission differences explicit.

No new recipe or shared review is introduced. The qualified concrete fingerprint
migration is structural, after equality of the earlier effective rule is proven;
no earlier scientific decision is silently refreshed. New permissions are exact
3.8 cut-off scopes, not version-wide or cross-system-model approval. Native
biosphere exclusions and unquantified score effects remain limitations.

The older deferred-material authoring replay accepts only this content-pinned
concrete extension. Unknown profiles, altered old targets or old review changes
remain refused. Its tests also restore the earlier epoxy authoring stage using the
immutable later review's exact previous-rule witness: that pre-existing stage
mismatch predates this batch. Actual migration tests continue to use final resources.
The production guard is not relaxed for arbitrary epoxy changes.

Verification covers signs, uncertainty, ownership, earlier profile preservation,
refusals, atomic replay, cold/warm warnings, signed finite LCIs and installed wheels.
Generator replay and original electricity callers are checked separately. Public
heat remains disabled pending remaining mappings and integration/export checks.
