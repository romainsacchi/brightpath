Heat, wastewater and recycled aluminium
======================================

Seven inspected ecoinvent 3.9 cut-off correspondences extend UVEK fuel
preparation. Six direct matches change; the allocated sewage-gas rule remains
unchanged and receives an exact Premise approval. No runtime Python changes,
new recipes, physical input omissions or recovery credits are introduced.

Functional references and proxies
---------------------------------

* Useful biogas cogeneration heat uses the complete Swiss native exergy-allocated
  heat inventory per MJ. Fuel consumption, efficiencies and allocation differ;
  no fuel-ratio multiplier or additional electricity credit is applied.
* The Europe-without-Switzerland and rest-of-world natural-gas heat markets use
  the complete native ENTSO-E heat mix per MJ. Its country coverage and supplier
  portfolio replace the source mixes. The European proxy for rest-of-world is
  explicitly warned; shared Premise transformations remain responsible for
  scenario updates. No alternative market engine is introduced.
* Average wastewater treatment uses the complete Swiss class-1 sewage treatment
  per cubic metre. Factor -1 translates the negative source service reference
  to the positive native reference. Composition, treatment scale, sludge,
  emissions and geography differ; no COD or density multiplier is inferred.
* Secondary wrought aluminium uses complete native old-scrap remelting and
  billet production, rather than the primary/secondary production mix. The
  positive metal-product reference remains positive despite the source's
  treatment name. Scrap preparation, alloy, yield and residue differences are
  warned; no yield multiplier or virgin-production credit is added.

Two distinct allocation cases
-----------------------------

Native Swiss municipal-incineration heat has one positive production exchange
and no other exchanges. The original EcoSpold archive explicitly assigns all
burdens to disposal and none to energy. It is a complete native allocated
co-product, not an invented empty helper or an omitted caller exchange.

The source heat supply is not burden-free: three of its 58 suppliers have
nonempty inventories, together representing 6.5877% of supply. Its actual LCI is
nonzero. Using native heat therefore changes allocation and technology, with
an unquantified score effect. No fabricated heat burden, disposal charge or
avoided-boiler credit compensates for this difference. The frozen review
records the original archive witness and source LCI evidence.

Sewage-digestion gas is a separate case: the inspected source gas product itself
has only a positive production reference, while the paired negative-reference
sludge service carries treatment burdens. Its existing allocated-stock helper
and all older profile targets remain unchanged. This does not make sludge
treatment burden-free or equate it to native gas storage with a different
inventory boundary.

Verification and limitations
----------------------------

Evidence is frozen in ``data/export/uvek_fuels_39_heat_recycling_review.json``.
Maintainer replay validates its hash, exact references, target identities,
allocation evidence and unchanged helper dependencies before mutation.
Licensed raw inventories remain private. Strict conversion tests preserve
signed amounts and uncertainty, require documented-proxy policy and reject
unreviewed profiles. Native functional-output LCIs are checked separately;
structurally empty inventory is distinguished from numerical solver residuals.

These correspondences alone do not qualify a prospective fuel transformation
or database export. Exact preparation, package and workflow results are recorded
in Premise's companion verification receipt.
