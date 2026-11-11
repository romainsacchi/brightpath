Nutrient references and pesticide production proxies
====================================================

Twelve independently inspected ecoinvent 3.8 cut-off correspondences cover
nitrogen, phosphorus and potassium supply in Indonesia, Switzerland and the
United States, plus three pesticide classes. Nine existing Brightpath rules
remain byte-identical; three incorrect physical matches change for this exact
source profile. No runtime Python, new helper, source omission or application
emission factor is introduced.

Nutrient supply
---------------

Source mineral-fertiliser markets reference kg N, kg P2O5 or kg K2O nutrient
equivalent. The complete native European average fertiliser records explicitly
declare the same nutrient references. A caller's one kg nutrient therefore
remains one kg nutrient, rather than becoming one kg of commercial fertiliser,
another nutrient or waste treatment. Two nitrogen matches previously pointing
to nitrogen-containing plastic disposal and phosphoric acid now use native
average nitrogen supply.

The native historical European carrier mixes replace the source country-specific
mixes and NPK co-product allocation. Manufacturing routes, nutrient availability,
carrier grades, co-allocation, freight, geography and vintage differ, with
unquantified score effects. No elemental-P/P2O5 or K/K2O molecular conversion,
commercial-product concentration, crop uptake, yield, application efficiency or
avoided co-nutrient credit is inferred. Separately listed crop and field flows
remain unchanged.

The original native input mixtures are not perfectly normalized: nutrient-carrier
coefficients sum to 0.973 for N, 1.223 for P2O5 and 1.0013 for K2O per declared
one-kg output. These are explicit original-data consistency limitations. The
complete native models are retained as screening proxies; no checked nutrient
mass balance is claimed and no silent renormalization changes source data.

Pesticide production and field releases
---------------------------------------

Unspecified pesticide and bipyridylium-class stock use their complete native
Swiss production records per kg active substance. Their mixture, chemistry,
plant, delivery, geographical and vintage boundaries replace the global source
markets. Native bipyridylium production is itself estimated from generic
herbicide production; it is not ingredient-specific production or toxicity data.

The pyrethroid market previously mapped to polysulphide sealing compound. It
now uses complete native European generic insecticide production as an
explicitly broader-class **production-burden screening proxy**. The database has
no pyrethroid-specific production supplier. Native generic insecticide chemistry,
manufacture and toxicity inventory are not the source arithmetic mean of
representative pyrethroids. Score differences are unquantified, and important
pyrethroid-production contributions require ingredient-specific sensitivity or
detailed review. This proxy does not establish equivalent pest-control efficacy,
dose, persistence, field toxicity or active ingredients.

Separately listed application emissions keep their original chemical identities
and quantities. No production proxy supplies new field releases or changes a
Cypermethrin release to generic insecticide. No solvent, dilution, pesticide
effectiveness, toxicity multiplier or avoided-pest credit is introduced.

Evidence and qualification
--------------------------

``data/export/uvek_fuels_38_nutrients_review.json`` freezes source/producer and
native witnesses, functional references, original input-sum limitations and the
three corrections. Replay rejects drift before mutation, preserves nine existing
rules and introduces no new recipes. Premise authorizes only the inspected
3.8 cut-off fingerprints, preserving earlier approvals and their evidence.

Tests cover strict native links, signed caller demand, normal/lognormal
uncertainty, formulas, exact source-profile scope, unchanged rules and separately
listed pesticide application emissions. Actual installed native LCIs, source
non-mutation, complete resource replay and package contents are checked separately.
Native finiteness and mapping acceptance do not qualify prospective fuel
transformation/export or other registered sectors. Premise's companion receipt
records exact tests and licensed preparation results.
