Chemical-steam heat: retain physical-water uncertainty
======================================================

The exact ecoinvent 3.8 cut-off RoW chemical-heat market references one MJ of
thermal energy. It is not kg fresh steam. The original native heat definition
also supplies one MJ and excludes water for closed steam systems. Use its
complete heat inventory once, retaining the source's separate make-up-water
inputs and physical water releases. No 0.36 kg/MJ conversion or duplicated
fuel/electricity burden is introduced.

A scoped refinement keeps the same nominal energy and water quantities while
retaining the original lognormal amount, location, scale, pedigree and supported
statistics on all seven water-supply entries and both water releases. The
country shares stay separate. Native Swiss water is the existing geographic
screening proxy; no share normalization, omission or water-density multiplier
is added. Receiving compartments and cubic-metre water units remain exact.

The two background release formulas refer to the original producer's parameter
context and are not transplanted into the native helper. Caller formulas are
preserved. Source energy-system uncertainty, background correlations and
parameter recalculation are not reconstructed. Native heat/water technology,
sourcing, efficiency, allocation, infrastructure, vintage and internal
uncertainty differences remain explicit warnings, with unquantified score
effects.

Only the existing 3.8 market profile selects the refined helper. All eight other
profile selections and every older helper remain unchanged. Evidence is frozen
in ``export/uvek_fuels_38_chemical_heat_review.json`` and binds the original
native XML/MJ water-boundary review. Raw source energy inventories and
background formulas remain private.

A temporary actual Brightway foreground write linked both water flows to the
installed 3.10 biosphere, preserved all nine uncertainty records, and produced
a finite LCI. The temporary database was removed and the source inventory was
unchanged. This check does not qualify the complete fuel-sector update/export.
