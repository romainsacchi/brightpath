Reviewed fossil-well Water quantity projection
==============================================

For the exact UVEK 2025 cutoff / ecoinvent 3.10 biosphere target, the
canonical ``Water / water / fossil well / cubic meter`` flow
``2256a142-8242-4b4f-b9aa-a167803989ca`` uses the installed native
``Water / emissions to water / fossilwater`` flow
``7c98e916-5edf-3cc3-984d-3278fa1015cf`` on its existing **Mass / kg** basis.
The exported quantity is the canonical quantity multiplied by 1000.

This is an explicit reviewed format quantity correspondence. The published
SimaPro 9.4 to ecoinvent 3.7 correspondence identifies this exact water
compartment with match condition ``=`` and kg-to-m3 factor 0.001. This is the
same water accounting convention already used by the SimaPro codec; it is
not a measured density for individual produced-water inventories. The native
BAFU 2025 Version 2 archive independently supplies the exact native flow,
Mass property, kg unit, and reference factor 1. It has no secondary Volume
property. No Volume property or new native flow is added.

The separate ``native_quantity_projections.json`` resource records the source
UUID and full identity, target profile/build, native identifiers, factor and
primary mapping evidence. Its manifest and runtime scope checks prevent this
rule from becoming a general density conversion or a fallback for other Water
compartments, resource extraction, other native builds or UVEK 2026. The
binding to the native UUID is a reviewed semantic correspondence, rather than
a claim that the native publisher supplied that particular UUID crosswalk.

Both export paths preserve the canonical inventory on detached copies. Amounts,
linear bounds and formulas scale together. A lognormal location receives
``ln(1000)`` and its dimensionless spread remains unchanged; independent nominal
amounts and sampled centres stay independent. ILCD uses its existing explicit
reference-factor and inverse path. JSON-LD renders native Mass/kg references;
the native Monte Carlo graph is generated on that same kg basis. Readback
validates identifiers and graph structure before inverse scaling. Original
formula text is recovered only by undoing the exact emitted multiplication
wrapper, never by overwriting changed wire values from source metadata.

For the projected JSON-LD graph, canonical document loading requires native
uncertainty decoding. ``decode_native_uncertainty=False`` refuses that specific
combination; the raw ZIP remains available for literal kg graph inspection.
Other profiles and ordinary reference mappings retain their existing behaviour.

Round-trip tests cover actual JSON-LD/ILCD writing and canonical
readback, signed quantities, independent centres, intervals, formulas,
identifier tampering, repeat exports and input nonmutation. These checks do
not establish native application import, LCIA equivalence or general
compatibility beyond the exact quantity correspondence described above.

Primary evidence: https://github.com/brightway-lca/simapro_ecoinvent_elementary_flows/blob/f81c05f0aa6724dd3db07a59cad155b8d2d0f7c1/Mapping/Output/Mapped_files/SimaProv94-ecoinventEFv3.7.csv
