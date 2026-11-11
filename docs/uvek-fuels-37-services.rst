Rail, factory services and compressed-air supply
===============================================

Four requested ecoinvent 3.7 cut-off references use explicitly limited 3.7.1
functional evidence. The source context stays 3.7; unpatched vectors and
upstream results are not asserted equal to the patch. Only the named services
are qualified.

* Rail freight retains metric tonne-km of complete transport, with traction,
  shunting, rolling stock and tracks. Native European mix replaces the original
  country mix. No second traction or infrastructure, payload, distance or
  gross/net-tonnage conversion is added.
* Factory ancillary operation retains kg final metal product. Average heat,
  lighting, water and waste services remain once; process-specific machine
  work and factory construction remain separate.
* Factory construction retains a complete infrastructure unit. The documented
  4.58e-10 units/kg use recommendation is not applied again to an already
  unit-based caller quantity. No capacity, area or lifetime conversion is added.
* Compressed air retains its declared cubic-metre reference at 700 kPa gauge,
  equivalent to 7 bar gauge. The old compressor-only candidate omits the source
  supply-network boundary. A new helper uses complete native average, best and
  optimised >30 kW network installations, including distribution material and
  leakage. The observed patch market's three equal nominal shares are adopted
  as an explicit proxy split, not independently recovered unpatched data.

Native network generation factors remain 1.30, 1.10 and 1.15. The inspected
patch's leakage convention differs; neither electricity nor compressor capital
is forced to match it. No extra loss, efficiency, pressure/gas-law or volume
multiplier is added. Unspecified ambient reference conditions are not inferred.
Native site-installation exclusions remain. The xx-labelled best/optimised
network processes were checked as nonempty complete services; factory-specific
leakage and efficiency still require sensitivity/detail review when important.
Other air profiles remain refused pending their own reviews.

Signed caller quantities, formulas and supported uncertainty retain factor +1.
Native geography, technology, allocation, internal uncertainty, vintage and
biosphere exclusions differ, with unquantified score effects. Source/native
vectors and LCIA results are not claimed equal. Four weighted native LCIs are
finite. Raw licensed vectors/comments remain private; the frozen
``uvek_fuels_37_services_review.json`` binds physical evidence and limitations.
No runtime Python module changes. Full fuel transformation/export remains
unfinished.
