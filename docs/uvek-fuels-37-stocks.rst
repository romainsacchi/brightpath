Six 3.7 stock and processing-service proxies
==========================================

The requested source context remains ecoinvent 3.7 cut-off. Reference/function
witnesses were inspected in the local 3.7.1 archive for exact matching
name/product/location identities and product UUIDs. The review makes a limited,
explicit inference about functional references using the `official patch report
<https://support.ecoinvent.org/hubfs/Knowledge%20Base/Database/Releases/change%20report_v3%207%201_20201217.pdf>`_.
None of these inspected roots or producers is among its named corrections.
This is not direct inspection of an unpatched inventory vector, source-context
relabeling or a claim of equal upstream inventories, uncertainty or scores.

Six complete native proxies retain signed caller quantities, formulas and
uncertainty at factor +1:

* Finished technical EPDM elastomer per kg, including extrusion/vulcanisation;
  the generic name does not mean raw polymer or every rubber chemistry.
* Unspecified metal-working machine material per kg of machine, with no
  machine-count, operating-hour or throughput conversion.
* Steel-construction hall infrastructure per square metre, excluding operation.
  Its illustrative geometry and 50-year life are not extra conversion factors.
* Multi-storey building infrastructure per cubic metre, excluding operation.
  Historic archetypes and an 80-year life do not justify density, height or
  repeated annualisation factors.
* Seamless-pipe drawing service per kg processed, excluding steel stock. Its
  existing 110% wire-drawing approximation is retained without applying 1.1 again.
* Powder-coating service per square metre, with existing phosphating, coating
  and curing. The 80 micrometre coating and 2 mm sheet heating assumption do
  not justify caller thickness or steel-density conversions.

Native inventories remain intact. Source supplier mixes, delivery and internal
uncertainty are replaced, not reconstructed; geography, technology, grade,
design, allocation, vintage and biosphere exclusions remain limitations with
unquantified score effects. Separate caller stock, transport and emissions
remain. The weak building archetypes require better data or sensitivity work
when their contribution is substantial.

The frozen ``uvek_fuels_37_stocks_review.json`` records the limited evidence
transfer, physical boundaries, reference signs, native finite LCIs and witness
hashes. Raw licensed vectors/comments remain private. Existing migration rules
and runtime modules are unchanged. No other release or system model borrows
these six exact approvals; complete fuel transformation/export remains unfinished.
