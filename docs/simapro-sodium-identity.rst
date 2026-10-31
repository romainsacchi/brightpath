Aqueous sodium in SimaPro exports
================================

Ecoinvent contains distinct aqueous elementary flows named ``Sodium``
(UUID ``723f56f9-c68e-5692-a969-0fb2a70bce20``) and ``Sodium, ion``
(UUID ``5ac961f0-8e53-42ff-b13f-1a7fb9133868``; named ``Sodium I`` in
newer releases). Equal amounts and compartments do not make these the same
flow for impact assessment.

For older releases including 3.6, 3.7 and 3.8, BrightPath's SimaPro name
mapping writes both as ``Sodium``. The bw2io-based reader resolves that label
to the ionic flow. This loses the generic flow's original identity.

SimaPro preflight now reports ``simapro_biosphere_identity_ambiguous`` and
refuses to write an affected generic-flow export before opening its output
file. It checks the exact biosphere catalogue, both UUIDs, the water
compartment, kilogram unit and rendered labels. Deterministic zero exchanges
without a formula are ignored; potentially active zero exchanges are checked.
Ionic-only exports retain their existing mapping. The guard also covers newer
releases 3.9 and 3.9.1: bw2io still interprets the plain native ``Sodium`` label as ionic
sodium even when the catalogue calls that flow ``Sodium I``.
From 3.10, the generic UUID is absent from the catalogue; adapting an older
inventory to those releases remains an explicit background migration.

Use Brightway, openLCA JSON-LD or ILCD when this error occurs, subject to those
formats' own validation requirements. BrightPath does not substitute the
ionic flow, invent a native SimaPro substance label, or modify stored source
inventories. A future lossless SimaPro representation requires evidence for
a distinct native substance identifier or label.
