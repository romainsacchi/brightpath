Aqueous sodium in SimaPro exports
=================================

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
Ionic exports use the UUID-bound native label described below. The guard also covers newer
releases 3.9 and 3.9.1: bw2io still interprets the plain native ``Sodium`` label as ionic
sodium even when the catalogue calls that flow ``Sodium I``.
From 3.10, the generic UUID is absent from the catalogue; adapting an older
inventory to those releases remains an explicit background migration.

Use Brightway, openLCA JSON-LD or ILCD when this error occurs, subject to those
formats' own validation requirements. BrightPath does not substitute the
ionic flow, invent a native SimaPro substance label, or modify stored source
inventories. A future lossless SimaPro representation requires evidence for
a distinct native substance identifier or label.

Verified native ionic labels
----------------------------

The stock SimaPro aqueous substance is named ``Sodium``, with CAS
``17341-25-2`` and definition ``Na+``. Its public SimaPro9.4 mapping contains
exact, factor-one kilogram matches to the following ecoinvent ionic UUIDs:

.. list-table:: Separate compartment bindings
   :header-rows: 1

   * - Canonical water compartment
     - Native subcompartment
     - Ionic ecoinvent UUID
   * - unspecified
     - blank
     - ``5ac961f0-8e53-42ff-b13f-1a7fb9133868``
   * - ground-
     - groundwater
     - ``ee9fdd7c-5e29-4bdb-852a-3258f06ec2ab``
   * - ground-, long-term
     - groundwater, long-term
     - ``fb83d61e-6fe3-4da7-8eb2-b0e2dc85a76d``
   * - ocean
     - ocean
     - ``9310f497-3205-4717-aaa8-1a94c321f7d8``
   * - surface water
     - river
     - ``1fc409bc-b8e7-48b2-92d5-2ced4aa7bae2``

BrightPath checks the exact declared biosphere catalogue, UUID, compartment
and kilogram unit before writing this label, including when a newer catalogue
calls the ionic flow ``Sodium I``. Reading the native literal resolves the unique
same ionic UUID in the selected catalogue before generic name aliases. A missing,
ambiguous or unverified identity is refused. A custom biosphere flowset without
a UUID-bearing catalogue provider cannot establish the binding and is refused;
with a provider, the restored identity must also belong to that flowset.
Source amounts, formulae and
uncertainty fields are not used to infer the identity or recover lost values.

The rule follows the biosphere family, independently of the technosphere family;
UVEK2025 with an ecoinvent biosphere uses the same check. Native UVEK2026
biosphere naming is unaffected. Unknown custom names, units or grades do not
receive an ionic alias.

Generic ``Sodium`` in groundwater is also a distinct UUID
(``73a1b292-e31d-4100-a413-304795a3656f``). Generic flows in any of these
compartments remain refused when their catalogue UUID differs from the proven
ionic UUID and their rendered label would be the stock ``Sodium`` label.
Equal names in the public mapping are insufficient: that table also contains a
same-name generic match. The independent ionic substance definition is required;
this rule does not substitute a generic flow with an ionic flow.

Metadata provenance: `SimaPro9.4-to-ecoinventEFv3.7 mapping, pinned revision
f81c05f0aa6724dd3db07a59cad155b8d2d0f7c1
<https://github.com/brightway-lca/simapro_ecoinvent_elementary_flows/blob/f81c05f0aa6724dd3db07a59cad155b8d2d0f7c1/Mapping/Output/Mapped_files/SimaProv94-ecoinventEFv3.7.csv>`_.
The matching SimaPro substance list identifies the substance as ionic sodium.
A licensed SimaPro9.5 metadata-only scan independently witnesses the literal
``Sodium`` in these water contexts; its CSV did not export platform IDs.
Canonical round-trip tests and metadata proof do not establish desktop-native
application acceptance or LCIA result equivalence.
