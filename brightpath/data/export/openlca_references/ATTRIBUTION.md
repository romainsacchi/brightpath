# openLCA reference-catalog provenance

The JSON resource in this directory contains identity fields and openLCA UUID
references only. It does not contain background exchange amounts, process
descriptions, or complete UVEK or ecoinvent inventories.

`uvek__2025__cutoff__ecoinvent__3.10.json` was generated with
`scripts/generate_openlca_reference_catalog.py` from these locally installed
sources:

- the UVEK 2025 Brightway database and its source openLCA process filenames;
- the ecoinvent 3.10 biosphere identity database used by UVEK 2025; and
- the “UVEK 2025 Version 2” openLCA database backup dated 2026-03-09, which
  supplied exact process, product-flow, elementary-flow, flow-property, unit,
  and location UUIDs.

The source database was distributed under the legacy BAFU label. BrightPath
normalizes that input name and publishes it as UVEK. The resource records exact
coverage for this database build: all 11,747 technosphere identities and 3,954
of the 4,362 ecoinvent 3.10 biosphere identities have matching openLCA
references. Export rejects a missing exact reference rather than creating an
unlinked lookalike flow.

The `legal_review_required` status in `RESOURCE_MANIFEST.json` is intentional.
The manifest records provenance and integrity; it is not a license grant for
the source databases.
