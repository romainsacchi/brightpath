# openLCA reference-catalog provenance

The JSON resources in this directory contain identity fields and openLCA UUID
references only. It does not contain background exchange amounts, process
descriptions, or complete UVEK or ecoinvent inventories.

`uvek__2025__cutoff__ecoinvent__3.10.json` was generated with
`scripts/generate_openlca_reference_catalog.py` from these locally installed
sources:

- the UVEK 2025 Brightway database and its source openLCA process filenames;
- the ecoinvent 3.10 biosphere identity database used by UVEK 2025; and
- the “UVEK 2025 Version 2” openLCA database backup dated 2026-03-09, which
  supplied exact process categories plus process, product-flow,
  elementary-flow, flow-property, unit, and location UUIDs.

The source database was distributed under the legacy BAFU label. BrightPath
normalizes that input name and publishes it as UVEK. The resource records exact
coverage for this database build: all 11,747 technosphere identities and 3,959
of the 4,362 ecoinvent 3.10 biosphere identities have matching openLCA
references directly. One separately reviewed fossil-well Water quantity
projection brings the packaged total to 3,960 references, leaving 402 source
identities unavailable. Export rejects a missing exact reference rather than
creating an unlinked lookalike flow.

Of these biosphere references, 3,954 join on the source Brightway UUID. Five
additional references use the explicitly reviewed correspondences in
`biosphere_correspondences.json`: Argon to unspecified air; Oxygen to
unspecified air and from air; Nitric acid to urban air; and Sulfuric acid to
unspecified water. The native backup gives each matching flow a different UUID.
Their names, compartments, Mass flow properties, and kilogram reference units
were checked against read-only native metadata. Existing shared-UUID flows
establish the canonical/native compartment-label correspondences. No amount,
uncertainty, or unit conversion is applied.

The correspondence resource pins the exact UVEK 2025 build and ecoinvent 3.10
biosphere profile. Its five source/native UUID pairs are not a general matching
rule. Regeneration requires `raw_references.native_source` to match the recorded
database build and archive SHA-256, and each selected native flow row must carry
the recorded `category_path`, `flow_type`, `unit_group_ref_id`, and
`reference_unit_factor`, in addition to the existing flow/property/unit fields.
Ambiguous UUID rows, changed identities, quantities, units, or native builds
are errors. The generator never searches for a replacement by name.

Of the 403 identities outside direct reference coverage, fossil-well Water uses
the separately reviewed `native_quantity_projections.json` correspondence;
the remaining 402 have no packaged reference. Fossil-well Water is not covered
by the five identity-only correspondences above. Its explicit quantity
projection is not a general volume-to-mass conversion or a substitute
compartment. These metadata links do not establish LCIA or Monte Carlo
equivalence between tools.

The `legal_review_required` status in `RESOURCE_MANIFEST.json` is intentional.
The manifest records provenance and integrity; it is not a license grant for
the source databases.

## UVEK 2026: supplied native v1.1

`uvek__2026__cutoff__uvek__2026.json` supplies exact references only for the
paired UVEK 2026 cut-off technosphere and UVEK 2026 biosphere. Its native input
is **BAFU-2026 v1.1 openLCA.zolca**, SHA-256
`e63b7e7ef5cde04bfe1176a783a55b5f72e2b73f42c36632b6150af39d348c4c`.
The filename identifies the supplied revision; no independent internal release
tag was recovered. This build is not relabeled as the original **v1**
EcoSpold1 archive, SHA-256
`ebaab56717a6227b5eb271fcb4c86fb9db206115eb937fb0ec0b10d7accf26c7`.
Metadata correspondence does not establish equality of the background recipes
between these two builds.

The catalog contains 11,947 technosphere references and 1,781 of the 2,679
canonical biosphere identities. The other **898 biosphere identities remain
unavailable**. A missing exact reference is an error, not a request to invent
a native identifier or select another compartment. The 135 current validation
cases have all 419 used technosphere and 29 used biosphere identities covered;
this subset is not a full native-background, LCIA, or Monte Carlo guarantee.

The reproducible metadata join uses the original EcoSpold1 process UUIDs and
reference-function headers, and native read-only process/reference-product,
location, category, flow-property, unit-group, and unit metadata. Only the
quantitative-reference exchange's identity/unit/property links are needed;
no native inventory vectors or characterization factors are queried. Join
the unique original process UUID first, then require the exact product and
the recorded location and quantity correspondence. Ambiguous or absent
references remain unresolved. The catalog includes the actual selected-unit
and reference-unit definitions, not process descriptions or background values.

Of the technosphere identities, 11,033 have identical name/product/location
and normalized unit keys. Explicit correspondences cover 17 metre-year,
13 kilometre-year and 166 person-kilometre labels; 48 location-code labels;
one source-generator whitespace trim; and 669 provider-specific normal-volume
labels. In particular:

- Original `my` is metre times year. Native `m*a` belongs to **Length*Time**,
  with factor 0.001 relative to `km*a`. One source metre-year selects one
  native `m*a`; the exchange amount is not multiplied by 0.001. `m2*a`
  belongs to a different quantity and is not an alias.
- Every normal-volume correspondence is enumerated by exact provider,
  product, location and source unit. Its original reference function declares
  `Nm3`; the same native provider UUID and description select `m3` in this
  build. This is not a global `Nm3 = m3` rule. No unrecorded temperature,
  pressure, or thermodynamic conversion is inferred.
- The six biogenic CO2/CO air identities use the explicit name-only rules in
  bw2io's `get_biosphere_2_3_name_migration_data`, already used by the UVEK
  2026 resource generator: biogenic maps to non-fossil at unchanged kg and
  exact compartment. Generic and fossil carbon identities are not fallbacks.
  The map's source is
  <https://github.com/brightway-lca/brightway2-io/blob/main/bw2io/data/__init__.py>.

No fresh-water/river/lake merge or amount/uncertainty/formula transformation
is part of this catalog. Its profile key does not apply to UVEK 2025,
ecoinvent biospheres, other exact UVEK versions, or consequential models.
As for the existing resource, the integrity/provenance manifest is not a
license grant for the source database.

Native technosphere rows may additionally record `native_flow_type` and
`native_reference_is_input`. The supported exact contracts are product/output
and waste/input. JSON-LD foreground exchanges use the opposite direction to
the provider reference, while retaining the source signed scalar, formula,
and marginal uncertainty. ILCD applies the same rule only after checking the
external identity, provider, flow UUID, property and flow type against the
exact paired catalog. This is distinct from the existing reversible sign
coordinate transform for locally authored negative-reference waste models.
Catalogs without the new fields retain their legacy product/output rendering,
with native reference direction explicitly unverified (`None`);
no waste or production model is inferred from a provider name.

The UVEK 2025 catalog's 11,747 native roles were also joined independently on
both the original 2025 process and product-flow UUIDs, using the same Version 2
backup (SHA-256
`28294c2201a4ac9a336ca52a31cbd02fa028c49d04ecb8455a87965d3d7aa89d`).
These verified roles comprise 981 waste/input and 10,766 product/output
references. Only the two role fields were added: the existing IDs, quantities,
profiles, category names and biosphere references were retained. No 2026 flow
IDs were copied into the 2025 catalog. In particular, the solvent-treatment
provider uses its own 2025 flow `aef7e2a0-7858-4d9a-aa56-42b862959d6a`,
not the different 2026 flow, despite sharing a process UUID across builds.
