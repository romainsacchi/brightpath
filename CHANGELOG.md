# Changelog

All notable changes to this project will be documented in this file.

## 1.0.0a11 - 2026-10-29

- Add seven publisher-record process correspondences in both UVEK release
  directions. Require explicit information-loss acceptance for the four CH/RER
  wind-component geography revisions; retain the washing-machine unit blocker.
- Add five exact modern land mappings and seven independently checked reverse
  mappings, raising native biosphere coverage to 1,935/2,679 and reverse coverage
  to 1,297/4,362. Preserve prior rules and all land-class qualifiers.
- Convert generated recipe biosphere flows independently to the requested
  context. Fix the CdTe factory export with a preserved native UVEK biosphere;
  report helper findings separately and roll back the whole export on failure.
- Require explicit information-loss acceptance before omitting a biosphere
  exchange whose unit conversion lacks a factor. Strict execution rolls back.

- Require explicit heuristic-mapping acceptance: strict migration now rejects
  heuristic compatibility resources. Add `on_heuristic_mapping` and the CLI
  `--allow-heuristic-mapping` option; accepting heuristics leaves other strict
  checks intact. Record the policy choice and limitations in migration reports.
- Add an independent native UVEK 2026 route audit covering strict and warning
  policies, both background axes and exact packaged catalog versions. Expose
  unapproved export suppliers and unavailable routes separately; partial audits
  cannot pass the release gate.
- Add 158 forward radioactivity correspondences with exact publisher names,
  contexts and UUIDs and the explicit SI factor 0.001 from Bq to kBq. Increase
  native biosphere coverage at this review stage to 1,930 of 2,679 identities,
  leaving 749 unresolved before the subsequent land review.
  Preserve the previous 1,772 rules and all reverse mappings. Flag 56 additional
  lake/river compartment merges; strict policy accepts the other 102 additions.
  Scale amounts, supported uncertainty distributions and BrightPath expressions
  together. Retain CLIC's separate guard against formula-bearing unit changes.
- Add 55 further forward native biosphere mappings: 32 publisher context
  correspondences, 19 guarded CAS-based synonyms, and four exact cubic-meter
  name-suffix correspondences. Increase native coverage from 1,717 to 1,772
  identities, leaving 907 unresolved. Preserve all prior mappings and reverse
  resources. Retain long-term groundwater contexts and flag nine additional
  surface-water coarsening rules. Reject regional water, dioxin-equivalent,
  carbon-origin and contradictory-CAS substitutions.
- Preserve SimaPro ammonium-ion aliases against exact modern ecoinvent catalogs
  and restore water amount expressions after reversible mass/volume wire scaling.
  Keep caller-owned inputs and the separate CLIC formula-migration guard intact.
- Add 427 reviewed forward native UVEK 2026 biosphere mappings to ecoinvent
  3.10, increasing coverage from 1,290 to 1,717 of 2,679 identities. Bind 402
  correspondences to publisher UUIDs, 22 to documented lake/river merges,
  and three to explicit pure-water mass/volume factors. Leave 962 unresolved
  identities, including nine CAS conflicts, and preserve the reverse resource.
  Report 81 compartment-coarsening rules through the information-loss policy;
  strict execution refuses them, while an explicit warning policy retains
  quantities, uncertainty and per-exchange loss findings.
- Validate native UVEK 2026 biosphere compartment labels without rewriting them
  to ecoinvent compartments. Preserve them through Brightway Excel and openLCA
  round trips and restore unique exact native identities when parsing SimaPro.
  Refuse SimaPro compartments without a supported section instead of omitting
  their exchanges. These corrections were found by CLIC integration tests.
- Add 1,070 reviewed reference-function correspondences from UVEK 2025/2026
  to ecoinvent 3.12 cut-off, expanding approved process coverage from 354 to 1,424.
  Retain all prior rules, proxies and supporting recipes unchanged.
- Derive the additions from a pinned public ecoinvent correspondence workbook,
  exact source labels, unchanged units/geography, and stable target product UUIDs.
  Keep ambiguous rows, change flags, multi-output processes and waste-role cases
  unresolved. This process expansion is independent of the biosphere review.
- Expand the migration audit to 52,074 probes: 50,552 supported, 1,497 blocked
  and 25 explicitly omitted with warnings. Preserve all 42,444 prior outcomes,
  output hashes, policies and 483 collision findings.
- Verify a publisher-correspondence export from native UVEK 2026 to ecoinvent
  3.12 in installed-package smoke checks, preserving quantities, uncertainty,
  caller-owned input and the explicitly unchanged native biosphere.
- Fix Anaconda publication by invoking the standalone client, with targeted
  recovery from an already published GitHub/PyPI release.

## 1.0.0a10 - 2026-10-28

### ILCD process exchange

- Add ILCD 1.1 single-reference process import/export, local reference resolution,
  pinned offline schemas, deterministic ZIPs and transactional publication.
- Preserve per-process ILCD metadata and reference closure in JSON-compatible
  snapshots. Add exact reference mappings and explicit openLCA supplier extensions.
- Add process-scoped arithmetic, representable uncertainty, candidate analysis,
  structured preflight errors and ILCD CLI configuration/dry runs.
- Add independent openLCA import/export verification using synthetic processes.
- Resolve ILCD openLCA background references automatically from exact ecoinvent
  UUID catalogs and the verified UVEK build, with used-reference XML generation
  and consistent quantity, formula, and uncertainty conversion.

### UVEK 2026 background migration

- Add the original UVEK 2026 reference catalog with 11,947 process identities
  and 2,679 native biosphere identities. Retain native flow names and compartments;
  the separately remapped ecoinvent 3.10 biosphere remains an explicit profile.
- Add independent directed UVEK 2025/2026 cut-off process resources and separate
  native UVEK 2026/ecoinvent 3.10 biosphere resources. Compose cross-family routes
  through the existing UVEK 2025 mappings and preserve an unchanged biosphere.
- Apply 755 source-reviewed factor-of-1 reference-unit label correspondences
  in both process directions, retaining quantities, uncertainty and caller-owned
  input. Bind each rule to the historical source review and its integrity hashes;
  these factors do not define general gas-volume or time-unit conversions.
- Support 11,739 process correspondences in each direction. Keep seven unmatched
  2025 identities, 207 unmatched 2026 identities and the unapproved washing-machine
  reference unit explicit; strict execution rejects affected links transactionally.
- Include 1,290 one-to-one biosphere correspondences, with explicit water factors.
  Retain ambiguous and unmatched biosphere flows as unresolved: coverage is partial
  and reference correspondence does not establish equivalent LCIA results.
- Add reproducible catalog generation and unit-review utilities, exact-resource
  manifests, capability discovery, CLI dry-run coverage and installed-wheel checks.

### Packaging

- Preserve checksummed ILCD schemas and UVEK unit-review evidence across Windows
  checkouts, and write the standalone ILCD test fixture explicitly as UTF-8.
- Resolve local schema file URIs on Windows while retaining offline-only schema
  resolution and rejecting external paths and remote hosts.
- Pin the alpha version in installation instructions and extend installed-package
  smoke checks to load the native UVEK 2026 catalog and all four directed release
  resources, then exercise both process and biosphere migration directions.
- Refresh the migration and reference-catalog manifest digests in the release
  regression baseline after an installed-wheel audit. All 42,444 existing outcomes,
  output hashes, policies and 483 collision findings are unchanged: 40,922 cases
  remain supported, 1,497 blocked and 25 explicitly omitted with warnings.

## 1.0.0a9 - 2026-10-23

- Add XSD-validated ecoSpold1 XML detection, reading, and writing for process
  datasets with one reference output and no allocation records.
- Preserve ecoSpold1 provenance as structured dataset metadata and require an
  explicit YAML/JSON metadata profile when other formats do not provide the
  mandatory source, person, classification, and time-period fields. Free-form
  descriptions are not interpreted.
- Add adapter options shared by conversion preflight and writing so dry runs
  validate the same ecoSpold1 metadata used for serialization.
- Add XSD-validated ecoSpold2 activity detection, reading, and deterministic
  single-file, ZIP, and directory writing for the single-output process subset.
- Add structured ecoSpold2 export metadata and exact UUID reference catalogs;
  free-form descriptions are never parsed for required person or time fields.
- Load exact packaged ecoinvent ecoSpold2 UUID references automatically from
  the document context, with manifest-audited per-profile coverage and a strict
  exact-release enrichment script.
- Include complete activity, intermediate-flow, and elementary-flow UUID
  coverage for every packaged ecoinvent profile from 3.6 through 3.12,
  including the 3.9.1 and 3.10.1 cut-off patch profiles.
- Refresh the migration-audit resource digest for the expanded reference
  catalogs without changing the established route-result baselines.
- Verify generated ecoSpold2 packages with ``bw2io`` extraction, strategies,
  and database writing in an isolated Brightway project.
- Route content-detected ecoSpold1 and ecoSpold2 uploads through the public
  analysis API, returning canonical candidate snapshots, structured format and
  background-link issues, preserved metadata, and the exact source context.

## 1.0.0a8 - 2026-10-22

- Include exchange `database` labels in Brightway Excel, CSV and TSV exports
  for Activity Browser imports. Production and links to included foreground
  activities use the document database name. Missing external labels default
  to the exact technosphere profile (for example `ecoinvent-3.12-cutoff`) or
  `biosphere`; explicit external database names and input-key labels survive.
- Keep database labelling confined to export copies, preserving source data,
  formulas, parameters, uncertainty and independent background profiles.
- Database labels describe dependencies; users can map them to their local
  database names in Activity Browser. Background databases are not included.

## 1.0.0a7 - 2026-10-16

### Shared migration resources

- Use the validated `brightpath.migration` schema, version 3, for both UVEK
  technosphere directions. The common structure follows ecoinvent's `replace`,
  `disaggregate` and `delete` operations, with explicit source/target profiles,
  optional confidence scores, shared review records and foreground recipes.
- Remove duplicated unresolved supplier catalogs, comparison revisions, activity
  identifiers, evidence hashes and generation metadata from runtime mappings.
  Keep decision history in the existing authoring ledgers and review documents.
  Rename runtime mapping methods to omit the historical `clic_` prefix.
- Preserve all 31,488 forward mappings, 354 reviewed reverse mappings, confidence
  scores, quantity factors, proxy classifications and seven public helper recipes.
  Mapping coverage and scientific limitations remain unchanged. Upstream ecoinvent
  rule bodies and attribution are retained; their legacy files normalize on load.
- Package the offline JSON Schema and require `jsonschema>=4.18,<5` in both pip
  and Conda installations. Extend the installed-package smoke test to load and
  validate both UVEK technosphere resources.
- Read and write authoring JSON and migration-resource test inputs explicitly as
  UTF-8, preserving non-ASCII identities and evidence on Windows. Exercise the
  authoring utilities with both UTF-8 and legacy Windows default encodings.

### Quantity and uncertainty preservation

- Scale uncertainty locations, spreads, bounds, signs and formulas together with
  exchange amounts when generic background migration applies a conversion factor.
  Share the existing reviewed UVEK export scaling implementation across directions.
- Roll back unsupported uncertainty distributions and malformed statistics with
  a structured `migration.amount_scaling_invalid` error under strict and permissive
  policies. Exercise every forward supplier rule against exact supported catalogs.

### Compatibility and upgrade notes

- Keep public inventory import/export APIs unchanged. Code that directly reads
  migration resources must adopt v3: `rules` becomes `replace`, `factor` becomes
  `conversion_factor`, helper references use `target.recipe_ref`, and profile
  scopes use `source_profiles`. Review evidence is resolved through `review_ref`
  and `reviews`; unresolved suppliers are absent rules, counted in `coverage`.
  See the shared migration-schema guide and maintainer upgrade script.
- Refresh the migration fingerprint so applications can invalidate cached exports.
  Downstream applications should update their exact dependency pin and rebuild
  all conversion processes together. CLIC's UVEK test fixtures that directly read
  the previous resource structure need corresponding updates.
- Retain the 42,444-case reverse-route regression baseline: 40,922 supported,
  1,497 blocked and 25 explicit biosphere omissions. Unreviewed suppliers and
  ambiguous older-target migrations remain subject to the same policies.

## 1.0.0a6 - 2026-10-08

### Background migration

- Add the missing RER tissue-paper-output rename between ecoinvent 3.10/3.10.1
  and 3.11. Licensed process metadata confirms recycled, deinked production;
  the existing waste-paper-output rules remain unchanged. Native forward and
  backward migrations and composed UVEK routes preserve quantities and
  uncertainty and still validate exact targets. See the recycled-paper migration
  evidence guide; this does not assert equal background impacts.

### Migration release safeguards

- Fix a biosphere reverse-migration crash discovered by the exhaustive audit:
  shared targets with list-valued categories now compare structurally instead
  of being used as unhashable dictionary keys. Distinct candidates remain
  ambiguous; quantities and uncertainty are unchanged.

- Audit every approved UVEK supplier and every source biosphere identity across
  exact packaged ecoinvent cut-off targets, including composed migrations and
  generated helpers. Report blocked identities without choosing new proxies.
- Inventory forward/reverse resource collisions and gate CI and publication on
  reviewed coverage/output baselines using the newly built wheel. Unexpected
  exceptions and broken rollback or preservation invariants always fail.
- Retain full diagnostic reports as CI artifacts; distinguish regression-gate
  success from scientific equivalence or universal export compatibility.

### CLIC compatibility and remaining limits

- Retain the reviewed aluminium reverse proxy released in 1.0.0a5. The
  corrected paper mapping passes the 20-version public TOPCon basket in
  Brightway, SimaPro and openLCA at ecoinvent 3.10.1, 3.11 and 3.12.
- The 42,444-probe audit records 40,922 supported cases, 1,497 blocked cases
  and 25 explicit biosphere omissions. All 354 approved supplier probes pass
  at 3.10, 3.10.1, 3.11 and 3.12 under the documented policy; older-target
  ambiguities and unreviewed suppliers remain errors.
- CLIC's traceable SimaPro category shortening is a separate application fix;
  installing this release alone does not change CLIC or relax BrightPath's
  Desktop validation. Rebuild CLIC web and export-worker images together with
  its matching application update. No published inventory rewrite is required.

## 1.0.0a5 - 2026-10-08

### Background migration

- Resolve the ambiguous reverse mapping for European primary aluminium in
  ecoinvent 3.12-to-3.11 migrations, including composed UVEK exports to 3.11 and
  3.10.1. A documented compatibility proxy retains primary production instead
  of selecting the merged import activity.
- Report the proxy's regional and supply-composition limitations, preserve
  quantities and uncertainty, and retain strict rejection of unreviewed
  ambiguities. Refresh the resource manifest and conversion fingerprint.
- Include the fix from commit `8feafa6` and regressions for native and composed
  routes, information-loss policy rejection, and unchanged forward mappings.

### Known limitation

- The expanded CLIC photovoltaic basket still contains SimaPro category folder
  names exceeding the Desktop limit of 60 characters. This release fixes the
  background migration ambiguity; those category names must be corrected before
  the complete basket can export to SimaPro. Strict format validation is retained.

## 1.0.0a4 - 2026-10-07

### SimaPro Desktop compatibility

- Enforce SimaPro's separate 60-character category-folder limit in addition to
  the total path limit, and include the required category in system descriptions.
- Preserve the native `Water, salt, ocean` resource name for ecoinvent quantities
  in cubic metres instead of mapping it to the incompatible legacy cooling-water
  substance. Quantities and uncertainty are unchanged.

- Keep generated SimaPro process identifiers within the numeric range accepted
  by Desktop and detect duplicate identifiers before writing.
- Write `Waste treatment allocation` for waste-treatment processes instead of
  the disallowed multiple-output and substitution-allocation fields. Reject
  non-default metadata in disallowed fields rather than silently losing it.
- Reject category paths longer than 255 characters and system-description names
  or references longer than 50 characters with structured diagnostics.
- Add synthetic regressions based on Desktop 9.5.0.2 import logs, independently
  of the source ecoinvent version. Exercise the shared format rules with both
  ecoinvent and UVEK profiles, without requiring Premise.

### SimaPro export preservation

- Retain reviewed ecoinvent biosphere flows across releases instead of omitting
  oxygen, radionuclides, turbine water, road/reservoir occupation and biomass energy.
- Render the two evidenced landfill inventory indicators in Final waste flows and
  preserve native final-waste rows on import. Unsupported indicators are explicit errors.
- Infer non-waste category types independently of waste status, retaining energy,
  transport, processing and use when supported by product evidence. Unknown roles
  require review instead of silently defaulting to material.
- Recognize positive mass-based recyclable-material markets from CPC 392/393,
  preserving supplier-link signs. Explicit categories remain authoritative.
- Add a local-reference audit and cross-version/system-model regression tests.

### Validation and release tooling

- Confirm native SimaPro import without errors or warnings for a full ecoinvent
  3.12 cut-off scenario containing 42,758 processes and 1,331,278 exchange rows.
  This confirms that export's importability; LCIA equivalence is not implied.
- Run Conda build and upload tools in the base environment where they are
  installed, fixing the previous release's missing `conda build` command.
- Consolidate the unreleased 1.0.0a3 fixes into this release.

## 1.0.0a2 - 2026-10-06

### Packaging

- Build the mandatory `olca-schema` Conda dependency from a checksum-pinned PyPI
  source and publish it alongside BrightPath. It remains required for both pip
  and Conda installations.
- Pin Conda build and test environments to Python 3.12 and verify an openLCA
  JSON-LD round trip in the installed BrightPath package.
- Include the Conda recipes and package smoke test in source distributions.

### Breaking

- Raised the minimum supported Python version to 3.12.
- Changed generated openLCA flow UUIDs to distinguish compartments and supplier locations.
  Re-export affected inventories into a fresh target database to avoid obsolete synthetic flows.
- Corrected openLCA lognormal uncertainty serialization. Regenerate existing JSON-LD exports
  with unconverted parameters from their original inventories.
- Deleted `BrightwayConverter` and `SimaproConverter` and replaced them with the independent
  `BrightwayInventory` and `SimaProInventory` APIs.
- Separated file-format operations from background family, version, and system-model migration.
- Removed implicit UVEK amount conversion and distribution-transport injection from SimaPro
  serialization; background transformations now require an explicit migration route.
- Replaced the combined background assumption with exact, independent technosphere and biosphere
  profiles. Ecoinvent patch versions are preserved instead of truncated to major/minor.
- Made migration and conversion loss handling strict by default. Inferred reverse routes require an
  explicit permissive policy, and unsafe unit-changing rules are never applied without factors.
- Unified operation failures around immutable reports; upload validation and writer validation now
  use the same exception hierarchy.
- Removed the legacy public `brightpath.migrations.migrate_inventory` entry point, which could not
  represent independent biosphere contexts or transactional validation.
- Enforced software-format identity at facade and reader boundaries; contradictory exact and legacy
  context arguments now fail instead of being ignored.
- Kept every SimaPro parse tied to an exact `InventoryContext`; upload analysis can now construct
  exact candidate contexts from a complete technosphere profile while comparing biosphere catalogs.

### Added

- Preserved shared parameter scopes in upload analysis, SimaPro exchange expressions
  and parameter metadata, and openLCA calculated/input parameter semantics.
- Preserved distinct UVEK SimaPro activity/product identities and bundled foreground
  links with bounded, validated round-trip metadata.
- Resolved linked UVEK openLCA imports against the exact integrity-checked reference
  catalog, rejecting conflicting identifiers, quantities and metadata.
- Kept biosphere chemical formulas out of exchange amount-expression fields and
  advanced the migration fingerprint to invalidate old fingerprint-checked exports.

- Added opt-in ISIC/CPC waste inference for SimaPro, preserving explicit categories
  and requiring review of ambiguous cases. Reference-product names, units and signs
  constrain recovered-product inference; conflicting metadata and positive treatment
  references require review.
- Added caller-supplied SimaPro folder hierarchies independently of waste classification.

- Added detection, reading, and writing of process-only openLCA JSON-LD ZIP packages.
- Added optional openLCA reference linking for UVEK 2025 with its ecoinvent 3.10 biosphere,
  and native process-category inference from classifications and reference catalogs.
  Explicit source categories are preserved.
- Added opt-in mapping to locally supplied ecoinvent openLCA method packages for explicit
  biosphere versions 3.5–3.12, preserving patch releases. Mapping uses exact elementary-flow
  and quantity references, UUID-scoped gas-volume labels, and an inventory coverage sidecar.
  Missing package flows retain their source UUIDs. `.zolca` backups require JSON-LD conversion.
- Added an explicit option to preserve method-package metadata conflicts as separate,
  uncharacterized flows with stable UUIDs and coverage diagnostics. Strict rejection remains
  the default, and contradictory source identifiers still fail validation.
- Added a reviewed, directional UVEK 2025 to ecoinvent 3.12 cut-off migration route with
  documented proxies and supporting recipes, composable with older ecoinvent migration routes.
  Coverage is partial: proxies require explicit permission and unresolved suppliers still fail.
- Added exact ecoinvent 3.9.1 and 3.10.1 catalogs and audited compatibility edges that retain
  patch-version identities, plus a documented UVEK export workflow.
- Added inference of existing SimaPro process categories.
- Added SimaPro biosphere-profile inference across all available exact catalogs. A profile is
  returned only for a unique best exchange-coverage result; ties and no-match outcomes are reported.
- Added copy-on-write `InventoryDocument`, `BrightwayInventory`, and `SimaProInventory` models.
- Added a versioned canonical schema, exact `InventoryContext`, namespaced extensions, immutable
  operation reports, and explicit conversion/migration policies.
- Added an injected format adapter registry with bounded content probes. Brightway Excel, Brightway
  CSV/TSV, and SimaPro CSV are first-class read/write adapters; ambiguous CSV is never guessed.
- Added adapter-owned format validation and conversion-preflight contracts so new formats can
  extend the pipeline without central format switches.
- Added explicit adapter contract flags and version/dialect compatibility allowlists; incomplete
  adapters are rejected before registry capability discovery.
- Added the dependency-injected `InventoryPipeline` for detection, parsing, normalization,
  independent validation, migration, format conversion, writing, and audit sidecars.
- Added independent technosphere and biosphere catalog providers, validation coverage metrics,
  migration planning, transactional execution, and exact source/target validation.
- Added the `brightpath` CLI with `formats`, `inspect`, `validate`, `convert-format`, and
  `migrate-background` commands, dry runs, JSON reports, and stable exit codes.
- Added independent Brightway Excel loading, normalization, structured validation, and writing.
- Added independent SimaPro CSV loading, rendering, structured validation, parameter preservation,
  ecoinvent cut-off/consequential marker checks, and ecoinvent/UVEK naming profiles.
- Added exact catalog injection for SimaPro biosphere normalization instead of applying one fixed
  ecoinvent flow list to every declared biosphere release.
- Added bidirectional ecoinvent cut-off migration routes from 3.5 through 3.12 with structured step
  reports for replacements, disaggregation, aggregation, irreversible deletion, ambiguity, and unit
  changes.
- Packaged and attributed Premise's CC-BY-4.0 ecoinvent technosphere and biosphere migration
  resources.
- Added auditable heuristic mappings from packaged ecoinvent 3.6–3.12 identities to UVEK 2025 and
  its ecoinvent 3.10 biosphere, with exact target validation and per-rule confidence metadata.
- Normalized `BAFU` to the canonical `UVEK` background family name.
- Preserved nested unknown Brightway workbook fields through tagged JSON values during round trips.
- Reserved `openlca_excel` and `ecospold2` format identifiers for future adapters.
- Added task-oriented Sphinx documentation, generated API reference, strict CI documentation builds,
  and a modern Read the Docs configuration.
- Added SHA-256 integrity manifests for catalog and migration resources, with explicit provenance
  and a stable-release legal-review gate for generated reference catalogs.

### Fixed

- Use short pytest parameter IDs for oversized SimaPro metadata so the Windows
  test run stays within the environment-variable length limit.

- Aligned default ecoinvent SimaPro process display names with Premise: reference
  product, location, activity and system model. Explicit names are preserved.
- Filled SimaPro Geography from the dataset location when no explicit geography is supplied.
- Preserved full SimaPro market supplier names and qualifiers, rejecting ambiguous
  serialized supplier labels before export.
- Restricted SimaPro market-name rewriting to actual market prefixes, preserving
  distinct suppliers whose names contain “to generic market for”.

- Wrote waste-treatment reference flows with positive magnitudes, preserving their
  quantities, while sign-flipping links to waste suppliers in both ecoinvent and
  UVEK exports. Ordinary signed inputs remain unchanged.
- Preserved ecoinvent waste credits with reversible sign conversion instead of
  absolute values. Reflected uncertainty locations, signs, and bounds together,
  and kept ordinary inputs inside waste processes signed.
- Used explicit SimaPro process categories and included supplier categories for
  waste classification, and explicit CSV sections on import. Conflicting supplier
  categories and unsupported reflected export distributions/formulas now fail.
- Converted SimaPro water emissions only when supplied in cubic meters, scaling
  normal uncertainty and distribution bounds with the amount while preserving
  lognormal scale. Already-kilogram emissions remain unchanged. Unsupported
  water units, distributions, and unevaluated exchange formulas now fail explicitly.
- Rendered explicit empty or `unspecified` SimaPro biosphere subcompartments as
  blank fields without remapping other unknown compartments.
- Aligned Conda requirements with Python 3.12 and openLCA dependencies, added strict
  documentation checks to the release workflow, and included maintainer scripts
  and the changelog and required review fixtures in source distributions.
- Included shared pytest fixtures in source archives so SimaPro tests using the
  fixed export clock also run from an extracted distribution.
- Passed manually supplied release versions through environment variables before
  shell evaluation, avoiding direct interpolation into release scripts.
- Replaced credential-like catalog-test examples with synthetic values.
- Prevented synthetic openLCA flow UUID collisions between elementary-flow compartments and
  product supplier locations. Explicit UUIDs remain unchanged; incompatible definitions of
  one UUID fail serialization.
- Prevented olca-schema-generated IDs and timestamps from overriding deterministic identities
  or being mistaken for source metadata.
- Preserved shared-flow exchange metadata by scoping it to process and exchange IDs, with
  support for reading the older metadata layout. Removed repeated copying of accumulated
  metadata to improve large scenario exports.
- Converted Brightway lognormal log-space parameters to openLCA geometric parameters and back,
  including signed distributions and process/global parameters. Invalid lognormal parameters
  and unsupported uncertainty types now fail instead of producing incomplete uncertainty data.
- Supported legacy JSON-LD category objects and reference flags in method-package mappings,
  and semicolon-delimited biosphere source CSVs.
- Fixed Brightway biosphere-category serialization, SimaPro foreground process rendering,
  and Unicode normalization in SimaPro CSV exports.
- Improved reviewed ecoinvent-to-UVEK mappings, UUID-less reverse biosphere matching, and
  unambiguous compartment fallbacks while retaining safeguards for unsafe unit changes.
- Accepted `hectare` and `ha` as valid Brightway technosphere units during inventory validation and catalog matching.
- Accepted `person kilometer` plus legacy `product` technosphere fields during Brightway inventory normalization.
- Replaced repeated full-catalog canonical technosphere scans with indexed matching to keep large workbook analysis responsive.
- Fixed SimaPro uncertainty labels to use values accepted by `bw2io` and kept duplicate parsed
  identities available for validation instead of aborting file loading.
- Normalized SimaPro parameter identifiers across supported `bw2io` releases while preserving
  database, project, and process parameter scopes.
- Initialized Brightway core migrations inside SimaPro loading so clean environments do not depend
  on an earlier Brightway import.
- Routed compatibility catalog access through exact independent providers and combined verified
  model-specific biosphere shards into one version-specific biosphere catalog.
- Separated intrinsic format validation from conversion representability/loss policy and treated
  reconstructible BW2IO `input`/`output` graph keys as non-lossy metadata.
- Declared setuptools 77 or newer and Twine 6.1 or newer for reproducible SPDX license metadata
  builds and Metadata 2.4 release validation.

## 0.0.4 - 2026-05-14

### Added

- Added direct Brightway Excel export from `SimaproConverter.convert_to_brightway(format="excel", filename=...)`.
- Added `--excel-output` to the ecoinvent 3.10 Si wafer import diagnostic script.
- Documented direct SimaPro CSV to Brightway Excel conversion in the README.

## 0.0.3 - 2026-05-14

### Added

- Added `AGENTS.md` with repository-specific contributor guidance.
- Added a comprehensive pytest suite covering converter defaults, validation, SimaPro parsing, CSV output safety, pure helper behavior, and packaging expectations.
- Added development extras for tests, formatting, linting, security checks, and builds.
- Added `BrightwayConverter.unused_exchanges` to expose unused exchange reports programmatically.
- Added an optional `filename` argument to `BrightwayConverter.convert_to_simapro()` for explicit CSV export names.
- Added ecoinvent 3.10-specific SimaPro-to-Brightway biosphere mappings and import diagnostics.
- Added a release workflow for PyPI and Anaconda publishing on tags or GitHub releases.

### Changed

- Refactored Brightway-to-SimaPro row generation into smaller internal helpers while preserving the existing converter entrypoint.
- Replaced CI auto-format commits with check-only quality, test, security, and build jobs.
- Expanded CI tests across Linux, macOS, and Windows.
- Tightened package manifests to exclude cache files and operating-system metadata from built distributions.
- Made UVEK conversion and distribution transport helpers return transformed copies instead of mutating caller-owned inputs.
- Made SimaPro biosphere formatting return a copy by default.

### Fixed

- Fixed `BrightwayConverter` initialization when `export_dir` or metadata is omitted.
- Prevented repeated conversions from mutating source inventory data.
- Prevented SimaPro inventory cleanup from writing surprising sibling `_edited.csv` files by default.
- Replaced silent duplicate dataset dropping with explicit duplicate validation.
- Hardened SimaPro technosphere and biosphere parsing for malformed names, locations, and categories.
- Normalized common ecoinvent 3.10 SimaPro biosphere names and final waste indicators to reduce false unlinked exchanges.
- Added CSV formula escaping for text values that spreadsheet software could interpret as formulas.
- Added collision-safe default CSV export filenames.
