# Migration Resource Attribution

The exact `ecoinvent__3.10.1__consequential.json` reference catalog is generated
from the locally installed `ecoinvent-3.10.1-consequential` and
`ecoinvent-3.10.1-biosphere` databases using a read-only metadata snapshot and
`brightpath.catalogs.write_background_catalog`. It contains 20,772 native
activity/reference-flow UUID pairs and 4,362 biosphere UUIDs. Native patch
activity UUIDs are retained; this is not a relabelled 3.10 catalog. The biosphere
definitions and UUIDs match the existing exact 3.10.1 cut-off shard. Only
validation identities and UUID references are distributed, without full licensed
inventories or descriptions. Catalog availability does not authorize migration
between cut-off and consequential system models.

Exact source metadata fingerprints (length-prefixed records ordered by code):

- Technosphere: `41cd9cc19370bf3065765935d17db3aa7c2b4dcb909a16800ebfd8c8025e03ec`.
- Biosphere: `f68400bccbbaed2905d0f1f6a28d53791c3f0f05ea61babede8bcd53c350c1a6`.
- Generated catalog: `7a62d5958352ec3cb4fd7a09213460995320361b2bc3452416cb85eaebd6d91d`.

## Complete uncontrolled-disposal functions (2026-11-07)

`brightpath/data/export/uvek_uncontrolled_disposal_review.json` binds 217 exact
cut-off open-dump, unsanitary-landfill and open-burning source inventories and
47 operating-service inventories: 45 source market/production vectors and two
native services. Independent reverse targets are included in those 45 source
vectors. Original 3.9.1 and 3.10.1 databases were inspected directly. Eleven
original 3.9 references are positive; no absent patch counterpart inherits them.

The public review contains fingerprints, identities and derived target
coefficients. Complete licensed inventories remain private. Source-specific
carbon, air/water/soil/land fate and every signed release coefficient are
retained; the only technosphere proxies are the complete native diesel-burning
service per MJ and skid-steer excavation per cubic metre, with geographic,
technological and background warnings. Reverse suppliers were inspected
independently. The current canonical publisher-identity review supplies 281
flow definitions, including 57 occurrences of the two same-CAS groundwater
synonyms, without new chemical or receiving-context inference.

The three explicitly marked waste accounting indicators retain the existing
optional-indicator warning policy. Physical releases are never omitted. Native
UVEK 2026 gaps retain normal target-coverage semantics: default policies reject
incomplete links, while explicit permissive policies may retain unresolved
physical exchanges for manual linking before scoring. Pending native ionic-air
proxies and the residual-landfill-only land refinement remain outside this
review. No executable consequential profile or inverse treatment is added.

## Canonical biosphere publisher identities (2026-11-07)

`scripts/generate_uvek_canonical_biosphere.py` builds the UVEK compatibility
bridge to the canonical ecoinvent 3.10 biosphere from catalog flow identifiers,
units and complete compartments. Each exact source biosphere release has its
own executable decision. Historical system-model catalog shards supply only
independent biosphere identity metadata; they authorize no consequential
technosphere route. Unsupported chemical, unit or compartment correspondences
are explicit unresolved entries and cannot be guessed or deleted at runtime.

`brightpath/data/export/uvek_canonical_biosphere_identity_review.json` records
two older groundwater-ion synonyms whose UUIDs changed. Their chemical identity
was checked against original source CAS metadata and the ecoinvent elementary
flow definitions shipped with bw2io: matching CAS, kg, water/ground- and an
explicit publisher synonym. The review publishes only identity metadata and
evidence hashes. No licensed inventory or characterization factor is bundled.
Same-UUID correspondences require unchanged unit and receiving context;
manganese labels reused across releases have separate profile decisions.
This review does not extend native UVEK chemical or land-class proxy approvals.

## Active system-model scope (2026-11-06)


The executable ecoinvent-to-UVEK technosphere resource now contains cut-off
source profiles only. Consequential-only identities, profile overrides and
unreachable helper recipes have been removed. The runtime rejects every
cross-system-model route, including routes composed through another release.
The independently reviewed reverse resource remains cut-off to cut-off.

Earlier scientific review descriptions and sign ledgers below retain historical
observations of both ecoinvent models for reproducibility. They do not authorize
consequential-to-UVEK conversion. The generator replays those bound historical
inputs and publishes only its cut-off projection; surviving cut-off component
quantities and approved omissions are preserved.

## Sewer infrastructure capacity classes (2026-11-06)

`brightpath/data/export/uvek_sewer_grid_review.json` binds 105 read-only
ecoinvent cut-off construction/market inventories and metadata across 3.6--3.12,
plus five independently inspected UVEK grid inventories and native product roles.
The documented person equivalents, network lengths, 100-year lifetime and
renovation after 70 years establish five matching capacity classes. Source title
lengths of 6, 30 and 110 km are rounded descriptions of 6.13, 30.3 and 109.4 km;
neither those totals nor annual throughput scale a per-kilometre exchange.

The standing matching-function/unit and geographic-proxy approval covers these
fifteen direct corrections. They use factor +1 and report Swiss construction,
technology, supply-mix, end-of-life and emission approximations. Sparse
fingerprints and derived target correspondences are published; complete licensed
inventories remain private. The five reverse correspondences already select the
matching ecoinvent 3.12 cut-off CH constructions and are unchanged. Volumetric
rural/urban sewer services, sewage-treatment overlap, new helpers and omissions
are outside this review. Consequential conversion remains unsupported.

The dedicated `uvek/uvek-2025-to-ecoinvent-3.12-cutoff.json` resource is generated
by `scripts/generate_uvek_export.py`. Its source evidence is the IEA PVPS
T12-33:2026 conversion ledger (https://doi.org/10.69766/WJTE1771), the public
premise PV/PAN supporting inventories, and explicit directional proxy decisions.
The source hashes, citations and rationales are preserved in
`docs/uvek_export_evidence.json` and `docs/uvek_export_overrides.json`. Public
supporting recipes are not ecoinvent inventory extracts. Existing cut-off
disaggregation recipes retain their upstream provenance. No numeric similarity
confidence is invented for this directional resource. The full catalog review
lists unresolved suppliers separately; generated candidates are not approval.
The shared schema-version-3 resource retains approved rules, supporting recipes,
and coverage counts. The publisher baseline includes seven public recipes;
later independently reviewed gas reconstructions add two. It does not duplicate unresolved
source identities from the packaged UVEK catalog. Those identities, reasons and
candidate suggestions belong to `docs/uvek_export_review.json`; an absent rule
always remains unresolved at runtime.

The BrightPath 1.0.0a11 publisher-correspondence cohort adds 1,070 reference-function
rules while retaining all 354 original rules unchanged. Its source is
ecoinvent's public **Correspondence File for Intermediate Exchanges**, dated
2013-09-04, linked from https://support.ecoinvent.org/ecoinvent-version-3.0 :

https://support.ecoinvent.org/hubfs/Knowledge%20Base/Database/Releases/correspondence_file_intermediate-exchangesv2.2_to_v3.0_20130904.xlsx

The source SHA-256 is
`cd598b9c357a1c0f55ca5e00fc9861a753cbdce772a07dd43e8acffcb8f2b959`.
The publisher's terms apply; the workbook does not declare an explicit reuse
license. Only correspondence metadata is derived here, never proprietary
inventories, amounts or LCIA factors. The public workbook itself is not bundled.
`scripts/generate_uvek_export.py` records the workbook revision, exact worksheet
rows, generation method, original-rule fingerprint and catalog digests in
`docs/uvek_export_lineage_evidence.json`. It requires unchanged units/geography,
a reference-product row marked `x` without change flags, an unambiguous current
activity/product identity and the same reference-product UUID. Waste-role and
multi-output cases are deferred. This is BrightPath's authored nomenclature
correspondence, not a publisher endorsement of UVEK equivalence. The existing
directional 2026-to-2025 bridge composes these rules for native UVEK 2026.

Except for the new 3.11→3.12 biosphere edge described below, the ecoinvent migration JSON files in
`ecoinvent/cutoff/` and `ecoinvent/biosphere/` were originally copied from the `premise` repository
at commit `43355ef7c21e8587812d1615cc956252c5a7c4e6`:

`premise/data/utils/import/migrations/`

Each JSON resource contains its own contributors, creation timestamp, generator version, homepage,
and license metadata. At the time of import, these resources identify `ecoinvent_migrate` as their
generator and declare the Creative Commons Attribution 4.0 International (`CC-BY-4.0`) license.

On 2026-10-08, BrightPath supplemented the 3.10-to-3.11 cut-off resource with one
reviewed tissue-paper-output rename in RER. The upstream rules cover a different
reference product, `waste paper, sorted`, and remain unchanged. Read-only comparison
of licensed 3.10 and 3.11 cut-off metadata verifies the same tissue-paper product
UUID, unit, geography and whitespace-normalized recycled/deinked process description.
The rule's `brightpath_review` records its rationale and evidence link. Review
dates, activity/product UUIDs and the description digest stay in
`docs/recycled-paper-migration.rst`. Only sparse identity metadata is published,
not proprietary inventories. Exact patch-target validation remains required and
no equivalence of background coefficients or impacts is claimed.

The `ecoinvent-3.11-biosphere-ecoinvent-3.12-biosphere.json` resource was generated directly with
`ecoinvent_migrate` 0.6.2 from licensed ecoinvent 3.11 and 3.12 master data on 2026-07-22. The
official change report contains an empty `EE Deletions` sheet, which causes the high-level generator
to return without serializing its supplemental master-data comparison. BrightPath therefore invoked
the package's `supplement_biosphere_changes_with_real_data_comparison` function directly. It found
eleven stable-UUID name changes. One additional whitespace-only `Ferrocyanide` identity correction
was verified against the exact packaged 3.11 and 3.12 reference catalogs and added so target catalog
validation covers every obsolete 3.11 identity.

The inventory data represented by these mappings is not included. BrightPath packages only mapping
rules describing identity changes, disaggregation, aggregation, and biosphere migration between
ecoinvent releases.

On 2026-07-22, every standard ecoinvent biosphere rule source was enriched with its exact Brightway
`(name, categories, unit)` identity while retaining the upstream UUID. The values were resolved by
UUID and verified by name against exact ecoinvent 3.5, 3.6, 3.7.1, 3.8, 3.9.1, 3.10.1, and 3.11
biosphere databases. Existing Brightway projects were reused where they covered every required
source; isolated biosphere-only projects were created for missing exact releases. The reproducible
workflow is `scripts/enrich_biosphere_migration_sources.py`. It imports only the release master-data
biosphere when explicitly invoked with `--import-missing`; no proprietary process inventory is
packaged here.

The files under `uvek/` are generated by
`scripts/generate_uvek_migration_resources.py` from BrightPath's packaged reference identity
catalogs, the legacy `data/export/ecoinvent_to_uvek_mapping.csv` correspondence table, and its
documented unit-conversion factors. Curated correspondences take precedence where the target exists
and the units are compatible or have an explicit factor. All other targets are selected by a
deterministic name, product, compartment, location, and unit similarity heuristic.

The technosphere resource additionally applies the audited decisions in
`data/export/uvek_reviewed_overrides.json`, derived from Karin Treyer's CLIC
`uvek-mapping-3.10` workbook at revision
`ecf7a035492d9e2042d7180091b3299aae16bf6e` (https://github.com/romainsacchi/clic).
Both UVEK directions use the named `brightpath.migration` schema, version 3,
with ecoinvent-style `replace`, `disaggregate` and `delete` operations. Supplier
identities are inline; evidence is shared through `review_ref` and `reviews`.
Confidence scores, factors, classifications and mapping explanations are preserved;
missing confidence stays absent. Legacy review `reason` becomes `rationale`.
Upstream ecoinvent rules and attribution stay unchanged and normalize on loading. The source identities
and all 2,576 review links are retained; 104 distinct runtime review records are stored
once. See `docs/workflows/ecoinvent-to-uvek.rst` for fields and current coverage.
The authoring decision ledger retains previous targets, source provenance, and selection
reason. See `docs/uvek-mapping-review.md` for safeguards and deferred conflicts.
The expanded pairwise review additionally checks energy carrier/fuel/technology,
waste role/material/route, nutrient mass basis, and co-product chemical families.
Coarse proxies and deprecated/geographic targets are explicitly flagged. These
are identity-level compatibility improvements, not equivalence claims.

Review version 3 adds scoped adjudication of the remaining low-confidence
alternatives and traces agreeing targets through the branch's decision files.
The follow-up audit distinguishes exact-source records, inherited-location
choices and scripted reuse; agreement alone does not raise confidence. See
`docs/uvek_followup_decisions.json` and `docs/uvek_followup_summary.json` for
decisions, limitations and input hashes. Equivalent passenger-distance unit
spellings retain an explicit factor of 1.0; no inventory amount is rescaled.

Review version 4 additionally inspects the local ecoinvent 3.10 cut-off and UVEK
2025 reference production and selected operating-burden metadata for the seedling
and CHP priority batch. Fifty-nine selections are third candidates from the full
UVEK catalog, not targets selected by the CLIC branch. The authoring ledger
labels the branch as the comparison source and separately identifies the inventory
evidence and candidate origin. See `docs/uvek_priority_inventory_evidence.json`
and `docs/uvek_priority_summary.json`. Only sparse interpretive evidence is
published, not full proprietary inventories. Different versions, allocation models,
plant capacities and emission-control performance are not established as equivalent.

Runtime resources omit comparison-branch methods, revision identifiers, activity
codes, evidence hashes and duplicate input catalog filenames. Detailed history
remains in the ledgers and documents cited above; the integrity manifest still
verifies every packaged migration resource. Generators retain only rationale,
limitations and necessary evidence citations in shared runtime reviews.
Runtime method labels omit the historical `clic_` prefix; the authoring ledger
retains the original labels and source attribution. These labels describe how a
mapping was selected and do not create a dependency on CLIC.

These resources enable compatibility workflows only. They do not assert scientific equivalence.
Each rule records its method and confidence, planning emits `migration.heuristic_mapping`, and
execution verifies every resulting link against the exact UVEK 2025 technosphere or ecoinvent 3.10
biosphere catalog before committing.
# Exact patch links and reverse proxies

## Reference-flow sign review (2026-11-04)

`brightpath/data/export/uvek_reference_flow_sign_review.json` is BrightPath-authored
sparse coordinate evidence from installed ecoinvent 3.6--3.12 cut-off and
consequential backgrounds and the CLIC UVEK 2025 Brightway background. It records
reference signs, counts, digests, profile provenance and rule identity bindings,
not proprietary process inventories. Independently verified native UVEK waste
and product reference roles come from the packaged openLCA reference catalog
and its existing attribution. Both migration directions retain their independent
scientific mapping reviews. Ecoinvent's reference convention is described at
https://support.ecoinvent.org/activities-products and
https://support.ecoinvent.org/system-models .
Signed coordinates do not prove scientific equivalence. Conflicting signs and
unapproved waste/product role changes remain blocked; no label-based sign inference or
automatic inversion of the forward crosswalk is performed. Regenerate with
`scripts/review_uvek_reference_signs.py` before refreshing the integrity manifest.

The subsequent approved RoW/GLO PET-incineration proxies use the inspected Swiss
UVEK disposal service. Their ledger retains original fleece targets and sparse
functional evidence (PET, municipal incineration, 0.2% water, kilogram reference),
source-profile coverage and explicit geographic/inventory limitations. These are
BrightPath-authored screening decisions, not publisher equivalence claims. The
user's similar-case authorization requires independent inventory checks; it does
not introduce a runtime geographic fallback or approve PET dust and landfill.

The separately approved wastewater cohort covers 11 exact treatment identities
for hard fibreboard, medium-density board and particle board, plus two older
100%-treated global markets for hard-fibreboard and medium-density-board effluents.
Read-only inspection
confirms negative cubic-meter Waste references in every available source profile
and positive treatment-service references in the corresponding UVEK providers.
Native UVEK PRODUCT_FLOW output metadata is preserved; the exact approved
functional interpretation is recorded independently. Sparse boundary evidence
distinguishes older sewer-inclusive source treatments from newer treatments
whose sewering is in upstream markets. Warnings disclose possible double counting,
geographic proxies and plant-model/composition differences. No full proprietary
process recipe or unqualified inventory/LCIA equivalence claim is published.

Five subsequent wastewater-market approvals explicitly accept full-volume
treatment proxies for partially untreated hard-fibreboard, medium-density-board
and particle-board wastewater markets. All 40 available source/profile pairs
were inspected read-only. Sparse evidence records rounded treatment shares and
the presence of direct emissions; no pollutant recipe is retained. Runtime
warnings disclose omitted untreated emissions and possible bias from replacing
the source coverage by 100% treatment. The exact forward approvals do not imply
reverse equivalence or authorize other effluent types.

The generic composition-proxy cohort separately approves treatment and market
references for PET pelletising, PET recycling and PP recycling. All 24 available
source/profile pairs in ecoinvent 3.11--3.12 were inspected read-only. Sparse
documentation evidence records source COD 0.373, 0.213 and 0.425 kg/m3 versus
0.1556 kg/m3 for UVEK's generic CH class-1 sewage treatment. These concentrations
disclose information loss; they do not rescale volume or impacts. Market raw
discharge omissions and direct-treatment sewer boundaries remain explicit.
Two prior recycling-treatment sign corrections retain their original factor
history after the approved supplier change. The same-case authorization also
covers four modern RER/RoW textile treatment/market references in 3.11--3.12,
both models, with documented municipal WWT and COD 0.93446 kg/m3. These ten
generic correspondences total 40 inspected source/profile pairs. Larger textile
composition differences and regional treatment coverage are disclosed explicitly.
No full pollutant inventory is published; leachate and fully untreated direct-
disposal cases remain outside this approval.

The subsequent instruction approves omission, with a warning, of six exact
fully untreated textile direct-disposal/market references in GLO, BD and IN.
All 30 available source/profile combinations were inspected read-only. The
runtime resource replaces the legacy NF3 correspondences with explicit deletion
rules; stable identifiers and shared reviews retain the decision evidence.
The authoring sign audit preserves the original six replacement records and
factor history for baseline comparison and replay. Runtime loss reports retain
caller-owned source exchanges; no proprietary background inventory is published
or reconstructed. Generic deletion/loss policies are unchanged outside these
explicitly approved warning-only omissions.

The approved reverse biowaste omission supersedes the publisher-derived
correspondence from UVEK's Biowaste, at collection point (CH, kilogram) to the
ecoinvent biowaste treatment market. Read-only inspection identifies two UVEK
collection/transport inputs and atmospheric CO2 uptake, versus a target market
of composting, digestion and incineration. Sparse functional evidence and the
original publisher record/review remain in the sign audit. Runtime conversion
omits the input with an explicit warning and loss record; collection transport
and uptake effects are not represented. No proprietary inventory recipe is
published, no scientific equivalence is asserted, and no forward omission is
inferred from this reverse decision.

The 2026-11-05 approval adds two independent forward omissions for industrial
biowaste composting in CH and RoW, kilogram, across ecoinvent 3.6--3.12 cut-off
and consequential. All 28 source/profile reference signs are negative. Inspection
of the 3.12 cut-off treatment inventories identifies active treatment inputs and
emissions; UVEK 2025 bafu-509472, Composting organic waste (RER), contains only a
reference exchange and explicitly documents an empty recycling/composting cut-off
process. Conversion therefore omits the inputs with a warning naming excluded
composting burdens instead of using collected biowaste or a zero-burden treatment
placeholder. Exact prior targets, sparse evidence and original exchange reports
remain auditable. Other biowaste functions are excluded from this approval.

The 2026-11-05 approval independently validates three CH paper-disposal
correspondences: newspaper incineration/landfill and packaging-paper incineration.
Read-only inspection covers all 18 available exact source/profile inventories,
including non-empty positive-reference 3.9 cut-off inventories and negative-reference
consequential inventories. UVEK 2025 bafu-341912, bafu-602962 and bafu-445807 have
positive references and active treatment inputs/emissions. Factors are +1 for the
three 3.9 cut-off entries and -1 for the enumerated consequential entries; missing
profiles remain blocked. Shared reviews disclose treatment-model differences and
the newer consequential landfill energy credits absent from the UVEK service.
Original review/factor history remains in the authoring audit. No complete
background inventories or automatically inverted reverse decisions are published.

The subsequent 2026-11-05 approval adds the RER PET-recycling-dust incineration
proxy and auto-approves the independently inspected RER post-industrial LDPE
milk-carton fraction. All ten source/profile inventories have negative references
and active incineration inputs/emissions. PET dust in 3.11--3.12 has 2.7027% water
and gross heating value 37.1 MJ/kg; its UVEK target bafu-514203 has 0.2% water
and upper heating value 23.13 MJ/kg. The LDPE fraction in 3.10--3.12 is explicitly
100% LDPE, with 13.775% water and 38.79 MJ/kg; bafu-275730 represents polyethylene
with 0.4% water and 42.82 MJ/kg. Both targets have positive reference amounts,
verified native waste-input references and non-empty treatment inventories.
Factor -1 preserves wet waste mass without heat/moisture scaling. Reviews
disclose composition, geography, treatment-model differences and missing
consequential energy credits. Original manufacturing targets/factors and sparse
functional evidence remain in the sign audit. No full background inventory is
published; unrelated CRT, sludge, composite-carton, ash and energy-product
references are not approved by this batch.

The subsequent CRT-coating approval and similar-case authorization covers twelve
exact incineration/coating-market rules, inspected read-only across all 100
source/profile inventories (seven coating rules/64 pairs; five residue rules/36
pairs). Negative source references become positive service coordinates with
factor -1. UVEK bafu-269681 and bafu-377324 have positive native waste-service
references and active treatment inventories. Six screen-manufacturing targets
are replaced; one older CH residue target is corrected from zinc-only car-shredder
disposal; five existing matching services receive aligned scientific disclosures.
All 28 coating-market inventories supply 100% incineration of the same coating
plus freight only; market freight is explicitly omitted by the service proxy.
Sparse reviews preserve metal-fate/ash/leaching and geographical limitations,
inconsistent legacy LCD-use wording, and original targets/factors. No complete
background inventory is published and no reverse equivalence is inferred. Glass,
whole-screen dismantling, sludge, ash-disposal and energy-product references are
not approved by this batch.

The same authorization separately auto-approves the GLO/kg CRT-glass processing
service and market in all 28 ecoinvent 3.6--3.12 cut-off/consequential source/profile
inventories. Both process descriptions document panel/funnel/mixed-glass
co-products and 100% disposal allocation. UVEK bafu-285219 is a non-empty positive
native waste-reference service. All fourteen source markets supply exactly one
kilogram of glass processing plus four freight inputs; additional market freight
is explicitly omitted, while the proxy retains its own internal transport model.
Factor -1 preserves mass; original screen-manufacturing targets and factor history
remain audited. Reviews disclose recovery, downstream metal fate, allocation and
credit uncertainties without publishing complete inventories. Glass-cullet
landfill, whole-screen dismantling and recovered-glass product references are
outside this approval; reverse mappings remain independent.

Two further independently inspected manual-dismantling correspondences are
auto-approved under the same authorization: CH in 3.6--3.12 and RoW in
3.10--3.12, cut-off and consequential (20 source/profile pairs). Both sources
manually dismantle CRT displays into fractions. UVEK bafu-448178 supplies that
function per kilogram, with a positive native waste reference and active
technosphere inputs. Factor -1 preserves mass without a device-count conversion;
reviews disclose Swiss transfer coefficients, yields, allocation, downstream fate
and credit uncertainties. Mechanical treatment is excluded because the UVEK
candidate explicitly adds manual dismantling before shredding. Across the
incineration, glass-processing and manual-dismantling batches, 16 exact rules
cover 148 inspected source/profile pairs; no full inventories are published.

The subsequent mechanical-CRT approval and similar-case authorization covers
four more exact rules and 56 inspected source/profile pairs in ecoinvent
3.6--3.12 cut-off/consequential. GLO mechanical treatment uses UVEK bafu-201280
with factor -1, explicitly accepting its documented manual-dismantling stage
before shredding/separation. The GLO whole-screen market and UVEK bafu-352244
share 23% manual / 77% mechanical routes; the exact UVEK mix references 19.9 kg
per unit. Factor -1/19.9 preserves mass and route shares without assuming a
generic device weight. The market's four freight inputs are explicitly omitted.
Two existing CH/RoW residue markets retain their reviewed disposal supplier and
factor -1, with newly disclosed freight omission and treatment-model differences.
All references and market supply patterns were inspected read-only; native UVEK
waste references are positive. Original suppliers/factors and minimal conversion
evidence remain audited. No complete inventory dump or reverse equivalence is
introduced; glass-cullet landfill and recovered-material references are excluded.

The subsequent lead-containing-cullet landfill approval and similar-case
authorization covers ten exact CRT-cullet/production-frit treatment and market
rules, inspected read-only across 120 source/profile inventories. Six GLO cullet
rules (84 pairs) use UVEK bafu-506481 with factor -1. All source cullet landfills
exclude direct leachate emissions; the ordinary-glass proxy includes generic
metal releases. This composition/boundary difference and lack of validated
metal/toxicity equivalence remain explicit. Four frit rules (36 pairs) use the
dedicated residual-landfill service bafu-466727; direct CH/RoW older and GLO newer
treatments retain their existing supplier/factor with reviewed disclosures.
All markets supply 100% landfill of their own waste plus freight, without active
direct biosphere releases. Additional market freight is omitted (four suppliers
through 3.9, one from 3.10). Both target services have positive native waste
references and non-empty inventories. Original suppliers/factors and sparse
function/sign/boundary evidence remain audited; no complete background inventory
or inferred reverse rule is published. CRT-production wastewater sludge/ash and
recovered-material references remain outside this approval.

The subsequent CRT-production wastewater-sludge approval and similar-case
authorization omits 18 exact unsupported treatment/market identities: 70%-water
sludge incineration, 75%-water sludge sanitary landfill, 97%-water sludge
landfarming, and wastewater-derived bottom ash/residue landfill. Read-only
inspection covers 144 source/profile pairs in ecoinvent 3.9--3.12, both models.
All 40 direct sludge observations specify zero biogenic carbon. The 72 market
observations supply only their own treatment plus freight, without active direct
biosphere exchanges. Prior targets represent frit, municipal/paper/iron-chloride
sludge, mechanical CRT-screen residue treatment or a landfill component rather
than a suitable complete counterpart. Fourteen prior native waste-coordinate
matches do not prove equivalence; four prior product-role mappings were blocked.
Approved omissions warn about excluded burdens, credits and market freight;
score effects remain method-dependent and unquantified. Complete caller exchange
snapshots remain in loss reports, with foreground suppliers, production and
separate biosphere exchanges preserved. Former targets/factors and sparse role,
function and boundary evidence remain replayable. No complete proprietary
background inventory or inferred reverse omission is published.

The subsequent coal-tar approval covers two exact GLO/kg references only in
consequential ecoinvent 3.6--3.9: furnace treatment and its market, eight pairs.
Read-only inspection finds production -1 kg, active combustion burdens and
26.6 MJ/kg heat substitution credit; markets add train/lorry freight to 100%
furnace treatment. UVEK bafu-521869 supplies allocated coke-plant tar production,
not that treatment function. Approved profile omissions exclude these burdens,
credits and market freight with warnings and complete caller exchange snapshots;
score effects remain unquantified. The market's ten positive-reference production
profiles retain their separate +1 proxy approval. Exact profile decisions,
original targets/factors and sparse function/sign evidence remain replayable.
No complete background inventory or inferred reverse rule is published.

The subsequent basic-oxygen-furnace-dust recovery-handoff approval and similar-case
authorization covers sixteen exact RER/RoW kilogram renaming identities across
ecoinvent 3.7--3.12, both models: 192 source/profile pairs. These cover BOF dust,
sludge, slag and secondary-metallurgy slag; blast-furnace dust/sludge; and EAF
slag/secondary-metallurgy slag. Read-only inspection confirms production -1, a
single -1 recovered-material input and no active direct biosphere exchanges.
All 96 cut-off chains end at a +1-reference process with no other active exchanges;
all 96 consequential chains lead to supplementary-cementitious-material markets.
Former UVEK bafu-607456, bafu-167904 and bafu-339275 targets supply economically
allocated slag production (factors 0.0077, 0.015 and 0.0052 respectively), not the
same renaming boundary or recovery-credit function. Two exact dust handoffs were
explicitly approved and fourteen independently inspected similar cases auto-approved.
Profile-specific omissions warn about excluded consequential credits, with score
effects unquantified. Complete caller exchange snapshots remain in loss reports;
separate emissions, production and foreground suppliers survive. Missing profiles
remain blocked. Prior targets/factors and sparse role/function/boundary evidence
remain replayable; no complete inventory or inferred reverse rule is published.

The subsequent zinc-concentrate recovery-credit approval and similar-case
authorization covers three exact GLO/kg source references in consequential
ecoinvent 3.6--3.12: 21 pairs. Read-only inspection confirms reference production
-1, a sole negative input to a +1-reference product market, and no additional
active processing or biosphere exchanges. The inspected credit quantities are
0.0622641509433962 kg zinc concentrate/kg metalliferous hydroxide sludge and
1 kg zinc or bronze/kg respective post-consumer scrap. These quantities are
functional conversion evidence; no complete background process inventories are
published. UVEK bafu-370641, bafu-236764 and bafu-368900 provide the credited
concentrate/metal/alloy with native PRODUCT_FLOW output and reference +1 kg.
The original lithium-hydroxide, zinc-concentrate-as-metal and manufactured-bronze
contour mappings remain archived. Explicit role-conversion reviews retain the
incoming credit sign with positive factors; ordinary treatment-sign inversion
does not apply. Grade, allocation, global-market freight and consequential
model differences remain disclosed, along with European zinc geography, Swiss
95% copper/5% tin bronze and its documented poor quality. The sludge source
already omits Waelz processing burdens. Source/supplier references and exact
credit quantities have replayable per-profile digests; additional processing,
supplier/quantity drift or new profiles require review. No reverse waste
reconstruction is inferred from primary product mappings.

The same single-product substitution-credit authorization separately auto-approves
the GLO/kg dichloropropene-to-unspecified-pesticide linking reference in consequential
3.6--3.9: four pairs. Read-only inspection confirms production -1 kg, a sole -1 kg
credit to a +1-reference generic pesticide market, and no other active exchanges.
UVEK bafu-285276 supplies unspecified pesticide production at regional storehouse,
RER/kg, native PRODUCT_FLOW output and production +1. Factor +1 preserves the
documented generic credit, replacing the unrelated diode-production target.
Generic mixture, technology, geography, toxicity-inventory and market-freight/model
differences remain disclosed; no ingredient-specific equivalence or application
emissions are inferred. The original target/factor and four independently collected
credit quantities are retained, with later/unreviewed profiles blocked. No complete
inventory or reverse chemical reconstruction is published.

The subsequent feed-credit approval covers five exact GLO/kg nutritional links
for DDGS, fodder yeast, grass fibre, sugar beet pulp and sweet sorghum grain,
only in consequential ecoinvent 3.6--3.12: 35 pairs. Read-only inspection finds
production -1 kg, only negative gross-energy-feed (MJ) and, where present,
100%-crude-protein-feed (kg) inputs, with no direct biosphere exchanges. Grass
fibre has no protein credit. Sparse functional substitution quantities are retained
in approval evidence; no complete background inventory is reproduced.
The similar-case authorization independently covers the five corresponding
consequential markets: 35 more pairs with exactly -1 kg of their own link and no
direct biosphere releases. DDGS, yeast and sorghum markets add train/lorry freight;
grass fibre adds lorry freight; beet pulp has none. Omissions warn about excluded
nutritional contributions/credits and actual additional market freight, with
score effects unquantified. Original source exchanges remain in loss reports,
with production, matching local suppliers and other foreground exchanges preserved.
The markets' 35 cut-off counterparts have production +1 and retain pending
independent product-supplier reviews. Prior crop/product targets, factors and
reference signs remain replayable; no reverse reconstruction is inferred.

Related-reference inspection applies the same omission authorization to 95 exact
normalized nutrient identities: 45 gross-feed-energy MJ references and 50
100%-crude-protein kg references, across 692 inspected positive-reference
source/profile pairs. Former heuristic targets supplied industrial heat/fuel,
crop mass, crude oil or unspecified whey protein concentrate; their unit factors
of one did not establish nutritional equivalence. The fixed sparse whitelist
`data/export/uvek_feed_nutrient_omission_review.json` binds source identities,
prior targets and inspected signs/profiles without publishing full inventories.
Warnings disclose excluded nutritional supply contributions and any associated
credits, with unquantified score effects. Exact-profile omissions preserve full
source snapshots and matching foreground suppliers; ordinary coproduct-mass
supplies and the five pending cut-off markets are outside their scope. No
nutrient keyword deletion, missing-profile approval or reverse rule is inferred.

The 2026-11-05 refrigerant-venting approval replaces five exact GLO/kg
manufacturing proxies with physical complete-release recipes for CFC-12,
HFC-134a and isobutane. Forty-two source/profile inspections establish negative
waste references, one-to-one air release and no active processing inputs.
`data/export/uvek_refrigerant_venting_review.json` retains exact identities,
previous targets, inspected profiles and hashes of locally installed exchange
records, without redistributing those inventories. Six BrightPath-authored
recipes follow mass conservation and distinguish the additional 3.12
waste-disposal indicator. Their coefficients are physical model definitions,
not copied proprietary inventory recipes or LCIA-score calibrations.
Exact recipe-local biosphere targets bind the corresponding native UVEK 2026
chemical air flows; its isobutane unspecified-air approximation and unsupported
inventory indicators warn and retain loss evidence. The air release is mandatory.
Source provenance, uncertainty and avoided-venting signs are retained; other
refrigerant treatment boundaries and independent reverse resources are unchanged.

The subsequent approved refrigerant final-disposal cohort covers five identities
and 42 exact source/profile pairs. Read-only inspection establishes a -1 kg waste
reference, regional average hazardous-waste incineration supplies totaling
-0.999 kg, and +0.001 kg direct release of the identified refrigerant, with only
3.12 adding a waste-disposal indicator. Sparse exact identities, previous targets
and exchange-record fingerprints are recorded in
`data/export/uvek_refrigerant_disposal_review.json`; no complete source inventory
is published. Six independently authored proxy recipes replace the regional
incineration mix with the CH/kg UVEK 2025 average hazardous-waste incineration
service (`bafu-165198`, 25% water, +1 production, native WASTE_FLOW input), while
preserving the source direct-leakage assumption. This is not a claim of measured
destruction efficiency, molecular fluorine/chlorine balance or equal LCIA scores.
Explicit warnings/losses disclose geography, composition, treatment, allocation,
energy recovery and background differences. Old R600a cable-treatment coordinates
had already been sign-corrected; its historical factor remains audited when
superseded by the disposal recipe. Native gas bindings, isobutane compartment
warnings and optional 3.12 indicators follow the earlier venting approval.
The existing venting recipes, unrelated supplier mappings, approved omissions
and independent reverse resource remain unchanged.

The subsequent R134a-reclamation approval covers three identities and 21 exact
source/profile pairs. Fourteen waste-reference profiles retain electricity use,
rejected-material incineration and the direct refrigerant release; only the seven
consequential profiles credit recovered primary supply. Seven cut-off coproduct
profiles have one +1 kg production reference and no active other exchanges.
`data/export/uvek_r134a_reclamation_review.json` binds these inventories and all
seven credited markets to sparse exact-identity metadata and fingerprints.
No complete source inventories are published. The two BrightPath service proxies
use GLO UVEK medium-voltage electricity, CH average hazardous incineration and
the separate HFC-134a release. The consequential primary-supply credit uses RER
UVEK production in place of the inspected 25% RER/75% RoW supply market, with
explicit proxy limitations. A production-only foreground supplier preserves
the observed empty cut-off product allocation boundary, including material
quantities. This is not a universal zero-impact or recovered-product-credit rule.
Inputs, source-profile signs, uncertainty, formulas and original-exchange evidence
remain auditable. Existing venting/disposal recipes and independent reverse
resources are unchanged.

The subsequent produced-water discharge approval covers four identities and 56
exact ecoinvent 3.6--3.12 cut-off/consequential source/profile pairs. Read-only
inspection found separate onshore/offshore direct discharge services and their
single-supplier markets, all with reference production -1 kg. Corresponding
UVEK services have +1 kg production and native PRODUCT_FLOW output references;
factor -1 preserves discharge mass. The sparse review in
`data/export/uvek_produced_water_review.json` retains 56 source inventory hashes
and two target inventory hashes without publishing complete inventories.
These are explicitly approved emission-inventory proxies: DOC concentrations,
chromium speciation, older air/water-balance coverage and onshore extrapolation
differ. Source/target identities, exchange paths and limitations remain in
policy-controlled warning and loss records. No helper datasets or reverse
correspondence changes are added.

The subsequent antifouling-emission omission approval covers five identities and
70 inspected ecoinvent 3.6--3.12 cut-off/consequential source/profile pairs. The
source market supplies 50% copper-based and 50% tin-based marine-emission services
through PE/RoW providers; direct services have marine biosphere releases. The
prior uranium-milling market proxy and four lorry tyre-wear assignments are
rejected as scientific correspondences, including the four with previously
verified reference coordinates. Sparse source fingerprints and prior target,
factor and sign-review facts are retained in
`data/export/uvek_antifouling_omission_review.json`; no complete inventories are
published. Omission warnings disclose excluded marine metal/organotin releases
and unquantified ecotoxicity effects. Loss records retain complete caller-owned
exchange details. Paint manufacturing, foreground suppliers and separate
emissions remain intact; no helpers or reverse-rule changes are added.

The subsequent user-approved industrial-gas batch covers 103 power-plant service
identities and 721 inspected ecoinvent 3.6--3.12 consequential source/profile pairs.
The 69 blast-furnace-gas and 34 coke-oven-gas service identities have production
-1 MJ and electricity/heat credits. The sparse review in
`data/export/uvek_industrial_gas_review.json` retains inventory fingerprints and
derived UVEK credit inputs; 140 existing energy correspondences are independently
bound against later mapping changes. UVEK RER gas-combustion primitives supply
processing proxies. The coke-oven burner includes a deterministic +1 MJ upstream
fuel input; a -1 MJ input of that same provider isolates waste-gas combustion.
The 2026 burner was independently inspected to retain the same fuel quantity.
Source energy credit quantities remain signed; regional combustion, infrastructure,
emissions, internal uncertainty and consequential marginal supply differences are
explicit information-loss findings. No complete source inventories are published.
Gas markets, cut-off product suppliers and reverse resources remain independent.

The standing unsupported CRT industrial wastewater-residue omission authorization
was applied to 198 independently inspected matching nonbiogenic treatment/market
identities in LCD/backlight, solar-tube-collector, PV, wafer, liquid-crystal, glass,
concrete, black-chrome, ammonium-paratungstate and plastic-processing chains.
The 1,520 inspected source/profile inventories include 760 direct treatment
comments documenting 0% biogenic carbon; markets supply 100% treatment of their
own residue plus freight. Most source identities occur in ecoinvent 3.9--3.12;
sixteen plastic-processing identities occur only in 3.11--3.12. No dedicated
complete UVEK counterpart for the relevant residue-specific disposal routes was
established. Sparse prior targets, fingerprints and the scientific approval basis
are retained in `data/export/uvek_wwt_residue_omission_review.json`. Omission
warnings disclose processing, metal/leachate, downstream residue, freight and
energy/fertilizer-credit exclusions; 0% biogenic carbon is not a zero-impact
claim. Complete caller-owned exchange details remain in loss records. No new
helper or reverse-rule changes are added by this omission batch.

`compatibility/patch-links.json` is BrightPath-authored compatibility metadata.
Patch routes retain shared link identities and require exact-target validation;
they do not update background inventories or reproduce corrected LCIA scores.
Evidence: https://support.ecoinvent.org/ecoinvent-version-3.10.1 and
https://support.ecoinvent.org/ecoinvent-version-3.9.1 .

The reverse-only unspecified-organic-chemical proxy uses the documented replacement
of the legacy generic flow by six organic product groups in ecoinvent 3.12:
https://support.ecoinvent.org/ecoinvent-version-3.12 (Change Report section 5.6).
This is an explicitly lossy proxy decision, not an inverted equivalence mapping.

The reverse-only European primary-aluminium proxy (2026-10-08) resolves the two
3.11 source activities merged into `aluminium production, primary, ingot` in
`IAI Area, Western and Central Europe` by the packaged Premise 3.11-to-3.12
resource. It retains the older production activity in `IAI Area, EU27 & EFTA`,
rather than changing the supplier role to `aluminium, ingot, primary, import
from Rest of Europe`. This is consistent with the directional UVEK rule
`uvek-ei312-3a6bb39ff4b6f558`, whose stated intent is primary European production.
The reverse choice is a documented proxy, not a unique mathematical inverse:
regional coverage, supply composition and impacts may differ. Execution records
`migration.reverse_proxy`, respects the information-loss policy, and validates
the result against the exact target catalog. Both original forward rules remain
unchanged. No exchange amount, uncertainty or published inventory is rewritten.
## Independent reverse industrial-gas review

`../export/uvek_reverse_industrial_gas_review.json` records 41 independently
reviewed UVEK-to-ecoinvent correspondences and sparse source/target inventory
fingerprints. The two per-MJ gas-burner reconstructions and their explicit
RER-to-DE proxy warnings were approved by the user. Matching coke-gas supply
and industrial HFO-heat proxies use standing same-product/geographic approvals.
The additional 37 electricity proxies have independently inspected positive
one-kWh reference inventories and a unique matching-region high-voltage market
or market group. Supply-mix, grid-loss, infrastructure and emissions differences
remain documented approximations. Whole ordered review batches are validated
separately; an arbitrary incomplete subset cannot bypass the baseline guard.
`scripts/review_uvek_reverse_gas.py` compiles these additions and validates them
separately while retaining the original publisher/legacy baseline fingerprint.
Fuel-boundary adjustments are explicit; no forward-rule inversion or LCIA
equivalence is claimed. Full licensed source inventories are not distributed.

## Composed markets and additional nonbiogenic residues

`../export/uvek_gas_market_review.json` records two exact gas markets across 28
profiles, their source/allocated-supplier fingerprints and the unchanged approved
gas-treatment recipe dependencies. It distinguishes cut-off allocated supply
from consequential treatment mixes. The explicit empty cut-off blast-furnace-gas
supply chain is an inspected allocation boundary, not missing-provider handling.

`../export/uvek_market_composition_review.json` records 248 exact markets across
2,251 profiles. Helpers use derived target-side coefficients from existing
resolved directional component mappings. Source inventory hashes and exact
profile-selected component/recipe hashes guard regeneration. These compositions
inherit component proxy limitations; internal component uncertainty/formulas and
scientific or LCIA equivalence are not reconstructed or claimed.

The nonbiogenic industrial-residue review now enumerates 226 exact identities
and 1,704 source/profile inventories. Its latest 28 pending cases have explicit
zero-biogenic-carbon direct-treatment documentation and, for markets, 100%
same-residue treatment plus freight. Earlier 198 reviews remain verbatim.
Omissions retain warnings and complete original-exchange loss records, including
associated energy/fertilizer credits. Broader industrial-residue candidates are
not approved merely because they have zero biogenic carbon or similar names.

## Processing compositions

The additional `../export/uvek_processing_composition_review.json` review
enumerates 100 complete processing references across 795 source/profile
inventories. These contain active technosphere inputs only, a unit-magnitude
self-linked production reference, no active direct biosphere exchanges and no
unsupported exchange types. Helpers retain all processing inputs and genuine
product credits through the existing exact-profile component mappings; no
missing, omitted or empty component is inserted. Sparse inventory fingerprints,
component-rule fingerprints and recipe dependency hashes protect replay.
`scripts/review_uvek_processing_compositions.py` reuses the checked composition
authoring functions with its own rationale and namespace. Inherited component
approximations and unreconstructed internal uncertainty/formulas warn; no
scientific equivalence or inverse is inferred. Full licensed inventories are
not included.

## Independent reverse ash-treatment review

`../export/uvek_reverse_ash_treatment_review.json` records 23 same-region ash
disposal/refill proxies and independent source/target inventory fingerprints.
UVEK native waste-input references have positive Brightway production; the
ecoinvent 3.12 cut-off treatment references are negative, requiring factor -1.
The source ash type, regional endpoint, kg quantity and disposal route are
retained. UVEK specifies 0% water; all inspected target comments report H2O as
unavailable, so moisture equivalence is not claimed or used for rescaling.
Underlying infrastructure, emissions and long-term fate models can differ.
`scripts/review_uvek_reverse_ash.py` compiles the independently reviewed exact
batch, validates its fingerprints, and guards it separately from the original
publisher and gas review baselines. Existing forward service correspondences
are tested separately; they are not used as scientific approval of an inverse.

## Refinery-gas flaring service review

`../export/uvek_refinery_flare_review.json` records two exact source identities,
28 independently inspected source/profile fingerprints and one UVEK supplier
fingerprint. Existing matching-material/function/unit service-proxy approvals
cover the direct GLO/MJ flare and its market, whose only input is that service.
The source references are -1 MJ; the native UVEK product output describes a
positive-reference combustion service, requiring factor -1. Both boundaries
exclude flare infrastructure, upstream fuel and energy credits. Nine active
source biosphere exchanges versus ten in UVEK establish different emission
coverage; emission factors, speciation and underlying geography can also differ.
No equal LCIA score is claimed. `scripts/review_uvek_refinery_flare.py` binds
authoring to exact profiles and inventory fingerprints. Warnings preserve these
limitations; caller-owned foreground and separate emissions remain intact.
Natural-gas flares and fuel-production proxies are excluded; no inverse is
inferred. No licensed background inventory is redistributed.

## Legacy EAF-waste landfill service review

`../export/uvek_eaf_landfill_review.json` retains three exact CH/kg source
identities, their prior allocated slag-production suppliers, six ecoinvent 3.6
cut-off/consequential inventory fingerprints and three UVEK supplier fingerprints.
Standing matching-material/function/unit disposal-service proxy approvals apply
after independent inspection: the new targets describe residual-material
landfilling of alloyed EAF dust, unalloyed EAF dust and unalloyed EAF slag.
Source -1 kg references and native UVEK waste-input services with +1 kg production
require factor -1. Dust moisture differs (10.541% documented in the source versus
15.4% in UVEK names); source slag moisture is unavailable while UVEK names specify
0%. Original kg is preserved without moisture scaling. Alloyed-dust cement and
hydrated-cement disposal remain represented, with additional freight in UVEK.
Metal speciation, leaching, short- and long-term releases, infrastructure and
background models can differ. `scripts/review_uvek_eaf_landfills.py` validates
exact inventory fingerprints and profile coverage; warnings disclose these
limitations. Other profiles and reverse routes remain independently reviewed.
Full licensed inventories are not redistributed.

## Independent reverse refinery-flaring service review

`../export/uvek_reverse_industrial_gas_review.json` adds one exact GLO/MJ flaring
service as a separate complete review batch. Independent read-only inspection
verified source +1 and target -1 self-linked production references, the UVEK
native product-output role denoting physical combustion, and source/target
inventory fingerprints. Both processes exclude upstream fuel, flare
infrastructure and recovered-energy credits. Ecoinvent supplies nine active
biosphere exchanges versus ten in UVEK; emission factors, coverage, speciation
and underlying plants differ. `scripts/review_uvek_reverse_gas.py` records the
exact service approval and its factor -1; the sign author rejects different
roles, quantities or endpoints. Source foreground emissions remain separate.
The original gas and publisher batches retain their exact guards. This reverse
review was not inferred from the forward flare mapping; no broad product/waste
role conversion is authorized. No full licensed inventories are included.

## Dedicated LCD treatment review

`../export/uvek_lcd_treatment_review.json` records five exact service identities,
50 inspected source/profile fingerprints and three dedicated UVEK suppliers.
Standing similar display-treatment approvals cover manual dismantling, the
explicitly warned broader manual-plus-mechanical boundary and dedicated LCD
shredder-residue municipal incineration. Sources have -1 kg references; target
+1 kg production corresponds to native waste inputs. Wet mass is preserved with
factor -1. Newer source mechanical descriptions specify four-step shredding and
separation without a separate manual supplier; the UVEK process includes manual
dismantling. Composition, geography, recovery, emissions, speciation, leaching,
downstream handling and credits differ and warn. Two previously sign-verified
manual suppliers are corrected alongside three pending service identities.
`scripts/review_uvek_lcd_treatments.py` binds decisions to exact profiles and
fingerprints; full source inventories are not published. No reverse is inferred.

The LCD market joins `../export/uvek_market_composition_review.json`: 14 profiles
retain 23% manual and 77% mechanical treatment and four freight inputs without
renormalization. Caller foreground, signed quantities, uncertainty, formulas,
separate credits and emissions remain intact.

## Same-mode freight corrections and dependent-helper re-review

`../export/uvek_modal_transport_review.json` records three exact positive-reference
barge/rail services across 12 source profiles and two whole-life UVEK suppliers.
Independent inspection supports matching modes and tkm quantities. RER fleet
proxies replace incorrect lorry targets; global fleet mix, country weights,
loading, energy, infrastructure, technology and emissions differences warn.
Factor +1 preserves incoming signed demand and genuine avoided-transport credits.
`scripts/review_uvek_modal_transport.py` protects exact roles, profiles, endpoints
and inventory fingerprints.

The market and processing composition reviews retain explicit re-review batches
for 71 dependent source identities across 256 profiles. Each affected raw source
inventory was rechecked read-only. Original quantities and credits were replayed
through the corrected component mappings, with updated binding fingerprints and
recipe dependencies. Superseded definitions were removed only when no root,
profile or nested helper still referenced them. The normal authoring, schema and
stale-binding guards verified the complete candidate before publication. No
licensed background inventory is redistributed.

## Dedicated regional and legacy vehicle disposal

`../export/uvek_dedicated_disposal_review.json` records seven explicit identities,
32 independently inspected source/profile fingerprints and seven UVEK supplier
fingerprints. Standing matching-material/function/unit disposal, independently
reviewed vehicle-dismantling and geographic-proxy approvals cover this cohort.
Three legacy RoW EAF dust/slag references are restricted to 3.6; the scooter
glider/powertrain and passenger-car engine references to 3.6 and 3.7. RoW PET
sanitary landfill spans the inspected 3.6--3.12 backgrounds. Every source has
self-linked -1 kg production and an active treatment inventory; targets have
+1 kg and independently checked native waste-input references. Dust moisture
is 10.541% versus the target's 15.4%; slag water is unavailable versus the
0% target label. PET is 0.2% in both. Signed wet mass is preserved without
moisture or example-device-weight scaling. Explicit warnings cover geographic
technology, freight/cement, recovery credits, allocation and short-/long-term
emissions and leaching differences. Scientific/LCIA equivalence is not claimed.
The source-specific recovery-credit model is not reconstructed. Foreground
suppliers, separate credits and emissions remain untouched. Exact profile and
inventory hashes are enforced by `scripts/review_uvek_dedicated_disposals.py`.
The seven reverse endpoints were independently checked as unsupported.

Five newly complete markets join the existing composition review across 16
source/profile inventories: alloyed EAF dust, unalloyed slag and the three
vehicle components. Fresh read-only source inventories support every original
share and freight coefficient. Frozen selected-rule bindings retain component
limitations and signs; no omitted, empty or unresolved component is concealed.
Only derived target-side coefficients and sparse metadata are packaged. Full
licensed inventories remain private and are not redistributed.

## Spent Formox catalyst carrier: separate disposal review batch

`../export/uvek_dedicated_disposal_review.json` adds one exact RER/kg Formox
carrier treatment across six inspected ecoinvent 3.10--3.12 cut-off/consequential
profiles. Independent read-only inspection confirmed self-linked -1 kg source
references, active landfill inventories, and documentation of the unrecycled
carrier after metal recovery. The UVEK CH/kg catalyst-base CH2O landfill documents
100% Formox carrier and has self-linked +1 kg production and a separately checked
native waste-input reference. This satisfies the standing matching-material,
function/unit and regional landfill-proxy approvals. Moisture is unavailable
in the source versus the target's 0% label; wet kg is preserved without scaling.
The source has 24 active biosphere releases versus eight in UVEK. Metal/ion
speciation, leaching, hydrology, time horizons, infrastructure and regional
conditions differ and warn; no emission-coverage or LCIA equivalence is claimed.
Neither endpoint adds cement solidification, catalyst manufacture, metal recovery
or a steam-production credit. No full licensed inventory is redistributed.

The original seven-reference/32-profile batch retains its exact rationale,
inventory evidence and frozen component bindings. Per-batch evidence permits the
additional source without rewriting existing approvals. Only the intended RER
disposal and market mappings change. The market joins the existing composition
review across six profiles, preserving its exact 0.9999999998340274 kg treatment
coefficient and 0.077 tkm freight through existing resolved supplier mappings.
No old helper definition is modified or removed. The UVEK endpoint was separately
checked as unsupported in reverse; no inverse rule is inferred or activated.


`brightpath/data/export/uvek_cutoff_stock_review.json` records 34 explicitly named
positive-reference Recycled Content cut-off stocks across all 168 installed
source/profile pairs. Read-only inspection found exactly one self-linked +1
production exchange and no other exchanges in each. Their source allocation
boundary is retained through warned production-only foreground suppliers, without
adding the former UVEK production/disposal targets' burdens or credits. This is not
zero impact for actual recycling or treatment and does not cover consequential,
negative-reference empty, missing or uninspected inventories. Only metadata and
fingerprints are distributed; source inventories remain private.

Exact direct-rule/profile/review-hash dependency bindings permit these approved
supplies inside matching market/processing helpers. All affected frozen bindings
were re-reviewed against their unchanged raw source inventory fingerprints; 52
existing parent identities were refreshed. An additional 114 market identities
have 612 approved profiles and four processing identities have 18. Their 672
other observed profiles remain explicitly unresolved, and all 118 identities
remain in the review queue. No inverse background correspondence is inferred.


`brightpath/data/export/uvek_heuristic_candidate_baselines.json` freezes nine
historical heuristic authoring candidates from this package's existing forward
crosswalk. Their shortlist used set iteration when equal-frequency tokens were
tied. Deterministic token ordering and score summation now reproduce the same
resource across Python hash seeds. The baseline records exact reference metadata,
confidence, candidate fingerprints and source/target bindings; they contain no
source inventories and grant no scientific or runtime approval. They precede the
existing reviewed overrides and unchanged independent sign audit. All retained
runtime targets, helpers, factors, omissions and blocked states remain unchanged.


### Modern sweet-natural-gas production flaring

`../export/uvek_natural_gas_flare_review.json` records two exact GLO/MJ source
identities across 28 inspected profiles: 16 approved ecoinvent 3.9–3.12 cut-off
and consequential profiles and 12 explicitly pending 3.6–3.8 counterparts.
`scripts/review_uvek_natural_gas_flares.py` binds source and target inventory
bytes and activity comments. Both modern flare inventories reference the
Meili et al. (2021) approach. Modern source comments assign CH4, NOx and SO2 to
main production; neither inspected flare contains them. Parent production
inventories are not claimed equivalent. The source withdraws 1/36 standard m3
of natural gas per MJ; UVEK withdraws one standard m3 and releases 36 MJ waste
heat per reference m3. This independently establishes the reference conversion,
without removing the resource withdrawal or adding purchased-fuel production.
Factor -1/36 replaces the former gas-turbine target with the dedicated UVEK
sweet-gas flare. Its native PRODUCT_FLOW output denotes a physical flaring
service. Each market contains only -1 MJ of that same service.

The independently reviewed reverse maps exactly that UVEK service to ecoinvent
3.12 cut-off with factor -36; its inventories and signs are independently
checked. A separate strict legacy projection preserves the original publisher
baseline. Native UVEK 2026 uses the explicit release bridge's normal-cubic-metre
identity. The older sweet-gas CH4/NOx boundary and all sour-gas profiles remain
pending. Nine UVEK biosphere exchanges replace seven/eight modern source
exchanges; oxygen balancing, N2O, waste heat, coefficients and geographic/time
representation differ and warn. No equal LCIA score or general inverse is
claimed. No full licensed source inventories are included.


### Feedstock-specific esterquat market credits

`../export/uvek_esterquat_market_review.json` records independent inspection of
42 ecoinvent supplier inventories and two UVEK feedstock-specific production
inventories for the two esterquat markets. The market-composition review records
28 exact-profile market inventory hashes and existing component-rule bindings.
Both source and target references are positive one-kg products. Coconut/palm-
kernel production consumes 0.3 kg of each oil; tallow production uses about
0.6 kg of tallow. Cut-off markets retain the observed 0.883720930232558/
0.116279069767442 coconut/tallow shares, while consequential markets retain
100% coconut/palm-kernel supply. Every original freight coefficient and incoming
credit sign is retained. This prevents the former all-tallow market target
from cancelling a tallow operating primitive spuriously.

This batch uses the standing complete market-composition approval and existing
component correspondences; it introduces no new process technology, omission
or allocation decision. Forty-four complete supplier inventory hashes are
rechecked by `scripts/review_uvek_esterquat_markets.py`; full source inventories
remain private. Geography, allocation, feedstock, technology, marginal supply
and freight remain proxies and warn. The tallow-treatment service is still
unapproved, and both UVEK production endpoints independently lack an explicit
reverse correspondence. No inverse is inferred.


### Physical cut-off feed coproducts and separate nutrient omissions

`../export/uvek_cutoff_feed_product_review.json` records nine physical cut-off
coproduct suppliers (63 exact profiles), six UVEK product inventories and three
markets (21 cut-off and 21 consequential profiles). Inventory bytes and boundary
comments for all 111 records are bound by
`scripts/review_uvek_cutoff_feed_products.py`. Source and target positive one-kg
references and native product-output roles are inspected independently. Five
DDGS suppliers formerly mapped to ethanol output; the replacements use the
matching corn, rye and potato DDGS coproducts. Grass fibres distinguish digestion
from fermentation. Both sorghum inventories document 9.1% grain moisture.

This batch applies the standing independently inspected matching-product/
physical-reference proxy and complete market-composition approvals. It retains
all unrounded cut-off supply shares and freight, with no component omission or
normalization. The earlier 21 consequential nutritional market omissions retain
their exact original approval records, review references and loss warnings.
There is no new nutrient substitution or omission authorization. Fodder-yeast
and beet-pulp cut-off supply remains pending. Target allocation, process energy,
agricultural inputs, geography, water/emissions and backgrounds remain proxies;
ecoinvent's DDGS 90%-dry-matter description is not asserted equivalent to UVEK's
wet-basis kg without independently quantified identical moisture. No arbitrary
nutritional, moisture or quantity conversion is introduced. Six separately
selected reverse pairs were independently re-read against the corresponding
ecoinvent 3.12 cut-off positive-kg coproducts. Corn DDGS retains US, rye DDGS RER,
both grass-fibre processes CH, and sorghum grain CN; Swiss potato DDGS explicitly
uses the inspected RoW supplier because no Swiss supplier exists. Each applies
factor +1 with allocation, moisture, technology and geography warnings. The
strict reverse batch projection preserves the original publisher baseline.
These explicit directional selections do not reconstruct the many-to-one
forward source geography, original market mixes or approved nutrient omissions.
Source full inventories remain private; no generic inverse or equal LCIA score
is claimed.


### Beet-pulp and whey-yeast physical cut-off extension

The existing cut-off product review adds seven suppliers/35 observed profiles,
two markets/14 cut-off compositions, their 14 unchanged consequential omissions,
and two UVEK product inventories. The cumulative evidence covers 176 records;
37 new supplier/product records additionally bind formal product information.
Old `beet sugar production` names occur through 3.10 and `sugar beet processing`
occurs in 3.11–3.12. Source and target sugar-refinery pulp specify 25.6% dry
matter. Beet-ethanol pulp retains its existing refinery-pulp product proxy with
explicit process, allocation and unquantified identical-moisture limitations.
Whey-fermentation legacy metadata names yeast paste as the fodder-yeast
coproduct. This is physical product supply, not a nutrient-market conversion.

All source and target references are positive one-kg products. Existing supply
shares, freight, caller metadata and all 70 consequential nutrient omissions
remain unchanged. The earlier nine product decisions, three market recipes and
six reverse rules are preserved. Two independently re-read reverse pairs use
Swiss ecoinvent 3.12 cut-off sugar-beet-processing pulp and whey fodder yeast,
both factor +1. Strict legacy projection allows only the complete six-rule
prefix or complete extended eight-rule batch. Allocation, moisture, primary
process, Irish whey technology, old Swiss background and water/emissions remain
explicit proxy limitations; no nutritional equivalence, market reconstruction
or equal LCIA score is claimed. Full licensed inventories remain private.


### Ecoinvent 3.6 sawing-slurry compositions

Standing complete processing/market composition approvals apply to RER and RoW
slurry-treatment references and their GLO market in both installed 3.6 models.
Independent read-only inspection covers all 28 treatment and 14 market profiles,
plus the selected positive-reference electricity, plant and chemical suppliers.
Only four treatment and two market profiles are compiled. Their exact raw
inventory fingerprints and component-rule/recipe bindings are retained; full
licensed inventories remain private. Two market shares are preserved without
rounding. The wrong agricultural spreading target and its 0.001 conversion
factor are replaced by positive per-litre helpers with incoming factor -1.
The exact original factor remains an authoring guard, with a default of one
for all earlier compositions. Existing bindings and recipes remain unchanged.

Consequential silicon-carbide and triethylene-glycol credits remain negative;
cut-off treatment does not gain credits. The source itself uses silicone-plant
infrastructure, and source/target descriptions identify the same chemical-plant
proxy. Electricity, geography, waste treatment, allocation, upstream supply and
emissions remain approximations. Internal uncertainty/formulas are not rebuilt;
caller quantities/uncertainty/formulas, foreground suppliers and separate
emissions remain preserved. No equal LCIA score or inverse correspondence is
claimed. The 36 newer profile decisions remain blocked pending their sludge
recovery, co-processing and landfill boundaries. The reverse resource is
independently regenerated and remains byte-identical.


### Physical cut-off ground-granulated slag products

Standing matching-product/function/kg and geographic-proxy approvals cover four
physical GGBFS producers across 28 cut-off profiles. Their raw stock is the
explicitly inspected positive production-only Recycled Content cut-off supply;
the existing ten market-profile recipes retain its freight. Use the UVEK
no-burdens operating product plus each original raw-slag quantity and existing
market helper. This removes the former erroneous economic raw-slag allocation
without dropping the source's raw-supply transport. The two UVEK variants have
the same operations/emissions; with-burdens adds allocated raw slag and haulage.
Forty-seven inventory/comment/product-information fingerprints bind products,
raw markets, empty cut-off stocks and both UVEK variants. Full licensed source
inventories remain private. No price-allocation reconstruction, new omission,
negative-reference treatment or constrained-SCM credit is approved here.

One independent reverse recipe maps the UVEK RER no-burdens physical product to
one kg of the ecoinvent 3.12 cut-off US product minus its exact raw-slag input,
1.0011225250000004 kg from the US market. This excludes raw supply/freight and
retains processing. RER-to-US technology, energy, water/emissions, material
grade, moisture, granulometry, yield, disposal and background differences remain
explicit proxies. Incoming signs, supported uncertainty/formulas, foreground
ownership and separate emissions remain unchanged. No inverse is approved for
the economically allocated with-burdens UVEK product. Earlier reverse rules,
recipes, reviews and publisher fingerprints remain protected exactly.

### Unspecified pure-water discharge to native UVEK 2026

`docs/uvek_2026_water_review.json` and `scripts/review_uvek_2026_water.py`
independently establish one ecoinvent-to-native correspondence: Water emitted
to unspecified water, cubic metre, to native Water emitted to unspecified water,
kilogram. The ecoinvent 3.10/3.12 elementary identity is
`2404b41a-2eed-4e9d-8ab6-783946fdf5d6`, CAS 7732-18-5; fresh metadata from the
original UVEK archive independently confirms the same CAS and 1,858 uses of the
exact native emission. Factor 1,000 uses the ecoinvent pure-water LCA convention
already documented in the pinned biosphere review, not an assumed wastewater
composition. The incoming rule is authored separately from the existing native
reverse rule, which remains unchanged with factor 0.001. Exact archive, catalog,
parent-review and directional baseline digests guard regeneration. Existing
bw2io authoring metadata was produced with version 0.9.14; use that version to
replay the pinned historical resources. This review approves no other water
compartment, regional flow, resource withdrawal, omission or treatment proxy.
Full licensed process inventories are not included.

### Allocated sewage digestion and gas supply

`../export/uvek_sewage_digestion_review.json` binds 55 inspected inventory and
description fingerprints. Treatment and gas branches are paired by their
original activity identifier across seven base and two explicitly inspected
patch cut-off versions. The gas branches have no active allocated operating
burdens; 3.6--3.8 retain zero-valued biosphere records and later profiles are
production-only. Treatment references remain separate negative cubic metres
of sludge. Helpers retain each version's operating components and direct
emissions, including older biogenic uptake. No gas/sludge volume equivalence,
extra gas recovery credit or empty-treatment assumption is introduced.

The existing heat-commodity proxy replaces the Swiss source market's CHP heat
and irrelevant delivery losses with UVEK heat-only production and district
delivery (1.11 MJ heat input/MJ delivered plus district transport). The source
MJ quantity is preserved and these background allocation/delivery differences
are explicitly disclosed. The 3.12 waste inventory indicator preserves its
reported quantity where available; only an unavailable inventory indicator may
be omitted with a warning under the standing indicator policy. It never sets
sludge density or gas yield. Full licensed source inventories remain private.

The reverse per-m3 UVEK gas operating recipe is inspected independently against
ecoinvent 3.12 plant infrastructure, an unweighted inorganic chemical mix, CH
low-voltage supply and Europe-without-Switzerland industrial gas heat. It retains
UVEK's own CO2/methane quantities rather than the empty ecoinvent gas allocation.
Regional supply, technology, heat geography, asset and allocation proxies remain
explicit. Only compatible cut-off models are executable. Additional targeted
patch evidence is collected read-only from the original named base databases;
the historical 15-profile audit remains intact, with patch labels bound in the
new inventory evidence rather than inferred from another release.

### Slag-landfill construction and operating suppliers

`../export/uvek_slag_landfill_component_review.json` binds 85 read-only inventory
and description fingerprints: nine ecoinvent source identities in nine exact
cut-off releases, plus the dedicated and former UVEK construction and operating
suppliers. Standing independently inspected matching-function, material, unit
and geographic-proxy approvals cover these corrections. No full proprietary
source inventory is redistributed.

The dedicated 375,000 m3 slag compartment replaces the former 1,800,000 m3
sanitary-landfill proxy, without capacity scaling. Operating references retain
factor +1 even though UVEK uses a native input waste flow. Function, land-use
and primary energy quantities were checked independently; warnings disclose
heat, water, freight, supply, geography and vintage differences. Separate
caller emissions, signed amounts, formulas and uncertainty remain unchanged.
The existing reverse construction and operating correspondences were checked
against the actual UVEK and ecoinvent 3.12 inventories and are unchanged.

The authoring collector now requires `slag_landfill_component_inventories` in
addition to the earlier sparse review fields. Missing, duplicate or changed
inventory/description fingerprints, native reference roles, signs or exact
profiles require renewed review. Only the nine exact inspected cut-off versions, including 3.9.1 and 3.10.1,
are approved; further patch, consequential or whole-ash-treatment approval is
not inferred.

### Hot-rolled stainless material supply

`../export/uvek_stainless_stock_review.json` binds 23 complete inventories and
their comments/product information through sparse fingerprints: three ecoinvent
identities across seven cut-off versions plus the UVEK material and former
rolling-service boundaries. The inspected global market supplies RER/RoW
hot-rolled producers. Those producers each require one kg of 18/8 steel stock
and one kg of hot-rolling service. UVEK's RER `Chromium steel 18/8, at plant`
already includes one kg of hot rolling and its 0.63/0.37 converter/electric
stock mix. The previous `Sheet rolling, chromium steel` supplier is a cold
surface-finishing service with only 0.0516 kg loss-replacement stock per kg.
Standing matching-material/function/unit and geographic-proxy approvals cover
the two GLO-market/RoW-producer corrections, preserving factor +1 and signed kg.
Warnings disclose the replaced supply mix, freight, production technology and
background; equal LCIA scores and extra rolling burdens are not inferred.
Native reference metadata independently confirms both UVEK processes are
positive product outputs. The existing reverse material correspondence to the
inspected RER hot-rolled producer remains unchanged. No omission, new helper or
new reverse mapping is introduced; full licensed inventories stay private.

### Approved native water-emission screening proxies

`../export/uvek_2026_ash_water_review.json` records the user's explicit approval
on 2026-11-06 of 56 exact ecoinvent 3.10-to-native-UVEK-2026 water proxies.
Reference identities, publisher UUIDs and CAS metadata were checked read-only
in the actual ecoinvent 3.10 and 3.12 biosphere databases. Native names, receiving
compartments and available CAS metadata were inspected in the original UVEK
archive with SHA-256 `ebaab56717a6227b5eb271fcb4c86fb9db206115eb937fb0ec0b10d7accf26c7`.
Only reference metadata and authored correspondences are redistributed.

The approved scope explicitly discloses ionic-to-generic-element and
surface-to-unspecified-water approximations. Signed kilograms and named-anion
mass are retained without stoichiometric conversion; long-term groundwater
retains its compartment. Per-exchange losses contain original source CAS,
UUID and receiving compartment. No emission omission or inferred ionic inverse
is authorized. Existing forward rules and the complete independent reverse
resource are guarded by their exact baseline hashes.

Full native-resource generation uses the review's recorded bw2io 0.9.14.
Version 0.9.17 has identical name-migration data but changes the provenance
version string and therefore correctly fails the historical unit-review hash.
The original digest guard is not weakened.

### Waste-specific post-incineration bottom-ash landfill services

`../export/uvek_bottom_ash_review.json` binds 200 exact source inventory and
boundary-description fingerprints for 49 bottom-ash identities in six cut-off
versions: 3.9, 3.9.1, 3.10, 3.10.1, 3.11 and 3.12. The two patches were inspected
in their original databases, independently of base-version similarity. The
nine positive slag-landfill component reviews bind 85 inventory fingerprints
and 81 exact source/profile pairs, including 18 actual patch supplier profiles.

The generated target recipes retain waste-specific direct releases, inspected
regional weights, one kg of operating burdens and the reported 1.7778e-9 facility
units per kg ash. The -1 kg source reference becomes a +1 kg helper with factor
-1. No incineration, upstream wastewater treatment, generic original-waste
composition, recovery credit or mass-basis scaling is added. Core and patch
supplier bindings and 84 used native biosphere correspondences are independently
frozen; missing or changed quantities, descriptions, signs, exact versions or
component mappings require renewed review. Full licensed source inventories
remain outside the repository; only sparse fingerprints and derived target
correspondences are included.

Only the three marked waste/carbon inventory indicators can be omitted with
warnings when the requested target lacks them. All physical releases remain.
The user's separate native-water proxy approval supplies the reported
speciation/receiving-water approximations. Independent reverse operating and
facility links are unchanged; the forward service does not authorize an inverse
from generic native elements or generic operating kg to a whole ash treatment.

### Waste-specific residual-material-landfill services

`../export/uvek_residual_landfill_review.json` binds 625 source/target inventory
and description fingerprints, 25 source identities and 142 exact cut-off
profiles. Original 3.9.1 and 3.10.1 inventories were inspected independently.
Only derived target coefficients and sparse fingerprints are packaged; the full
licensed source vectors remain private. Ninety-five used native biosphere
correspondences, 48 exact-UUID canonical naming aliases and 80 scoped supplier
choices are bound independently. Alias labels retain source CAS/UUID evidence;
units and compartments do not change through canonical naming.

The complete helpers preserve stabilization, hydrated-cement treatment, direct
releases and the original old aggregate or modern expanded operating/construction
model. Modern pitch, concrete, PVC and qualified non-natural-gas heat use the
reviewed complete-material/service proxies with explicit limitations. No upstream
waste treatment, recovery credits, moisture conversion or normalization is added.
The two active positive-reference blast-furnace-slag services in 3.9 retain +1;
140 negative-reference profiles use -1. Seven complete-supplier reverse mappings are independently inspected: excavation,
Swiss quarry gravel and sand, Swiss/European allocated refinery bitumen, complete
PVC resin, and Swiss tap-water delivery. Preserve signed kg or m3 with factor +1
and individual boundary warnings. No forward inversion or full-service inverse
is inferred.

The user's `Approve scoped refinement with warnings` decision
(`call_95f827d200cf4991a94f64b4cfe56e9c`) covers only the 36 native UVEK 2026
land bindings in nine known residual-landfill helpers. The original UVEK archive
SHA256 is `ebaab56717a6227b5eb271fcb4c86fb9db206115eb937fb0ec0b10d7accf26c7`;
its resource/land input 85107 verifies the residual-material-landfill class.
Preserve source 5.7143e-5 m2/kg, not the native operation's 6.25e-5. Canonical
exports retain the generic label. No global land correspondence or occupation
year conversion is introduced. Each applied refinement reports information loss.
Generated helper identities bind the complete packaged recipe/dependencies and
creation biosphere profile; later migrations reject stale identities before
replaying scoped bindings and the already approved optional-indicator policy.

### Complete waste markets with reviewed residual-landfill children

`../export/uvek_resolved_child_market_review.json` binds 135 actual source,
freight and reverse-supplier inventory/description fingerprints. Eleven market
identities have 54 cut-off profiles, including independent original 3.9.1 and
3.10.1 evidence. Forty-six profiles are complete; eight older clinker-dependent
profiles remain pending. Nine identities are fully disposed and two have explicit
partial profile decisions. Full licensed source vectors remain private; packaged
content contains sparse fingerprints and derived target coefficients.

The market helpers reuse independently reviewed waste-specific landfill children
and bind their exact recipe/rule signatures. Forty-four scoped freight choices
preserve original ton-km and three exact electric/diesel traction mixes, using
complete road, rail and barge service proxies with explicit geography, technology,
filter, load, vintage and background limitations. No generic freight rule,
steam proxy, physical release omission, alternative fate, recovery credit or
normalization is introduced. Four existing reverse freight correspondences are
pinned independently to actual source and target inventories; no inverse is
inferred for a complete market or a native generic water-emission proxy.

### Positive cubic-metre sewer-throughput services

`../export/uvek_sewer_throughput_review.json` binds 449 actual inventory and
activity/product-description fingerprints: four source identities across 24
cut-off profiles, full suppliers, original patch waste submixes and independently
inspected reverse suppliers. The licensed source vectors remain private; the
package contains sparse fingerprints and derived target coefficients.

Rural and urban construction is already normalized per cubic metre of wastewater
conveyed. Complete helpers preserve every reported material, operation, disposal
and direct water coefficient without capacity, length or lifetime conversion.
The market aliases demand their matching construction helpers at exactly one m3.
Full resin supplies remain separate from plastic-pipe conversion; source and
native synthetic rubber both include EPDM extrusion/vulcanisation. Existing
material and disposal proxies retain all regional, production-mix, treatment,
moisture, market/freight and vintage limitations; their LCIA equivalence is not
asserted. Positive imported references for four native WASTE_FLOW disposal
services are pinned separately from their native input-side classifications.

Exact 3.9.1 and 3.10.1 polyethylene-waste groups retain original regional shares
using the existing country-market correspondences, without global patch fallback
or omission. Direct water volumes preserve their compartments; existing native
water rules convert m3 to kg at 1000. Fifteen existing reverse supplier choices
are independently bound to actual inventories and remain unchanged. No inverse
sewer helper, cross-system-model mapping or omitted physical release is added.

Whole sewer graphs currently contain six native suppliers without independently
approved reverse correspondences: the four disposal services, global
medium-voltage electricity and full emulsion-PVC resin. These are explicitly
recorded as unsupported; reverse conversion is tested to reject and roll back
rather than infer an inverse, drop a contribution or return a partial graph.
The 15 independently bound existing supplier choices are tested separately.

### Exact particulate labels and air contexts

`../export/uvek_2026_particulate_review.json` records thirteen size/context pairs,
26 actual source-flow metadata fingerprints from original ecoinvent 3.10 and
3.12 databases, and native flow definitions read from the original UVEK2026
archive identified by SHA-256. Only the missing ecoinvent-to-native direction is
added. All thirteen pre-existing publisher-UUID reverse rules are independently
pinned and unchanged. This is an exact physical label correspondence, distinct
from the pending ionic-to-generic metal-air proxy choice.

The three explicit particle-size bins remain separate at unchanged signed kg;
no PM10 aggregation, inferred fraction, compartment redistribution, time or
unit scaling is introduced. The user-facing correspondence report preserves
traceability; different LCIA method factors remain a property of their methods.
The authoring projection removes only these exact additions and verifies the
previous resource hash so older water, land, radioactivity and publisher guards
continue to validate their independently recorded boundaries.

### Complete packaging-paper wet-sludge landfarming

`../export/uvek_packaging_paper_landfarming_review.json` binds 25 actual inventory
and description fingerprints: four original cut-off source profiles, global
spreading markets and their actual regional suppliers, plus complete native and
independently reviewed reverse service inventories. Licensed raw vectors remain
private; only sparse evidence and derived target coefficients are packaged.

The -1 kg wet-sludge references become +1 kg helpers with factor -1. Every one
of the 39 source releases retains its canonical identity, signed quantity and
unit. The original 0.00097087 m3 service volume is retained without density or
97%-water adjustment. The complete Swiss tanker service explicitly excludes
sludge and its releases; operational and direct sludge burdens are separate.
Its geography, fleet, fuel mix and background differ from the global source.
The native Swiss background includes chromium VI and 9.84 MJ waste heat, while
the inspected Swiss ecoinvent supplier reports chromium III and no waste heat.
These component-level differences are disclosed without changing the sludge's
direct chromium-III release. The existing reverse CH service rule remains pinned
and unchanged. No inverse waste-treatment mapping is inferred.

The only optional omission is the specifically marked non-hazardous-waste
inventory indicator. Real soil metals, nutrient releases, water and heat remain.
Canonical ecoinvent biospheres support the complete emitted inventory; native
UVEK soil-ion and groundwater-volume coverage is an independent unresolved axis.
Default native conversion rejects missing physical flows rather than inferring
speciation, omitting pollutants or returning a partial committed graph.

The PET-waste market review (`uvek_pet_waste_market_review.json`) fingerprints
1,958 original source/native/independent-reverse inventory vectors and their
descriptions. Public evidence contains identities, signed derived target
coefficients and fingerprints; licensed inventory vectors remain private.
The 373 approved exact profiles preserve complete treatment/freight functions
under the previously approved material/function/geographic screening scope.
Forty-two patch-landfill uses are confined to these market helpers. No new
physical-flow omission, land-class refinement, FLUWA technology proxy,
negative-empty mineral allocation or consequential route is authorized.

The five further complete waste-market functions
(`uvek_five_simple_waste_market_review.json`) fingerprint 288 full original
source/native/independent-reverse vectors and their descriptions. Sixteen
profiles retain one wet kg of material-specific independently reviewed
uncontrolled-disposal service plus original complete road tonne-km. Only
derived target coefficients, identities, fingerprints and boundary evidence
are public; original inventory vectors and native fleet coefficients remain
private. No omission, new physical-flow proxy or consequential route is added.

The explicitly approved Swiss PET FLUWA market proxy
(`uvek_swiss_pet_fluwa_market_review.json`) fingerprints 455 full original
source/native/independent-reverse vectors and their descriptions. Nine exact
profiles retain their original treatment fractions and complete road/rail
quantities. The complete same-material 0.2%-water PET incinerator is a warned
technology proxy with no added zinc, sludge, energy or displacement credit.
Only derived target coefficients, identities, fingerprints and boundary
evidence are public; licensed source vectors remain private. Approval is
confined to these nine market profiles; no other-material FLUWA, physical-flow,
allocation, consequential or inverse approximation is inferred.

### Complete landfill-leachate wet-sludge landfarming

`../export/uvek_slf_landfarming_review.json` pins 103 full source, mechanical-service,
native and independently reviewed reverse inventory fingerprints. Twenty-two
functions across 82 original cut-off profiles retain all 2,368 biosphere entries,
46 exact publisher biosphere definitions and the original 0.00097087 m3 whole
spreading service per wet kg. The existing wet-sludge / dry-disposal heuristics
are replaced using standing complete-source and whole-service approvals, with
explicit fleet/geography/boundary warnings. Twenty-four generated recipes retain
profile differences; no new physical-flow proxy, energy allocation or inverse
sludge correspondence is approved. Source inventories and descriptions remain
private; the package contains derived target coefficients, exact identities,
fingerprints and independently checked supplier bindings.

### European PET market-group compositions

`../export/uvek_pet_market_group_review.json` pins 342 complete original group
and child inventory fingerprints across eighteen exact cut-off profiles. All
324 direct regional/Swiss child inventories match the previously published
regional PET and explicitly approved Swiss FLUWA reviews. The 333 original
supply weights are retained in eighteen nested group helpers; 319 transitive
existing recipe definitions and both scientific review resources are pinned.
Groups add no freight, loss, treatment, recovery, allocation or physical-flow
proxy, and no child correspondence or independent reverse rule is changed.
Original source vectors/descriptions remain private; only derived target
coefficients, identities, exact bindings and fingerprints are distributed.

### Allocated copper stock and its reviewed dependencies

`../export/uvek_copper_stock_review.json` binds 73 complete original inventory
fingerprints: nine exact self-production-only positive Recycled Content stock
profiles, 63 already-reviewed parent profiles and the nonempty former UVEK PV
recovery supplier. Standing explicit material-stock approval covers these exact
matching cases. The shared authoring API accepts the separate frozen review
without altering the earlier 34-stock review or its recipes. Forty-two direct
occurrences retain their exact signed copper kg; their 60 former dependent recipe
identities are rebuilt with exact source-profile allocation bindings, including
three vessel markets. Every other component and recovery quantity is preserved.
Historical consequential coefficients and recipe bodies are unchanged; 70
historical metadata bindings are refreshed to the shared cut-off-only review.
No consequential route, negative-empty service, recovered-energy stock or
physical PV-recycling inverse is approved. Raw original vectors and descriptions
remain private; distributed evidence contains derived target coefficients,
identities, contextual bindings and complete-inventory fingerprints only.

`brightpath/data/export/uvek_copper_cathode_review.json` records independent
read-only inspection of nine cut-off cathode-copper market profiles and fifteen
native UVEK process boundaries, including all direct copper-storage suppliers.
The original UVEK ecoSpold definitions corroborate the whole-metal/product/kg
reference and the unrelated former battery-electrode target. The reviewed factor
is +1 per kg; the complete native supply is retained as a screening geographic,
vintage, primary/secondary-allocation and logistics proxy, without a purity
normalization or empty-stock interpretation. Only hashes and derived boundary
findings are published; licensed source vectors remain private. The existing
independently authored reverse copper proxy is fingerprinted and preserved.

`brightpath/data/export/uvek_whole_freight_review.json` records six whole-freight
correspondences in 32 exact cut-off source profiles, with 2,111 full-inventory and
metadata fingerprints. The review independently inspected full source freight
markets/operators, native complete services and vehicle lifecycle boundaries,
and 687 existing parent/ancestor profiles. Derived target coefficients preserve
1,031 exact freight occurrences, other components and genuine credits. All
historical consequential recipe bodies and coefficients remain provenance only.
Explicit scoped artifact namespaces distinguish the cut-off decisions. Existing
independently scoped freight reconstructions retain their own approvals.

`brightpath/data/export/uvek_packed_wool_review.json` records 47 full-inventory
fingerprints: nine delivered markets, eighteen packed producers, their eighteen
unpacked production counterparts and the two linked native factory modules.
Derived helper coefficients retain both producer shares and four original
freight quantities per market. The complete packed-rock-wool proxy replaces the
unrelated glass-wool target without density, thermal-performance or packing-mass
normalization. Factory and fleet assumptions remain explicit screening limits.

The same reviews independently bind two additional reverse product correspondences
against actual native and ecoinvent 3.12 cut-off inventories: Swiss light-duty
freight to the Swiss light-commercial market, and Swiss packed rock wool at plant
to the Swiss packed stone-wool factory. No forward-market inversion is inferred.
Only fingerprints, identities, derived target coefficients and scientific boundary
findings are published; licensed full source vectors remain private. Unrelated
reverse rows, recipes, omissions and their rationale are unchanged; whole-file
reverse evidence pins were refreshed only after checking this invariance.

`brightpath/data/export/uvek_chemical_heat_review.json` fingerprints 141 complete
original heat, water-supply and native service inventories and both original UVEK
heat/steam XML definitions. Four heat-energy identities have 36 exact cut-off
profiles across 3.6--3.12, including actual 3.9.1 and 3.10.1 databases. Equal-unit
chemical-plant heat is used under standing whole-function/geographic screening
proxy approval. The original native MJ service explicitly excludes water input,
so derived helpers retain every inspected make-up-water coefficient and both
physical water releases in the original canonical receiving compartments. Native
fuel/electricity mixtures and water-supply geography remain explicit screening
limitations, with no second energy-generation inventory or physical-flow
omission. Exact source and native descriptions and inventories stay private;
only identities, fingerprints, derived target coefficients and boundary findings
are distributed. Physical kg-steam and inferred reverse heat/water cancellation
are outside this review.

`brightpath/data/export/uvek_pam_frontier_stock_review.json` records eighteen
original positive production-only aluminium/iron cut-off stock inventories and
two complete former native preparation suppliers. All original source vectors
and descriptions remain private; published evidence contains identities,
fingerprints and allocation-boundary findings. Standing user approval for exact
positive, single-self-production Recycled Content cut-off stocks applies.
Nineteen source-identified same-target preparation recipe bodies were fingerprinted;
the regional freight follow-up retains their preparation/stock coefficients and
records the exact affected freight branches separately. No negative empty treatment, recovered-energy boundary,
whole-treatment omission or inverse native preparation mapping is approved.

`brightpath/data/export/uvek_regional_freight_review.json` binds 470 complete
source, native supplier and parent inventories by identity and full-vector and
description fingerprints. Four regional diesel-road markets and the Swiss rail
market have ten exact cut-off profiles in ecoinvent 3.11/3.12. Standing approval
for independently inspected same-function services applies: road markets use the
complete RER heavy fleet, and the Swiss rail market retains its original electric
and filtered-diesel coefficients through complete Swiss rail services. Differences
in regional fleet composition, vintage, fuel/electricity intensity, shunting,
assets and emissions remain explicit screening limitations. No second vehicle or
track inventory, payload scaling, source coefficient normalization or inverse is
introduced. The propagation covers 424 parent profiles, 605 source-identified
freight occurrences and 966 other unchanged inputs; 216 independently scoped
custom profiles retain their approved recipe bodies. Historical consequential
evidence remains provenance only. Licensed full vectors and descriptions are not
distributed; published coefficients are derived target inputs.

### Complete average wastewater-sludge landfarming

`../export/uvek_average_sludge_landfarming_review.json` binds 221 full original
inventory and description fingerprints. Eight source identities have 48 exact
cut-off profiles in ecoinvent 3.9, 3.9.1, 3.10, 3.10.1, 3.11 and 3.12. The review
uses the actual two patch databases, complete source treatment/market and
machinery inventories, full native spreading/road/rail boundaries, and the
previous incineration supplier. Standing approval for independently inspected
same-function services and complete source helpers applies.

Twenty-two distinct helpers retain 1,040 physical biosphere occurrences,
24 conditional waste-accounting indicators, the exact original mechanical
spreading volume and 42 freight occurrences. Historical soil carbon remains
only in 3.9/3.9.1 profiles. Six historical calcium/sodium/strontium label
refinements retain the same published flow UUID, unit and receiving compartment
across those two versions; they are not native ionic-metal proxies. Native
physical-flow gaps retain the established strict-refusal and explicit
permissive-unlinked-preservation policy. Geography, machinery capacity, vintage,
energy intensity and fleet assumptions remain screening limitations.

The independent reverse mechanical-service correspondence is pinned to its
original source and target inventories. The former incineration supplier has no
inferred landfarming inverse. All 2,989 previous helper bodies and unrelated
forward rules, reverse resources, biosphere resources and historical
consequential decisions remain unchanged. Only fingerprints, identities,
derived target coefficients and boundary findings are published; original
licensed vectors and descriptions remain private.

### Older regional all-size road markets

`../export/uvek_older_regional_road_review.json` binds 1,215 complete original
source, operator, selected-parent and native-supplier inventory fingerprints.
Four positive tonne-kilometre market identities have 26 exact cut-off profiles
in ecoinvent 3.6, 3.7, 3.8, 3.9, 3.9.1, 3.10 and 3.10.1; the Brazilian market
starts at 3.8. Both patch releases were inspected in their actual original
databases. Source all-size lorry operators and both former native light-duty
targets were inspected separately from the complete native heavy-fleet service.
Standing same-function geographic screening approval applies.

Derived target inputs retain 674 signed freight quantities across 553 parent
profiles and preserve every other source coefficient and exchange field.
Five hundred other inspected parent profiles already have correct whole-fleet
choices; their selected recipes remain exact, together with independently
scoped custom freight choices. Historical consequential recipes and all rail
services remain unchanged. Geographic fleet composition, EURO standards,
loading, fuel intensity and vintage remain explicit limits. No second vehicle
inventory, capacity/lifetime normalization, treatment omission or inverse
correspondence is introduced.

Whole-freight source evidence and coefficients remain unchanged; only 31 exact
parent-rule binding hashes are refreshed for the corrected selected suppliers.
The dependent average-sludge review updates that shared-review fingerprint
without a new sludge or physical-flow modelling decision. Full licensed
inventories and descriptions remain private; published evidence consists of
identities, hashes, derived target coefficients and boundary findings.

### Exact patch allocation-stock and recovery-handoff boundaries

`../export/uvek_exact_patch_boundary_review.json` records 39 full-inventory and
description fingerprints for actual core counterparts, exact 3.9.1/3.10.1
cut-off sources and their empty positive Recycled Content endpoints. Existing
standing approvals cover four positive production-only stock profiles and
eleven exact recovery-handoff profiles; each source identity, signed reference,
unit, complete inventory and endpoint was independently inspected read-only.

The two stock identities reuse their existing allocation-only suppliers. Each
handoff has self-production -1 and one -1 kg input into a positive stock with
no other inventory, so only its external caller exchange is omitted with the
existing warning/audit workflow. All previous profile decisions, 3,011 helper
bodies, 651 shared runtime reviews, reverse resources and biosphere resources
remain unchanged. Every new entry carries exact patch evidence; no release
alias supplies an uninspected profile.

Four stock and two handoff profiles match all non-identifier core exchange
attributes. Nine 3.9.1 handoffs additionally contain property comments and
unused variable names; independent comparison confirms identical signs,
quantities, units, property values and uncertainty attributes, with no exchange
formulas. Published process UUID normalization was verified against original
provider metadata. Only identities, fingerprints and boundary findings are
distributed; full licensed vectors, descriptions and property comments remain
private. These decisions describe allocated source inventory boundaries and
do not claim that physical recycling or treatment has zero impact.

The complete exact-patch boundary review (`uvek_complete_patch_boundary_review.json`)
extends existing approvals to 40 stock and 21 handoff profiles, with 136 complete
inventory and description fingerprints. Original licensed inventories remain
outside the repository; packaged evidence contains identities, fingerprints,
reference signs, exchange counts and existing action/helper bindings. No additional
physical inventory, proxy, consequential route or inverse mapping is inferred.

The cable/rail functional-boundary review (`uvek_cable_rail_review.json`) records
535 complete source, parent and native-service inventory/description fingerprints.
Original licensed vectors and comments remain outside the repository. Four
source identities cover 34 exact cut-off profiles: nine cable operators, eighteen
Swiss/RoW wire-plastic markets and seven older Swiss freight-train markets.
Twenty-four component helper bodies preserve signed source quantities; 321
source-witnessed parent profiles propagate those corrections without changing
unaffected inventory leaves. Four existing exact-patch freight bindings remain
unchanged. Facility units are retained without capacity or lifetime scaling.

The user separately approved the nine Swiss wire-plastic fly-ash-extraction
proxies with warnings. The native wire-plastic incinerator matches waste type,
3.55% water content and kg basis but lacks that extraction and metal recovery.
Warnings retain the ash-treatment, recovery, emissions and allocation limitations;
no additional recovery credit is introduced. Empty recovered-copper stocks,
building-conduit disposal and operation-only rail processes are not substitutes
for these complete treatment and freight functions. No consequential helper or
inverse mapping is added. This is a screening reconstruction, not demonstrated
LCIA equivalence between databases.

The textile extension of `uvek_uncontrolled_disposal_review.json` adds nine
complete RoW unsanitary-landfill profiles and source-witnessed textile markets.
The full review now pins 310 source/provider/native inventory and description
fingerprints. Original licensed vectors, comments and product descriptions
remain private. The original 217 uncontrolled-disposal profiles, source
coefficients and helper bodies are retained exactly. The added profiles retain
277 physical biosphere rows, 18 complete operating-service inputs and 12
existing optional accounting-indicator rows; no physical flow is omitted.
Existing verified UUID/full-context definitions and full-service operating
correspondences supply all added inputs. The four native freight witnesses
include complete sea-container, rail, barge and heavy-road services.

Five older global textile-market profiles retain the original Indian/RoW
landfill shares and each sea, rail, inland-barge and road ton-km quantity.
Four modern RoW market profiles retain the original road-freight quantity,
including the independently inspected exact 3.10.1 profile. A controlled
municipal-landfill proxy is replaced by the source's own RoW unsanitary
physical boundary; no jute-production stock, waste-moisture scaling, Indian
climate/degradation coefficients, extra purchased diesel or recovery credit
is introduced. Existing operating/freight technology and background differences
remain warnings. Unsupported native physical biosphere links retain strict
refusal or explicit permissive-mode warnings. Neither native air-ion proxies
nor cross-system-model or reverse correspondences are inferred.


### CEM III/A concrete output-grade correction

`scripts/review_uvek_concrete_grade.py` records six source producer and two
native concrete inventory/metadata fingerprints in
`brightpath/data/export/uvek_concrete_grade_review.json`. Original Brightway
databases were inspected read-only. No proprietary complete inventory vectors
or publisher comments are redistributed. The published CRB NPK table
(https://www.crb.ch/_Resources/Persistent/9/f/0/0/9f00d31e6a562da930eebcf635901e8375bb34d8/Anhang_188D11.pdf)
identifies NPK A as C20/25 and NPK B as C25/30. The existing CEM III/A
whole-process screening proxy is corrected to NPK B per cubic meter under the
standing matching-function/unit/material screening authorization. Recipe,
cement-dose, geography and operating differences remain explicit warnings.
No arbitrary normalization, other binder substitution, market reconstruction
or reverse correspondence is inferred.


### Source gas-flare inventories and exact markets

`scripts/review_uvek_remaining_gas_flares.py` compiles derived target service
coefficients from 36 independently inspected cut-off source profiles and one
unchanged native flare witness. Complete original vectors and publisher
comments remain private; the review resource publishes fingerprints, canonical
flow definitions and derived target coefficients. Four source identities cover
nine installed source versions. Twenty-eight new source helper profiles retain
132 physical biosphere occurrences with their marginal lognormal fields; the
eight existing modern sweet-gas proxy profiles retain their endpoint, factor
and scientific sign review. Shared source parameter correlations and formulas
are not reconstructed; caller-owned fields remain preserved.

The old natural-gas resource unit spelling is normalized only for the same
UUID and compartment without changing its numeric coefficient. Primary evidence:
[ecoinvent 3.0 change report, page 16, Table 4.1](https://forum.ecoinvent.org/files/report_of_changes_ecoinvent_2.2_to_3.0_20130904.pdf)
defines the natural-gas volume reference conditions, and the
[ecoinvent 3.9 change report, page 32, Table 12](https://support.ecoinvent.org/hubfs/Change-Report-v3.9.pdf?hsLang=en)
reports both old and new resource properties per standard cubic metre. No
heating-value, source dose or generic gas-state conversion is inferred.
Native mercury, NMVOC and natural-gas-resource gaps remain explicit strict
errors or permissively retained physical links, without new chemical proxies
or omissions. Reverse resources and cross-system-model restrictions are unchanged.

### Precious-metal anode-slime markets

`scripts/review_uvek_precious_anode_markets.py` binds eleven exact cut-off market
profiles to eighteen nonempty source refineries and the positive UVEK Swedish
precious-metal-in-anode-slime refinery product. The public review contains thirty
complete-vector/metadata fingerprints, identity and boundary findings, and
only derived target coefficients. Original licensed vectors and comments remain
private. Each source market has a unit positive reference and only refinery
supplier inputs, which are preserved without normalization or service-sign
inversion. The native whole-process proxy retains its own allocated burdens;
regional electricity, allocation, vintage, water and technology differences are
explicit limitations. No zeroing, input-scrap substitution, emission-ratio scaling,
physical omission or inferred inverse is approved. Existing system-model
restrictions and independent reverse resources are unchanged.

### Metalliferous hydroxide sludge allocation stock

`scripts/review_uvek_metalliferous_stock.py` compiles the standing approval for
explicit positive Recycled Content cut-off stocks. All nine independently read
complete original inventories contain exactly one self-linked production +1 kg
and no other exchanges. The new production-only helper preserves that observed
allocation boundary instead of adding the former fresh lithium hydroxide
process. Public evidence contains inventory and description fingerprints, not
licensed vectors or comments. Original source databases are opened read-only.
Signed caller data, separate emissions and foreground ownership are preserved;
no physical treatment is zeroed, and no reverse correspondence is inferred.

The associated market is independently inspected in all nine cut-off versions.
Only its source-traced allocated-stock occurrence changes; its four original
freight inputs remain unchanged. Two patch markets receive explicit inventory
fingerprints and exact component bindings. Historical consequential profile
vectors and all other composition-review rows remain unchanged. No global
replacement of genuine lithium hydroxide inputs is performed.

### Polypropylene uncontrolled source inventories

`scripts/review_uvek_pp_uncontrolled_source.py` compiles eighteen independently
inspected BIO-only source profiles into positive-reference treatment helpers.
All 1,719 original physical coefficients and marginal lognormal fields are
retained under canonical publisher identities. The three older Nickel-ion
water/groundwater occurrences reuse only the existing exact-release same-CAS
publisher-synonym review in `uvek_canonical_biosphere_identity_review.json`.
No source parameter, formula, processing or construction input is absent from
the inspected boundary. Licensed vectors/comments remain private; public
resources contain fingerprints, definitions and derived target coefficients.
Current native gaps remain explicit strict errors or permissively retained
physical links, without new chemical proxies or omissions. Existing registered
native water volume/mass bridges apply independently of waste composition.
No fly-ash-extraction incinerator proxy or reverse mapping is approved here.


Ghanaian uncontrolled refrigerator-dismantling review
---------------------------------------------------

`uvek_ghana_uncontrolled_review.json` records nine independently inspected exact
cut-off profile fingerprints and derived target coefficients for an appliance
service. It retains 45 direct-emission occurrences, 63 material-treatment/recovery
inputs and 40 constant arithmetic formulas with marginal uncertainty. Existing
component mappings and allocated recovery stocks are bound individually; their
proxy limitations remain explicit. Licensed full inventories and comments remain
in the local private review workspace and are not distributed. No cross-model
or reverse correspondence is inferred.


Parameterized refrigerant patch and appliance-market review
----------------------------------------------------------

`uvek_refrigerant_patch_formula_review.json` records 38 new exact cut-off profile
inventories across 18 identities, plus independently inspected positive supply
and native endpoint witnesses. Derived target recipes retain 59 direct emissions,
226 technosphere inputs and 104 formulas. The review pins every old profile
decision and helper retained by its 16 extensions. Regional formula allocations,
negative waste-reference coordinates and local parameter definitions are explicit.
Fourteen existing native chemical/compartment bindings are reused only for their
matching physical flows and retain their warnings. Full licensed vectors and
comments remain in the private local workspace. No inverse or cross-model mapping
is inferred.


The digester-sludge stock correction is generated by
`scripts/review_uvek_digester_stock.py`, with independently inspected evidence in
`brightpath/data/export/uvek_digester_stock_review.json`. It applies the existing
approval for strictly positive, production-only Recycled Content cut-off material
stocks to nine exact source profiles and retains freight in thirteen material
markets. The public review contains identities, inventory/description fingerprints,
derived target coefficients and prior public decisions; licensed source vectors
and descriptions are not distributed. The former native incineration process is
independently fingerprinted and remains distinct from the allocated stock.

### Four exact native chemical synonyms in urban air

`../export/uvek_2026_chemical_alias_review.json` records the independently
inspected hydrochloric acid/hydrogen chloride, pentachlorobenzene,
hexachlorobenzene and pentachlorophenol correspondences in the explicit
urban/high-population air compartment. Each pair retains its reported CAS
number and kilogram basis. The review records original native archive filenames
and flow numbers, canonical source UUIDs and exact catalog hashes; licensed
inventory vectors and comments are not redistributed. The authoring replay is
`scripts/review_uvek_2026_chemical_aliases.py`. Only four forward rules are added;
all independently reviewed publisher-UUID reverse rules remain unchanged.
No ionic-metal, dioxin-equivalent, dilution, water-mass or compartment proxy is
inferred, and physical correspondence does not establish LCIA equivalence.

### Swiss CEM II product-market delivery

`../export/uvek_swiss_cement_market_review.json` records twelve exact Swiss
CEM II/A and CEM II/B positive-product markets in six cut-off versions. It
fingerprints 26 complete original market, production and native inventories
and their full metadata without distributing licensed vectors or comments.
Generated inputs retain one kg of complete matching native class production
and the original separate road/rail delivery with uncertainty and pedigree.
The replay is `scripts/review_uvek_swiss_cement_markets.py`. Matching names are
qualified by reference product: separately allocated hard-coal-ash functions
remain outside this review. No inverse, concrete formula reconciliation,
density scaling or wider cement-grade substitution is inferred. The existing
independent reverse resource remains byte-exact.

Normal digester-stock replay validates the seven deliberately retired parent
bodies against their retained hashes and public audit definitions. Those bodies
remain in `../export/uvek_digester_stock_review.json` and retained base decisions,
while the executable helper table and current decision recipe definitions omit
them. No allocation, inventory or signed quantity is changed by this replay fix.


### Swiss copper bottom-ash disposal and collection

`../export/uvek_copper_ash_review.json` records four exact source profiles for the
Swiss copper bottom-ash market and slag-compartment treatment in ecoinvent 3.11
and 3.12 cut-off. The source-preserving helper approval, existing Swiss slag
component review, copper water-flow proxies and nonphysical inventory-indicator
policy cover this correction. The original inventories and metadata were inspected
read-only; only identities, fingerprints, reviewed boundaries and derived target
coefficients are packaged. No original database comments or raw source vectors
are redistributed.

`scripts/review_uvek_copper_ash.py` validates the complete source fingerprints,
negative one-kg references, original component and collection selections, and the
independent reverse-resource fingerprint. The helpers retain both copper releases,
slag operation, the original facility fraction, and separate road/rail collection.
They replace the unrelated ash-horse-dung landfarming and operation-only proxies.
Only the two exact nonphysical waste indicators may be omitted when unavailable,
with warnings. Physical releases are never omitted. Existing component and water
proxy limitations remain explicit; no incineration-FAE, recovered-energy allocation,
new system model or reverse correspondence is approved by this review.
