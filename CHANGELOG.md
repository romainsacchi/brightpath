# Changelog


- Extend existing allocation-stock and recovery-handoff approvals to 61 independently inspected ecoinvent 3.9.1/3.10.1 cut-off profiles, retaining warnings and original-exchange audit records.

All notable changes to this project will be documented in this file.

## Unreleased

- Correct the Premise electricity pilot's inspected quicklime, nylon 66,
  metallurgical-silicon and water-based alkyd-paint supplier identities.
  Preserve signed kg coordinates for the first three; convert dry paint to the
  native 60% water-based mixture with factor 1/0.60, including uncertainty and
  formulas. Exact cut-off profile guards and screening limitations remain
  explicit; the overall heuristic route and independent reverse rules retain
  their existing policy gates.

- Resolve five older RoW PET-waste market profiles through complete freight-mode
  screening proxies, retaining their treatment shares and transport quantities.
  Reconstruct the positive ecoinvent 3.9 polyester-waste transfer from its eight
  PET treatment demands. Preserve physical emissions and native-biosphere refusals.

- Preserve the Swiss waste-concrete inert-landfill route and original collection
  across nine exact cut-off versions, replacing mismatched whole-waste proxies
  with reviewed operating/facility helpers and explicit screening limitations.

- Preserve the exact native UVEK 2026 long-term groundwater label in SimaPro
  exports, guarded by the native catalog identity and unit. Preserve the exact
  verified native name instead of applying legacy ecoinvent aliases. Copper-ash
  helpers now round-trip through SimaPro without dropping or moving physical releases.

- Reconstruct Swiss copper bottom-ash disposal and collection for ecoinvent 3.11/3.12 cut-off,
  retaining copper-specific releases, slag infrastructure and signed quantities. Replace the
  unrelated ash-landfarming and operation-only proxies with reviewed helpers and explicit losses.

- Retain Swiss CEM II/A and CEM II/B cement production and original road/rail
  delivery in twelve exact cut-off market profiles. Keep the positive cement
  products separate from the same-named allocated ash-treatment functions.
- Reproduce the applied digester-stock correction during normal authoring by
  validating seven retired parent helpers against their retained audit bodies
  and keeping them out of executable definitions.

- Add four exact native UVEK 2026 air-emission chemical synonyms with matching
  CAS numbers, signed kilograms and urban-air context. Preserve caller fields
  and the independently reviewed reverse correspondences.

- Preserve the allocated digester-sludge material stock in nine exact cut-off
  profiles and correct thirteen stock-market profiles to retain their freight
  without importing municipal-incineration burdens or credits. Keep actual sludge
  treatment separate and report the production-only allocation boundary.

- Retain complete polypropylene open-burning and wet-open-dump emissions in
  eighteen exact cut-off profiles instead of substituting municipal incineration;
  preserve all 1,719 exchanges and their uncertainty, with explicit native gaps.

- Preserve the production-only metalliferous hydroxide sludge cut-off stock in
  nine inspected versions instead of importing fresh lithium hydroxide burdens
  or credits; propagate the correction through all nine inspected stock-market
  profiles while retaining their freight, signed caller quantities and warnings.

- Replace the incoming-scrap proxy for precious-metals-in-anode-slime markets
  with complete refinery-product helpers in eleven inspected cut-off profiles,
  retaining supplier shares, signed product credits and explicit proxy warnings.

- Retain complete older sweet-gas and sour-gas flaring inventories and exact
  market handoffs across nine cut-off versions, preserving physical releases
  and uncertainty while keeping the eight modern native-proxy profiles intact.
  Unsupported native biosphere links remain explicit; no physical flows are omitted.

- Correct the CEM III/A 25 MPa concrete proxy from NPK A to NPK B for six
  inspected cut-off profiles, retaining volume and explicit recipe/geography
  warnings without inferring reverse or market mappings.

- Reconstruct all nine cut-off RoW textile unsanitary-landfill profiles from
  their physical releases and complete operating services. Resolve the five
  older global textile markets, inspect the exact 3.10.1 RoW market, and retain
  all regional landfill shares and freight quantities without controlled-landfill
  substitution. Native physical-coverage gaps remain explicit errors or warnings.

- Reconstruct cut-off cable treatment from its electricity, manual facility and
  wire-plastic treatment inputs instead of an empty recovered-copper stock.
  Preserve wire-market freight and use an explicitly approved warning-bearing
  proxy for Swiss fly-ash extraction. Restore complete older Swiss rail services
  and propagate only source-witnessed corrections through affected parent helpers.

- Extend previously reviewed allocation boundaries to fifteen exact patch
  profiles in the original ecoinvent 3.9.1/3.10.1 cut-off databases. Four stock
  profiles reuse the existing production-only suppliers; eleven recovery
  handoffs receive scoped omissions with warnings and full original-exchange
  audit records. Preserve earlier profile decisions and all helper bodies.

- Correct four older regional all-size road markets across 26 exact cut-off
  profiles to complete heavy-lorry freight services. Propagate 674 signed
  freight quantities through 553 parent profiles while preserving 500 already
  correct profiles and independently scoped custom freight choices. Retain
  explicit geography, fleet-class, EURO, loading and vintage limitations.

- Correct eight average wet-sludge treatment and market identities across 48
  exact cut-off profiles, replacing incineration with complete landfarming
  helpers. Preserve physical releases, legacy soil carbon, original mechanical
  spreading volumes and 42 delivery-freight quantities. Retain strict refusal
  for unavailable native physical flows and independent reverse boundaries.

- Package the exact ecoinvent 3.10.1 consequential reference catalog with native
  activity and flow UUIDs. Preserve exact patch identity and shared biosphere
  definitions, invalidate catalog-dependent cache fingerprints, and retain clear
  refusal of cut-off/consequential conversions.

- Correct four regional unspecified diesel-road markets to complete heavy-lorry
  fleet services and replace a Swiss rail-to-road mismatch with the original
  electric/diesel rail mix. Propagate 605 signed freight quantities through 424
  exact cut-off parent profiles without changing other inputs or custom freight
  choices. Retain explicit regional, vintage and energy-intensity limitations.

- Preserve eighteen exact aluminium/iron cut-off stock profiles through
  allocation-only suppliers, without adding unrelated native preparation burdens
  or changing preparation coefficients and reverse correspondences.

- Preserve chemical-process heat in MJ and retain its source make-up-water supply
  and physical water releases in scoped helpers for nine cut-off versions;
  report energy-mix and water-supply proxy limitations and keep reverse heat
  boundary decisions explicit.

- Preserve complete freight services and vehicle classes through reviewed parent
  inventories; retain packed stone-wool producer shares and delivery freight
  using rock-wool manufacture, with independent light-duty and packed-wool
  reverse proxies and explicit regional, fleet, vintage and material caveats.

- Correct bulk cathode-copper supply to the complete UVEK copper-storage process
  instead of an NMC battery electrode; preserve signed mass and caller metadata
  and disclose the regional supply, vintage and recycling-allocation proxy limits.

- Preserve the production-only copper Recycled Content cut-off stock across
  nine exact profiles. Remove invented PV-recycling burdens and credits from
  its 42 uses in five ship-disposal services and a copper-scrap market, then
  propagate the correction through three vessel markets. Preserve all signed
  copper quantities, other recovery credits and separate freight.

- Replace two European PET market-group fleece-production heuristics across
  eighteen exact cut-off profiles with complete nested reviewed child graphs.
  Retain all 333 original supply weights without normalization or added
  transport, treatment, recovery or allocation. Resolve two original identities;
  221 remain pending. Preserve independently checked reverse limitations.

- Repair 22 landfill-leachate wet-sludge landfarming matches across 82 exact
  cut-off profiles, replacing dry-waste heuristics with complete wet-sludge
  helpers. Retain 2,368 biosphere entries and original whole spreading-service
  volumes, with independent reverse checks and explicit native physical gaps.
  These are prerequisites; the original pending queue remains 223 identities.

- Apply the explicitly approved PET incinerator screening proxy to nine Swiss
  FLUWA market profiles, with ash/recovery/emission/allocation warnings and no
  added credit. Retain complete open-dump/open-burning helpers and original
  road and diesel/electric rail quantities. Resolve one original identity.

- Reconstruct sixteen exact profiles of five further waste markets using their
  reviewed complete disposal inventories and original road tonne-km. Resolve
  five original identities, including independently inspected 3.10.1 cases,
  with no omission, new biosphere proxy or inferred reverse correspondence.

- Reconstruct 373 exact profiles of 42 regional PET-waste markets, retaining
  original treatment fractions, complete uncontrolled-disposal inventories and
  road freight. Scope two inspected landfill patch links
  to these helpers; retain five steam-dependent RoW profiles as unresolved; Swiss
  fly-ash-extraction profiles have a separate explicit approval. Resolve 41 original identities
  and partially approve one without changing independent reverse mappings.

- Reconstruct 36 uncontrolled-disposal identities across 217 exact cut-off
  profiles, retaining all 21,844 biosphere entries and 270 complete operating
  service inputs in 124 helpers. Preserve eleven positive original 3.9 references
  and 206 negative references, verify both reverse operating suppliers, and keep
  native physical gaps explicit with default rollback or caller-selected
  permissive warnings for manual mapping. Add no controlled-disposal proxy or
  physical omission. The pending queue falls from 306 to 270 identities.

- Replace guessed canonical biosphere correspondences on the ecoinvent-to-UVEK
  bridge with source-release publisher identities, matching units and complete
  receiving contexts, plus two reviewed same-CAS groundwater synonyms. Resolve
  reused manganese labels per release, preserve caller quantities and emission
  metadata, and roll back unsupported physical flows even under permissive policy.

- Reconstruct 76 exact profiles of fourteen further cut-off waste markets,
  retaining waste-specific treatment fractions, complete freight and 72 warned
  recovery-handoff omissions, including 18 inspected patch cases. Keep 24 older
  steam-dependent profiles pending. Resolve eight identities and partially
  approve six; preserve existing helpers and independent reverse mappings.

- Reconstruct fourteen industrial-waste markets across 84 exact cut-off profiles
  using reviewed waste-specific treatment children and complete road freight.
  Preserve original treatment and freight coefficients without new omissions or
  normalization. Disclose fleet and geographic differences, independently check
  the reverse freight supplier, and retain inherited native-biosphere gaps.

- Reconstruct seven industrial wet-sludge landfarming identities across 42 exact
  cut-off profiles. Retain fossil/biogenic carbon, local water fate, metals,
  nitrogen/phosphorus, heat and the original complete spreading-service volume.
  Preserve reported soil carbon only in older profiles that list it. Disclose
  whole-service regional/technology differences and independent reverse checks;
  keep physical native-biosphere gaps explicit without new emission proxies.

- Reconstruct 32 steel-waste landfill profiles and 36 market profiles, preserving
  waste-specific releases, cement stabilization, unrenormalized landfill shares
  and complete freight services. Reuse 18 explicitly warned recovery-handoff
  omissions, including four scoped patch cases; retain 20 clinker/steam profiles
  as pending. Update three dependent market compositions and independently check
  reverse suppliers. Keep native land-class gaps separate from technosphere
  approval, without extending the earlier scoped biosphere refinement.

- Resolve packaging-paper sludge landfarming across four exact cut-off profiles.
  Preserve all 39 releases, the wet-sludge basis and the original tanker-service
  volume with the corrected waste-reference factor. Keep native soil chemistry
  and groundwater biosphere gaps explicit and separate from canonical exports.

- Add thirteen missing ecoinvent-to-native particulate correspondences with exact
  size bins, receiving-air contexts and signed kilograms preserved. Keep the
  independently authored native-to-ecoinvent rules unchanged, and preserve the
  historical native-resource review guards.

- Replace incorrect sewer-throughput matches to vented gas, uranium tailings and
  concrete volume with complete rural/urban sewer helpers. Preserve all reported
  construction, operation, disposal and water releases per cubic metre in 24
  exact cut-off profiles, including original 3.9.1 and 3.10.1 waste submixes.
  Retain explicit supplier-proxy warnings and independent reverse checks.

- Reconstruct 46 exact profiles of eleven waste markets using reviewed landfill
  children and complete road/rail/barge services at original quantities and
  traction shares. Resolve nine identities and partially resolve two; retain
  eight older clinker-dependent profiles as blocked, without omissions or
  renormalization. Preserve optional-indicator review metadata on helpers that
  have no contextual land binding so later native migration can warn correctly.

- Resolve 25 residual-material-landfill identities across 142 inspected cut-off
  profiles. Preserve waste-specific releases, stabilization and construction,
  including two active positive-reference 3.9 slag treatments. Apply the approved
  native residual-landfill land class only inside 36 exact helper profiles, at
  unchanged signed area and with warnings. Retain packaged recipe verification
  for contextual biosphere bindings and approved indicators during later helper
  migration; reject stale helpers and preserve transactional strict-policy failure.
  Add seven independently inspected reverse construction/material suppliers with
  signed kg or volume preserved and individual boundary warnings.

- Correct the CH and RoW sewage-sludge markets across nine inspected cut-off
  versions. Reuse the matching full digestion helper with the correct negative
  reference factor, preserving operating burdens and emissions without adding
  duplicate helpers or equating sludge volume to gas volume. Add eight
  independently inspected reverse plant and regional electricity suppliers,
  preserving unit demands and warning about background-system differences.

- Reconstruct 42 bottom-ash markets across 176 inspected cut-off profiles,
  preserving child treatment, road quantities and the source diesel/electric
  rail shares. Scope complete freight-service proxies to these helpers, with
  explicit fleet and geography warnings. Add an independently inspected reverse
  correspondence for complete Swiss electric rail freight with shunting.

- Reconstruct 49 bottom-ash landfill services across 200 inspected cut-off
  profiles, including 3.9.1/3.10.1. Preserve waste-specific releases and source
  facility/operating demand, correct the negative treatment reference, and warn
  when optional inventory indicators cannot be represented. Extend nine
  supporting slag-supplier reviews to the independently inspected patches.

- Add 56 explicitly approved water-emission screening proxies to native UVEK
  2026. Preserve signed kg and named-anion mass, warn about generic element and
  unspecified-water approximations, and retain source CAS/UUID details in losses.
  Keep the independently authored reverse resource unchanged.

- Correct nine slag-landfill construction and operation suppliers across 63
  cut-off profiles to the dedicated UVEK slag compartment. Preserve facility
  units and signed operating kg, disclose water, heat and freight differences,
  and retain independently checked reverse correspondences.

- Preserve the separate cut-off sludge-treatment and allocated biogas boundaries
  for sewage digestion across nine inspected versions, including 3.9.1/3.10.1.
  Retain operating inputs and version-specific emissions without equating sludge
  and gas volumes. Add one independently inspected reverse gas-operating helper;
  optional unsupported waste indicators warn without dropping emissions.

- Correct the hot-rolled stainless global market and RoW producer to use full
  UVEK material supply instead of a cold-rolling service. Preserve signed kg
  without adding hot rolling twice, bind 23 inspected inventories, and disclose
  supply-mix differences. Keep the independently verified reverse rule unchanged.

- Add the independently reviewed unspecified water discharge from ecoinvent to
  native UVEK 2026, using 1,000 kg/m3. Preserve helper emissions, signed quantities,
  uncertainty and formulas; retain the existing independently reviewed reverse rule.

- Correct fifteen sewer-infrastructure mappings across 105 cut-off profiles to
  match UVEK's documented capacity class and network design. Preserve kilometre
  quantities and signs without throughput or total-length scaling, and report
  Swiss supply and technology approximations. Existing reverse rules remain unchanged.

- Remove consequential-to-UVEK mappings, profile overrides and unused helpers.
  Require compatible system models before selecting any direct or composed
  technosphere route, in either direction and across database families.
  Preserve the existing cut-off mappings and historical scientific audit evidence.

- Correct 28 physical cut-off ground-granulated-slag product profiles to retain
  processing and raw-supply freight without adding economic pig-iron allocation.
  Bind 47 product, raw-market, stock and UVEK-variant inventories and metadata.
- Add one independently inspected reverse no-burdens slag-product helper,
  preserving kg and subtracting the exact upstream raw-slag supply. Keep the
  with-burdens inverse and negative-reference treatment/credit cases unsupported.

- Replace agricultural slurry-spreading proxies for four ecoinvent 3.6
  sawing-slurry treatment profiles with complete per-litre helpers, preserving
  waste-treatment demands and consequential carbide/glycol recovery credits.
  Compose both global market profiles without rounding their regional shares.
  Keep 36 newer counterparts explicitly pending; reverse mappings are unchanged.

- Resolve the remaining beet-pulp and whey-yeast cut-off markets, preserving
  supplier shares, freight and all earlier nutritional omissions. Verify seven
  suppliers across 35 profiles, including the sugar-production name transition.
  Bind formal product information in addition to inventories and comments.
- Add two independently inspected Swiss product reverse correspondences with
  kg, allocation and moisture warnings. Preserve the earlier six-rule batch
  exactly; reverse coverage reaches 1,498 suppliers.

- Resolve three physical cut-off feed-product markets across 21 profiles through
  nine independently inspected coproduct suppliers. Replace five DDGS-to-ethanol
  mappings with feedstock-specific DDGS and distinguish grass-fibre fermentation
  from digestion. Preserve kg, supplier shares, freight and all 21 existing
  consequential nutritional omissions. Bind 111 inventories and comments;
  keep unresolved moisture/nutritional equivalence and reverse routes explicit.
- Add six independently reviewed reverse physical coproduct correspondences to
  ecoinvent 3.12 cut-off, preserving signed kg and warning about allocation,
  moisture and potato-DDGS geography. Reverse coverage reaches 1,496 suppliers;
  original market mixes and omitted nutrient credits are not reconstructed.
- Restore conditional omission evidence when regenerating a rule that combines
  physical-product helper profiles with approved omission profiles.

- Preserve the feedstock-specific esterquat supply mix and original freight in
  28 exact market profiles. Consequential credits now use coconut/palm-kernel
  esterquat; cut-off supply retains its 88.4%/11.6% coconut/tallow mix. Bind 44
  independently inspected provider inventories and keep tallow-treatment
  reconstruction and unsupported reverse routes pending.

- Replace gas-turbine suggestions for sweet-natural-gas flaring with its dedicated
  UVEK service across 16 modern exact profiles. Preserve the independently
  established 36 MJ/m3 reference basis with factor -1/36 and an independently
  reviewed reverse factor -36. Warn about emission-coverage differences and
  retain the flared gas resource withdrawal; older sweet-gas and all sour-gas
  profiles remain pending. Reverse coverage now reaches 1,490 suppliers.

- Make heuristic candidate shortlists and token-score sums independent of Python
  hash iteration order. Preserve nine historical authoring inputs before existing
  reviews, reproducing the forward resource without changing runtime decisions.

- Preserve 34 independently inspected positive cut-off stock allocation boundaries
  across 168 exact profiles, with signed material flows and allocation warnings.
  Bind nested stock supplies to their exact reviewed source profiles and refresh
  52 existing parent identities without changing source quantities or credits.
- Add 630 exact-profile assemblies across 118 partially approved identities;
  retain all 672 unresolved counterpart profiles in the blocked review queue.

- Use the dedicated Formox carrier landfill for the RER treatment and its market
  across six exact source profiles each, replacing steam-production proxies.
  Preserve wet kg, the market's unrounded treatment share and 0.077 tkm freight;
  disclose moisture, geography and emission-coverage differences. Keep earlier
  supplier bindings and helpers unchanged; the reverse endpoint remains unsupported.

- Replace seven regional landfill and legacy vehicle-manufacture proxies with
  dedicated matching-material waste services across 32 inspected source profiles.
  Preserve signed wet kg and disclose moisture, geography, recovery-credit and
  release-model differences. Compose five newly complete markets across 16
  profiles without changing treatment shares or freight; reverse routes remain
  independently unsupported.

- Use dedicated LCD dismantling and shredder-residue services across 50 inspected
  profiles, correcting two related manual-dismantling suppliers. Disclose the
  newer mechanical process's broader manual-treatment boundary. Preserve the
  LCD market's 23%/77% route shares and four freight quantities, resolving four
  original queue identities.
- Correct three lorry substitutions for barge/rail freight with independently
  inspected same-mode RER services. Re-review 71 dependent composed inventories
  across 256 source profiles, preserving raw quantities and recovery credits,
  recording new bindings and removing superseded unused helper definitions.
- Independently add the reverse refinery-gas flare service with factor -1 and
  emission-coverage warnings. Preserve signed MJ quantities, uncertainty and
  formulas through both directions; reverse coverage reaches 1,489 suppliers.
- Replace three legacy EAF-waste production proxies with dedicated Swiss landfill
  services across six exact ecoinvent 3.6 profiles. Preserve wet mass with factor
  -1 and disclose moisture, cement, freight and metal-fate differences; later
  versions and reverse routes require independent review.
- Resolve the direct refinery-gas flaring service and its market across 28 exact
  source profiles using the matching UVEK per-MJ flare, factor -1 and explicit
  emission-inventory warnings. Preserve caller quantities, uncertainty, formulas
  and separate emissions; reject uninspected profiles and do not infer an inverse.
- Independently reconstruct two UVEK gas-combustion services from allocated
  German ecoinvent electricity and heat, with explicit fuel boundaries and
  proxy warnings. Add matching coke-gas supply and industrial HFO-heat proxies,
  adding 37 independently
  inspected matching-region electricity proxies, while preserving the frozen
  publisher baseline. Forward waste-gas helpers can now convert back without
  applying the coke-gas fuel correction twice.
- Add 23 independently reviewed same-region ash-treatment reverse proxies,
  preserving quantity and scaling uncertainty/formulas by the waste-reference
  factor -1. Disclose unverified moisture equivalence and fate-model differences;
  reverse coverage now reaches 1,488 suppliers.
- Represent two gas markets with separate cut-off supply and consequential
  treatment boundaries. Compose 255 additional inspected markets from existing
  resolved component mappings, retaining supply and freight coefficients with
  inherited proxy warnings and exact-profile inventory/binding guards.
- Compose 100 complete processing inventories across 795 inspected profiles from
  existing resolved component mappings. Preserve processing inputs and recovery
  credits separately, caller uncertainty/formulas, foreground suppliers and
  separate emissions; warn about inherited component approximations.

- Apply the existing unsupported-industrial-residue omission approval to 226
  independently inspected nonbiogenic industrial WWT residue mappings
  across 1,704 source/profile pairs. All direct treatment inventories document
  zero biogenic carbon. Omit unmatched sludge, ash and residue treatment with
  warnings and full original-exchange loss records, including associated credits
  and market freight. Replace rollback fixtures that depended on real pending
  mappings with a synthetic unreviewed correspondence.
- Preserve consequential energy recovery for 103 industrial-gas power-plant
  services across 721 inspected source/profile pairs. Generate reviewed
  blast-furnace/coke-oven-gas combustion helpers with the original electricity
  and heat credit quantities through existing UVEK energy mappings. Cancel the
  coke-oven burner's deterministic 1 MJ fuel-production input to isolate waste
  combustion. Disclose regional, emissions, infrastructure and marginal-energy
  approximations; preserve caller uncertainty, formulas and legitimate negative
  net scores. Gas markets and cut-off product references remain separate.
- Omit five unsupported antifouling-paint emission exchanges, covering 70
  inspected source/profile pairs, with warnings and complete original-exchange
  loss records. Remove the uranium-milling market proxy and four tyre-wear
  assignments whose sign checks did not establish scientific equivalence.
  Disclose excluded marine metal/organotin releases and possible ecotoxicity
  understatement; preserve paint manufacturing and foreground emissions.
  Bind audit replay to inventory fingerprints; keep reverse resources independent.
- Approve four produced-water discharge mappings across 56 exact ecoinvent
  source/profile pairs. Map onshore and offshore treatment services and their
  single-supplier markets directly to UVEK with factor -1. Warn about pollutant
  concentrations, chromium speciation and omitted older air/water-balance flows.
  Bind approvals to source and target inventory fingerprints; preserve mass,
  signed avoided-discharge inputs, uncertainty, formulas and foreground suppliers.
  Missing exact profiles remain blocked; no new helpers or reverse rules are added.
- Preserve R134a reclamation allocation across three identities and 21 inspected
  profiles. Generate separate service helpers with electricity, rejected-material
  incineration and direct leakage, retaining the recovered-product credit only
  under consequential accounting. Preserve seven inspected empty cut-off product
  suppliers as explicitly reviewed production-only helpers with material quantities
  and warnings. Guard empty recipes against missing review evidence, unapproved
  profiles, altered inventories and credited-market changes; retain original
  exchanges, uncertainty, formulas and transactional rollback.
- Replace five refrigerant final-disposal proxies across 42 exact source/profile
  pairs with reviewed services containing 0.999 kg Swiss average hazardous-waste
  incineration and 0.001 kg direct refrigerant air release per kilogram accepted.
  Preserve disposal/avoided-disposal signs, uncertainty and original provenance.
  Disclose the incineration proxy through policy-controlled warnings and loss
  records; retain the existing native-biosphere and 3.12 indicator handling.
  Keep reclamation, treatment markets, venting and reverse resources independent.
- Replace five refrigerant-manufacturing proxies for venting with approved,
  mass-conserving foreground service recipes across 42 exact source/profile
  pairs. Preserve chemical identity, air releases, avoided-venting signs,
  uncertainty and original exchange provenance. Retain 3.12 waste indicators
  where supported; warn about their exclusion and the native UVEK 2026
  isobutane compartment approximation. Bind recipes to inspected inventories,
  reject missing profiles/targets and collisions, and preserve transactional
  rollback. Keep reverse routes and other refrigerant treatment choices independent.
- Apply the same approved omission rule to 95 exact normalized nutrient references
  across 692 inspected positive-reference source/profile pairs. Replace unsupported
  feed-energy-to-industrial-heat and crude-protein-to-crop/oil substitutions with
  warnings about excluded nutritional supply contributions and credits. Keep full
  original exchanges, exact profile boundaries and foreground suppliers intact.
- Omit five approved nutritional feed-credit links and five independently inspected
  consequential markets across 70 source/profile pairs in ecoinvent 3.6--3.12.
  Warn about excluded energy/protein substitution and market freight where present;
  retain complete source exchanges and foreground inputs/emissions. Keep the
  markets' 35 cut-off product-supply decisions pending. Distinguish partial
  approvals in the remaining review queue: 1,281 identities in nine families.
- Auto-approve the documented dichloropropene-to-generic-pesticide credit only
  in consequential ecoinvent 3.6--3.9. Replace the unrelated diode producer with
  the RER UVEK generic pesticide provider and factor +1. Preserve credit signs,
  uncertainty, formulas and separate emissions; disclose mixture, geography,
  toxicity-inventory and market-boundary differences, and block unreviewed profiles.
- Preserve three independently reviewed consequential metal-recovery credits
  across 21 source/profile pairs: sludge-to-zinc-concentrate (factor
  0.0622641509433962), zinc scrap (1) and bronze scrap (1). Replace unrelated
  product proxies, explicitly validate the change from waste reference to product
  credit, and preserve incoming signs, uncertainty and formulas. Disclose grade,
  geography, allocation, market-transport and source processing limitations.
  Bind reinspection to exact credit quantities and suppliers. Add a hash-bound
  screening queue for the remaining 1,287 identities in ten review families;
  family counts do not predict additional approval requests.
- Omit sixteen exact steelmaking waste-to-recyclable handoffs only in inspected
  ecoinvent 3.7--3.12 cut-off/consequential profiles, covering 192 pairs. Replace
  inappropriate allocated-slag production proxies with warnings about excluded
  consequential recovery credits; distinguish the empty cut-off boundary.
  Preserve full source snapshots and foreground links, retain prior mapping
  evidence, and keep unreviewed profiles and unrelated treatments blocked.
- Omit coal-tar furnace treatment and its market only in consequential ecoinvent
  3.6--3.9, warning about excluded combustion burdens, heat credits and freight.
  Add exact-profile omission decisions with validated shared evidence and complete
  loss reports. Preserve the market's ten approved production mappings, foreground
  suppliers, production and separate emissions; missing profiles remain blocked.
- Omit 18 exact unsupported CRT-production wastewater sludge/ash/residue
  treatment and market inputs after inspecting 144 source/profile pairs.
  Continue conversion with warnings about excluded treatment burdens, credits
  and market freight; score effects remain unquantified. Preserve complete
  source exchanges in loss reports, foreground suppliers and separate emissions.
  Retain former proxies and sign evidence for replay; reverse rules stay independent.
- Approve ten exact CRT-cullet/frit landfill treatment and market proxies after
  inspecting 120 source/profile inventories. Replace seven manufacturing
  substitutions and align three existing frit services. Preserve mass with
  factor -1; disclose generic-glass metal releases, excluded source leachate,
  landfill-model differences and omitted market freight. Keep production sludge
  and ash outside this approval and retain all original mapping evidence.
- Approve the broader mechanical CRT treatment boundary and auto-review the
  whole-screen and residue markets. Preserve the screen market's 23%/77% route
  shares and mass with factor -1/19.9 for the exact UVEK unit-based mix; disclose
  added/manual-stage double counting, omitted freight and model differences.
  Retain original mappings and reject invalid reference-mass conversions.
- Approve sixteen exact CRT-coating/residue incineration, coating-market,
  glass-processing and manual-dismantling proxies after inspecting 148 source/profile inventories. Open ten blocked
  manufacturing substitutions, correct a zinc-only residue supplier, and align
  warnings on five existing matching services. Preserve mass with factor -1;
  disclose metal fate, ash/leaching controls, legacy LCD wording and omitted
  market freight. Keep glass-cullet landfill, mechanical CRT treatment and mixed
  whole-screen markets outside this scope.
- Replace the PET-recycling-dust and post-industrial LDPE milk-carton-fraction
  manufacturing proxies with CH municipal-incineration services and factor -1.
  Preserve wet waste mass without moisture/heating-value scaling; disclose
  composition, geography and missing consequential energy credits. Keep
  unreviewed CRT, sludge, composite-carton and ash references outside this scope.
- Approve three Swiss paper-disposal proxies with exact source-profile factors:
  +1 for ecoinvent 3.9 cut-off and -1 for inspected consequential releases.
  Disclose treatment-model differences and missing newer landfill energy credits;
  preserve blockers for uninspected profiles and other pending supplier reviews.
- Omit the CH/RoW industrial-biowaste-composting inputs during ecoinvent-to-UVEK
  conversion, warning about excluded treatment burdens. Preserve original
  exchanges in loss reports; avoid collected-material proxies and the empty UVEK
  composting cut-off placeholder. Keep other biowaste functions blocked.
- Omit the UVEK biowaste-at-collection-point input during reverse conversion,
  warning that its collection transport and CO2 uptake are also omitted. Share
  audited omission handling across both directions, preserve original exchanges
  in reports, and keep the independent forward biowaste mapping unchanged.
- Omit six fully untreated textile-disposal exchanges during ecoinvent-to-UVEK
  conversion with an unconditional reviewed-omission warning. Keep the full source
  exchange in the loss report, preserve caller data and foreground links, and
  retain ordinary deletion policies and rollback on unrelated errors.
- Approve ten exact PET-pelletising/PET-recycling/PP-recycling and modern textile
  wastewater proxies using generic CH class-1 sewage treatment and factor -1. Preserve volume without
  COD scaling; disclose composition, sewer boundaries and market discharge loss.
  Retain earlier factor history when approved supplier corrections are replayed.
- Approve five matching-effluent wastewater-market proxies for the full source
  volume, with factor -1. Explicitly disclose replacement of partially untreated
  markets by 100% treatment, omitted untreated-discharge emissions and possible
  impact bias; preserve separately represented foreground pollutants.
- Approve 11 inspected hard-fibreboard, medium-density-board and particle-board
  wastewater treatment proxies and two older 100%-treated markets with factor -1.
  Keep newer partially untreated markets blocked. Recognize their native UVEK
  product references as treatment-service outputs only within the exact approved
  scope; disclose version-specific sewer boundaries and plant-model differences.
- Replace the RoW/GLO PET-incineration fleece proxies with the inspected CH
  disposal service and factor -1. Disclose geographic and inventory limitations;
  keep PET dust, landfill and recovered-energy references outside the approval.
- Approve the Li-ion 50/50 treatment-market proxy and auto-review matching GLO/kg
  NiMH pyrometallurgical treatment and market proxies. Disclose omitted market
  freight and retain all unreviewed material, technology and geography differences.
- Approve the separate pyrometallurgical Li-ion waste-treatment correction,
  replacing the anode-manufacturing proxy with GLO, kilogram disposal and factor
  -1. Keep other reference products outside the approval.
- Replace the graphite-anode manufacturing proxy for the used-Li-ion-battery
  hydrometallurgical waste reference with the matching UVEK treatment supplier
  and factor -1. Preserve recovered cobalt/lithium mappings and disclose recovery
  and allocation limitations.
- Approve the coal-tar production proxy for ten inspected positive-reference
  source profiles, with factor +1 and explicit allocation/market limitations.
  Keep consequential 3.6--3.9 and unreviewed profiles blocked.
- Record exact source-version and system-model sign decisions for 195
  model-dependent mappings. Keep supplier substitutions pending independent
  scientific approval and reject missing profile decisions without fallback.
- Correct 4,352 ecoinvent-to-UVEK waste-treatment reference signs and retain
  44 independently verified reverse-direction sign factors. Apply explicit
  technosphere factors even when units do not change, including uncertainty,
  bounds and formulas. Block 1,369 forward and one reverse mapping with
  conflicting reference signs or waste/product roles instead of guessing.

## 1.0.0a14 - 2026-11-01

- Add optional validation of exported SimaPro substance names, compartments and
  units against an explicitly supplied software-version catalogue. Omitting the
  target SimaPro version skips this additional check. No substance list is bundled.
- Preserve native SimaPro biosphere identities, foreground product references,
  active zero exchanges, arithmetic expressions and supported uncertainty
  distributions. Reject ambiguous sodium identities rather than substituting a
  different species. Distinguish normal and standard cubic-metre units.
- Preserve native openLCA provider and elementary-flow UUIDs, declared quantity
  references, and product-versus-waste reference directions. Add UVEK 2026
  reference metadata and resolve native unit choices without guessing.
- Correct ILCD resource directions, signed lognormal handling, person-transport
  units and uncertainty serialization. Report format limitations explicitly.
- Preserve uncertainty and formula semantics through supplier disaggregation and
  reverse biosphere migrations. Keep unsupported or ambiguous routes blocked.
- Preserve exact biosphere UUIDs instead of legacy reverse aliases. Older targets
  now refuse 2,4-D dimethylamine-salt flows and soil Quizalofop-ethyl when the
  preserved identities are unavailable. Quantities and uncertainty stay unchanged;
  the reviewed scope and target versions are recorded in the migration-audit review.

## 1.0.0a12 - 2026-10-29

- Replace openLCA ZIP exports atomically. Reusing an output path no longer
  appends duplicate processes or retains removed entities, and failed writes
  preserve the previous archive. Complete native UVEK inventory checks exposed
  this issue when repeated against an installed wheel.

## 1.0.0a11 - 2026-10-29

- Preserve product-exchange classification categories in openLCA round trips
  and retain case-sensitive foreground identities and local links when using
  ecoinvent SimaPro naming. Restore SimaPro's sulfur spelling against exact
  target compartments. Complete source-inventory checks exposed these issues.
- Keep annual-crop and unspecified-arable land transformations distinct in
  SimaPro exports, independently of the technosphere family. Preserve exact
  biosphere and format version strings in Brightway Excel, CSV and TSV.
- Preserve the exact review JSON bytes on Windows so publisher-evidence
  checksums and resource regeneration agree across platforms.

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

- Reconstruct nine exact cut-off Ghanaian uncontrolled refrigerator-dismantling profiles, retaining all five refrigerant/blowing-agent emissions, seven material-treatment/recovery inputs, marginal uncertainty and constant arithmetic formulas. Existing disposal proxies retain explicit limitations; no controlled-treatment proxy or inverse is inferred.

- Preserve authored local parameters, formulas and uncertainty in generated helpers in both ecoinvent/UVEK directions; validate parameter definitions and keep dependency scopes independent.

- Extend 16 reviewed refrigerant services/markets to their inspected cut-off patch profiles and reconstruct nine controlled Ghanaian dismantling plus nine global used-appliance markets, preserving source parameters and signed regional formulas.
