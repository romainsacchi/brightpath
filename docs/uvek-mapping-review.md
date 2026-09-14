# CLIC UVEK mapping review

This review compares BrightPath's original resource at `5e2a6d1` with CLIC
`ecf7a035492d9e2042d7180091b3299aae16bf6e`, branch `uvek-mapping-3.10`.
The source is Karin Treyer's
`data/migrations/uvek/mapping_ecoinvent-3.10-cutoff_to_bafu-2025.xlsx` workbook
in https://github.com/romainsacchi/clic.

## Latest inventory-backed review (version 4)

The reference-amount and allocation priority batch adds **62 improved targets**:
4 seedling mappings, 6 diesel CHP mappings and 52 wood CHP mappings. This brings
the cumulative number of reviewed target changes to **2,576**: 2,517 select a
branch target, and 59 select a different candidate from the full UVEK catalog.
All 31,488 source identities and all exchange amount factors are preserved.

**21,618 entries (68.7%) remain below 0.5 confidence**, down by 10. The 52 improved
wood CHP proxies intentionally remain at 0.45; changing to a less unsuitable
target is not evidence of scientific equivalence. Older review artifacts below
remain historical snapshots, not current totals.

### Seedling reference basis

The imported ecoinvent 3.10 cut-off and UVEK 2025 inventories both have a reference
production amount of 1 in unit `unit`. The source descriptions retain a
1,000-seedling batch description, and the target names contain `1000 units`.
Crucially, their normalized input fingerprints match: electricity 0.0704 kWh,
machine diesel 0.0184 MJ, and heat 0.801 MJ for the heated variant or 0.0027 MJ
for the unheated variant, per imported reference unit. This supports a migration
factor of **1.0**, not 0.001 or 1,000, for these specific imported records.
It does not resolve every ambiguity in legacy batch terminology or justify a
general name-based conversion rule.

All four sources now map to the corresponding tree-seedling greenhouse process,
not strawberry seedlings. The heated RER source also corrects the branch's
unheated candidate. Background energy suppliers differ, and RoW → RER remains
a geographic proxy. End-to-end regression tests verify that migrating an amount
of 250 preserves 250 and selects the correct greenhouse condition.

### CHP allocation and operating burdens

The two branch targets labelled `allocation heat` have one technosphere input
(electricity-specific infrastructure), no direct biosphere exchanges and no fuel
input. That is an allocation convention, not necessarily corrupted data, but it
does not represent the operating burdens present in these source inventories.

- **Diesel (6 sources):** the source documentation explicitly names exergy
  allocation. The selected active `Electricity, at cogen 300kWth, diesel,
  allocation exergy` target also documents exergy allocation and carries fuel
  combustion through its supplier. Its own zero direct-biosphere count must not
  be mistaken for zero combustion burdens. Confidence is 0.55: source 200 kW
  electrical and target 300 kW thermal ratings are not interchangeable, and SCR
  equivalence is not established. The 200 kW energy-allocation alternative loses
  the documented allocation basis; the high-voltage grid wrapper adds transmission
  infrastructure and an electricity input factor of 1.033 to a production process.
- **Wood (52 sources):** the source describes an ORC with particulate control and
  SNCR. The selected full-catalog `xx Electricity, at cogen ORC 1400kWth, wood,
  emission control, allocation exergy` preserves those broad technology features
  and includes fuel and combustion emissions. The active ORC alternative lacks
  the explicit emission-control designation, while the active CHP grid proxy uses
  a different plant and adds transmission. The chosen target is deprecated and
  has a different plant scale; the source comment does not establish its numerical
  allocation split. Older target operating factors also differ from the source's
  2014 update; identical particulate-filter and SNCR performance is not established.
  These remain **low-confidence proxies**, not resolved equivalents.

Selection uses reference production, technology and operating-burden evidence;
it does not minimize LCIA scores or select whichever score looks closest.

### Evidence and reproduction

- `docs/uvek_priority_review.csv`: 62 decisions, with previous, branch and selected
  targets, confidence and limitations.
- `docs/uvek_priority_inventory_evidence.json`: sparse reference-production,
  aggregate-input and supplier-burden evidence, exact activity codes, project and
  database metadata. No complete proprietary inventories or source narratives
  are redistributed.
- `docs/uvek_priority_summary.json`: counts and integrity hashes.
- `scripts/review_uvek_priority_inventory.py`: repeatable read-only inspection of
  existing Brightway databases. It writes decisions and reports, not inventories.

With a copy of the version-3 resource before this batch and its follow-up report:

```
python scripts/review_uvek_priority_inventory.py BEFORE_PRIORITY.json docs/uvek_followup_review.csv OUTPUT_DIRECTORY --source-project ecoinvent-3.10-cutoff --source-database ecoinvent-3.10-cutoff --target-project clic-bafu-2025-ef31 --target-database bafu
```

The checks reject unexpected reference amounts, changed greenhouse conditions,
missing exact identities, changed allocation evidence and missing operating
burdens. Append the resulting 62 decisions to the version-3 overrides, apply with
the normal generator helper, and regenerate the integrity manifest. The packaged
override file already contains all 2,576 decisions and survives normal rebuilding.
New runtime provenance distinguishes the comparison branch revision from the
inventory evidence and the origin of the selected candidate.

Only ecoinvent **3.10 cut-off** and the identified imported UVEK **2025** inventories
were inspected. The existing shared-identity resource and its routes are unchanged;
this evidence is not validation of allocation equivalence for other versions or
system models. The 167 sludge and 166 residue cases from the preceding review
remain pending composition, moisture and treatment-route checks.

## Previous follow-up (review version 3)

The follow-up selects **164 additional branch targets**, bringing the cumulative
total to **2,514**. All 31,488 source identities remain present. **21,628 (68.7%)**
still have confidence below 0.5, down from 21,778 before this follow-up. Fourteen
new choices remain below 0.5: an improved proxy is not necessarily a reliable
equivalent. No confidence is raised merely because the two mappings agree.

The 1,563 previously unresolved low-confidence alternatives were inspected in
697 product/target groups, with these dispositions:

| Disposition | Entries |
|---|---:|
| Select better branch target | 164 |
| Retain existing specificity or unresolved geographic tie | 293 |
| Reject branch product, operating-state or waste-class mismatch | 91 |
| Need additional inventory, composition or reference-amount evidence | 1,015 |

Examples include stone wool → rock wool (rather than stone meal/glass wool),
coal tar → tar at a coke plant (rather than hard coal), CNC turning → the same
metal and turning variant under CNC control, and refrigerated road freight →
lorry freight (rather than a carbon-dioxide pipeline). Refrigeration, geography,
equipment capacity and manufacturing boundaries remain explicit limitations.
Two passenger-transport choices use the equivalent `person kilometer` and
`person-kilometer` spellings with an explicit conversion factor of 1.0; amounts
are unchanged. Seedling targets labelled `1000 units` remain blocked even though
their catalog unit also says `unit`.

The 5,828 low-confidence agreements received a provenance audit and identity-risk
screen, not individual inventory validation. Among the 3,278 with branch method
`review`, 394 inherit a choice from another source location and 422 originate
from comments identifying scripted rules, reuse or fixes; these categories
overlap. Only 2,481 are exact-source recorded decisions without those script
markers, which still does not prove independent expert validation. In 88 inherited
cases, the workbook confidence exceeds the originating decision's confidence.
The generator also treats 75 unrecognized fit labels as its default confidence.
**All 5,828 current scores and targets remain unchanged.**

The follow-up audit is in:

- `docs/uvek_followup_review.xlsx`: alternatives and same-target provenance,
  filterable and sorted by prior confidence.
- `docs/uvek_followup_review.csv`: all 1,563 alternatives with decisions and reasons.
- `docs/uvek_same_target_provenance.csv`: all 5,828 agreements and lineage/risk flags.
- `docs/uvek_followup_summary.json`: counts and input hashes.
- `docs/uvek_followup_decisions.json`: scoped, editable adjudication ledger, with
  source/target-scope hashes preventing accidental expansion to different entries.

Retaining a mapping, or rejecting its alternative, does not validate the retained
target. In particular, allocation-to-heat electricity, sludge moisture, residue
composition and equipment normalization still need inventory evidence. Agreement
between two related mapping efforts is not independent scientific corroboration.

## Earlier expanded review (version 2)

The expanded review selects **2,350** branch targets: the original 35 corrections
plus **2,315 additional changes**. Of these, 1,763 replace BrightPath mappings
with confidence below 0.5, including 1,014 below 0.35. All 31,488 existing source
identities remain covered; no source or deletion rule is added or removed.

| Improvement | Entries |
|---|---:|
| Energy carrier/fuel | 1,454 |
| Energy technology | 88 |
| Functional/product-family match | 188 |
| Waste material or supplied-product/disposal distinction | 313 |
| Waste treatment route | 14 |
| Generic chemical-family proxy | 177 |
| Nutrient mass basis | 28 |
| Product identity | 51 |
| Initial inspected corrections | 35 |
| Additional product proxy | 2 |

The butane co-product of `2-butanol production by hydration of butene` now uses
`Chemicals organic, at plant` instead of `Butane-1,4-diol, at plant`, in RER and
RoW; the market-for-butane identity changes likewise. This is explicitly a coarse
proxy (confidence capped at 0.4), not a chemically equivalent dataset. More
specific catalog candidates remain a possible subsequent review, outside this
comparison of the two existing mappings.

## Selection and limitations

Every pair is compared using its exact source name, reference product, location,
and unit. Scripts apply explicit family checks, informed by inspection of grouped
source/target disagreements and rejected counterexamples. This is not individual
expert validation of thousands of inventories or LCIA scores.

- Energy comparisons preserve output carrier, fuel, and explicit technology.
  Generic grid supply can be replaced with source-fuel generation. Explicit
  voltage, onshore/offshore, reactor, PV, and hydro subtype conflicts are rejected.
- Waste comparisons prioritize the reference-product material and treatment
  role, not misleading waste-origin words. Ash/residue composition, unresolved
  allocation-to-heat electricity, and sludge moisture differences remain flagged.
- Generic organic/inorganic chemicals may replace low-confidence wrong-compound
  matches. Classification follows the co-product, not the activity name. Specific
  related chemical matches, nutrient bases, and fabricated products are protected.
- Functional checks recognize inspected alternatives such as soybean versus
  soybean oil, computer use versus electrolyzer operation, and road versus rail
  freight. Equal-unit compatibility alone is insufficient evidence.
- Existing curated mappings and unresolved ties remain unchanged. Branch drops
  and unmapped entries are flagged, never silently converted to deleted exchanges.

Confidence values from the two inputs are not directly comparable. Accepted
entries retain branch confidence subject to evidence-specific caps (0.4 for
generic chemical proxies; up to 0.9 for the initial corrections). They are
uncalibrated compatibility indicators, not probabilities or equivalence claims.
57 selected targets are marked deprecated but exist in the exact packaged catalog;
2,109 are geographic proxies. These limitations are recorded per decision.

## Earlier audit and reproduction

The following `uvek_mapping_review*` artifacts remain the version-2 snapshot;
use the follow-up artifacts above for subsequent dispositions and current totals.

- `docs/uvek_mapping_review.xlsx`: filterable Accepted, Retained, Rejected branch,
  and Needs review sheets, sorted by original BrightPath confidence. The Summary
  also counts identical targets and source identities absent from the branch.
- `docs/uvek_mapping_review.csv`: all accepted changes with both targets and reasons.
- `docs/uvek_mapping_review_summary.json`: exhaustive outcome counts.
- `docs/uvek_mapping_review_statistics.json`: selection and confidence statistics.
- `brightpath/data/export/uvek_reviewed_overrides.json`: reproducible decisions,
  previous targets, evidence, limitations, revision and input hashes.

With Python 3.12 and the project's dependencies plus openpyxl:

```
python scripts/review_uvek_mapping.py WORKBOOK BASELINE_RESOURCE OUTPUT_DIRECTORY --revision ecf7a035492d9e2042d7180091b3299aae16bf6e
```

Use the original resource from BrightPath `5e2a6d1` as `BASELINE_RESOURCE`, not the
already merged file. The script also writes a complete CSV including identical
and out-of-scope entries to the output directory. Matching rules are in
`scripts/uvek_pairwise.py`; the initial 35 decisions remain reproducible.

The normal resource generator reapplies the packaged overrides and rejects
stale target conflicts. Run `python scripts/generate_migration_manifest.py` after
regenerating resources. A previous full rebuild produced unrelated differences
in unselected heuristic targets, so byte-for-byte regeneration of the entire
baseline is not assumed. Unselected targets are preserved by this merge.

The existing runtime resource shares exact identities across versions and system
models. Selection is limited to identities in the branch's 3.10 cut-off workbook;
an identical four-field key inherits that choice wherever the existing resource
uses it. This does not establish equivalence across allocation models or versions.
No runtime route, biosphere resource, or exchange amount calculation is changed.

### Reproduce the follow-up

First reproduce the version-2 review above, and apply its 2,350 overrides to a
copy of the original resource with `apply_reviewed_overrides` from
`scripts/generate_uvek_migration_resources.py`. Save that result as
`BEFORE_FOLLOWUP.json`; do not use today's already-merged resource. Use the full
CSV from the version-2 output directory, not its accepted-only documentation CSV.
Extract `mapping_decisions.json`, `manual_overrides.json`,
`reviewed_from_unmapped_3.10.json` and `reconciled_curated_3.10.csv` from the same
CLIC branch revision into `BRANCH_CURATION_DIRECTORY`.

```
python scripts/review_uvek_followup.py BEFORE_FOLLOWUP.json VERSION2_OUTPUT/uvek_reviewed_overrides.json VERSION2_OUTPUT/uvek_mapping_review.csv BRANCH_CURATION_DIRECTORY FOLLOWUP_OUTPUT
```

The script does not mutate inputs or the runtime resource. It produces cumulative
overrides plus audit reports. Install the generated overrides, apply them with the
same generator helper, and regenerate the integrity manifest. The normal resource
generator always reapplies all 2,514 packaged decisions. The scoped follow-up
rejects stale comparisons, previously reviewed choices, missing catalog targets,
unsupported units and unexplained bundled quantities before emitting an overlay.
