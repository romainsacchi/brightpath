# Alpha14 migration-audit baseline review

## Scope and provenance

The published GitHub alpha13 candidate at commit
`121313142625298ba596bb30bd54ba78ca34d902` failed the installed-wheel
migration-coverage baseline in release run
[41350212796](https://github.com/romainsacchi/brightpath/actions/runs/41350212796).
Quality checks and all three Python 3.12 test jobs passed: each reported
4,272 passed and one skipped. UVEK 2026 auditing and both publishers were
skipped. This review does not claim that alpha13 reached PyPI or Anaconda,
or that either alpha14 audit gate has already run.

The complete migration report has 52,074 synthetic identity probes across
18 routes. The comparison report is the successful alpha12 release run
[40950219186](https://github.com/romainsacchi/brightpath/actions/runs/40950219186).
Its baseline exactly matches the previous checked-in baseline. No inventory
coefficients or native LCI/LCIA vectors were used in this review.

The updated `migration-audit-baseline.json` is exactly the alpha13 full
report's `baseline` object, after the following exhaustive delta review.
Runtime code, policies, migration rules and reference catalogs were not
changed to obtain this result. Alpha14 must still pass both installed-wheel
release gates before publication.

## Resource and route differences

Only the migration-resource manifest hash changed among the three baseline
resource pins. Its reviewed data delta consists of three earlier approved
UVEK commodity-target corrections. Reference-catalog and migration-schema
manifest hashes are unchanged. Resource-collision findings are unchanged.

Thirteen route summaries are unchanged. Five biosphere routes differ:
ecoinvent targets 3.6, 3.7, 3.8, 3.9 and 3.9.1. Every supplier-route summary
is unchanged. There are no audit exceptions. Route warning-code and loss-code
sets are unchanged.

Across those five routes, 237 case records changed, representing 53 unique
source identities. A bounded replay reconstructed all 237 legacy and current
outcomes. Every reconstructed legacy data hash matched the alpha12 artifact;
every current hash or refusal matched the alpha13 artifact. Legacy reconstruction
suppressed only the new same-UUID preference inside a disposable diagnostic
process; no legacy outcome was accepted without the independent artifact hash.

- **226 supported to supported:** full dataset comparisons differ only in the
  biosphere exchange's `name` and `uuid`. Amounts, uncertainty, formulas, units,
  categories, production, dataset fields and exchange ordering are unchanged.
  All 226 current results resolve to exact target-catalog identities.
- **11 supported to blocked:** four source flow identities are unavailable under
  their preserved UUIDs in the final older target catalogs. They remain explicit
  refusals, not successful exports.

The blocked-count increases are four each for targets 3.6 and 3.7, and three
for 3.8. Counts for 3.9 and 3.9.1 are unchanged; their output identities changed.

## Exact UUID preservation and its qualification

For 223 successful cases, the final UUID equals the original source-catalog
UUID; the alpha12 output used another UUID. Every intervening preservation
decision is witnessed by the same UUID in both exact step catalogs, with
unchanged compartment and unit.

The remaining three successes are Naphthalene to targets 3.6, 3.7 and 3.8.
The source subcompartment UUID is `4239278c-5559-4b0d-80fc-702f6d8de310`.
Both old and new routes already use the reported parent-compartment fallback
from `air/non-urban air or from high stacks` to `air`. Alpha12 resolved
`Naphtalene`, UUID `7346ba9f-476c-4343-9f37-d1bb174eed6d`; the current route
resolves target-catalog `Naphthalene`, UUID
`3df4c2f4-c854-4493-b514-f2f3accdc307`. Neither final UUID equals the source
subcompartment UUID. This existing fallback is qualified by the audit's
information-loss policy; no new compartment change or warning/loss code was
introduced. These three cases must not be described as exact final source-UUID
preservation.

## Eleven retained refusals

All rows use kilogram. Exact source UUIDs exist in the intermediate catalogs
where preservation occurs, but are absent from the listed final targets.
Alpha12's alternative UUIDs are present in those targets and have different
flow identities. This review does not infer chemical interchangeability from
names or from a legacy replacement rule.

| Source identity and compartment | Preserved source UUID | Alpha12 alternative UUID | Final targets refusing source UUID |
| --- | --- | --- | --- |
| 2,4-D dimethylamine salt; soil/agricultural | `41a4e1fe-34ee-54d9-b049-3a60ada1ae3e` | `0eba8639-12c1-40ce-b1b6-e3394ec94dc0` | 3.6, 3.7, 3.8 |
| 2,4-D dimethylamine salt; water/surface water | `b8db7f8d-c4a3-5aea-b95f-f5729d5656fd` | `c7dcf08c-f297-495b-b7db-f4f54799814b` | 3.6, 3.7, 3.8 |
| 2,4-D dimethylamine salt; air/non-urban air or from high stacks | `b6b4201e-0561-5992-912f-e729fbf04e41` | `4f777e05-70f9-4a18-a406-d8232325073f` | 3.6, 3.7, 3.8 |
| Quizalofop-ethyl; soil/agricultural | `9ae11925-3df9-5fde-b7af-1627c0818347` | `f9c73aca-3d5c-4072-81dd-b8e0643530a6` | 3.6, 3.7 |

The three legacy 2,4-D alternatives are named `2,4-D amines`; the Quizalofop
alternative is named `Quizalofop ethyl ester`. Preserving the original native
identity and refusing an unavailable older target is the reviewed boundary.
No quantities are dropped, no replacement identity is fabricated, and no
scientific equivalence for different UUIDs is asserted. The existing structured
coverage/unresolved-link errors and transactional rollback remain in force.

## Reproducibility pins

- Alpha12 full migration report SHA256:
  `41d4a2f8ccf4ed993da912813038a1963e2b65702219e287c6cbf34574772836`.
- Alpha13 full migration report SHA256:
  `997c2c9822454f4d6a2446da68f13b6d111eb36739160bcdb5bfb7451ee5fc36`.
- Exhaustive changed-case scientific ledger SHA256:
  `43f7d656be6639cfb9ccd076d65332730cc628d00357cacaf7383ad2413e1dbc`.

The UVEK 2026 baseline is reviewed separately. A baseline comparison against this
complete saved report is not a claim of a fresh alpha14 migration run, and does
not replace the pending release workflow gates.
