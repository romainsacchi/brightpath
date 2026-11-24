# Migration audit baseline review after alpha 15

## UVEK 2025 export and biosphere audit

The approved alpha 14 report came from the installed wheel at `7a300db`.
Its baseline matches the pre-update baseline exactly. The alpha 15 report
came from the installed wheel at `d4f3d1b`. The reports are available in the
[alpha 14 release run](https://github.com/romainsacchi/brightpath/actions/runs/41357297059)
and [alpha 15 CI run](https://github.com/romainsacchi/brightpath/actions/runs/45428864321).

The complete alpha 15 audit has 52,920 synthetic probes across 18 routes.
All nine biosphere routes and the collision records match alpha 14 exactly.
Of the 52,074 existing cases, 52,065 match exactly, including output hashes
and structured refusals. The nine changed cases all concern the same Swiss
biowaste collection input. The reviewed rule now records an explicit omission
because the former replacement represents waste treatment and the source
represents material collection. The loss report retains the original exchange
and identifies the associated collection and uptake contributions. See
[the biowaste review](workflows/uvek-export.rst).

The 94 added UVEK supplier identities produce 846 new probes. All 94 source
identities match existing independent review records. For 90 direct rules,
the anchor target and conversion factor match the review. The four remaining
rules refer to generated recipes with reviewed input records.

| Reviewed additions | Suppliers |
| --- | ---: |
| Industrial gas and its components | 42 |
| Ash treatment | 23 |
| Sewage gas and its components | 9 |
| Physical cut-off feed coproducts | 8 |
| Residual landfill components | 7 |
| Electric rail freight, light commercial freight, packed wool, natural gas flaring and ground slag | 5 |

Among the new cases, 839 validate against their exact target catalogs. Seven
refuse the same Swiss sugar-refinery pulp input in older ecoinvent cut-off
profiles from 3.6 through 3.10.1. The independently reviewed wet-kg proxy
uses the 3.12 Swiss `sugar beet processing` coproduct as its anchor. The route
resolves for 3.11 and 3.12. The seven earlier profiles lack the selected
identity and retain their target-coverage and unresolved-link errors.

The policy snapshot adds an empty exact-rule approval list. Existing policy
actions and complete target coverage are unchanged. The migration manifest,
reference catalog manifest, and schema pins now reflect the reviewed compiled
resources shipped with alpha 15. The audit reports 51,371 supported cases,
1,515 refusals, 34 documented omissions, and zero audit errors.

### Report checksums

- Alpha 14 report SHA-256
  `c1f566077465b5b5ac6af25e1b3d4099329783488de41e4d390bf052dfa0ec98`.
- Alpha 15 report SHA-256
  `c30cd00476d3c7d21f8bc8a16bfff4be237d81e27f005fd417ebc90c5502b501`.

## Native UVEK 2026 audit

The alpha 14 native artifact matches the approved baseline across 90 routes
and 892,550 probes. Alpha 15 enforces compatible system models in both
technosphere directions. Fourteen previously executed consequential routes
accounted for 273,000 probes. These routes now appear as explicit planning
refusals. The catalog addition for ecoinvent 3.10.1 consequential brings the
number of directed system-model refusals to 16, including both directions
for each packaged consequential profile. The executable native audit covers
76 routes and 621,242 probes. The additional 1,692 probes are the 94 reviewed
outgoing suppliers across nine target releases and two policies.

### Historical recipe binding regression

The alpha 15 report exposed 19 false stale-review refusals. Materializing a
helper from a reviewed inventory reused across releases includes the caller
and inventory model profiles in its identity. Replaying its biosphere review
now recalculates that same identity and verifies both exact profiles against
the packaged declarations. The rule still binds the complete recipe,
dependencies and original biosphere profile. The regression test checks the
intact binding and edits to either profile.

All 19 formerly stale probes were replayed after the fix. The ecoinvent 3.12
cut-off RER white alkyd paint market now validates. The other 18 probes
reach the native target catalog checks and report exact biosphere links
that remain unresolved.

The full rerun changes exactly 22 case records relative to the alpha 15
wheel. Replay of the affected 128-probe batch reproduces the old and new
hashes for the three supported companion probes. Their own datasets match
exactly. The four generated paint helpers now appear in the successful
batch and contribute to its hashes.

### Native biosphere decisions

All 18 outgoing biosphere routes match alpha 14, including their case
hashes. The incoming routes gain 136 supported probes under strict policy
and 676 under the warning policy. These outcomes use the individually
reviewed native air and water correspondences and the exact canonical
ecoinvent bridge. The strict routes continue refusing 392 water probes
whose reviewed targets lose information. Their diagnostics now identify
the water correspondence and its loss explicitly. Both directions retain
full target coverage and the original rollback checks.

### Native technosphere decisions

Both directions between UVEK releases match alpha 14 case for case. In the
outgoing ecoinvent routes, the additional cohort and biowaste omission have
the same sources and boundaries as the historical audit. The strict policy
also records the reviewed biowaste omission on the direct 3.12 route.

For incoming ecoinvent cut-off suppliers, the 198,298 strict probes still
refuse the heuristic route. Under the warning policy, 7,123 previously
supported probes refuse because their exact source profile lacks a verified
reference-flow sign. A further 1,081 refuse because generated helpers need
native biosphere links absent from the exact catalog. The sign dispositions
and missing target identities were checked against the packaged rules and
catalogs. All 2,143 newly omitted incoming probes match explicit
exact-profile omission approvals.

The rebuilt wheel reports 298,195 supported probes, 320,844 refusals, 2,203
documented omissions, and zero audit errors. The installed-wheel historical
audit reproduces every reviewed case with the signature fix. The native
baseline matches the complete rebuilt-wheel report.

### Native report checksums

- Alpha 14 report SHA-256
  `ca4545e2377ef786ba7c8740535ca681c3649be07c7a9d04de2e42d85ba20942`.
- Original alpha 15 report SHA-256
  `1b5f49b89079dabbf66b2849e33776f8556bb8904760e5bc08de64679f34e1c3`.
- Rebuilt wheel report SHA-256
  `88c975dc1520ce66ea61f43de20aaf63d45e43f22e6b871bc0ed1b829434c3a6`.
