# UVEK 2026 audit baseline review for alpha 14

This review updates the expected regression baseline after classifying every
changed case. It changes no migration code, policy, threshold, source catalogue,
physical quantity, or native-reference mapping.

## Complete installed-wheel audit

The audited runtime is the installed `1.0.0a13` candidate from BrightPath commit
`121313142625298ba596bb30bd54ba78ca34d902`. Alpha 14 changes release-version
metadata and the reviewed baseline/documentation; the package version is not a
baseline field. The audit used two workers, a private Brightway directory, no
network access, and no user inventory or database.

All **90 routes and 892,550 synthetic catalogue probes** completed. The totals
remain 443,401 supported, 449,099 blocked, and 50 omitted with explicit warnings.
Every route's total, status counts, error counts, warning codes, loss codes,
source scope, and strict/warning policy are unchanged. There are no audit errors.
Blocked and omitted probes are not successful conversions.

The prior successful alpha-12 artifact, from release run `40950219186`, exactly
matches the previous baseline. A streaming comparison consumed both complete
reports, verified their hashes and all route/probe counts, and found **223 changed
records**. Source exchange metadata is unchanged. The 67 unchanged route hashes
were retained; only 23 case hashes and the migration-resource manifest pin change.

## Review of all changed records

- **48 technosphere records:** three previously reviewed commodity-target
  corrections across sixteen incoming ecoinvent profiles. Copper sheet rolling
  now targets copper rather than chromium steel; steel sheet rolling targets
  steel rather than copper; silica sand targets silica sand at DE rather than
  sheet rolling at RER. These are specific corrected supplier bindings, not
  equivalence claims between the old and new commodities.
- **173 supported biosphere records:** only selected catalogue name/UUID fields
  differ. Exact source/target UUID witnesses at each ecoinvent reverse-preference
  step justify the correction; quantity, distribution, unit, compartment, formula,
  production and order are unchanged. Native UVEK UUIDs are not asserted equal to
  ecoinvent UUIDs across the separate reviewed family bridge.
- **Two still-blocked records:** `Methylchloride` from ecoinvent 3.11 and 3.12
  remains unresolved in native UVEK 2026. Only the diagnostic identity changes
  from `Chloromethane` to `Methylchloride`; error codes, severity and refusal remain.

All old and new artifact hashes, or exact error records, were reconstructed.
Technosphere replays retained the original 128-case batch/helper context and
checked unchanged companions. Each old and new `product` alias must equal its own
`reference product`. Quantities, normal means/spreads, other uncertainty fields,
formulas, production and caller-owned inputs remain unchanged. No disaggregation
rule matched the changed technosphere probes; the biosphere replays invoked no
technosphere disaggregation. This does not claim that disaggregation is unused
elsewhere in the complete audit.

## Reproducing the gate

Install the reviewed wheel without a source-package override, then run outside
the checkout with the script and baseline paths explicitly supplied:

```bash
python /path/to/brightpath/scripts/audit_uvek_2026_routes.py \
  --workers 2 \
  --baseline /path/to/brightpath/docs/uvek-2026-audit-baseline.json \
  --output /private/audit/uvek-2026-audit.json
```

The full report exceeds one gigabyte. Stream it rather than loading old and new
reports together. The separately pinned, complete computed baseline can be
checked with the unchanged `compare_baseline` function against the final expected
baseline; this is not another scientific audit or a policy relaxation. Original
historical-baseline failures and the subsequent reviewed comparison are retained
separately.

This regression audit does not establish native Desktop execution, complete
SimaPro-stock membership, or LCIA equivalence. It does not authorize an unreviewed
flow substitution or make blocked routes usable by default.
