# Exact heat dependency profiles

`uvek_heat_profile_extension_review.json` records five independently inspected
ecoinvent 3.9 cut-off correspondences for the Premise UVEK 2026 heat work.
These are screening qualifications, not findings of LCIA equivalence.

| Dependency | Reviewed disposition |
| --- | --- |
| Vehicle-grade natural gas | Complete native Swiss service station per signed kg. Both source and native documentation identify 0.1–1 bar as the initial pressure. Do not add a pressure, density or heating-value conversion. Global source supply shares are replaced by the native Swiss boundary. |
| SCR | Complete native service per kg NOx retained. Source 85% and native approximately 80% removal do not justify rescaling the retained mass or altering caller emissions. |
| Liquid nitrogen | Complete native cryogenic liquid product per kg. Preserve allocation, supply and delivery differences; do not apply gaseous-volume or purity factors. |
| Chemical heat | Existing exact 3.9 helper, including original make-up water and both water releases. Its independently reviewed source inventory hash and actual water coefficients match. Do not borrow the older 3.8 profile or substitute physical kg-steam. |
| Liquid epoxy | Extend the existing native functional proxy to 3.9. Retain the unresolved native CAS/technology conflict and older eco-profile limitations. No concentration or chemical-identity correction is invented. |

The replay utility `scripts/review_uvek_heat_profiles.py` changes only the gas
rule and the epoxy profile list. SCR, nitrogen and chemical heat rules remain
unchanged; their exact-profile permission is recorded by the caller's reviewed
approval ledger. Appending epoxy's 3.9 entry changes its raw-rule fingerprint,
but its earlier 3.12 effective rule, factor, supplier and shared review remain
unchanged. All previous helper recipes and shared reviews are retained.

Signed quantities, supported uncertainty and caller formulas are preserved.
Only the exact inspected profile is newly qualified. Other releases, reverse
routes and conversions across system models cannot inherit the approval.
The original native biosphere exclusions and unquantified score differences
remain limitations. This batch does not enable the public heat update.

The current observed heat cohort still requires a compressed-air decision:
source supply includes distribution infrastructure and losses, while the native
candidate covers generation. The exact volume basis must be checked rather
than inferred from matching pressure labels.
